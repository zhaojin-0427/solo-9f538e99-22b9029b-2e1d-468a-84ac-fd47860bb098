"""FastAPI surface for coating-thickness reporting and P95 queries."""

from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from datetime import date
from typing import Annotated, Iterator

from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field, StrictInt, StrictStr

from . import __version__, db
from .domain import Bucket, TargetSpec, ValidationError
from .service import (
    CoilState,
    LineP95,
    get_coil,
    query_p95,
    refine_bucket,
    submit_bulk,
    submit_reports,
)

DOCS = "涂层厚度 P95 查询服务"


class BucketIn(BaseModel):
    lo: StrictInt = Field(description="闭区间下界，整数微米（含）")
    hi: StrictInt = Field(description="闭区间上界，整数微米（含）")
    count: StrictInt = Field(gt=0, description="落在该档内的测点数，正整数")

    def to_domain(self) -> Bucket:
        return Bucket(lo=self.lo, hi=self.hi, count=self.count)


class TargetIn(BaseModel):
    lo: StrictInt = Field(description="原档下界，整数微米（含）")
    hi: StrictInt = Field(description="原档上界，整数微米（含）")
    count: StrictInt | None = Field(
        default=None,
        gt=0,
        description="可选：原档计数。给出时必须与当前报送一致，用于防误改。",
    )

    def to_domain(self) -> TargetSpec:
        return TargetSpec(lo=self.lo, hi=self.hi, count=self.count)


class BucketOut(BaseModel):
    lo: int
    hi: int
    count: int


class ReportIn(BaseModel):
    line_id: StrictStr = Field(min_length=1, description="产线编号")
    production_day: date = Field(description="生产日，YYYY-MM-DD")
    coil_id: StrictStr = Field(min_length=1, description="卷号")
    buckets: list[BucketIn] = Field(
        min_length=1,
        description="日汇总分档计数。同一卷内的闭区间必须互不重叠，计数为正整数。",
    )


class BulkReportIn(BaseModel):
    reports: list[ReportIn] = Field(
        min_length=1, description="多卷日汇总；整批要么全部生效，要么全部不生效。"
    )


class RefineIn(BaseModel):
    production_day: date = Field(description="该卷的生产日")
    target: TargetIn = Field(description="要被替换的当前档，按闭区间定位")
    children: list[BucketIn] = Field(
        min_length=1,
        description=(
            "更窄的子档：须完全落在原档内、彼此互不重叠，"
            "计数之和必须等于原档计数。"
        ),
    )


class CoilOut(BaseModel):
    line_id: str
    coil_id: str
    production_day: date
    version: int = Field(
        description="现行版本：单卷报送、细化或批量发布每成功一次加一；失败不变"
    )
    buckets: list[BucketOut]


class CandidateBucket(BaseModel):
    coil_id: str = Field(description="需要补细分数据的卷号")
    production_day: date
    lo: int
    hi: int
    count: int


class P95Out(BaseModel):
    line_id: str
    start: date
    end: date
    has_data: bool = Field(description="该产线在该日期范围内是否有任何测点")
    measurements: int | None = Field(
        default=None, description="测点总数（各档计数之和，不展开测点）"
    )
    rank: int | None = Field(
        default=None, description="P95 秩：ceil(0.95 × 测点数)，从 1 开始"
    )
    p95_min: int | None = Field(default=None, description="P95 的最小可能值")
    p95_max: int | None = Field(default=None, description="P95 的最大可能值")
    candidate_buckets: list[CandidateBucket] = Field(
        default_factory=list,
        description=(
            "两端不同时，与 [p95_min, p95_max] 交集至少包含两个整数微米值的分档；"
            "这些卷值得补细分数据。两端相同时为空。"
        ),
    )
    coil_ids: list[str] = Field(
        default_factory=list,
        description="candidate_buckets 中出现的卷号（去重、保持顺序）",
    )


def _coil_out(state: CoilState) -> CoilOut:
    return CoilOut(
        line_id=state.line_id,
        coil_id=state.coil_id,
        production_day=state.production_day,
        version=state.version,
        buckets=[BucketOut(lo=b.lo, hi=b.hi, count=b.count) for b in state.buckets],
    )


def get_conn(request: Request) -> Iterator[sqlite3.Connection]:
    conn = request.app.state.connect()
    try:
        yield conn
    finally:
        conn.close()


Conn = Annotated[sqlite3.Connection, Depends(get_conn)]


def create_app(database: str | None = None) -> FastAPI:
    database = database or db.database_path()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        db.init_db(database)
        yield

    app = FastAPI(
        title=DOCS,
        description=(
            "按卷报送分档计数，用更窄子档细化；按产线与日期范围查询 P95 的"
            "最小/最大可能值，并列出需要补细分数据的卷与档。"
            "P95 秩为第 ceil(0.95×测点数) 小的测值；只读日汇总（分档计数），"
            "不展开测点。\n\n"
            "批量报送 /reports/bulk 采用“分块准备 + 整批发布”：准备数据写入"
            "私有批次区（每个写事务最多 1024 条分档），期间单卷报送和细化"
            "（含本批卷）照常提交；发布只切换批次指针、不读写分档表，整批"
            "同时生效。所有查询只读取已生效的卷报，且单次查询的全部数据来自"
            "同一个已提交快照。"
        ),
        version=__version__,
        docs_url="/docs",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )
    app.state.connect = lambda: db.connect(database)

    @app.exception_handler(ValidationError)
    async def domain_error(_: Request, exc: ValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content={"detail": str(exc)},
        )

    @app.exception_handler(RequestValidationError)
    async def request_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content={"detail": exc.errors()},
        )

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    @app.get("/health", summary="健康检查")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/reports",
        response_model=CoilOut,
        status_code=status.HTTP_201_CREATED,
        summary="按卷号报送产线、生产日与分档计数",
        description=(
            "卷由 (产线, 生产日, 卷号) 定位。重复报送同一键会整体替换该日汇总；"
            "分档与日汇总在同一事务中生效，任何校验失败都不会写入。"
        ),
    )
    def post_report(payload: ReportIn, conn: Conn) -> CoilOut:
        state = submit_reports(
            conn,
            [
                (
                    payload.line_id,
                    payload.production_day,
                    payload.coil_id,
                    [b.to_domain() for b in payload.buckets],
                )
            ],
        )[0]
        return _coil_out(state)

    @app.post(
        "/reports/bulk",
        response_model=list[CoilOut],
        status_code=status.HTTP_201_CREATED,
        summary="批量报送多卷日汇总（分块准备，整批同时发布生效）",
        description=(
            "适合班末几千卷的补报。请求与响应格式与单卷报送一致：入参为 reports "
            "列表，返回每卷生效后的分档与版本，顺序与入参相同。\n\n"
            "处理分两阶段：\n"
            "1. 准备（staging）：各卷分档写入私有批次区，每个 SQLite 写事务最多 "
            "1024 条分档，事务都很短；准备中的分档对外不可见。此期间单卷报送与"
            "细化（包括本批涉及的卷）仍可正常提交。\n"
            "2. 发布（publish）：一个不读写任何分档表的短事务只切换各卷的批次"
            "指针，整批同时生效。各卷版本取发布前现行版本加一，全新卷从 1 起。\n\n"
            "整批失败时本批分档始终不可见，准备期间已提交的单卷报送与细化照常"
            "保留。同一批内不得重复同一个 (产线, 生产日, 卷号)；任一卷分档校验"
            "失败则整批不发布。"
        ),
    )
    def post_bulk(
        payload: Annotated[BulkReportIn, Body()], conn: Conn
    ) -> list[CoilOut]:
        states = submit_bulk(
            conn,
            [
                (
                    report.line_id,
                    report.production_day,
                    report.coil_id,
                    [b.to_domain() for b in report.buckets],
                )
                for report in payload.reports
            ],
        )
        return [_coil_out(state) for state in states]

    @app.post(
        "/reports/{line_id}/{coil_id}/refinements",
        response_model=CoilOut,
        summary="用更窄子档替换当前某一档",
        description=(
            "子档须落在原档内、彼此互不重叠，计数之和等于原档计数。新档位与日汇总"
            "在同一事务提交；任一条不满足则整体回滚，沿用原数据。"
        ),
    )
    def post_refinement(
        line_id: str, coil_id: str, payload: RefineIn, conn: Conn
    ) -> CoilOut:
        state = refine_bucket(
            conn,
            line_id=line_id,
            production_day=payload.production_day,
            coil_id=coil_id,
            target=payload.target.to_domain(),
            children=[b.to_domain() for b in payload.children],
        )
        return _coil_out(state)

    @app.get(
        "/reports/{line_id}/{coil_id}",
        response_model=CoilOut,
        summary="查看某卷某生产日当前的分档与版本",
    )
    def read_coil(
        line_id: str,
        coil_id: str,
        conn: Conn,
        production_day: Annotated[date, Query(description="生产日，YYYY-MM-DD")],
    ) -> CoilOut:
        try:
            state = get_coil(
                conn,
                line_id=line_id,
                production_day=production_day,
                coil_id=coil_id,
            )
        except ValidationError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _coil_out(state)

    @app.get(
        "/p95",
        response_model=P95Out,
        summary="按产线与日期范围查询 P95 的最小/最大可能值",
        description=(
            "只读取已生效卷报的日汇总（分档计数），准备中的批量分档不可见；"
            "按秩 ceil(0.95×N) 计算；下界取各档下界的加权第 k 小，上界取各档"
            "上界的加权第 k 小。下界、上界、秩与候选分档在同一只读事务快照中"
            "取得，即来自同一次提交。无测点时 has_data=false 且各值为 null。"
        ),
    )
    def read_p95(
        conn: Conn,
        line_id: Annotated[str, Query(min_length=1, description="产线编号")],
        start: Annotated[date, Query(description="起始生产日（含），YYYY-MM-DD")],
        end: Annotated[date, Query(description="结束生产日（含），YYYY-MM-DD")],
    ) -> P95Out:
        result: LineP95 = query_p95(conn, line_id=line_id, start=start, end=end)
        if not result.has_data:
            return P95Out(
                line_id=result.line_id,
                start=result.start,
                end=result.end,
                has_data=False,
            )
        candidates = [
            CandidateBucket(
                coil_id=candidate.coil_id,
                production_day=candidate.production_day,
                lo=candidate.bucket.lo,
                hi=candidate.bucket.hi,
                count=candidate.bucket.count,
            )
            for candidate in (result.candidates or ())
        ]
        coil_ids: list[str] = []
        for candidate in candidates:
            if candidate.coil_id not in coil_ids:
                coil_ids.append(candidate.coil_id)
        return P95Out(
            line_id=result.line_id,
            start=result.start,
            end=result.end,
            has_data=True,
            measurements=result.total,
            rank=result.rank,
            p95_min=result.minimum,
            p95_max=result.maximum,
            candidate_buckets=candidates,
            coil_ids=coil_ids,
        )

    return app


app = create_app()
