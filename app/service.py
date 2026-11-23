"""Business service: reporting, bulk staging/publishing, refinement and P95.

Concurrency model
-----------------

A bulk batch is handled in two phases:

1. **staging** – each coil's buckets are validated and written into the
   ``buckets`` table under a private ``batch_id``, in write transactions of at
   most :data:`app.db.MAX_BUCKETS_PER_TRANSACTION` bucket rows.  Staged data is
   invisible: every reader goes through ``effective_buckets`` which only joins
   published batches.  Single-coil submissions and refinements therefore keep
   committing while staging runs, including for coils of this very batch.
2. **publishing** – one short write transaction bumps each coil's version off
   its *current* published version and swaps the coil's ``batch_id`` pointer,
   then marks the batch published.  That transaction never reads or writes a
   bucket table, so the whole batch becomes visible atomically regardless of
   its size.  If publishing (or any staging step) fails, the batch is marked
   ``failed`` and its rows never become visible; single-coil work committed in
   the meantime stays in place.

A coil's version therefore always means "version of the currently effective
report": every successful single-coil submission, refinement *or* bulk publish
adds one to whatever is live at that moment, and a brand-new coil starts at 1.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date

from . import db
from .domain import (
    Bucket,
    TargetSpec,
    ValidationError,
    p95_rank,
    validate_disjoint,
    validate_subdivision,
)

Key = tuple[str, str, str]  # (line_id, production_day ISO, coil_id)

LIVE_BATCH = 0


@dataclass(frozen=True)
class CoilState:
    line_id: str
    coil_id: str
    production_day: date
    version: int
    buckets: tuple[Bucket, ...]


def _key(line_id: str, production_day: date, coil_id: str) -> Key:
    return (line_id, production_day.isoformat(), coil_id)


def _load_buckets(conn: sqlite3.Connection, key: Key) -> tuple[Bucket, ...]:
    """Buckets of the coil's currently effective report (one snapshot)."""
    rows = conn.execute(
        "SELECT lo, hi, count FROM effective_buckets "
        "WHERE line_id = ? AND production_day = ? AND coil_id = ? ORDER BY position",
        key,
    ).fetchall()
    return tuple(Bucket(lo=r["lo"], hi=r["hi"], count=r["count"]) for r in rows)


def _insert_live_buckets(
    conn: sqlite3.Connection, key: Key, buckets: list[Bucket]
) -> None:
    line_id, day, coil_id = key
    conn.executemany(
        "INSERT INTO buckets (line_id, production_day, coil_id, position, "
        "lo, hi, count, batch_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (line_id, day, coil_id, pos, b.lo, b.hi, b.count, LIVE_BATCH)
            for pos, b in enumerate(buckets)
        ],
    )


def _write_live_summary(
    conn: sqlite3.Connection,
    key: Key,
    buckets: list[Bucket],
    version: int,
) -> None:
    """Upsert a coil with fresh live (batch 0) buckets.

    Any older live bucket rows of this coil are removed first; bucket rows of
    staged (unpublished) batches for the same coil are separate rows and are
    never touched here.
    """
    line_id, day, coil_id = key
    conn.execute(
        "DELETE FROM buckets "
        "WHERE line_id = ? AND production_day = ? AND coil_id = ? AND batch_id = ?",
        (*key, LIVE_BATCH),
    )
    _insert_live_buckets(conn, key, buckets)
    conn.execute(
        "INSERT INTO coils (line_id, production_day, coil_id, version, batch_id) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT (line_id, production_day, coil_id) DO UPDATE SET "
        "version = excluded.version, "
        "batch_id = excluded.batch_id, "
        "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')",
        (line_id, day, coil_id, version, LIVE_BATCH),
    )


def _current_version(conn: sqlite3.Connection, key: Key) -> int:
    row = conn.execute(
        "SELECT version FROM coils "
        "WHERE line_id = ? AND production_day = ? AND coil_id = ?",
        key,
    ).fetchone()
    if row is None:
        raise ValidationError("unknown coil report")
    return int(row["version"])


def submit_reports(
    conn: sqlite3.Connection,
    reports: list[tuple[str, date, str, list[Bucket]]],
) -> list[CoilState]:
    """Create or replace live day summaries for several coils in one transaction.

    Each element is ``(line_id, production_day, coil_id, buckets)``.  Reporting
    the same key again replaces it; the whole call is all-or-nothing.  These
    are always live (batch 0) writes, so they commit even while a bulk batch
    involving the same coils is still staging.
    """
    if not reports:
        raise ValidationError("at least one coil report is required")
    prepared: list[tuple[Key, date, list[Bucket]]] = []
    seen: set[Key] = set()
    for line_id, production_day, coil_id, buckets in reports:
        if not line_id or not line_id.strip():
            raise ValidationError("line_id must not be empty")
        if not coil_id or not coil_id.strip():
            raise ValidationError("coil_id must not be empty")
        if not buckets:
            raise ValidationError(f"coil {coil_id} needs at least one bucket")
        key = _key(line_id, production_day, coil_id)
        if key in seen:
            raise ValidationError(
                f"coil {coil_id} reported twice for {key[1]} on line {line_id}"
            )
        seen.add(key)
        validate_disjoint(buckets)
        prepared.append((key, production_day, list(buckets)))

    states: list[CoilState] = []
    with db.transaction(conn):
        for key, production_day, buckets in prepared:
            row = conn.execute(
                "SELECT version FROM coils "
                "WHERE line_id = ? AND production_day = ? AND coil_id = ?",
                key,
            ).fetchone()
            version = 1 if row is None else int(row["version"]) + 1
            _write_live_summary(conn, key, buckets, version)
            states.append(
                CoilState(key[0], key[2], production_day, version, tuple(buckets))
            )
    return states


def _validate_batch(
    reports: list[tuple[str, date, str, list[Bucket]]],
) -> list[tuple[Key, date, list[Bucket]]]:
    if not reports:
        raise ValidationError("at least one coil report is required")
    prepared: list[tuple[Key, date, list[Bucket]]] = []
    seen: set[Key] = set()
    for line_id, production_day, coil_id, buckets in reports:
        if not line_id or not line_id.strip():
            raise ValidationError("line_id must not be empty")
        if not coil_id or not coil_id.strip():
            raise ValidationError("coil_id must not be empty")
        if not buckets:
            raise ValidationError(f"coil {coil_id} needs at least one bucket")
        key = _key(line_id, production_day, coil_id)
        if key in seen:
            raise ValidationError(
                f"coil {coil_id} reported twice for {key[1]} on line {line_id}"
            )
        seen.add(key)
        validate_disjoint(buckets)
        prepared.append((key, production_day, list(buckets)))
    return prepared


def _discard_batch(conn: sqlite3.Connection, batch_id: int) -> None:
    """Best-effort removal of a batch that failed staging or publishing."""
    try:
        with db.transaction(conn):
            conn.execute("DELETE FROM buckets WHERE batch_id = ?", (batch_id,))
            conn.execute(
                "DELETE FROM bulk_batch_coils WHERE batch_id = ?", (batch_id,)
            )
            conn.execute(
                "UPDATE bulk_batches SET status = 'failed' WHERE id = ?",
                (batch_id,),
            )
    except BaseException:
        # The original failure must surface; the startup purge reclaims any
        # leftovers from a failed discard attempt.
        pass


def submit_bulk(
    conn: sqlite3.Connection,
    reports: list[tuple[str, date, str, list[Bucket]]],
) -> list[CoilState]:
    """Stage a bulk batch in short transactions and publish it atomically.

    Staging writes at most ``MAX_BUCKETS_PER_TRANSACTION`` bucket rows per
    write transaction, so concurrent single-coil submissions and refinements
    (also for coils of this batch) keep committing.  Publishing swaps coil
    pointers in one short transaction that never reads or writes bucket rows;
    versions are computed off the versions live just before the swap.

    On failure the batch's staged data never becomes visible and the function
    raises, while everything committed by other submissions in the meantime
    remains.  The response, when successful, preserves the original bulk
    contract: one :class:`CoilState` per input report, in input order.
    """
    prepared = _validate_batch(reports)

    with db.transaction(conn):
        cur = conn.execute(
            "INSERT INTO bulk_batches (status) VALUES ('staging')"
        )
        batch_id = int(cur.lastrowid)

    pending: list[tuple[Key, date, list[Bucket]]] = list(prepared)
    try:
        # Stage coils in bucket-budgeted write transactions of <= 1024 rows.
        # A coil's own buckets always land together in one transaction.
        index = 0
        while index < len(pending):
            coil_buckets = pending[index][2]
            if len(coil_buckets) > db.MAX_BUCKETS_PER_TRANSACTION:
                raise ValidationError(
                    "coil "
                    f"{pending[index][0][2]} has {len(coil_buckets)} buckets, "
                    f"more than the {db.MAX_BUCKETS_PER_TRANSACTION} bucket limit "
                    "of one staging write transaction"
                )
            chunk: list[tuple[Key, date, list[Bucket]]] = []
            budget = db.MAX_BUCKETS_PER_TRANSACTION
            while index < len(pending) and len(pending[index][2]) <= budget:
                chunk.append(pending[index])
                budget -= len(pending[index][2])
                index += 1
            with db.transaction(conn):
                for key, _production_day, buckets in chunk:
                    line_id, day, coil_id = key
                    conn.execute(
                        "INSERT INTO bulk_batch_coils "
                        "(batch_id, line_id, production_day, coil_id) "
                        "VALUES (?, ?, ?, ?)",
                        (batch_id, line_id, day, coil_id),
                    )
                    conn.executemany(
                        "INSERT INTO buckets (line_id, production_day, coil_id, "
                        "position, lo, hi, count, batch_id) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [
                            (
                                line_id,
                                day,
                                coil_id,
                                pos,
                                b.lo,
                                b.hi,
                                b.count,
                                batch_id,
                            )
                            for pos, b in enumerate(buckets)
                        ],
                    )

        # Publish: pointer swap only, no bucket reads or writes.  Versions
        # come from whatever report is effective right before the swap.
        states: list[CoilState] = []
        with db.transaction(conn):
            row = conn.execute(
                "SELECT status FROM bulk_batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if row is None or row["status"] != "staging":
                raise ValidationError("bulk batch is no longer publishable")
            for key, production_day, buckets in prepared:
                line_id, day, coil_id = key
                current = conn.execute(
                    "SELECT version FROM coils "
                    "WHERE line_id = ? AND production_day = ? AND coil_id = ?",
                    key,
                ).fetchone()
                version = 1 if current is None else int(current["version"]) + 1
                conn.execute(
                    "INSERT INTO coils (line_id, production_day, coil_id, "
                    "version, batch_id) VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT (line_id, production_day, coil_id) DO UPDATE SET "
                    "version = excluded.version, "
                    "batch_id = excluded.batch_id, "
                    "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')",
                    (line_id, day, coil_id, version, batch_id),
                )
                states.append(
                    CoilState(
                        line_id,
                        coil_id,
                        production_day,
                        version,
                        tuple(buckets),
                    )
                )
            conn.execute(
                "UPDATE bulk_batches SET status = 'published', "
                "published_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?",
                (batch_id,),
            )
    except BaseException:
        _discard_batch(conn, batch_id)
        raise
    return states


def refine_bucket(
    conn: sqlite3.Connection,
    *,
    line_id: str,
    production_day: date,
    coil_id: str,
    target: TargetSpec,
    children: list[Bucket],
) -> CoilState:
    """Replace one existing bucket by narrower sub-buckets, atomically.

    The target interval ``[lo, hi]`` must match a bucket of the coil's
    currently effective report; when the target also carries a count it must
    match too, so a stale refinement cannot silently rewrite the wrong data.
    The refined buckets are written as fresh live (batch 0) rows and take
    effect immediately, even while a bulk batch is being staged for this coil.
    On any rule violation the transaction rolls back and the effective report
    remains unchanged.
    """
    key = _key(line_id, production_day, coil_id)
    with db.transaction(conn):
        version = _current_version(conn, key) + 1
        current = _load_buckets(conn, key)
        matches = [b for b in current if b.lo == target.lo and b.hi == target.hi]
        if not matches:
            raise ValidationError(
                f"bucket [{target.lo},{target.hi}] is not part of the current report"
            )
        original = matches[0]
        if not target.matches(original):
            raise ValidationError(
                f"bucket count is {original.count}, not the reported {target.count}"
            )
        validate_subdivision(original, children)

        refined = [b for b in current if b != original] + list(children)
        validate_disjoint(refined)
        ordered = sorted(refined, key=lambda b: (b.lo, b.hi))
        _write_live_summary(conn, key, ordered, version)
    return CoilState(line_id, coil_id, production_day, version, tuple(ordered))


def get_coil(
    conn: sqlite3.Connection, *, line_id: str, production_day: date, coil_id: str
) -> CoilState:
    key = _key(line_id, production_day, coil_id)
    with db.snapshot(conn):
        version = _current_version(conn, key)
        buckets = _load_buckets(conn, key)
    return CoilState(line_id, coil_id, production_day, version, buckets)


@dataclass(frozen=True)
class Candidate:
    coil_id: str
    production_day: date
    bucket: Bucket


@dataclass(frozen=True)
class LineP95:
    line_id: str
    start: date
    end: date
    total: int | None
    rank: int | None
    minimum: int | None
    maximum: int | None
    # ``None`` when there is no data or when both bounds coincide; the spec
    # only asks for candidate buckets when the two ends differ.
    candidates: tuple[Candidate, ...] | None

    @property
    def has_data(self) -> bool:
        return self.total is not None


SCOPED = """
    SELECT b.coil_id, b.production_day, b.position, b.lo, b.hi, b.count
    FROM effective_buckets b
    JOIN coils c
      ON c.line_id = b.line_id
     AND c.production_day = b.production_day
     AND c.coil_id = b.coil_id
    WHERE c.line_id = ? AND c.production_day BETWEEN ? AND ?
"""


def query_p95(
    conn: sqlite3.Connection,
    *,
    line_id: str,
    start: date,
    end: date,
    min_intersection: int = 2,
) -> LineP95:
    """P95 range and candidate buckets for a line over an inclusive day range.

    Only day summaries (bucket counts) of effective reports are read; staged
    bulk batches are invisible and no measurement is expanded.  The
    aggregation happens in SQL so a long range never materialises one Python
    object per bucket.  The lower bound, upper bound, rank and bucket list all
    come from a single snapshot transaction, hence from one committed
    submission and only from effective coil reports.

    With ``k = ceil(0.95 * n)``:

    * smallest P95 = the ``k``-th smallest bucket lower bound, weights = counts;
    * largest P95  = the ``k``-th smallest bucket upper bound, weights = counts.

    A bucket is a candidate when its overlap with ``[minimum, maximum]`` spans
    at least ``min_intersection`` integer thicknesses.
    """
    if start > end:
        raise ValidationError("start date must not be after end date")
    if min_intersection < 1:
        raise ValidationError("min_intersection must be at least 1")

    params = (line_id, start.isoformat(), end.isoformat())
    with db.snapshot(conn):
        totals = conn.execute(
            f"WITH scoped AS ({SCOPED}) "
            "SELECT COALESCE(SUM(count), 0) AS n FROM scoped",
            params,
        ).fetchone()
        total = int(totals["n"])
        if total == 0:
            return LineP95(line_id, start, end, None, None, None, None, None)
        rank = p95_rank(total)

        minimum = conn.execute(
            f"WITH scoped AS ({SCOPED}), "
            "running AS (SELECT lo, SUM(count) OVER (ORDER BY lo) AS cum FROM scoped) "
            "SELECT lo FROM running WHERE cum >= ? ORDER BY lo LIMIT 1",
            (*params, rank),
        ).fetchone()["lo"]
        maximum = conn.execute(
            f"WITH scoped AS ({SCOPED}), "
            "running AS (SELECT hi, SUM(count) OVER (ORDER BY hi) AS cum FROM scoped) "
            "SELECT hi FROM running WHERE cum >= ? ORDER BY hi LIMIT 1",
            (*params, rank),
        ).fetchone()["hi"]

        candidates: tuple[Candidate, ...] | None = None
        if minimum != maximum:
            rows = conn.execute(
                f"WITH scoped AS ({SCOPED}) "
                "SELECT coil_id, production_day, lo, hi, count FROM scoped "
                "WHERE MIN(hi, ?) - MAX(lo, ?) + 1 >= ? "
                "ORDER BY production_day, coil_id, position",
                (*params, maximum, minimum, min_intersection),
            ).fetchall()
            candidates = tuple(
                Candidate(
                    row["coil_id"],
                    date.fromisoformat(row["production_day"]),
                    Bucket(lo=row["lo"], hi=row["hi"], count=row["count"]),
                )
                for row in rows
            )

    return LineP95(
        line_id,
        start,
        end,
        total,
        rank,
        minimum,
        maximum,
        candidates,
    )
