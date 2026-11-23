"""Two-phase bulk reporting: chunked staging, pointer publish, restart cleanup."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import date

import pytest
from fastapi.testclient import TestClient

from app import db
from app.db import MAX_BUCKETS_PER_TRANSACTION, connect, init_db
from app.domain import Bucket, TargetSpec, ValidationError
from app.main import create_app
from app.service import (
    get_coil,
    query_p95,
    refine_bucket,
    submit_bulk,
    submit_reports,
)

DAY = date(2026, 11, 20)


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "test.db"
    init_db(str(path))
    return str(path)


@pytest.fixture()
def client(db_path):
    app = create_app(db_path)
    with TestClient(app) as test_client:
        yield test_client


def bulk_payload(n_coils, buckets_per_coil=1, start=0):
    return {
        "reports": [
            {
                "line_id": "L1",
                "production_day": "2026-11-20",
                "coil_id": f"C{start + i}",
                "buckets": [
                    {"lo": 10 * j, "hi": 10 * j + 4, "count": j + 1}
                    for j in range(buckets_per_coil)
                ],
            }
            for i in range(n_coils)
        ]
    }


def test_bulk_stages_in_transactions_of_at_most_1024_bucket_rows(
    client, monkeypatch
):
    # 2500 coils x 1 bucket -> staging inserts split into 1024/1024/452 rows.
    per_transaction: list[int] = []
    original_transaction = db.transaction

    @contextmanager
    def counting_transaction(conn):
        before = conn.execute(
            "SELECT COALESCE(MAX(rowid), 0) AS m FROM buckets"
        ).fetchone()["m"]
        with original_transaction(conn):
            yield
        after = conn.execute("SELECT COUNT(*) AS n FROM buckets").fetchone()["n"]
        inserted = conn.execute(
            "SELECT COUNT(*) AS n FROM buckets WHERE rowid > ?", (before,)
        ).fetchone()["n"]
        if inserted:
            per_transaction.append(inserted)
        assert after >= 0  # keep ``after`` used for clarity of the snapshot

    monkeypatch.setattr(db, "transaction", counting_transaction)

    response = client.post("/reports/bulk", json=bulk_payload(2500))
    assert response.status_code == 201, response.text
    assert per_transaction == [1024, 1024, 452]
    assert max(per_transaction) <= MAX_BUCKETS_PER_TRANSACTION == 1024
    assert sum(per_transaction) == 2500


def test_bulk_publish_uses_current_version_plus_one_and_new_coils_start_at_one(client):
    # C1 already live at version 2; C2 is brand new.
    for _ in range(2):
        r = client.post(
            "/reports",
            json={
                "line_id": "L1",
                "production_day": "2026-11-20",
                "coil_id": "C1",
                "buckets": [{"lo": 1, "hi": 2, "count": 3}],
            },
        )
        assert r.status_code == 201
    response = client.post(
        "/reports/bulk",
        json={
            "reports": [
                {
                    "line_id": "L1",
                    "production_day": "2026-11-20",
                    "coil_id": "C1",
                    "buckets": [{"lo": 5, "hi": 6, "count": 9}],
                },
                {
                    "line_id": "L1",
                    "production_day": "2026-11-20",
                    "coil_id": "C2",
                    "buckets": [{"lo": 7, "hi": 8, "count": 4}],
                },
            ]
        },
    )
    assert response.status_code == 201, response.text
    body = {item["coil_id"]: item for item in response.json()}
    assert body["C1"]["version"] == 3
    assert body["C1"]["buckets"] == [{"lo": 5, "hi": 6, "count": 9}]
    assert body["C2"]["version"] == 1

    read = client.get(
        "/reports/L1/C1", params={"production_day": "2026-11-20"}
    ).json()
    assert read["version"] == 3
    assert read["buckets"] == [{"lo": 5, "hi": 6, "count": 9}]


def test_single_coil_submission_commits_between_staging_transactions(
    client, db_path, monkeypatch
):
    original_transaction = db.transaction
    staging_tx = {"n": 0, "injected": False}

    @contextmanager
    def injecting_transaction(conn):
        staging_tx["n"] += 1
        with original_transaction(conn):
            yield
        # Transaction 1 creates the batch; transaction 2 is the first staging
        # chunk.  After it commits the write lock is released, so a single-coil
        # submission can commit from another connection while staging continues.
        if staging_tx["n"] == 2 and not staging_tx["injected"]:
            staging_tx["injected"] = True
            other = connect(db_path)
            try:
                states = submit_reports(
                    other,
                    [("L1", DAY, "SOLO", [Bucket(lo=1, hi=1, count=7)])],
                )
                assert states[0].version == 1
            finally:
                other.close()

    monkeypatch.setattr(db, "transaction", injecting_transaction)

    response = client.post("/reports/bulk", json=bulk_payload(1500))
    assert response.status_code == 201, response.text
    assert staging_tx["injected"] is True

    # The concurrently committed single-coil report survives the bulk publish.
    live = client.get("/reports/L1/SOLO", params={"production_day": "2026-11-20"})
    assert live.json()["version"] == 1
    assert live.json()["buckets"] == [{"lo": 1, "hi": 1, "count": 7}]
    # The bulk coils all publish at version 1.
    versions = {item["coil_id"]: item["version"] for item in response.json()}
    assert versions["C0"] == 1 and versions["C1499"] == 1


def test_refinement_of_a_batch_coil_commits_during_staging(client, db_path, monkeypatch):
    seeded = client.post(
        "/reports",
        json={
            "line_id": "L1",
            "production_day": "2026-11-20",
            "coil_id": "C0",
            "buckets": [{"lo": 1, "hi": 100, "count": 100}],
        },
    )
    assert seeded.status_code == 201

    original_transaction = db.transaction
    state = {"n": 0, "injected": False}

    @contextmanager
    def injecting_transaction(conn):
        state["n"] += 1
        with original_transaction(conn):
            yield
        if state["n"] == 2 and not state["injected"]:
            state["injected"] = True
            other = connect(db_path)
            try:
                refined = refine_bucket(
                    other,
                    line_id="L1",
                    production_day=DAY,
                    coil_id="C0",
                    target=TargetSpec(1, 100, 100),
                    children=[Bucket(1, 50, 50), Bucket(51, 100, 50)],
                )
                assert refined.version == 2
            finally:
                other.close()

    monkeypatch.setattr(db, "transaction", injecting_transaction)

    response = client.post("/reports/bulk", json=bulk_payload(1200))
    assert response.status_code == 201, response.text
    assert state["injected"] is True
    # The refinement committed at v2 and stays in history; the batch then
    # publishes C0 at v3 off the version live just before the swap.
    published = client.get(
        "/reports/L1/C0", params={"production_day": "2026-11-20"}
    ).json()
    assert published["version"] == 3
    assert published["buckets"] == [{"lo": 0, "hi": 4, "count": 1}]


def test_staged_data_is_invisible_until_publish(client, db_path, monkeypatch):
    original_transaction = db.transaction
    state = {"n": 0, "checked": False}

    @contextmanager
    def checking_transaction(conn):
        state["n"] += 1
        with original_transaction(conn):
            yield
        # Transaction 1 creates the batch; transaction 2 commits the first
        # staging chunk.  Check invisibility after that chunk, before publish.
        if state["n"] == 2 and not state["checked"]:
            state["checked"] = True
            other = connect(db_path)
            try:
                with pytest.raises(ValidationError):
                    get_coil(other, line_id="L1", production_day=DAY, coil_id="C0")
                p95 = query_p95(other, line_id="L1", start=DAY, end=DAY)
                assert p95.has_data is False
                row = other.execute(
                    "SELECT bb.status FROM buckets bk "
                    "JOIN bulk_batches bb ON bb.id = bk.batch_id LIMIT 1"
                ).fetchone()
                assert row["status"] == "staging"
            finally:
                other.close()

    monkeypatch.setattr(db, "transaction", checking_transaction)
    response = client.post("/reports/bulk", json=bulk_payload(1100))
    assert response.status_code == 201, response.text
    assert state["checked"] is True

    # After publish the same readers see exactly the batch's data.
    p95 = client.get(
        "/p95", params={"line_id": "L1", "start": "2026-11-20", "end": "2026-11-20"}
    ).json()
    assert p95["has_data"] is True
    assert p95["measurements"] == 1100  # one bucket of count 1 per coil


def test_failed_batch_never_becomes_visible_and_keeps_concurrent_work(
    db_path, monkeypatch
):
    # Concurrent work committed before the failure must remain untouched.
    seed_conn = connect(db_path)
    submit_reports(
        seed_conn, [("L1", DAY, "KEPT", [Bucket(lo=1, hi=2, count=5)])]
    )
    seed_conn.close()

    conn = connect(db_path)
    reports = [
        ("L1", DAY, f"C{i}", [Bucket(lo=0, hi=1, count=i + 1)])
        for i in range(2500)
    ]

    calls = {"n": 0}
    original_transaction = db.transaction

    @contextmanager
    def flaky_transaction(c):
        calls["n"] += 1
        # 1 = batch creation, 2/3/4 = staging chunks.  Fail before entering
        # the third staging transaction so no transaction is left open.
        if calls["n"] == 3:
            raise RuntimeError("simulated staging failure")
        with original_transaction(c) as cm:
            yield cm

    monkeypatch.setattr(db, "transaction", flaky_transaction)
    with pytest.raises(RuntimeError):
        submit_bulk(conn, reports)
    monkeypatch.undo()

    statuses = [
        r["status"]
        for r in conn.execute(
            "SELECT status FROM bulk_batches WHERE id != 0"
        ).fetchall()
    ]
    assert statuses == ["failed"]
    assert (
        conn.execute(
            "SELECT COUNT(*) AS n FROM buckets WHERE batch_id != 0"
        ).fetchone()["n"]
        == 0
    )

    kept = get_coil(conn, line_id="L1", production_day=DAY, coil_id="KEPT")
    assert kept.version == 1 and kept.buckets == (Bucket(1, 2, 5),)
    conn.close()


def test_restart_purges_unpublished_batches_but_keeps_published_reports(db_path):
    conn = connect(db_path)
    submit_bulk(conn, [("L1", DAY, "PUB", [Bucket(lo=1, hi=2, count=3)])])
    with db.transaction(conn):
        cur = conn.execute(
            "INSERT INTO bulk_batches (status) VALUES ('staging')"
        )
        stale_id = int(cur.lastrowid)
        conn.execute(
            "INSERT INTO buckets (line_id, production_day, coil_id, position, "
            "lo, hi, count, batch_id) VALUES "
            "('L1','2026-11-20','GHOST',0,1,9,99,?)",
            (stale_id,),
        )
    conn.close()

    init_db(db_path)  # simulated restart

    alive = connect(db_path)
    try:
        assert (
            alive.execute(
                "SELECT COUNT(*) AS n FROM bulk_batches "
                "WHERE id != 0 AND status != 'published'"
            ).fetchone()["n"]
            == 0
        )
        assert (
            alive.execute(
                "SELECT COUNT(*) AS n FROM buckets WHERE batch_id = ?",
                (stale_id,),
            ).fetchone()["n"]
            == 0
        )
        pub = get_coil(alive, line_id="L1", production_day=DAY, coil_id="PUB")
        assert pub.version == 1 and pub.buckets == (Bucket(1, 2, 3),)
    finally:
        alive.close()


def test_published_reports_remain_queryable_after_restart(client, db_path):
    client.post("/reports/bulk", json=bulk_payload(10))
    init_db(db_path)  # restart: nothing unpublished, published data survives

    with TestClient(create_app(db_path)) as restarted:
        body = restarted.get(
            "/p95",
            params={"line_id": "L1", "start": "2026-11-20", "end": "2026-11-20"},
        ).json()
        assert body["has_data"] is True
        assert body["measurements"] == 10
        read = restarted.get(
            "/reports/L1/C3", params={"production_day": "2026-11-20"}
        ).json()
        assert read["version"] == 1


def test_startup_garbage_collects_orphan_buckets(db_path):
    conn = connect(db_path)
    submit_bulk(conn, [("L1", DAY, "C1", [Bucket(1, 2, 3)])])
    # Publishing again leaves the first batch's rows unreferenced by any coil.
    submit_bulk(conn, [("L1", DAY, "C1", [Bucket(4, 5, 6)])])
    assert conn.execute("SELECT COUNT(*) AS n FROM buckets").fetchone()["n"] == 2
    conn.close()

    init_db(db_path)
    alive = connect(db_path)
    try:
        assert alive.execute("SELECT COUNT(*) AS n FROM buckets").fetchone()["n"] == 1
        state = get_coil(alive, line_id="L1", production_day=DAY, coil_id="C1")
        assert state.buckets == (Bucket(4, 5, 6),)
    finally:
        alive.close()


def test_original_schema_data_volume_migrates_and_stays_queryable(tmp_path):
    path = str(tmp_path / "legacy.db")
    legacy = sqlite3.connect(path)
    legacy.executescript(
        """
        CREATE TABLE coils (
            line_id TEXT NOT NULL, production_day TEXT NOT NULL,
            coil_id TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            PRIMARY KEY (line_id, production_day, coil_id)
        );
        CREATE TABLE buckets (
            line_id TEXT NOT NULL, production_day TEXT NOT NULL,
            coil_id TEXT NOT NULL, position INTEGER NOT NULL,
            lo INTEGER NOT NULL, hi INTEGER NOT NULL,
            count INTEGER NOT NULL CHECK (count > 0),
            PRIMARY KEY (line_id, production_day, coil_id, position),
            CHECK (lo <= hi),
            FOREIGN KEY (line_id, production_day, coil_id)
                REFERENCES coils (line_id, production_day, coil_id) ON DELETE CASCADE
        );
        CREATE INDEX idx_coils_day ON coils (line_id, production_day);
        INSERT INTO coils (line_id, production_day, coil_id, version)
            VALUES ('L1','2026-11-20','OLD', 2);
        INSERT INTO buckets VALUES ('L1','2026-11-20','OLD',0,1,5,10);
        """
    )
    legacy.commit()
    legacy.close()

    init_db(path)
    conn = connect(path)
    try:
        state = get_coil(conn, line_id="L1", production_day=DAY, coil_id="OLD")
        assert state.version == 2 and state.buckets == (Bucket(1, 5, 10),)
        p95 = query_p95(conn, line_id="L1", start=DAY, end=DAY)
        assert p95.has_data and p95.total == 10

        # The two-phase bulk flow works on the migrated volume.
        submit_bulk(conn, [("L1", DAY, "NEW", [Bucket(7, 8, 4)])])
        new = get_coil(conn, line_id="L1", production_day=DAY, coil_id="NEW")
        assert new.version == 1
    finally:
        conn.close()


def test_bulk_rejects_duplicate_key_and_bad_buckets(client, db_path):
    item = {
        "line_id": "L1",
        "production_day": "2026-11-20",
        "coil_id": "A",
        "buckets": [{"lo": 1, "hi": 2, "count": 3}],
    }
    assert client.post("/reports/bulk", json={"reports": [item, item]}).status_code == 422
    bad = client.post(
        "/reports/bulk",
        json={
            "reports": [
                {
                    **item,
                    "buckets": [
                        {"lo": 1, "hi": 5, "count": 3},
                        {"lo": 4, "hi": 9, "count": 2},
                    ],
                }
            ]
        },
    )
    assert bad.status_code == 422
    conn = connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) AS n FROM buckets").fetchone()["n"] == 0
        assert (
            conn.execute(
                "SELECT COUNT(*) AS n FROM bulk_batches WHERE id != 0"
            ).fetchone()["n"]
            == 0
        )
    finally:
        conn.close()


def test_bulk_chunks_respect_1024_row_limit_with_multi_bucket_coils(client, monkeypatch):
    # Two coils of 600 buckets: 1024 budget fits the first coil (600), then
    # the second coil alone starts the next transaction (600 rows).
    payload = {
        "reports": [
            {
                "line_id": "L1",
                "production_day": "2026-11-20",
                "coil_id": "BIG1",
                "buckets": [
                    {"lo": 2 * j, "hi": 2 * j + 1, "count": 1} for j in range(600)
                ],
            },
            {
                "line_id": "L1",
                "production_day": "2026-11-20",
                "coil_id": "BIG2",
                "buckets": [
                    {"lo": 2 * j, "hi": 2 * j + 1, "count": 1} for j in range(600)
                ],
            },
        ]
    }
    per_transaction: list[int] = []
    original_transaction = db.transaction

    @contextmanager
    def counting_transaction(conn):
        before = conn.execute(
            "SELECT COALESCE(MAX(rowid), 0) AS m FROM buckets"
        ).fetchone()["m"]
        with original_transaction(conn):
            yield
        inserted = conn.execute(
            "SELECT COUNT(*) AS n FROM buckets WHERE rowid > ?", (before,)
        ).fetchone()["n"]
        if inserted:
            per_transaction.append(inserted)

    monkeypatch.setattr(db, "transaction", counting_transaction)
    response = client.post("/reports/bulk", json=payload)
    assert response.status_code == 201, response.text
    assert per_transaction == [600, 600]
    assert max(per_transaction) <= 1024


def test_bulk_rejects_coil_with_more_than_1024_buckets(client, db_path):
    payload = {
        "reports": [
            {
                "line_id": "L1",
                "production_day": "2026-11-20",
                "coil_id": "HUGE",
                "buckets": [
                    {"lo": j, "hi": j, "count": 1} for j in range(1025)
                ],
            }
        ]
    }
    response = client.post("/reports/bulk", json=payload)
    assert response.status_code == 422
    conn = connect(db_path)
    try:
        # The created staging batch was discarded: no coil, no bucket rows,
        # only a 'failed' batch marker (or nothing at all).
        assert conn.execute("SELECT COUNT(*) AS n FROM coils").fetchone()["n"] == 0
        assert conn.execute("SELECT COUNT(*) AS n FROM buckets").fetchone()["n"] == 0
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM bulk_batches WHERE id != 0 AND status != 'failed'"
        ).fetchone()["n"] == 0
    finally:
        conn.close()


def test_bulk_response_shape_matches_original_contract(client):
    response = client.post("/reports/bulk", json=bulk_payload(3))
    assert response.status_code == 201
    body = response.json()
    assert [item["coil_id"] for item in body] == ["C0", "C1", "C2"]
    for item in body:
        assert set(item) == {
            "line_id",
            "coil_id",
            "production_day",
            "version",
            "buckets",
        }
        assert item["line_id"] == "L1"
        assert item["production_day"] == "2026-11-20"
        assert item["version"] == 1
        assert item["buckets"] == [{"lo": 0, "hi": 4, "count": 1}]
