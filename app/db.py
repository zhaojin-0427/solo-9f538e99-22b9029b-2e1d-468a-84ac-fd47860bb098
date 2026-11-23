"""SQLite persistence.

A day summary is keyed by ``(line_id, production_day, coil_id)``: the same coil
number may legitimately appear on several production days, and each day's
buckets are one summary.  Its buckets live in a child table.

Published ("live") data and data staged for a bulk batch share the ``buckets``
table but are separated by ``batch_id``:

* ``batch_id = 0`` rows are live buckets, created by single-coil submissions
  and refinements;
* ``batch_id > 0`` rows belong to an unpublished bulk batch and stay invisible
  until the batch is published.  Staging writes are split into short
  transactions of at most ``MAX_BUCKETS_PER_TRANSACTION`` bucket rows each, so
  a thousand-coil batch never holds the write lock for long and single-coil
  submissions / refinements keep committing while staging runs.

Publishing never touches a bucket table: it only swaps the ``batch_id``
pointer on ``coils`` (and marks the batch published) in one short write
transaction, so a whole batch becomes visible atomically and every coil in it
takes effect at once.

All readers (single-coil lookup and P95) go through the ``effective_buckets``
view inside one read transaction, i.e. one committed snapshot, and only ever
see buckets of the currently published batch of each coil.

On startup every batch still in a non-published state is discarded: its
staged rows and coil entries are deleted, while published coil reports remain
queryable.  The SQLite data volume is otherwise reused as-is.

No individual measurement is ever stored or expanded: only bucket counts.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator

# Maximum number of bucket rows one staging write transaction may insert.
# Keeping each write transaction short lets concurrent single-coil
# submissions and refinements commit while a bulk batch is being staged.
MAX_BUCKETS_PER_TRANSACTION = 1024

SCHEMA = """
CREATE TABLE IF NOT EXISTS coils (
    line_id        TEXT NOT NULL,
    production_day TEXT NOT NULL,
    coil_id        TEXT NOT NULL,
    version        INTEGER NOT NULL DEFAULT 1,
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    batch_id       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (line_id, production_day, coil_id)
);

CREATE TABLE IF NOT EXISTS buckets (
    line_id        TEXT NOT NULL,
    production_day TEXT NOT NULL,
    coil_id        TEXT NOT NULL,
    position       INTEGER NOT NULL,
    lo             INTEGER NOT NULL,
    hi             INTEGER NOT NULL,
    count          INTEGER NOT NULL CHECK (count > 0),
    batch_id       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (batch_id, line_id, production_day, coil_id, position),
    CHECK (lo <= hi)
);

CREATE TABLE IF NOT EXISTS bulk_batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    status      TEXT NOT NULL CHECK (status IN ('staging', 'published', 'failed')),
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    published_at TEXT
);

-- Row 0 is the permanent "live" channel for single-coil submissions,
-- refinements and published coils before any bulk batch replaces them.
-- AUTOINCREMENT still hands out ids starting at 1 for real bulk batches.
INSERT INTO bulk_batches (id, status, published_at)
VALUES (0, 'published', NULL)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS bulk_batch_coils (
    batch_id       INTEGER NOT NULL,
    line_id        TEXT NOT NULL,
    production_day TEXT NOT NULL,
    coil_id        TEXT NOT NULL,
    PRIMARY KEY (batch_id, line_id, production_day, coil_id),
    FOREIGN KEY (batch_id) REFERENCES bulk_batches (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bulk_coils_key
    ON bulk_batch_coils (line_id, production_day, coil_id);

-- Buckets of the batch currently published for each coil.  Staged batches
-- (bulk_batches.status != 'published') are invisible to every reader.
CREATE VIEW IF NOT EXISTS effective_buckets AS
SELECT b.line_id, b.production_day, b.coil_id, b.position, b.lo, b.hi, b.count
FROM buckets b
JOIN coils c
  ON c.line_id = b.line_id
 AND c.production_day = b.production_day
 AND c.coil_id = b.coil_id
 AND c.batch_id = b.batch_id
JOIN bulk_batches bb
  ON bb.id = b.batch_id
 AND bb.status = 'published';
"""

# Bucket indexes are created from Python rather than inside SCHEMA: on a
# legacy data volume the buckets table is rebuilt to gain ``batch_id`` first,
# and an index referencing the new column before that rebuild would fail.
BUCKET_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_buckets_live "
    "ON buckets (line_id, production_day, coil_id, position) "
    "WHERE batch_id = 0",
    "CREATE INDEX IF NOT EXISTS idx_buckets_batch "
    "ON buckets (batch_id, line_id, production_day, coil_id, position)",
)

DEFAULT_DATABASE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "coating.db"
)


def database_path() -> str:
    return os.environ.get("DATABASE_PATH", DEFAULT_DATABASE_PATH)


def connect(path: str | None = None) -> sqlite3.Connection:
    path = path or database_path()
    if path != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(
        path, timeout=30.0, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    if path != ":memory:":
        conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _needs_buckets_rebuild(conn: sqlite3.Connection) -> bool:
    """The pre-bulk schema keyed buckets without ``batch_id``; rebuild then."""
    pk = conn.execute("PRAGMA index_list(buckets)").fetchall()
    for row in pk:
        if int(row["unique"]) == 1:
            index = conn.execute(f"PRAGMA index_info({row['name']})").fetchall()
            if [col["name"] for col in index] == [
                "line_id",
                "production_day",
                "coil_id",
                "position",
            ]:
                return True
    return False


def init_db(path: str | None = None) -> None:
    """Create the schema, migrate the original data volume, purge stale batches."""
    conn = connect(path)
    try:
        # ``executescript`` commits implicitly, so it must not run inside an
        # explicit BEGIN block.  Every statement is IF NOT EXISTS and thus
        # safe to replay on every start.
        conn.executescript(SCHEMA)

        existing = _table_columns(conn, "coils")
        if "batch_id" not in existing:
            conn.execute("ALTER TABLE coils ADD COLUMN batch_id INTEGER NOT NULL DEFAULT 0")

        if _needs_buckets_rebuild(conn):
            # The view references buckets; drop it while the table is rebuilt.
            # A previous startup may have crashed after creating buckets_new.
            conn.execute("DROP VIEW IF EXISTS effective_buckets")
            conn.execute("DROP TABLE IF EXISTS buckets_new")
            conn.execute("PRAGMA foreign_keys = OFF")
            try:
                conn.executescript(
                    """
                    CREATE TABLE buckets_new (
                        line_id        TEXT NOT NULL,
                        production_day TEXT NOT NULL,
                        coil_id        TEXT NOT NULL,
                        position       INTEGER NOT NULL,
                        lo             INTEGER NOT NULL,
                        hi             INTEGER NOT NULL,
                        count          INTEGER NOT NULL CHECK (count > 0),
                        batch_id       INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (batch_id, line_id, production_day, coil_id, position),
                        CHECK (lo <= hi)
                    );
                    INSERT INTO buckets_new
                        (line_id, production_day, coil_id, position, lo, hi, count, batch_id)
                    SELECT line_id, production_day, coil_id, position, lo, hi, count, 0
                    FROM buckets;
                    DROP TABLE buckets;
                    ALTER TABLE buckets_new RENAME TO buckets;
                    """
                )
            finally:
                conn.execute("PRAGMA foreign_keys = ON")
            conn.executescript(
                """
                CREATE VIEW IF NOT EXISTS effective_buckets AS
                SELECT b.line_id, b.production_day, b.coil_id, b.position, b.lo, b.hi, b.count
                FROM buckets b
                JOIN coils c
                  ON c.line_id = b.line_id
                 AND c.production_day = b.production_day
                 AND c.coil_id = b.coil_id
                 AND c.batch_id = b.batch_id
                JOIN bulk_batches bb
                  ON bb.id = b.batch_id
                 AND bb.status = 'published';
                """
            )

        for statement in BUCKET_INDEXES:
            conn.execute(statement)

        purge_unpublished_batches(conn)
    finally:
        conn.close()


def purge_unpublished_batches(conn: sqlite3.Connection) -> None:
    """Discard batches that never finished publishing (called on startup).

    Published coil reports and their buckets are untouched.  Orphaned bucket
    rows that no coil points at are garbage-collected as well.
    """
    with transaction(conn):
        stale = [
            int(row["id"])
            for row in conn.execute(
                "SELECT id FROM bulk_batches WHERE status != 'published'"
            ).fetchall()
        ]
        for batch_id in stale:
            conn.execute("DELETE FROM buckets WHERE batch_id = ?", (batch_id,))
            conn.execute(
                "DELETE FROM bulk_batch_coils WHERE batch_id = ?", (batch_id,)
            )
            conn.execute("DELETE FROM bulk_batches WHERE id = ?", (batch_id,))
        # Orphaned buckets: rows no coil currently points at, e.g. rows of a
        # batch superseded by a later publish or leftovers of an abandoned
        # batch.  Keep rows whose (batch_id, key) matches a coil pointer; the
        # correlated NOT EXISTS deletes everything else.
        conn.execute(
            """
            DELETE FROM buckets
            WHERE NOT EXISTS (
                SELECT 1
                FROM coils c
                WHERE c.line_id = buckets.line_id
                  AND c.production_day = buckets.production_day
                  AND c.coil_id = buckets.coil_id
                  AND c.batch_id = buckets.batch_id
            )
            """
        )


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Exclusive write transaction: commit on success, roll back on any error."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextmanager
def snapshot(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Read transaction: every SELECT inside sees the same committed state."""
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        conn.execute("COMMIT")
