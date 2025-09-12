import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

from config import DEFAULT_REWARD_PER_DEP


DB_PATH = "data.sqlite3"


@contextmanager
def open_db():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with open_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_user_id INTEGER PRIMARY KEY,
                reward_per_dep REAL NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_user_id INTEGER NOT NULL,
                event_type TEXT NOT NULL CHECK(event_type IN ('registration','first_dep')),
                played_id TEXT,
                btag TEXT,
                reward_snapshot REAL, -- only for first_dep
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (telegram_user_id) REFERENCES users(telegram_user_id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS shared_access (
                owner_telegram_user_id INTEGER NOT NULL,
                viewer_telegram_user_id INTEGER NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (owner_telegram_user_id, viewer_telegram_user_id)
            );
            """
        )


def ensure_user(telegram_user_id: int) -> None:
    with open_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT telegram_user_id FROM users WHERE telegram_user_id = ?", (telegram_user_id,))
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO users (telegram_user_id, reward_per_dep) VALUES (?, ?)",
                (telegram_user_id, DEFAULT_REWARD_PER_DEP),
            )


def set_reward(telegram_user_id: int, amount: float) -> None:
    ensure_user(telegram_user_id)
    with open_db() as conn:
        conn.execute(
            "UPDATE users SET reward_per_dep = ? WHERE telegram_user_id = ?",
            (amount, telegram_user_id),
        )


def get_reward(telegram_user_id: int) -> float:
    ensure_user(telegram_user_id)
    with open_db() as conn:
        row = conn.execute(
            "SELECT reward_per_dep FROM users WHERE telegram_user_id = ?",
            (telegram_user_id,),
        ).fetchone()
        print(bool(row))
        print(float(row[0]) if row else 0.0)
        return float(row[0]) if row else 0.0


def insert_event(
    telegram_user_id: int,
    event_type: str,
    played_id: Optional[str],
    btag: Optional[str],
) -> None:
    ensure_user(telegram_user_id)
    reward_snapshot: Optional[float] = None
    if event_type == "first_dep":
        # snapshot the reward at the time of first deposit
        reward_snapshot = get_reward(telegram_user_id)
    with open_db() as conn:
        conn.execute(
            """
            INSERT INTO events (telegram_user_id, event_type, played_id, btag, reward_snapshot)
            VALUES (?, ?, ?, ?, ?)
            """,
            (telegram_user_id, event_type, played_id, btag, reward_snapshot),
        )


def _period_bounds(period: str) -> Optional[datetime]:
    now = datetime.utcnow()
    if period == "hour":
        return now - timedelta(hours=1)
    if period == "day":
        return now - timedelta(days=1)
    if period == "week":
        return now - timedelta(weeks=1)
    if period == "month":
        return now - timedelta(days=30)
    return None  # all time


def aggregate_by_btag(telegram_user_id: int, period: str) -> Dict[str, Tuple[int, int, float]]:
    """
    Returns mapping: btag -> (registrations_count, first_deposits_count, total_reward_sum)
    period in {"all","hour","day","week","month"}
    """
    since = _period_bounds(period)
    params = [telegram_user_id]
    time_filter = ""
    if since is not None:
        time_filter = " AND created_at >= ?"
        params.append(since)

    sql = f"""
    WITH regs AS (
        SELECT btag, COUNT(*) AS reg_count
        FROM events
        WHERE telegram_user_id = ? AND event_type = 'registration'{time_filter}
        GROUP BY btag
    ),
    deps AS (
        SELECT btag, COUNT(*) AS dep_count, COALESCE(SUM(reward_snapshot), 0) AS reward_sum
        FROM events
        WHERE telegram_user_id = ? AND event_type = 'first_dep'{time_filter}
        GROUP BY btag
    )
    SELECT COALESCE(r.btag, d.btag) AS btag,
           COALESCE(r.reg_count, 0) AS registrations,
           COALESCE(d.dep_count, 0) AS first_deps,
           COALESCE(d.reward_sum, 0) AS reward_sum
    FROM regs r
    FULL OUTER JOIN deps d ON r.btag = d.btag
    ;
    """

    # SQLite doesn't support FULL OUTER JOIN. Emulate via UNION of LEFT JOINs.
    sql_left = f"""
    SELECT r.btag AS btag,
           r.reg_count AS registrations,
           COALESCE(d.dep_count, 0) AS first_deps,
           COALESCE(d.reward_sum, 0) AS reward_sum
    FROM (
        SELECT btag, COUNT(*) AS reg_count
        FROM events
        WHERE telegram_user_id = ? AND event_type = 'registration'{time_filter}
        GROUP BY btag
    ) r
    LEFT JOIN (
        SELECT btag, COUNT(*) AS dep_count, COALESCE(SUM(reward_snapshot), 0) AS reward_sum
        FROM events
        WHERE telegram_user_id = ? AND event_type = 'first_dep'{time_filter}
        GROUP BY btag
    ) d ON r.btag = d.btag
    """

    sql_right_only = f"""
    SELECT d.btag AS btag,
           0 AS registrations,
           d.dep_count AS first_deps,
           d.reward_sum AS reward_sum
    FROM (
        SELECT btag, COUNT(*) AS dep_count, COALESCE(SUM(reward_snapshot), 0) AS reward_sum
        FROM events
        WHERE telegram_user_id = ? AND event_type = 'first_dep'{time_filter}
        GROUP BY btag
    ) d
    WHERE d.btag NOT IN (
        SELECT btag FROM (
            SELECT btag
            FROM events
            WHERE telegram_user_id = ? AND event_type = 'registration'{time_filter}
            GROUP BY btag
        )
    )
    """

    results: Dict[str, Tuple[int, int, float]] = {}
    with open_db() as conn:
        rows_left = conn.execute(sql_left, params + params).fetchall()
        rows_right = conn.execute(sql_right_only, params + params).fetchall()
        for row in rows_left + rows_right:
            btag = row["btag"] or ""
            results[btag] = (int(row["registrations"]), int(row["first_deps"]), float(row["reward_sum"]))
    return results


def get_period_range(telegram_user_id: int, period: str) -> Optional[Tuple[datetime, datetime]]:
    """
    Returns (start_datetime, end_datetime) in UTC for the given user and period.
    For period == 'all', start is the earliest event created_at (if any).
    Returns None if there is no data at all for the user.
    """
    end = datetime.utcnow()
    start = _period_bounds(period)
    if period == "all":
        with open_db() as conn:
            row = conn.execute(
                "SELECT MIN(created_at) AS min_dt FROM events WHERE telegram_user_id = ?",
                (telegram_user_id,),
            ).fetchone()
            if not row or row[0] is None:
                return None
            # sqlite with detect_types should parse to datetime; if not, attempt parse
            min_dt = row[0]
            if isinstance(min_dt, str):
                try:
                    # Try ISO format
                    min_dt = datetime.fromisoformat(min_dt)
                except Exception:
                    return None
            start = min_dt
        return (start, end)
    # for bounded periods
    if start is None:
        return None
    # Check if there is any data in this period
    with open_db() as conn:
        row = conn.execute(
            """
            SELECT 1 FROM events
            WHERE telegram_user_id = ? AND created_at >= ?
            ORDER BY created_at ASC LIMIT 1
            """,
            (telegram_user_id, start),
        ).fetchone()
        if row is None:
            return None
    return (start, end)


def grant_access(owner_telegram_user_id: int, viewer_telegram_user_id: int) -> None:
    if owner_telegram_user_id == viewer_telegram_user_id:
        return
    with open_db() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO shared_access (owner_telegram_user_id, viewer_telegram_user_id)
            VALUES (?, ?)
            """,
            (owner_telegram_user_id, viewer_telegram_user_id),
        )


def revoke_access(owner_telegram_user_id: int, viewer_telegram_user_id: int) -> None:
    with open_db() as conn:
        conn.execute(
            "DELETE FROM shared_access WHERE owner_telegram_user_id = ? AND viewer_telegram_user_id = ?",
            (owner_telegram_user_id, viewer_telegram_user_id),
        )


def list_viewers(owner_telegram_user_id: int) -> Tuple[int, ...]:
    with open_db() as conn:
        rows = conn.execute(
            "SELECT viewer_telegram_user_id FROM shared_access WHERE owner_telegram_user_id = ? ORDER BY created_at ASC",
            (owner_telegram_user_id,),
        ).fetchall()
    return tuple(int(r[0]) for r in rows)


def list_available_owners(viewer_telegram_user_id: int) -> Tuple[int, ...]:
    with open_db() as conn:
        rows = conn.execute(
            "SELECT owner_telegram_user_id FROM shared_access WHERE viewer_telegram_user_id = ? ORDER BY created_at ASC",
            (viewer_telegram_user_id,),
        ).fetchall()
    return tuple(int(r[0]) for r in rows)


