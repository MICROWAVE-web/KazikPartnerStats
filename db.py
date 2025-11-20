import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional, List

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
                campaign_id TEXT,
                reward_snapshot REAL, -- only for first_dep
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (telegram_user_id) REFERENCES users(telegram_user_id)
            );
            """
        )
        # Add campaign_id column if it doesn't exist (for existing databases)
        cur.execute(
            """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='events'
            """
        )
        if cur.fetchone():
            # Check if campaign_id column exists
            cur.execute("PRAGMA table_info(events)")
            columns = [row[1] for row in cur.fetchall()]
            if 'campaign_id' not in columns:
                cur.execute("ALTER TABLE events ADD COLUMN campaign_id TEXT")


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
    campaign_id: Optional[str] = None,
) -> None:
    ensure_user(telegram_user_id)
    reward_snapshot: Optional[float] = None
    if event_type == "first_dep":
        # snapshot the reward at the time of first deposit
        reward_snapshot = get_reward(telegram_user_id)
    with open_db() as conn:
        conn.execute(
            """
            INSERT INTO events (telegram_user_id, event_type, played_id, btag, campaign_id, reward_snapshot)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (telegram_user_id, event_type, played_id, btag, campaign_id, reward_snapshot),
        )


def _period_bounds(period: str) -> Optional[Tuple[datetime, datetime]]:
    now = datetime.utcnow()
    if period == "hour":
        # Прошедший час (от часа назад до сейчас)
        hour_start = now - timedelta(hours=1)
        return (hour_start, now)
    if period == "day":
        # Сегодня (от начала дня до сейчас)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return (day_start, now)
    if period == "week":
        # Текущая неделя (понедельник - воскресенье)
        days_since_monday = now.weekday()  # 0 = Monday, 6 = Sunday
        week_start = (now - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
        return (week_start, now)
    if period == "month":
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return (month_start, now)
    if period == "last_week":
        # Прошлая неделя (понедельник - воскресенье прошлой недели)
        days_since_monday = now.weekday()
        # Находим начало прошлой недели (понедельник прошлой недели)
        last_week_start = (now - timedelta(days=days_since_monday + 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        # Конец прошлой недели (воскресенье прошлой недели, 23:59:59)
        last_week_end = (last_week_start + timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=999999)
        return (last_week_start, last_week_end)
    return None  # all time


def aggregate_by_btag(telegram_user_id: int, period: str) -> Dict[str, Tuple[int, int, float]]:
    """
    Returns mapping: btag -> (registrations_count, first_deposits_count, total_reward_sum)
    period in {"all","hour","day","week","last_week","month"}
    DEPRECATED: Use aggregate_by_campaign_and_btag instead
    """
    period_bounds = _period_bounds(period)
    params = [telegram_user_id]
    time_filter = ""
    if period_bounds is not None:
        time_filter = " AND created_at >= ? AND created_at <= ?"
        params.append(period_bounds[0])
        params.append(period_bounds[1])

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


def aggregate_by_campaign_and_btag(telegram_user_id: int, period: str) -> Dict[str, Dict[str, Tuple[int, int, float]]]:
    """
    Returns nested mapping: campaign_id -> {btag -> (registrations_count, first_deposits_count, total_reward_sum)}
    period in {"all","hour","day","week","last_week","month"}
    """
    period_bounds = _period_bounds(period)
    params = [telegram_user_id]
    time_filter = ""
    if period_bounds is not None:
        time_filter = " AND created_at >= ? AND created_at <= ?"
        params.append(period_bounds[0])
        params.append(period_bounds[1])

    # Get registrations grouped by campaign_id and btag
    sql_regs = f"""
    SELECT COALESCE(campaign_id, '') AS campaign_id, COALESCE(btag, '') AS btag, COUNT(*) AS reg_count
    FROM events
    WHERE telegram_user_id = ? AND event_type = 'registration'{time_filter}
    GROUP BY campaign_id, btag
    """

    # Get deposits grouped by campaign_id and btag
    sql_deps = f"""
    SELECT COALESCE(campaign_id, '') AS campaign_id, COALESCE(btag, '') AS btag, 
           COUNT(*) AS dep_count, COALESCE(SUM(reward_snapshot), 0) AS reward_sum
    FROM events
    WHERE telegram_user_id = ? AND event_type = 'first_dep'{time_filter}
    GROUP BY campaign_id, btag
    """

    results: Dict[str, Dict[str, Tuple[int, int, float]]] = {}
    with open_db() as conn:
        # Get all registrations
        regs_rows = conn.execute(sql_regs, params).fetchall()
        for row in regs_rows:
            campaign_id = row["campaign_id"] or ""
            btag = row["btag"] or ""
            if campaign_id not in results:
                results[campaign_id] = {}
            if btag not in results[campaign_id]:
                results[campaign_id][btag] = (0, 0, 0.0)
            regs, deps, reward = results[campaign_id][btag]
            results[campaign_id][btag] = (int(row["reg_count"]), deps, reward)

        # Get all deposits and merge
        deps_rows = conn.execute(sql_deps, params).fetchall()
        for row in deps_rows:
            campaign_id = row["campaign_id"] or ""
            btag = row["btag"] or ""
            if campaign_id not in results:
                results[campaign_id] = {}
            if btag not in results[campaign_id]:
                results[campaign_id][btag] = (0, 0, 0.0)
            regs, deps, reward = results[campaign_id][btag]
            results[campaign_id][btag] = (regs, int(row["dep_count"]), float(row["reward_sum"]))

    return results


def get_all_user_ids() -> List[int]:
    with open_db() as conn:
        rows = conn.execute("SELECT telegram_user_id FROM users").fetchall()
    return [int(row["telegram_user_id"]) for row in rows]
