from __future__ import annotations

import asyncio
import sqlite3
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import discord
from discord.ext import commands


RANKING_START_RATING = 1000
ELO_K_FACTOR = 32
MONTHLY_WIN_BONUS = 5
RANKED_RESULT_STATUS_SQL = "status IN ('completed', 'confirmed') AND winner_id IS NOT NULL AND loser_id IS NOT NULL"


async def create_db_pool(bot: commands.Bot) -> None:
    print("Connect to SQLite database...")
    try:
        bot.db = SqliteDatabase("dartliga.db")
        await bot.db.connect()
    except Exception as exc:
        bot.db = None
        print(f"Database unavailable, continuing without DB features: {type(exc).__name__}: {exc}")
    else:
        print("SQLite database connected")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def month_key_to_text(month_key: date) -> str:
    return month_key.strftime("%Y-%m")


def calculate_elo_winner_delta(winner_rating: int, loser_rating: int) -> int:
    expected_winner_score = 1 / (1 + 10 ** ((loser_rating - winner_rating) / 400))
    return max(1, int(round(ELO_K_FACTOR * (1 - expected_winner_score))))

#variante 2
def calculate_monthly_win_points(elo_change: int) -> int:
    return elo_change + MONTHLY_WIN_BONUS
#variante 1
def calculate_monthly_win_points_v1(winner_rating: int, opponent_rating: int) -> int:
    difference_bonus = max(0, min(6, (opponent_rating - winner_rating) // 90))
    return 10 + difference_bonus

def get_current_ranked_month_key() -> date:
    return datetime.now(timezone.utc).date().replace(day=1)


def to_ranked_database_average(value: str) -> str:
    return value.replace(",", ".")


async def ensure_ranked_storage(bot: commands.Bot) -> None:
    db = getattr(bot, "db", None)
    if db is None:
        return

    try:
        await db.ensure_ranked_storage()
        await rebuild_current_month_rankings(bot)
    except Exception as exc:
        print(f"Ranked DB persistence unavailable, falling back where possible: {type(exc).__name__}: {exc}")


async def fetch_active_ranked_matches(bot: commands.Bot) -> list[dict[str, Any]]:
    db = getattr(bot, "db", None)
    if db is None:
        return []

    try:
        return await db.fetch_active_ranked_matches()
    except Exception as exc:
        print(f"Ranked active-match restore failed: {type(exc).__name__}: {exc}")
        return []


async def fetch_pending_ranked_matches(bot: commands.Bot) -> list[dict[str, Any]]:
    db = getattr(bot, "db", None)
    if db is None:
        return []

    try:
        return await db.fetch_pending_ranked_matches()
    except Exception as exc:
        print(f"Ranked pending-match restore failed: {type(exc).__name__}: {exc}")
        return []


async def persist_pending_ranked_match(bot: commands.Bot, match: Any) -> bool:
    db = getattr(bot, "db", None)
    if db is None:
        return False

    try:
        player_one_id, player_two_id = match.player_ids
        await db.persist_pending_ranked_match(
            match_id=match.match_id,
            queue_name=match.queue_name,
            player_one_id=player_one_id,
            player_two_id=player_two_id,
            thread_id=match.thread_id,
        )
    except Exception as exc:
        print(f"Ranked pending-match persistence failed: {type(exc).__name__}: {exc}")
        return False

    return True


async def persist_active_ranked_match(bot: commands.Bot, match: Any) -> bool:
    db = getattr(bot, "db", None)
    if db is None:
        return False

    try:
        player_one_id, player_two_id = match.player_ids
        await db.persist_active_ranked_match(
            match_id=match.match_id,
            queue_name=match.queue_name,
            player_one_id=player_one_id,
            player_two_id=player_two_id,
            thread_id=match.thread_id,
        )
    except Exception as exc:
        print(f"Ranked active-match persistence failed: {type(exc).__name__}: {exc}")
        return False

    return True


async def mark_ranked_match_cancelled(bot: commands.Bot, match_id: int) -> None:
    db = getattr(bot, "db", None)
    if db is None:
        return

    try:
        await db.mark_match_cancelled(match_id)
    except Exception as exc:
        print(f"Ranked match cancel update failed: {type(exc).__name__}: {exc}")


async def rebuild_current_month_rankings(bot: commands.Bot) -> int:
    db = getattr(bot, "db", None)
    if db is None:
        return 0

    return await db.rebuild_current_month_rankings(get_current_ranked_month_key())




async def get_next_ranked_match_id(bot: commands.Bot, fallback_match_id: int) -> tuple[int, int]:
    db = getattr(bot, "db", None)
    if db is None:
        return fallback_match_id, fallback_match_id + 1

    try:
        match_id = await db.get_next_match_id()
    except Exception as exc:
        print(f"Ranked match-id fetch failed, falling back to memory: {type(exc).__name__}: {exc}")
        return fallback_match_id, fallback_match_id + 1

    return int(match_id), fallback_match_id



async def persist_ranked_match_result(
    bot: commands.Bot,
    match: Any,
    result: Any,
    *,
    guild_id: int,
    confirmed_by: int,
) -> tuple[bool, bool]:
    del guild_id, confirmed_by
    db = getattr(bot, "db", None)
    if db is None:
        return False, False

    player_one_id, player_two_id = match.player_ids
    return await db.persist_ranked_match_result(
        match_id=match.match_id,
        queue_name=match.queue_name,
        player_one_id=player_one_id,
        player_two_id=player_two_id,
        winner_id=result.winner_id,
        score=result.score,
        player_one_average=to_ranked_database_average(result.averages[player_one_id]),
        player_two_average=to_ranked_database_average(result.averages[player_two_id]),
        month_key=get_current_ranked_month_key(),
    )


async def mark_ranked_match_result_published(
    bot: commands.Bot,
    match_id: int,
    channel_id: int,
    message_id: int,
) -> None:
    db = getattr(bot, "db", None)
    if db is None:
        return

    await db.mark_match_result_published(match_id, channel_id, message_id)


async def fetch_world_ranking(bot: commands.Bot, limit: int | None = None) -> list[tuple[int, int, int, int]]:
    db = getattr(bot, "db", None)
    if db is None:
        return []

    return await db.fetch_world_ranking(limit)


async def fetch_monthly_ranking(bot: commands.Bot, month_key: date | None = None, limit: int | None = None) -> list[tuple[int, int, int, int]]:
    db = getattr(bot, "db", None)
    if db is None:
        return []

    if month_key is None:
        month_key = get_current_ranked_month_key()
    return await db.fetch_monthly_ranking(month_key, limit)


async def generate_monthly_ranking(
    bot: commands.Bot,
    month_key: date,
    limit: int | None = None,
) -> list[tuple[int, int, int, int]]:
    db = getattr(bot, "db", None)
    if db is None:
        return []

    return await db.generate_monthly_ranking(month_key, limit)


async def fetch_match_history(bot: commands.Bot, player: discord.Member):
    db = getattr(bot, "db", None)
    if db is None:
        return []

    return await db.fetch_match_history(player.id)


class SqliteDatabase:
    def __init__(self, path: Path | str = "dartliga.db") -> None:
        self.path = Path(path)
        self._lock = asyncio.Lock()
        self._connection: sqlite3.Connection | None = None

    async def connect(self) -> None:
        self._connection = sqlite3.connect(self.path)
        self._connection.row_factory = sqlite3.Row
        await self.initialize()

    @property
    def connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("SQLite database is not connected")
        return self._connection

    async def close(self) -> None:
        async with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    async def initialize(self) -> None:
        async with self._lock:
            self.connection.executescript(
                """

                CREATE TABLE IF NOT EXISTS players (
                    user_id INTEGER PRIMARY KEY,
                    rating INTEGER
                );

                CREATE TABLE IF NOT EXISTS monthly_points (
                    user_id INTEGER,
                    month TEXT,
                    points INTEGER,
                    PRIMARY KEY (user_id, month)
                );

                CREATE TABLE IF NOT EXISTS matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    player1_id INTEGER,
                    player2_id INTEGER,
                    winner_id INTEGER,
                    loser_id INTEGER,
                    platform TEXT,
                    status TEXT DEFAULT 'pending',
                    score TEXT,
                    winner_avg REAL,
                    loser_avg REAL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    elo_change INTEGER
                );
                """
            )

            columns = {
                str(row["name"])
                for row in self.connection.execute("PRAGMA table_info(matches)").fetchall()
            }
            if "thread_id" not in columns:
                self.connection.execute("ALTER TABLE matches ADD COLUMN thread_id INTEGER")
            self.connection.commit()

    async def ensure_ranked_storage(self) -> None:
        await self.initialize()

    async def get_next_match_id(self) -> int:
        async with self._lock:
            row = self.connection.execute("SELECT seq FROM sqlite_sequence WHERE name = 'matches'").fetchone()
            if row is None:
                max_row = self.connection.execute("SELECT COALESCE(MAX(id), 0) + 1 AS match_id FROM matches").fetchone()
                match_id = int(max_row["match_id"])
                self.connection.execute(
                    "INSERT INTO sqlite_sequence(name, seq) VALUES('matches', ?)",
                    (match_id,),
                )
            else:
                match_id = int(row["seq"]) + 1
                self.connection.execute(
                    "UPDATE sqlite_sequence SET seq = ? WHERE name = 'matches'",
                    (match_id,),
                )
            self.connection.commit()
            return match_id

    async def persist_active_ranked_match(
        self,
        *,
        match_id: int,
        queue_name: str,
        player_one_id: int,
        player_two_id: int,
        thread_id: int,
    ) -> None:
        async with self._lock:
            with self.connection:
                self.connection.execute(
                    """
                    INSERT INTO matches(id, player1_id, player2_id, platform, status, thread_id, timestamp)
                    VALUES(?, ?, ?, ?, 'active', ?, ?)
                    ON CONFLICT(id) DO UPDATE
                    SET player1_id = excluded.player1_id,
                        player2_id = excluded.player2_id,
                        platform = excluded.platform,
                        status = 'active',
                        thread_id = excluded.thread_id
                    """,
                    (match_id, player_one_id, player_two_id, queue_name, thread_id, utc_now()),
                )

    async def persist_pending_ranked_match(
        self,
        *,
        match_id: int,
        queue_name: str,
        player_one_id: int,
        player_two_id: int,
        thread_id: int,
    ) -> None:
        async with self._lock:
            with self.connection:
                self.connection.execute(
                    """
                    INSERT INTO matches(id, player1_id, player2_id, platform, status, thread_id, timestamp)
                    VALUES(?, ?, ?, ?, 'pending', ?, ?)
                    ON CONFLICT(id) DO UPDATE
                    SET player1_id = excluded.player1_id,
                        player2_id = excluded.player2_id,
                        platform = excluded.platform,
                        status = 'pending',
                        thread_id = excluded.thread_id
                    """,
                    (match_id, player_one_id, player_two_id, queue_name, thread_id, utc_now()),
                )

    async def fetch_active_ranked_matches(self) -> list[dict[str, Any]]:
        async with self._lock:
            rows = self.connection.execute(
                """
                SELECT id, player1_id, player2_id, platform, thread_id
                FROM matches
                WHERE status = 'active'
                  AND thread_id IS NOT NULL
                  AND player1_id IS NOT NULL
                  AND player2_id IS NOT NULL
                ORDER BY id ASC
                """
            ).fetchall()

            return [
                {
                    "match_id": int(row["id"]),
                    "queue_name": str(row["platform"] or "Ranked"),
                    "player_ids": (int(row["player1_id"]), int(row["player2_id"])),
                    "thread_id": int(row["thread_id"]),
                }
                for row in rows
            ]

    async def fetch_pending_ranked_matches(self) -> list[dict[str, Any]]:
        async with self._lock:
            rows = self.connection.execute(
                """
                SELECT id, player1_id, player2_id, platform, thread_id, timestamp
                FROM matches
                WHERE status = 'pending'
                  AND thread_id IS NOT NULL
                  AND player1_id IS NOT NULL
                  AND player2_id IS NOT NULL
                ORDER BY id ASC
                """
            ).fetchall()

            return [
                {
                    "match_id": int(row["id"]),
                    "queue_name": str(row["platform"] or "Ranked"),
                    "player_ids": (int(row["player1_id"]), int(row["player2_id"])),
                    "thread_id": int(row["thread_id"]),
                    "created_at": str(row["timestamp"] or ""),
                }
                for row in rows
            ]

    async def mark_match_cancelled(self, match_id: int) -> None:
        async with self._lock:
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE matches
                    SET status = 'cancelled',
                        timestamp = ?
                    WHERE id = ?
                    """,
                    (utc_now(), match_id),
                )

    async def rebuild_current_month_rankings(self, month_key: date) -> int:
        month_text = month_key_to_text(month_key)
        async with self._lock:
            rows = self.connection.execute(
                f"""
                SELECT id, winner_id, loser_id, elo_change, strftime('%Y-%m', timestamp) AS month_key
                FROM matches
                WHERE {RANKED_RESULT_STATUS_SQL}
                ORDER BY timestamp ASC, id ASC
                """,
            ).fetchall()

            ratings: dict[int, int] = {}
            monthly_points: dict[int, int] = {}

            for row in rows:
                winner_id = int(row["winner_id"])
                loser_id = int(row["loser_id"])
                winner_rating = ratings.get(winner_id, RANKING_START_RATING)
                loser_rating = ratings.get(loser_id, RANKING_START_RATING)

                elo_change = int(row["elo_change"] or calculate_elo_winner_delta(winner_rating, loser_rating))
                if str(row["month_key"] or "") == month_text:
                    monthly_points[winner_id] = monthly_points.get(winner_id, 0) + calculate_monthly_win_points(
                        elo_change,
                    )
                    monthly_points.setdefault(loser_id, 0)

                ratings[winner_id] = winner_rating + elo_change
                ratings[loser_id] = loser_rating - elo_change

            with self.connection:
                self.connection.execute("DELETE FROM monthly_points WHERE month = ?", (month_text,))
                for user_id, points in monthly_points.items():
                    self.connection.execute(
                        """
                        INSERT INTO monthly_points(user_id, month, points)
                        VALUES(?, ?, ?)
                        ON CONFLICT(user_id, month) DO UPDATE
                        SET points = excluded.points
                        """,
                        (user_id, month_text, points),
                    )

            return len(monthly_points)

    def _upsert_player(self, user_id: int) -> None:
        self.connection.execute(
            """
            INSERT INTO players(user_id, rating)
            VALUES(?, ?)
            ON CONFLICT(user_id) DO UPDATE
            SET rating = COALESCE(players.rating, excluded.rating)
            """,
            (user_id, RANKING_START_RATING),
        )

    def _upsert_monthly_player(self, user_id: int, month_text: str) -> None:
        self.connection.execute(
            """
            INSERT INTO monthly_points(user_id, month, points)
            VALUES(?, ?, 0)
            ON CONFLICT(user_id, month) DO NOTHING
            """,
            (user_id, month_text),
        )

    async def persist_ranked_match_result(
        self,
        *,
        match_id: int,
        queue_name: str,
        player_one_id: int,
        player_two_id: int,
        winner_id: int,
        score: tuple[int, int],
        player_one_average: str,
        player_two_average: str,
        month_key: date,
    ) -> tuple[bool, bool]:
        month_text = month_key_to_text(month_key)
        loser_id = player_two_id if winner_id == player_one_id else player_one_id

        async with self._lock:
            existing = self.connection.execute("SELECT status FROM matches WHERE id = ?", (match_id,)).fetchone()
            if existing is not None and existing["status"] == "confirmed":
                return True, True

            with self.connection:
                self._upsert_player(player_one_id)
                self._upsert_player(player_two_id)
                self._upsert_monthly_player(player_one_id, month_text)
                self._upsert_monthly_player(player_two_id, month_text)

                rows = self.connection.execute(
                    "SELECT user_id, rating FROM players WHERE user_id IN (?, ?)",
                    (player_one_id, player_two_id),
                ).fetchall()
                ratings = {int(row["user_id"]): int(row["rating"] or RANKING_START_RATING) for row in rows}

                winner_rating = ratings[winner_id]
                loser_rating = ratings[loser_id]
                elo_change = calculate_elo_winner_delta(winner_rating, loser_rating)
                monthly_points = calculate_monthly_win_points(elo_change)
                score_text = f"{score[0]}:{score[1]}"
                winner_average = player_one_average if winner_id == player_one_id else player_two_average
                loser_average = player_two_average if winner_id == player_one_id else player_one_average

                if existing is None:
                    self.connection.execute(
                        """
                        INSERT INTO matches(
                            id, player1_id, player2_id, winner_id, loser_id, platform,
                            status, score, winner_avg, loser_avg, timestamp, elo_change
                        )
                        VALUES(?, ?, ?, ?, ?, ?, 'confirmed', ?, ?, ?, ?, ?)
                        """,
                        (
                            match_id,
                            player_one_id,
                            player_two_id,
                            winner_id,
                            loser_id,
                            queue_name,
                            score_text,
                            float(winner_average),
                            float(loser_average),
                            utc_now(),
                            elo_change,
                        ),
                    )
                else:
                    self.connection.execute(
                        """
                        UPDATE matches
                        SET player1_id = ?,
                            player2_id = ?,
                            winner_id = ?,
                            loser_id = ?,
                            platform = ?,
                            status = 'confirmed',
                            score = ?,
                            winner_avg = ?,
                            loser_avg = ?,
                            timestamp = ?,
                            elo_change = ?
                        WHERE id = ?
                        """,
                        (
                            player_one_id,
                            player_two_id,
                            winner_id,
                            loser_id,
                            queue_name,
                            score_text,
                            float(winner_average),
                            float(loser_average),
                            utc_now(),
                            elo_change,
                            match_id,
                        ),
                    )

                self.connection.execute(
                    "UPDATE players SET rating = COALESCE(rating, ?) + ? WHERE user_id = ?",
                    (RANKING_START_RATING, elo_change, winner_id),
                )
                self.connection.execute(
                    "UPDATE players SET rating = COALESCE(rating, ?) - ? WHERE user_id = ?",
                    (RANKING_START_RATING, elo_change, loser_id),
                )
                self.connection.execute(
                    "UPDATE monthly_points SET points = COALESCE(points, 0) + ? WHERE user_id = ? AND month = ?",
                    (monthly_points, winner_id, month_text),
                )

            return True, False

    async def mark_match_result_published(self, match_id: int, channel_id: int, message_id: int) -> None:
        del match_id, channel_id, message_id

    async def fetch_world_ranking(self, limit: int | None) -> list[tuple[int, int, int, int]]:
        limit_sql = "" if limit is None else "LIMIT ?"
        params = (RANKING_START_RATING,) if limit is None else (RANKING_START_RATING, limit)
        async with self._lock:
            rows = self.connection.execute(
                f"""
                SELECT
                    p.user_id,
                    COALESCE(p.rating, ?) AS rating,
                    SUM(CASE WHEN m.winner_id = p.user_id THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN m.loser_id = p.user_id THEN 1 ELSE 0 END) AS losses
                FROM players p
                LEFT JOIN matches m
                    ON m.status IN ('completed', 'confirmed')
                   AND m.winner_id IS NOT NULL
                   AND m.loser_id IS NOT NULL
                   AND (m.winner_id = p.user_id OR m.loser_id = p.user_id)
                GROUP BY p.user_id, p.rating
                ORDER BY rating DESC, wins DESC, p.user_id ASC
                {limit_sql}
                """,
                params,
            ).fetchall()

        return [
            (int(row["user_id"]), int(row["rating"]), int(row["wins"] or 0), int(row["losses"] or 0))
            for row in rows
        ]

    async def fetch_monthly_ranking(self, month_key: date, limit: int | None) -> list[tuple[int, int, int, int]]:
        month_text = month_key_to_text(month_key)
        limit_sql = "" if limit is None else "LIMIT ?"
        async with self._lock:
            rows = self.connection.execute(
                f"""
                WITH monthly_stats AS (
                    SELECT
                        user_id,
                        SUM(wins) AS wins,
                        SUM(losses) AS losses,
                        SUM(elo_gain) AS elo_gain
                    FROM (
                        SELECT
                            winner_id AS user_id,
                            1 AS wins,
                            0 AS losses,
                            COALESCE(elo_change, 0) AS elo_gain
                        FROM matches
                        WHERE {RANKED_RESULT_STATUS_SQL}
                          AND strftime('%Y-%m', timestamp) = ?
                        UNION ALL
                        SELECT
                            loser_id AS user_id,
                            0 AS wins,
                            1 AS losses,
                            -COALESCE(elo_change, 0) AS elo_gain
                        FROM matches
                        WHERE {RANKED_RESULT_STATUS_SQL}
                          AND strftime('%Y-%m', timestamp) = ?
                    )
                    GROUP BY user_id
                )
                SELECT
                    mp.user_id AS user_id,
                    COALESCE(mp.points, 0) AS points,
                    COALESCE(ms.wins, 0) AS wins,
                    COALESCE(ms.losses, 0) AS losses,
                    COALESCE(ms.elo_gain, 0) AS elo_gain
                FROM monthly_points mp
                LEFT JOIN monthly_stats ms
                  ON ms.user_id = mp.user_id
                WHERE mp.month = ?
                ORDER BY
                    points DESC,
                    wins DESC,
                    CASE WHEN COALESCE(ms.wins, 0) + COALESCE(ms.losses, 0) = 0
                        THEN 0.0
                        ELSE CAST(COALESCE(ms.wins, 0) AS REAL) / (COALESCE(ms.wins, 0) + COALESCE(ms.losses, 0))
                    END DESC,
                    COALESCE(ms.wins, 0) + COALESCE(ms.losses, 0) DESC,
                    elo_gain DESC,
                    mp.user_id ASC
                {limit_sql}
                """,
                (month_text, month_text, month_text) if limit is None else (month_text, month_text, month_text, limit),
            ).fetchall()

        return [
            (int(row["user_id"]), int(row["points"]), int(row["wins"] or 0), int(row["losses"] or 0))
            for row in rows
        ]

    async def generate_monthly_ranking(
        self,
        month_key: date,
        limit: int | None,
    ) -> list[tuple[int, int, int, int]]:
        month_text = month_key_to_text(month_key)
        limit_sql = "" if limit is None else "LIMIT ?"
        params = (month_text, month_text) if limit is None else (month_text, month_text, limit)
        started_at = time.perf_counter()
        print(
            f"[monthly_ranking] DB query start month={month_text} "
            f"limit={limit or 'none'} bonus={MONTHLY_WIN_BONUS}"
        )
        async with self._lock:
            rows = self.connection.execute(
                f"""
                WITH monthly_stats AS (
                    SELECT
                        user_id,
                        SUM(points) AS points,
                        SUM(wins) AS wins,
                        SUM(losses) AS losses,
                        SUM(elo_gain) AS elo_gain
                    FROM (
                        SELECT
                            winner_id AS user_id,
                            COALESCE(elo_change, 0) + ? AS points,
                            1 AS wins,
                            0 AS losses,
                            COALESCE(elo_change, 0) AS elo_gain
                        FROM matches
                        WHERE {RANKED_RESULT_STATUS_SQL}
                          AND strftime('%Y-%m', timestamp) = ?
                        UNION ALL
                        SELECT
                            loser_id AS user_id,
                            0 AS points,
                            0 AS wins,
                            1 AS losses,
                            -COALESCE(elo_change, 0) AS elo_gain
                        FROM matches
                        WHERE {RANKED_RESULT_STATUS_SQL}
                          AND strftime('%Y-%m', timestamp) = ?
                    )
                    GROUP BY user_id
                )
                SELECT user_id, points, wins, losses
                FROM monthly_stats
                ORDER BY
                    points DESC,
                    wins DESC,
                    CASE WHEN wins + losses = 0
                        THEN 0.0
                        ELSE CAST(wins AS REAL) / (wins + losses)
                    END DESC,
                    wins + losses DESC,
                    elo_gain DESC,
                    user_id ASC
                {limit_sql}
                """,
                (MONTHLY_WIN_BONUS, *params),
            ).fetchall()

        result = [
            (int(row["user_id"]), int(row["points"] or 0), int(row["wins"] or 0), int(row["losses"] or 0))
            for row in rows
        ]
        duration = time.perf_counter() - started_at
        print(
            f"[monthly_ranking] DB query done month={month_text} "
            f"players={len(result)} limit={limit or 'none'} bonus={MONTHLY_WIN_BONUS} duration={duration:.3f}s"
        )
        return result

    async def fetch_match_history(self, player_id: int):
        async with self._lock:
            rows = self.connection.execute("""
                  SELECT player1_id, player2_id, winner_id, score, platform, elo_change
                  FROM matches
                  WHERE status = 'confirmed'
                    AND (player1_id = ? OR player2_id = ?)
                  ORDER BY id DESC LIMIT 10
                  """, (player_id, player_id))
            matches = rows.fetchall()
            if not matches:
                return []
            return matches
