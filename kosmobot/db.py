from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator


@dataclass(slots=True)
class UserProfile:
    telegram_id: int
    username: str | None
    full_name: str
    participant_name: str | None
    is_admin: bool


class Database:
    def __init__(self, path: Path):
        self.path = path
        self._init_db()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id INTEGER PRIMARY KEY,
                    username TEXT,
                    full_name TEXT NOT NULL,
                    participant_name TEXT,
                    is_admin INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS lineups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_name TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    round_no INTEGER NOT NULL,
                    player_name TEXT NOT NULL,
                    position TEXT,
                    submitted_by INTEGER NOT NULL,
                    submitted_at TEXT NOT NULL,
                    UNIQUE(participant_name, stage, round_no, player_name)
                );

                CREATE TABLE IF NOT EXISTS scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_name TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    round_no INTEGER NOT NULL,
                    player_name TEXT NOT NULL,
                    position TEXT,
                    goals INTEGER NOT NULL DEFAULT 0,
                    assists INTEGER NOT NULL DEFAULT 0,
                    goals_allowed INTEGER,
                    shutout INTEGER NOT NULL DEFAULT 0,
                    manual_adj REAL NOT NULL DEFAULT 0,
                    counted INTEGER NOT NULL DEFAULT 1,
                    points REAL NOT NULL DEFAULT 0,
                    created_by INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(participant_name, stage, round_no, player_name)
                );

                CREATE TABLE IF NOT EXISTS transfers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_name TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    round_no INTEGER NOT NULL,
                    quota_kind TEXT NOT NULL,
                    player_out TEXT,
                    player_in TEXT,
                    created_by INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )

    def upsert_user(self, profile: UserProfile) -> None:
        now = datetime.utcnow().isoformat()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO users (telegram_id, username, full_name, participant_name, is_admin, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    username=excluded.username,
                    full_name=excluded.full_name,
                    participant_name=COALESCE(users.participant_name, excluded.participant_name),
                    is_admin=excluded.is_admin,
                    updated_at=excluded.updated_at
                """,
                (
                    profile.telegram_id,
                    profile.username,
                    profile.full_name,
                    profile.participant_name,
                    1 if profile.is_admin else 0,
                    now,
                    now,
                ),
            )

    def list_users(self):
        with self.connect() as conn:
            return conn.execute("SELECT * FROM users ORDER BY participant_name, full_name").fetchall()

    def set_participant(self, telegram_id: int, participant_name: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE users SET participant_name=?, updated_at=? WHERE telegram_id=?",
                (participant_name, datetime.utcnow().isoformat(), telegram_id),
            )

    def get_user(self, telegram_id: int):
        with self.connect() as conn:
            return conn.execute("SELECT * FROM users WHERE telegram_id=?", (telegram_id,)).fetchone()

    def get_state(self, key: str, default: str | None = None) -> str | None:
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
            return row[0] if row else default

    def set_state(self, key: str, value: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO state (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def replace_lineup(self, participant_name: str, stage: str, round_no: int, selections: list[tuple[str, str | None]], submitted_by: int) -> None:
        now = datetime.utcnow().isoformat()
        with self.connect() as conn:
            conn.execute(
                "DELETE FROM lineups WHERE participant_name=? AND stage=? AND round_no=?",
                (participant_name, stage, round_no),
            )
            conn.executemany(
                """
                INSERT INTO lineups (participant_name, stage, round_no, player_name, position, submitted_by, submitted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [(participant_name, stage, round_no, name, pos, submitted_by, now) for name, pos in selections],
            )

    def get_lineup(self, participant_name: str, stage: str, round_no: int):
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM lineups WHERE participant_name=? AND stage=? AND round_no=? ORDER BY position, player_name",
                (participant_name, stage, round_no),
            ).fetchall()

    def get_effective_lineup(self, participant_name: str, stage: str, round_no: int):
        with self.connect() as conn:
            latest = conn.execute(
                """
                SELECT round_no, MAX(submitted_at) AS submitted_at
                FROM lineups
                WHERE participant_name=? AND stage=? AND round_no<=?
                GROUP BY round_no
                ORDER BY round_no DESC, submitted_at DESC
                LIMIT 1
                """,
                (participant_name, stage, round_no),
            ).fetchone()
            if not latest:
                return []
            return conn.execute(
                "SELECT * FROM lineups WHERE participant_name=? AND stage=? AND round_no=? ORDER BY position, player_name",
                (participant_name, stage, latest["round_no"]),
            ).fetchall()

    def get_latest_lineup_before(self, participant_name: str, stage: str, round_no: int):
        with self.connect() as conn:
            latest = conn.execute(
                """
                SELECT stage, round_no, MAX(submitted_at) AS submitted_at
                FROM lineups
                WHERE participant_name=?
                  AND NOT (stage=? AND round_no=?)
                GROUP BY stage, round_no
                ORDER BY submitted_at DESC
                LIMIT 1
                """,
                (participant_name, stage, round_no),
            ).fetchone()
            if not latest:
                return []
            return conn.execute(
                "SELECT * FROM lineups WHERE participant_name=? AND stage=? AND round_no=? ORDER BY position, player_name",
                (participant_name, latest["stage"], latest["round_no"]),
            ).fetchall()

    def has_stage_lineup(self, participant_name: str, stage: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM lineups WHERE participant_name=? AND stage=? LIMIT 1",
                (participant_name, stage),
            ).fetchone()
            return bool(row)

    def all_lineups(self):
        with self.connect() as conn:
            return conn.execute("SELECT * FROM lineups ORDER BY stage, round_no, participant_name, player_name").fetchall()

    def upsert_score(self, participant_name: str, stage: str, round_no: int, player_name: str, position: str | None,
                     goals: int, assists: int, goals_allowed: int | None, shutout: bool, manual_adj: float,
                     counted: bool, points: float, created_by: int) -> None:
        now = datetime.utcnow().isoformat()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO scores (participant_name, stage, round_no, player_name, position, goals, assists, goals_allowed, shutout,
                                    manual_adj, counted, points, created_by, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(participant_name, stage, round_no, player_name) DO UPDATE SET
                    position=excluded.position,
                    goals=excluded.goals,
                    assists=excluded.assists,
                    goals_allowed=excluded.goals_allowed,
                    shutout=excluded.shutout,
                    manual_adj=excluded.manual_adj,
                    counted=excluded.counted,
                    points=excluded.points,
                    updated_at=excluded.updated_at,
                    created_by=excluded.created_by
                """,
                (participant_name, stage, round_no, player_name, position, goals, assists, goals_allowed,
                 1 if shutout else 0, manual_adj, 1 if counted else 0, points, created_by, now, now),
            )

    def get_score(self, participant_name: str, stage: str, round_no: int, player_name: str):
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM scores WHERE participant_name=? AND stage=? AND round_no=? AND player_name=?",
                (participant_name, stage, round_no, player_name),
            ).fetchone()

    def get_scores(self, participant_name: str | None = None, stage: str | None = None, round_no: int | None = None):
        query = "SELECT * FROM scores WHERE 1=1"
        params: list[object] = []
        if participant_name is not None:
            query += " AND participant_name=?"
            params.append(participant_name)
        if stage is not None:
            query += " AND stage=?"
            params.append(stage)
        if round_no is not None:
            query += " AND round_no=?"
            params.append(round_no)
        query += " ORDER BY stage, round_no, participant_name, player_name"
        with self.connect() as conn:
            return conn.execute(query, params).fetchall()

    def leaderboard(self):
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT participant_name,
                       ROUND(SUM(CASE WHEN counted=1 THEN points ELSE 0 END), 2) AS total_points,
                       COUNT(*) AS entries
                FROM scores
                GROUP BY participant_name
                ORDER BY total_points DESC, participant_name ASC
                """
            ).fetchall()

    def replace_transfers(self, participant_name: str, stage: str, round_no: int,
                          transfers: list[tuple[str | None, str | None]], created_by: int, quota_kind: str) -> None:
        now = datetime.utcnow().isoformat()
        with self.connect() as conn:
            conn.execute(
                "DELETE FROM transfers WHERE participant_name=? AND stage=? AND round_no=?",
                (participant_name, stage, round_no),
            )
            if transfers:
                conn.executemany(
                    """
                    INSERT INTO transfers (participant_name, stage, round_no, quota_kind, player_out, player_in, created_by, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [(participant_name, stage, round_no, quota_kind, out_name, in_name, created_by, now) for out_name, in_name in transfers],
                )

    def count_regular_transfers_used(self, participant_name: str, exclude_stage: str | None = None, exclude_round: int | None = None) -> int:
        query = "SELECT COUNT(*) FROM transfers WHERE participant_name=? AND quota_kind='regular'"
        params: list[object] = [participant_name]
        if exclude_stage is not None and exclude_round is not None:
            query += " AND NOT (stage=? AND round_no=?)"
            params.extend([exclude_stage, exclude_round])
        with self.connect() as conn:
            return int(conn.execute(query, params).fetchone()[0])

    def get_transfers(self, participant_name: str | None = None, stage: str | None = None, round_no: int | None = None):
        query = "SELECT * FROM transfers WHERE 1=1"
        params: list[object] = []
        if participant_name is not None:
            query += " AND participant_name=?"
            params.append(participant_name)
        if stage is not None:
            query += " AND stage=?"
            params.append(stage)
        if round_no is not None:
            query += " AND round_no=?"
            params.append(round_no)
        query += " ORDER BY created_at, id"
        with self.connect() as conn:
            return conn.execute(query, params).fetchall()
