"""serving/prediction_store.py – Phase 10 Prediction Storage.

Stores every prediction made by the serving API so they can be joined with
realized prices later to compute rolling online accuracy.

Schema (matches the Phase 10 spec table):

    | timestamp | symbol | current_price | predicted_price | model_version | realized_error |

The ``realized_error`` column is initially NULL and is backfilled by
``backfill_realized_errors()`` once the actual future price is known.

Implementation note: SQLite is used here for the starter implementation —
no external database server required.  In production this would be
PostgreSQL / TimescaleDB (same schema, same queries).
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, Optional


@dataclass
class PredictionRecord:
    """A single stored prediction row."""

    id: int
    timestamp: str
    symbol: str
    current_price: float
    predicted_price: float
    predicted_return: float
    model_version: str
    realized_error: Optional[float]


_SCHEMA = """
CREATE TABLE IF NOT EXISTS predictions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       TEXT    NOT NULL,
    symbol          TEXT    NOT NULL,
    current_price   REAL    NOT NULL,
    predicted_price REAL    NOT NULL,
    predicted_return REAL   NOT NULL,
    model_version   TEXT    NOT NULL,
    realized_error  REAL,
    realized_return_error REAL
);

CREATE INDEX IF NOT EXISTS idx_predictions_symbol_ts
    ON predictions(symbol, timestamp);
"""


class PredictionStore:
    """SQLite-backed prediction storage.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.  Use ``:memory:`` for an ephemeral
        in-memory database (useful for tests).
    """

    def __init__(self, db_path: str = "predictions.db"):
        self.db_path = db_path
        # Use a single persistent connection so :memory: databases survive
        # across operations (each new sqlite3.connect(":memory:") creates a
        # fresh empty DB).  For file-based databases this is also fine —
        # SQLite handles concurrency via file locking.
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        try:
            self._conn.execute("ALTER TABLE predictions ADD COLUMN realized_return_error REAL")
        except sqlite3.OperationalError:
            pass  # column already exists
        self._conn.commit()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------
    def save_prediction(
        self,
        timestamp: str,
        symbol: str,
        current_price: float,
        predicted_price: float,
        predicted_return: float,
        model_version: str,
    ) -> int:
        """Insert a prediction row and return its auto-incremented ID."""
        cursor = self._conn.execute(
            """
            INSERT INTO predictions
                (timestamp, symbol, current_price, predicted_price,
                 predicted_return, model_version, realized_error)
            VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (timestamp, symbol, current_price, predicted_price,
             predicted_return, model_version),
        )
        self._conn.commit()
        return cursor.lastrowid

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------
    def get_prediction(self, prediction_id: int) -> Optional[PredictionRecord]:
        """Fetch a single prediction by ID."""
        row = self._conn.execute(
            "SELECT * FROM predictions WHERE id = ?", (prediction_id,)
        ).fetchone()
        return _row_to_record(row) if row else None

    def get_recent_predictions(self, symbol: str, limit: int = 10) -> list[PredictionRecord]:
        """Return the most recent predictions for a symbol."""
        rows = self._conn.execute(
            """
            SELECT * FROM predictions
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (symbol, limit),
        ).fetchall()
        return [_row_to_record(r) for r in rows]

    def get_unrealized_predictions(self, symbol: Optional[str] = None) -> list[PredictionRecord]:
        """Return predictions whose ``realized_error`` is still NULL."""
        if symbol:
            rows = self._conn.execute(
                "SELECT * FROM predictions WHERE realized_error IS NULL AND symbol = ?",
                (symbol,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM predictions WHERE realized_error IS NULL"
            ).fetchall()
        return [_row_to_record(r) for r in rows]

    def get_oldest_unrealized_prediction(self, symbol: str) -> Optional[PredictionRecord]:
        """Return the single oldest unrealized prediction for a symbol, or None.

        Used to match each prediction to the *next* observed price for that
        symbol, in strict chronological (FIFO) order — one tick realizes at
        most one prediction, so predictions and their realizations stay
        correctly paired even under bursty traffic.
        """
        row = self._conn.execute(
            """
            SELECT * FROM predictions
            WHERE symbol = ? AND realized_error IS NULL
            ORDER BY timestamp ASC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
        return _row_to_record(row) if row else None

    def realize_prediction(self, prediction_id: int, actual_price: float) -> None:
        """Backfill realized_error (price-space) and realized_return_error
        (return-space, scale-invariant) for one specific prediction by ID."""
        row = self._conn.execute(
            "SELECT current_price, predicted_return FROM predictions WHERE id = ?",
            (prediction_id,),
        ).fetchone()
        if row is None:
            return
        prev_current_price, predicted_return = row

        actual_return = (actual_price - prev_current_price) / prev_current_price
        return_error = actual_return - predicted_return

        self._conn.execute(
            """
            UPDATE predictions
            SET realized_error = ? - predicted_price,
                realized_return_error = ?
            WHERE id = ? AND realized_error IS NULL
            """,
            (actual_price, return_error, prediction_id),
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Backfill realized errors
    # ------------------------------------------------------------------
    def backfill_realized_errors(
        self,
        realized_prices: dict[str, float],
    ) -> int:
        """Backfill ``realized_error`` for predictions whose actual price is now known.

        Parameters
        ----------
        realized_prices:
            A mapping of ``timestamp → actual_price`` for the predictions that
            have matured (i.e. the next tick has arrived).  The timestamp must
            match the prediction's ``timestamp`` column exactly.

        Returns
        -------
        int
            The number of rows updated.
        """
        if not realized_prices:
            return 0

        updated = 0
        for ts, actual_price in realized_prices.items():
            cursor = self._conn.execute(
                """
                UPDATE predictions
                SET realized_error = ? - predicted_price
                WHERE timestamp = ? AND realized_error IS NULL
                """,
                (actual_price, ts),
            )
            updated += cursor.rowcount
        self._conn.commit()
        return updated

    # ------------------------------------------------------------------
    # Rolling online accuracy
    # ------------------------------------------------------------------
    def compute_rolling_rmse(self, symbol: Optional[str] = None) -> Optional[float]:
        """Compute the RMSE over all predictions with a realized error.

        Returns ``None`` if no predictions have been realized yet.
        """
        if symbol:
            row = self._conn.execute(
                """
                SELECT COUNT(*) as n, SUM(realized_error * realized_error) as sse
                FROM predictions
                WHERE realized_error IS NOT NULL AND symbol = ?
                """,
                (symbol,),
            ).fetchone()
        else:
            row = self._conn.execute(
                """
                SELECT COUNT(*) as n, SUM(realized_error * realized_error) as sse
                FROM predictions
                WHERE realized_error IS NOT NULL
                """
            ).fetchone()

        n = row["n"]
        if n == 0:
            return None
        return (row["sse"] / n) ** 0.5

    def compute_rolling_mae(self, symbol: Optional[str] = None) -> Optional[float]:
        """Compute the MAE over all predictions with a realized error.

        Returns ``None`` if no predictions have been realized yet.
        """
        if symbol:
            row = self._conn.execute(
                """
                SELECT COUNT(*) as n, SUM(ABS(realized_error)) as sae
                FROM predictions
                WHERE realized_error IS NOT NULL AND symbol = ?
                """,
                (symbol,),
            ).fetchone()
        else:
            row = self._conn.execute(
                """
                SELECT COUNT(*) as n, SUM(ABS(realized_error)) as sae
                FROM predictions
                WHERE realized_error IS NOT NULL
                """
            ).fetchone()

        n = row["n"]
        if n == 0:
            return None
        return row["sae"] / n


    def compute_rolling_rmse_return(self, symbol: Optional[str] = None) -> Optional[float]:
        """RMSE of realized_return_error — scale-invariant, comparable across symbols."""
        query = "SELECT COUNT(*) as n, SUM(realized_return_error * realized_return_error) as sse FROM predictions WHERE realized_return_error IS NOT NULL"
        params = ()
        if symbol is not None:
            query += " AND symbol = ?"
            params = (symbol,)
        row = self._conn.execute(query, params).fetchone()
        if row["n"] == 0:
            return None
        return (row["sse"] / row["n"]) ** 0.5


def _row_to_record(row: sqlite3.Row) -> PredictionRecord:
    return PredictionRecord(
        id=row["id"],
        timestamp=row["timestamp"],
        symbol=row["symbol"],
        current_price=row["current_price"],
        predicted_price=row["predicted_price"],
        predicted_return=row["predicted_return"],
        model_version=row["model_version"],
        realized_error=row["realized_error"],
    )