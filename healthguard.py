import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from typing import List, Optional


VITAL_RANGES = {
    "heart_rate": (60, 100),
    "bp_systolic": (90, 140),
    "bp_diastolic": (60, 90),
    "temperature": (36.1, 37.8),
    "spo2": (95, 100),
    "resp_rate": (12, 20),
}

REQUIRED_COLS = ["patient_id", "heart_rate", "bp_systolic",
                 "bp_diastolic", "temperature", "spo2", "resp_rate"]


@dataclass
class Alert:
    patient_id: str
    timestamp: float
    vital: str
    value: float
    severity: str
    message: str


class HealthPipeline:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self):
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS vitals ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, patient_id TEXT, timestamp REAL,"
            "heart_rate REAL, bp_systolic REAL, bp_diastolic REAL,"
            "temperature REAL, spo2 REAL, resp_rate REAL)"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS alerts ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, patient_id TEXT, timestamp REAL,"
            "vital TEXT, value REAL, severity TEXT, message TEXT)"
        )
        self.conn.commit()

    def ingest(self, records: List[dict]) -> int:
        df = pd.DataFrame(records)
        for col in REQUIRED_COLS:
            if col not in df.columns:
                raise ValueError(f"Missing column: {col}")
        if "timestamp" not in df.columns:
            df["timestamp"] = time.time()
        df[REQUIRED_COLS + ["timestamp"]].to_sql(
            "vitals", self.conn, if_exists="append", index=False
        )
        return len(df)

    def ingest_csv(self, filepath: str) -> int:
        df = pd.read_csv(filepath)
        return self.ingest(df.to_dict("records"))

    def detect_anomalies(self, patient_id: Optional[str] = None) -> List[Alert]:
        query = "SELECT * FROM vitals"
        params = []
        if patient_id:
            query += " WHERE patient_id = ?"
            params = [patient_id]
        df = pd.read_sql(query, self.conn, params=params)
        if df.empty:
            return []
        alerts = []
        for vital, (low, high) in VITAL_RANGES.items():
            span = high - low
            for _, row in df.iterrows():
                val = row[vital]
                dev = max((low - val) / span, (val - high) / span, 0)
                if dev > 0:
                    severity = "critical" if dev > 0.5 else "warning"
                    alerts.append(Alert(
                        patient_id=row["patient_id"], timestamp=row["timestamp"],
                        vital=vital, value=round(val, 2), severity=severity,
                        message=f"{vital}={val:.1f} outside [{low},{high}]",
                    ))
        if alerts:
            pd.DataFrame([asdict(a) for a in alerts]).to_sql(
                "alerts", self.conn, if_exists="append", index=False
            )
        return alerts

    def get_patient_summary(self, patient_id: str) -> dict:
        df = pd.read_sql(
            "SELECT * FROM vitals WHERE patient_id = ?",
            self.conn, params=[patient_id],
        )
        if df.empty:
            return {"patient_id": patient_id, "records": 0}
        vitals = list(VITAL_RANGES.keys())
        stats = {}
        for v in vitals:
            stats[v] = {
                "mean": round(df[v].mean(), 2),
                "min": round(df[v].min(), 2),
                "max": round(df[v].max(), 2),
                "std": round(df[v].std(), 2) if len(df) > 1 else 0.0,
            }
        return {
            "patient_id": patient_id,
            "records": len(df),
            "time_range": [float(df["timestamp"].min()), float(df["timestamp"].max())],
            "vitals_stats": stats,
        }

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# Persistence Layer
# ---------------------------------------------------------------------------


class PatientRepository:
    """SQLite-backed CRUD repository for patient profiles."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        with self.conn:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS patients ("
                "patient_id TEXT PRIMARY KEY, "
                "name TEXT NOT NULL, "
                "age INTEGER NOT NULL, "
                "medical_history TEXT DEFAULT '', "
                "created_at REAL NOT NULL)"
            )

    @contextmanager
    def _cursor(self):
        """Context manager providing a cursor with automatic commit/rollback."""
        cur = self.conn.cursor()
        try:
            yield cur
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def create_patient(self, name: str, age: int, medical_history: str = "") -> str:
        """Insert a new patient and return the generated patient_id."""
        patient_id = f"P-{uuid.uuid4().hex[:8]}"
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO patients (patient_id, name, age, medical_history, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (patient_id, name, age, medical_history, time.time()),
            )
        return patient_id

    def get_patient(self, patient_id: str) -> Optional[dict]:
        """Return a patient dict or None if not found."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM patients WHERE patient_id = ?", (patient_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def list_patients(self, limit: int = 20, offset: int = 0) -> List[dict]:
        """Paginated listing of patients ordered by creation time descending."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM patients ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            )
            return [dict(r) for r in cur.fetchall()]

    def delete_patient(self, patient_id: str) -> bool:
        """Delete a patient. Returns True if a row was actually removed."""
        with self._cursor() as cur:
            cur.execute("DELETE FROM patients WHERE patient_id = ?", (patient_id,))
            return cur.rowcount > 0

    def close(self):
        self.conn.close()


class HealthRecordRepository:
    """SQLite-backed repository for health-metric time-series data."""

    TABLE = "health_records"

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        # Performance pragmas for bulk writes
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-64000")
        self._init_schema()

    def _init_schema(self):
        with self.conn:
            self.conn.execute(
                f"CREATE TABLE IF NOT EXISTS {self.TABLE} ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "patient_id TEXT NOT NULL, "
                "timestamp REAL NOT NULL, "
                "heart_rate REAL, "
                "bp_systolic REAL, "
                "bp_diastolic REAL, "
                "temperature REAL, "
                "spo2 REAL, "
                "resp_rate REAL)"
            )
            self.conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_hr_pid_ts "
                f"ON {self.TABLE}(patient_id, timestamp)"
            )

    @contextmanager
    def _cursor(self):
        """Context manager providing a cursor with automatic commit/rollback."""
        cur = self.conn.cursor()
        try:
            yield cur
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    # -- write operations ------------------------------------------------

    def insert_record(
        self, patient_id: str, metrics: dict, timestamp: Optional[float] = None
    ):
        """Insert a single health-metric record."""
        if timestamp is None:
            timestamp = time.time()
        cols = ["patient_id", "timestamp"] + list(metrics.keys())
        vals = [patient_id, timestamp] + list(metrics.values())
        placeholders = ", ".join(["?"] * len(cols))
        col_str = ", ".join(cols)
        with self._cursor() as cur:
            cur.execute(
                f"INSERT INTO {self.TABLE} ({col_str}) VALUES ({placeholders})", vals
            )

    def bulk_insert(self, patient_id: str, df: pd.DataFrame):
        """Bulk-insert a DataFrame of health metrics for a given patient.

        Uses pandas ``to_sql`` for high throughput (>= 10 k rows/sec).
        """
        df = df.copy()
        df["patient_id"] = patient_id
        if "timestamp" not in df.columns:
            df["timestamp"] = time.time()
        with self._cursor() as _cur:
            df.to_sql(self.TABLE, self.conn, if_exists="append", index=False)

    # -- read operations -------------------------------------------------

    def query_records(
        self, patient_id: str, start_time: float, end_time: float
    ) -> pd.DataFrame:
        """Return records for *patient_id* whose timestamp is in [start, end]."""
        with self._cursor() as _cur:
            return pd.read_sql_query(
                f"SELECT * FROM {self.TABLE} "
                "WHERE patient_id = ? AND timestamp >= ? AND timestamp <= ? "
                "ORDER BY timestamp ASC",
                self.conn,
                params=(patient_id, start_time, end_time),
            )

    def get_latest(self, patient_id: str, n: int = 10) -> pd.DataFrame:
        """Return the *n* most recent records for a patient (newest first)."""
        with self._cursor() as _cur:
            return pd.read_sql_query(
                f"SELECT * FROM {self.TABLE} "
                "WHERE patient_id = ? ORDER BY timestamp DESC LIMIT ?",
                self.conn,
                params=(patient_id, n),
            )

    def close(self):
        self.conn.close()
