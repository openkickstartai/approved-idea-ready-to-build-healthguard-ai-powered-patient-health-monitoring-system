import sqlite3
import time
from dataclasses import dataclass, asdict
from typing import List, Optional

import pandas as pd

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
