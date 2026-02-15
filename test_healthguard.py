import csv
import time

import pytest

from healthguard import HealthPipeline


def make_record(patient_id="P001", **overrides):
    base = {
        "patient_id": patient_id,
        "timestamp": time.time(),
        "heart_rate": 75,
        "bp_systolic": 120,
        "bp_diastolic": 80,
        "temperature": 36.8,
        "spo2": 98,
        "resp_rate": 16,
    }
    base.update(overrides)
    return base


class TestIngest:
    def setup_method(self):
        self.pipe = HealthPipeline(":memory:")

    def teardown_method(self):
        self.pipe.close()

    def test_ingest_two_records(self):
        count = self.pipe.ingest([make_record(), make_record("P002")])
        assert count == 2

    def test_missing_column_raises(self):
        with pytest.raises(ValueError, match="Missing column"):
            self.pipe.ingest([{"patient_id": "P001"}])

    def test_ingest_csv(self, tmp_path):
        csv_file = tmp_path / "vitals.csv"
        with open(csv_file, "w", newline="") as f:
            rec = make_record()
            writer = csv.DictWriter(f, fieldnames=list(rec.keys()))
            writer.writeheader()
            writer.writerow(rec)
            writer.writerow(make_record("P002", heart_rate=50))
        count = self.pipe.ingest_csv(str(csv_file))
        assert count == 2


class TestAnomalyDetection:
    def setup_method(self):
        self.pipe = HealthPipeline(":memory:")

    def teardown_method(self):
        self.pipe.close()

    def test_normal_vitals_no_alerts(self):
        self.pipe.ingest([make_record()])
        alerts = self.pipe.detect_anomalies()
        assert len(alerts) == 0

    def test_high_heart_rate_warning(self):
        self.pipe.ingest([make_record(heart_rate=110)])
        alerts = self.pipe.detect_anomalies()
        hr = [a for a in alerts if a.vital == "heart_rate"]
        assert len(hr) == 1
        assert hr[0].severity == "warning"

    def test_critical_temperature(self):
        self.pipe.ingest([make_record(temperature=40.5)])
        alerts = self.pipe.detect_anomalies()
        temp = [a for a in alerts if a.vital == "temperature"]
        assert len(temp) == 1
        assert temp[0].severity == "critical"

    def test_low_spo2_critical(self):
        self.pipe.ingest([make_record(spo2=85)])
        alerts = self.pipe.detect_anomalies("P001")
        spo2 = [a for a in alerts if a.vital == "spo2"]
        assert len(spo2) == 1
        assert spo2[0].severity == "critical"


class TestReports:
    def setup_method(self):
        self.pipe = HealthPipeline(":memory:")

    def teardown_method(self):
        self.pipe.close()

    def test_summary_stats(self):
        self.pipe.ingest([
            make_record("P001", heart_rate=70),
            make_record("P001", heart_rate=80),
        ])
        s = self.pipe.get_patient_summary("P001")
        assert s["records"] == 2
        assert s["vitals_stats"]["heart_rate"]["mean"] == 75.0

    def test_empty_patient_summary(self):
        s = self.pipe.get_patient_summary("PXXX")
        assert s["records"] == 0
