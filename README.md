# 🏥 HealthGuard — AI-Powered Patient Health Monitoring System

A streaming data pipeline for ingesting patient vitals, detecting anomalies in real-time, and generating clinical summary reports. Built with pandas + SQLite for lightweight, high-throughput health data processing.

## Features

- **Vital Sign Ingestion** — CSV or programmatic input with schema validation
- **Anomaly Detection** — Threshold-deviation scoring across 6 vital dimensions
- **Patient Reports** — Rolling statistics (mean/min/max/std) per patient
- **CLI Interface** — `ingest`, `monitor`, `report` commands
- **Persistent Storage** — SQLite-backed for durability

## Monitored Vitals & Normal Ranges

| Vital | Low | High |
|-------|-----|------|
| Heart Rate | 60 | 100 bpm |
| BP Systolic | 90 | 140 mmHg |
| BP Diastolic | 60 | 90 mmHg |
| Temperature | 36.1 | 37.8 °C |
| SpO2 | 95 | 100 % |
| Resp Rate | 12 | 20 br/min |

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Ingest vitals from CSV

```bash
python main.py ingest vitals.csv
```

CSV format:
```csv
patient_id,heart_rate,bp_systolic,bp_diastolic,temperature,spo2,resp_rate
P001,75,120,80,36.8,98,16
```

### Monitor for anomalies

```bash
python main.py monitor
python main.py monitor --patient P001
```

### Patient summary report

```bash
python main.py report P001
```

## Run Tests

```bash
pytest test_healthguard.py -v
```

## License

MIT
