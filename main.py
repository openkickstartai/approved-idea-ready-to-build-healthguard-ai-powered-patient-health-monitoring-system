import argparse
import json
import sys

from healthguard import HealthPipeline


def cmd_ingest(args):
    pipe = HealthPipeline(args.db)
    count = pipe.ingest_csv(args.file)
    print(f"Ingested {count} records from {args.file}")
    alerts = pipe.detect_anomalies()
    if alerts:
        print(f"\u26a0 {len(alerts)} anomalies detected!")
        for a in alerts:
            icon = "\U0001f534" if a.severity == "critical" else "\U0001f7e1"
            print(f"  {icon} [{a.patient_id}] {a.message}")
    else:
        print("\u2705 All vitals normal.")
    pipe.close()


def cmd_monitor(args):
    pipe = HealthPipeline(args.db)
    alerts = pipe.detect_anomalies(args.patient)
    if not alerts:
        print("\u2705 All vitals within normal range.")
    else:
        for a in alerts:
            icon = "\U0001f534" if a.severity == "critical" else "\U0001f7e1"
            print(f"{icon} [{a.patient_id}] {a.severity.upper()}: {a.message}")
    pipe.close()


def cmd_report(args):
    pipe = HealthPipeline(args.db)
    summary = pipe.get_patient_summary(args.patient)
    print(json.dumps(summary, indent=2))
    pipe.close()


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="healthguard", description="AI-Powered Patient Health Monitoring"
    )
    parser.add_argument("--db", default="healthguard.db", help="SQLite database path")
    sub = parser.add_subparsers(dest="command")

    p_ingest = sub.add_parser("ingest", help="Ingest vitals from CSV")
    p_ingest.add_argument("file", help="CSV file path")

    p_report = sub.add_parser("report", help="Patient summary report")
    p_report.add_argument("patient", help="Patient ID")

    p_monitor = sub.add_parser("monitor", help="Check for anomalies")
    p_monitor.add_argument("--patient", default=None, help="Filter by patient")

    args = parser.parse_args(argv)
    commands = {"ingest": cmd_ingest, "report": cmd_report, "monitor": cmd_monitor}
    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
