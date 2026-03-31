from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = PROJECT_ROOT / "data" / "reports"
REJECTED_DIR = PROJECT_ROOT / "data" / "rejected"


def ensure_output_dirs() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)


def write_validation_report(results: list[dict]) -> Path:
    ensure_output_dirs()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = REPORTS_DIR / f"validation_report_{timestamp}.csv"
    txt_path = REPORTS_DIR / f"validation_report_{timestamp}.txt"

    df = pd.DataFrame(results)
    df.to_csv(csv_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as file:
        file.write("Validation Report\n")
        file.write("=================\n")
        for row in results:
            file.write(f"[{row['status']}] {row['check_name']}: {row['details']}\n")

    return csv_path


def write_reconciliation_report(results: list[dict]) -> Path:
    ensure_output_dirs()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = REPORTS_DIR / f"reconciliation_report_{timestamp}.csv"
    txt_path = REPORTS_DIR / f"reconciliation_report_{timestamp}.txt"

    df = pd.DataFrame(results)
    df.to_csv(csv_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as file:
        file.write("Reconciliation Report\n")
        file.write("=====================\n")
        for row in results:
            file.write(
                f"[{row['status']}] {row['source_dataset']} -> {row['target_table']}: "
                f"source={row['source_count']}, clean={row['clean_count']}, target={row['target_count']}\n"
            )

    return csv_path


def write_rejected_outputs(rejected_tables: dict[str, pd.DataFrame]) -> list[Path]:
    ensure_output_dirs()
    written_files: list[Path] = []

    for rejected_name, df in rejected_tables.items():
        output_path = REJECTED_DIR / f"{rejected_name}.csv"
        df.to_csv(output_path, index=False)
        written_files.append(output_path)

    return written_files
