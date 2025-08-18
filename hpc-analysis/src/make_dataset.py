#!/usr/bin/env python3
"""
ETL for SLURM job logs → cleaned Parquet.

Usage:
    python make_dataset.py \
        --raw-file data/raw/JOBS_2021_2025.csv \
        --out-file data/processed/jobs_clean.parquet
"""

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from clean_jobs import parse_hms_or_dhms, parse_reqmem

# required columns from `sacct -P` export
REQUIRED_COLS = [
    "JobID","JobName","UID","Partition","Account",
    "Submit","Start","End","Elapsed","UserCPU",
    "SystemCPU","TotalCPU","CPUTime","NCPUS",
    "ReqMem","AveRSS","MaxRSS","AveDiskRead",
    "MaxDiskRead","AveDiskWrite","MaxDiskWrite",
    "AvePages","MaxPages","State","ExitCode"
]

# We'll try to handle alternate spellings/variants
COLUMN_ALIASES = {
    "NNODES": ["NNODES", "NNodes", "Nnodes", "nnodes"],
    "NTASKS": ["NTASKS", "NTasks", "Ntasks", "ntasks"],
    "TimeLimit": ["TimeLimit", "TIMELIMIT", "timelimit", "time_limit"]
}


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

def parse_args():
    p = argparse.ArgumentParser(description="Clean SLURM logs to Parquet")
    p.add_argument("--raw-file",  type=Path, required=True, help="Path to raw CSV")
    p.add_argument("--out-file",  type=Path, required=True, help="Parquet output path")
    return p.parse_args()

def find_col(df, aliases):
    for a in aliases:
        if a in df.columns:
            return a
    return None

def main():
    setup_logging()
    args = parse_args()
    raw_csv = args.raw_file
    out_parquet = args.out_file

    logging.info(f"Reading raw data from {raw_csv}")
    df = pd.read_csv(raw_csv, sep="|", encoding="latin1", low_memory=False)

    # --- Map in possible variants for missing columns (aliases) ---
    for canonical, variants in COLUMN_ALIASES.items():
        actual = find_col(df, variants)
        if actual and actual != canonical:
            df[canonical] = df[actual]
        elif not actual:
            df[canonical] = np.nan  # add as empty

    # Now check for truly missing required columns
    missing = set(REQUIRED_COLS + list(COLUMN_ALIASES.keys())) - set(df.columns)
    if missing:
        logging.error(f"Missing columns in raw data: {missing}")
        raise RuntimeError("Raw file schema mismatch")
    # --- parse timestamps -------------------------------------------------
    for col in ("Submit", "Start", "End"):
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # --- compute wait time ------------------------------------------------
    df["wait_time_sec"] = (df["Start"] - df["Submit"]).dt.total_seconds()

    # --- durations / CPUTime ----------------------------------------------
    df["Elapsed_sec"] = df["Elapsed"].apply(parse_hms_or_dhms)
    df["CPUTime_sec"] = df["CPUTime"].apply(parse_hms_or_dhms)

    # --- memory -----------------------------------------------------------
    df["ReqMem_MB"] = df.apply(
        lambda r: parse_reqmem(
            r["ReqMem"], r.get("NNODES", np.nan), r.get("NCPUS", np.nan)
        ), axis=1
    )

    # --- derived metrics -------------------------------------------------
    df["core_seconds"] = df["Elapsed_sec"] * df["NCPUS"].fillna(0)
    df.loc[df["core_seconds"] == 0, "core_seconds"] = np.nan  # avoid /0
    df["efficiency"]   = df["CPUTime_sec"] / df["core_seconds"]

    # --- date parts for filtering/grouping -------------------------------
    df["year"]  = df["Start"].dt.year
    df["month"] = df["Start"].dt.month

    # ensure output directory exists
    out_parquet.parent.mkdir(parents=True, exist_ok=True)

    logging.info(f"Saving cleaned data ({len(df):,} rows) to {out_parquet}")
    df.to_parquet(out_parquet, index=False)

    logging.info("✅ Done.")

if __name__ == "__main__":
    main()
