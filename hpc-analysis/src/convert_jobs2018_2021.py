import pandas as pd
import numpy as np
import re
from pathlib import Path

# Column mappings and state translation based on your supervisor's email
STATE_MAP = {
    0: "PENDING",
    1: "RUNNING",
    2: "SUSPENDED",
    3: "COMPLETED",
    4: "CANCELLED",
    5: "FAILED",
    6: "TIMEOUT",
    7: "NODE_FAIL",
    8: "PREEMPTED",
    9: "BOOT_FAIL"
}

def parse_tres_alloc(tres_str):
    """Parses tres_alloc column to extract NCPUS and NNODES."""
    if pd.isna(tres_str) or tres_str == "":
        return np.nan, np.nan
    ncpus = np.nan
    nnodes = np.nan
    # Format: "1=16,4=3" where 1=cpu, 4=node
    for entry in str(tres_str).split(","):
        if "=" in entry:
            k, v = entry.split("=")
            if k == "1": ncpus = int(v)
            if k == "4": nnodes = int(v)
    return ncpus, nnodes

def main():
    # Adjust path as needed!
    IN_FILE = Path("data/raw/jobs_table_2018-2021.csv")
    OUT_FILE = Path("data/processed/jobs_2018_2021_clean.parquet")
    
    # If your file is XLSX, use read_excel. For real data, use read_csv!
    df = pd.read_excel(IN_FILE) if IN_FILE.suffix == ".xlsx" else pd.read_csv(IN_FILE)
    
    # Rename columns to match your new/clean jobs table
    df = df.rename(columns={
        "id_job": "JobID",
        "job_name": "JobName",
        "id_user": "UID",
        "partition": "Partition",
        "account": "Account",
        "from_unixtime(time_submit)": "Submit",
        "from_unixtime(time_start)": "Start",
        "from_unixtime(time_end)": "End",
        "timelimit": "TimeLimit",
        "state": "State",
        "exit_code": "ExitCode"
        # "tres_alloc": handled below
    })

    # Parse datetimes (if not already done)
    for col in ("Submit", "Start", "End"):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    
    # Parse NCPUS/NNODES from tres_alloc
    df[["NCPUS", "NNODES"]] = df["tres_alloc"].apply(lambda s: pd.Series(parse_tres_alloc(s)))
    
    # Map state codes to labels
    df["State"] = df["State"].map(STATE_MAP)
    
    # Elapsed time in seconds (if End/Start present)
    df["Elapsed_sec"] = (df["End"] - df["Start"]).dt.total_seconds()
    
    # Your unified cleaned jobs expects Elapsed as SLURM-style string (HH:MM:SS or D-HH:MM:SS)
    def seconds_to_slurm_str(secs):
        if pd.isna(secs):
            return ""
        secs = int(secs)
        days, rem = divmod(secs, 86400)
        h, rem = divmod(rem, 3600)
        m, s = divmod(rem, 60)
        if days > 0:
            return f"{days}-{h:02}:{m:02}:{s:02}"
        else:
            return f"{h:02}:{m:02}:{s:02}"
    df["Elapsed"] = df["Elapsed_sec"].apply(seconds_to_slurm_str)
    
    # CPUTime, ReqMem, etc. not available: fill as NaN or blank
    df["CPUTime"] = np.nan
    df["ReqMem"] = np.nan
    
    # Fill in missing columns for compatibility (even if all NaN)
    for col in [
        "UserCPU", "SystemCPU", "TotalCPU", "NTASKS", "AveRSS", "MaxRSS",
        "AveDiskRead", "MaxDiskRead", "AveDiskWrite", "MaxDiskWrite",
        "AvePages", "MaxPages"
    ]:
        if col not in df:
            df[col] = np.nan
    
    # Reorder columns to match your 2021-2025 jobs table
    FINAL_COLS = [
        "JobID","JobName","UID","Partition","Account",
        "Submit","Start","End","TimeLimit","Elapsed",
        "UserCPU","SystemCPU","TotalCPU","CPUTime",
        "NCPUS","NTASKS","NNODES","ReqMem",
        "AveRSS","MaxRSS","AveDiskRead","MaxDiskRead",
        "AveDiskWrite","MaxDiskWrite","AvePages","MaxPages",
        "State","ExitCode"
    ]
    df = df[FINAL_COLS]
    
    OUT_FILE.parent.mkdir(exist_ok=True, parents=True)
    df.to_parquet(OUT_FILE, index=False)
    print(f"✅ Saved {len(df):,} rows → {OUT_FILE}")

if __name__ == "__main__":
    main()
