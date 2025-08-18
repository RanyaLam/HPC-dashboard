import pandas as pd
from pathlib import Path

JOBS_1 = Path("data/processed/jobs_clean.parquet")
JOBS_2 = Path("data/processed/jobs_2018_2021_clean.parquet")
OUT = Path("data/processed/jobs_all.parquet")

df1 = pd.read_parquet(JOBS_1)
df2 = pd.read_parquet(JOBS_2)

# Standardize columns likely to cause issues as string
cols_to_str = ["JobID", "UID", "Partition", "Account", "ExitCode", "State"]
for col in cols_to_str:
    if col in df1.columns:
        df1[col] = df1[col].astype(str)
    if col in df2.columns:
        df2[col] = df2[col].astype(str)

df_all = pd.concat([df1, df2], ignore_index=True)
df_all = df_all.sort_values("Start")  # Sort by job start time

OUT.parent.mkdir(parents=True, exist_ok=True)
df_all.to_parquet(OUT, index=False)
print(f"✅ Merged {len(df_all):,} rows → {OUT}")
