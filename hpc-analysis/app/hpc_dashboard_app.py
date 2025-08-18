import streamlit as st
import pandas as pd
import numpy as np
import mysql.connector
from pathlib import Path

DATA_PATH = Path(__file__).parent.parent / "data/processed/jobs_all.parquet" 
df = pd.read_parquet(DATA_PATH)

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="hpc_stage",
)
df_user_meta = pd.read_sql("SELECT * FROM v_user_apps", conn)
conn.close()

df_user_meta.rename(columns={
    "id_utilisateur": "UID",
    "concat(des_etablissement,' , ',lib_ville)": "institution_city_meta"
}, inplace=True)

df["UID"] = df["UID"].astype(str)
df_user_meta["UID"] = df_user_meta["UID"].astype(str)

df_merged = df.merge(df_user_meta, on="UID", how="left")

df_user_meta.rename(columns={
    "id_utilisateur": "UID",
    "concat(des_etablissement,' , ',lib_ville)": "institution_city_meta"
}, inplace=True)

df_merged["institution_city"] = df_merged["institution_city_meta"].fillna("")

import re

def normalize_state(state):
    # If it matches CANCELLED by XXXX, return 'CANCELLED'
    if isinstance(state, str) and re.match(r"CANCELLED by \d+", state):
        return "CANCELLED"
    return state

df["State_Clean"] = df["State"].apply(normalize_state)

def main_partition(part):
    if isinstance(part, str):
        return part.split(",")[0].strip()
    return part

df["Partition_Main"] = df["Partition"].apply(main_partition)

def group_jobname(name):
    if isinstance(name, str):
        name_lower = name.lower()
        if name_lower.startswith("jupyter"):
            return "jupyter"
        elif name_lower.startswith("bash"):
            return "bash"
        elif name_lower.startswith("test"):
            return "test"
        elif name_lower.startswith("qe"):
            return "qe"
        elif name_lower in {"stream", "linpack", "osu", "iozone"}:
            return name_lower  # keep known benchmarks as-is
        elif len(name) < 4:
            return "short_code"
        else:
            return "other"
    return "unknown"

df["JobName_Grouped"] = df["JobName"].apply(group_jobname)

# --- NEW: Streamlit sidebar filters ---
st.sidebar.header("Filter Jobs")

# Date range filter (use min/max from your data)
date_min = df["Start"].min()
date_max = df["End"].max()
date_range = st.sidebar.date_input(
    "Job Start Date Range", [date_min, date_max],
    min_value=date_min, max_value=date_max
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    # Handle the case where only a single date is returned
    start_date = end_date = date_range if isinstance(date_range, (pd.Timestamp, pd.datetime, pd.date, pd._libs.tslibs.timestamps.Timestamp)) else pd.to_datetime(date_range)


# Partition filter
# Partition filter
partitions = df["Partition"].dropna().unique()
partition_sel = st.sidebar.multiselect("Partition", partitions, default=list(partitions))
st.sidebar.write("Partitions selected:", partition_sel)

# User filter
users = df["UID"].dropna().unique()
user_sel = st.sidebar.multiselect("User ID", users, default=list(users))
st.sidebar.write("Users selected:", user_sel)

# Status filter
statuses = df["State"].dropna().unique()
status_sel = st.sidebar.multiselect("Job State", statuses, default=list(statuses))
st.sidebar.write("States selected:", status_sel)

# App filter (from SQL metadata)
apps = df_merged["lib_application"].dropna().unique() if "lib_application" in df_merged.columns else []
app_sel = st.sidebar.multiselect("Application", apps, default=list(apps))
st.sidebar.write("Apps selected:", app_sel)

# --- DEBUG: Show what is available BEFORE filtering ---

# --- APPLY FILTERS ---
mask = (
    (df["Start"].dt.date >= pd.to_datetime(start_date).date()) &
    (df["Start"].dt.date <= pd.to_datetime(end_date).date()) &
    (df["Partition"].isin(partition_sel)) &
    (df["UID"].isin(user_sel)) &
    (df["State"].isin(status_sel))
)


if app_sel and "lib_application" in df_merged.columns:
    mask = mask & (df_merged["lib_application"].isin(app_sel))


st.write("All possible UIDs:", df["UID"].unique())
st.write("All possible Partitions:", df["Partition"].unique())
st.write("All possible Job States:", df["State"].unique())
st.write("Total jobs before filtering:", len(df))
st.write("Start date:", start_date)
st.write("End date:", end_date)

df = df[mask]
st.write("Number of jobs per UID after filtering:")
st.write(df["UID"].value_counts())

# --- DEBUG: Show what is left AFTER filtering ---
st.write("Filtered UIDs:", df["UID"].unique())
st.write("Filtered Partitions:", df["Partition"].unique())
st.write("Filtered Job States:", df["State"].unique())
st.write("Filtered number of jobs:", len(df))

df_merged = df_merged.loc[mask]

# --- rest of your code, but use filtered `df` and `df_merged` ---

if "sujet_recherche" in df_merged.columns:
    df_merged["sujet_recherche_cleaned"] = df_merged["sujet_recherche"].replace(
        r"sujet_recherche\d+", np.nan, regex=True
    )

st.set_page_config(page_title="HPC Job Dashboard", layout="wide")
st.title("HPC Cluster Job Dashboard")
st.write("""
_Analyze SLURM job usage and resource performance for the CNRST HPC cluster (MARWAN). 
Use the sidebar filters to explore job efficiency, user behavior, and identify optimization opportunities._
""")


st.markdown(" Overview")
st.metric("Total Jobs",     len(df))
st.metric("Unique Users",   df["UID"].nunique())

st.markdown("Top 5 Partitions Used")
part_counts = df["Partition_Main"].value_counts().head(5)
st.bar_chart(part_counts)

st.markdown("Top 10 Job Types (Grouped)")
jobtype_counts = df["JobName_Grouped"].value_counts().head(10)
st.bar_chart(jobtype_counts)


st.markdown("Jobs Submitted per Month")
jobs_per_month = (
    df["Submit"].dt.to_period("M")
      .value_counts()
      .sort_index()
)
st.line_chart(jobs_per_month)


st.markdown(" Top 10 Users by Number of Jobs")
user_counts = df["UID"].value_counts().head(10)
st.bar_chart(user_counts)



def parse_elapsed(el):
    try:
        h, m, s = map(int, el.split(":"))
        return h*3600 + m*60 + s
    except:
        return np.nan

df["Elapsed_sec"] = df["Elapsed"].apply(parse_elapsed)
st.markdown(" Top 10 Users by Avg Job Duration (seconds)")
avg_dur = df.groupby("UID")["Elapsed_sec"].mean().sort_values(ascending=False).head(10)
st.bar_chart(avg_dur)


st.markdown("Job Status Overview")
state_counts = df["State_Clean"].value_counts().head(10)
st.bar_chart(state_counts)



def parse_cputime(ct):
    try:
        h, m, s = map(int, str(ct).split(":"))
        return h*3600 + m*60 + s
    except:
        return np.nan

df["CPUTime_sec"] = df["CPUTime"].apply(parse_cputime)
st.markdown(" Top 10 Users by Total CPU Time Used")
cpu_top = df.groupby("UID")["CPUTime_sec"].sum().sort_values(ascending=False).head(10)
st.bar_chart(cpu_top)

st.markdown("## Job Efficiency Analysis")

if "CPUTime_sec" in df.columns and "core_seconds" in df.columns:
    st.markdown("#### Distribution of Job Efficiency (CPUTime / Elapsed × NCPUS)")
    st.write("Values close to 1 mean efficient CPU usage. Values ≪1 mean underutilization (CPU idle).")
    # Drop NaN/infinite
    eff = df["CPUTime_sec"] / df["core_seconds"]
    eff = eff.replace([np.inf, -np.inf], np.nan).dropna()
    st.bar_chart(eff.value_counts(bins=10, sort=False))

    st.markdown("#### Top 10 Most Efficient Users")
    eff_by_user = df.groupby("UID").apply(
        lambda g: np.nanmean(g["CPUTime_sec"] / g["core_seconds"])
    ).sort_values(ascending=False).head(10)
    st.bar_chart(eff_by_user)

else:
    st.info("No efficiency data available (missing CPUTime or core_seconds).")

st.markdown("## Failed/Cancelled Jobs Overview")

if "State" in df.columns:
    fail_states = df["State"].value_counts().filter(like="FAIL").sum() + \
                  df["State"].value_counts().filter(like="CANCEL").sum() + \
                  df["State"].value_counts().get("OUT_OF_MEMORY", 0)
    st.metric("Total Failed/Cancelled/OutOfMemory Jobs", int(fail_states))

    fail_by_user = df[df["State"].isin(
        ["FAILED", "CANCELLED", "OUT_OF_MEMORY"])].groupby("UID").size().sort_values(ascending=False).head(10)
    st.bar_chart(fail_by_user)
else:
    st.info("No job state data available.")


def mem_to_mb(mem):
    try:
        if "Mn" in mem:
            return int(mem.replace("Mn",""))
        if "Gn" in mem:
            return int(mem.replace("Gn","")) * 1024
        return np.nan
    except:
        return np.nan

df["ReqMem_MB"] = df["ReqMem"].apply(mem_to_mb)
st.markdown(" Average Requested Memory by Top Users")
mem_avg = df.groupby("UID")["ReqMem_MB"].mean().sort_values(ascending=False).head(10)
st.bar_chart(mem_avg)


if "lib_application" in df_user_meta.columns:
    unique_apps = df_user_meta["lib_application"].dropna().unique()
    if len(unique_apps) == 0:
        st.error("❌ No application data found in metadata.")
    elif len(unique_apps) == 1:
        st.warning(f"⚠️ Only one application value detected: {unique_apps[0]}. Check data quality.")
else:
    st.error("❌ Column 'lib_application' not found in SQL metadata.")


st.markdown(" Jobs by Application Used")
if "lib_application" in df_merged.columns:
    app_counts = df_merged["lib_application"].value_counts().head(10)
    st.bar_chart(app_counts)
else:
    st.error("❌ Column 'lib_application' not found in merged data.")


st.markdown(" Jobs by Research Topic (raw placeholders)")
if "sujet_recherche" in df_merged.columns:
    raw_topics = df_merged["sujet_recherche"].value_counts().head(10)
    st.bar_chart(raw_topics)
else:
    st.error("❌ Column 'sujet_recherche' not found in merged data.")

st.markdown("Cleaned Research Topics (placeholders removed)")
if "sujet_recherche_cleaned" in df_merged.columns:
    clean_topics = df_merged["sujet_recherche_cleaned"].value_counts()
    if clean_topics.empty:
        st.info("ℹ️ No meaningful research topics after cleaning.")
    else:
        st.bar_chart(clean_topics.head(10))
else:
    st.error("❌ Cleaned research topic column not found.")


st.markdown("Institutions & Cities (all users)")
if "institution_city_meta" in df_user_meta.columns:
    full_inst = (
        df_user_meta["institution_city_meta"]
        .value_counts()
        .reset_index()
        .rename(columns={"index": "Institution, City", "institution_city_meta": "User Count"})
    )
    st.dataframe(full_inst, use_container_width=True)
else:
    st.error("❌ Column 'institution_city_meta' not found in metadata.")


st.markdown(" Institutions & Cities (linked jobs only)")
if "institution_city" in df_merged.columns:
    linked_inst = (
        df_merged["institution_city"]
        .value_counts()
        .reset_index()
        .rename(columns={"index": "Institution, City", "institution_city": "Job Count"})
    )
    st.dataframe(linked_inst, use_container_width=True)
else:
    st.error("❌ Column 'institution_city' not found in merged data.")


if "lib_application" in df_merged.columns:
    num_linked = df_merged["lib_application"].notna().sum()
    st.markdown(f"ℹ️ _Jobs linked to SQL metadata: **{num_linked}** of {len(df)}_")


st.markdown("### Download Filtered Data")
import io

csv = df.to_csv(index=False)
st.download_button(
    label="Download jobs as CSV",
    data=csv,
    file_name="filtered_jobs.csv",
    mime="text/csv"
)

if "df_merged" in locals():
    merged_csv = df_merged.to_csv(index=False)
    st.download_button(
        label="Download merged jobs+metadata as CSV",
        data=merged_csv,
        file_name="filtered_jobs_merged.csv",
        mime="text/csv"
    )


st.markdown("---")
st.markdown("## 📋 Automated Recommendations")

# Example logic (customize as you wish)
high_fail_partitions = df["Partition"][df["State"] == "FAILED"].value_counts().head(3)
high_mem_users = df.groupby("UID")["ReqMem_MB"].mean().sort_values(ascending=False).head(3)
inefficient_users = df.groupby("UID").apply(
    lambda g: np.nanmean(g["CPUTime_sec"] / g["core_seconds"])
).sort_values().head(3)

if high_fail_partitions.size > 0:
    st.write(f"**Partitions with most failed jobs:** {', '.join(high_fail_partitions.index)}")

if high_mem_users.size > 0:
    st.write(f"**Users with highest average memory requests:** {', '.join(map(str, high_mem_users.index))}")

if inefficient_users.size > 0:
    st.write(f"**Users with lowest job efficiency:** {', '.join(map(str, inefficient_users.index))}")

if (high_fail_partitions.size == 0 and high_mem_users.size == 0 and inefficient_users.size == 0):
    st.info("No issues detected. Resource usage appears balanced.")

st.markdown("_This dashboard is part of an internship project to analyze SLURM HPC job usage at CNRST._")
