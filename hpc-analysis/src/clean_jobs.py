# src/clean_jobs.py
import re
import numpy as np
import pandas as pd

def parse_hms_or_dhms(s):
    """
    Convert 'DD-HH:MM:SS' or 'HH:MM:SS' into seconds (float).
    Returns np.nan for blanks or NaT.

    Examples:
        "01:02:03"     -> 3723
        "2-00:00:00"   -> 172800
        "" / None      -> np.nan
        "bad"          -> np.nan
    """
    if pd.isna(s) or s == "":
        return np.nan
    s = str(s)
    try:
        if "-" in s:
            days, rest = s.split("-")
            h, m, sec = map(int, rest.split(":"))
            total = int(days) * 86400 + h * 3600 + m * 60 + sec
        else:
            h, m, sec = map(int, s.split(":"))
            total = h * 3600 + m * 60 + sec
        return float(total)
    except Exception:
        return np.nan

_mem_re = re.compile(r"(?P<val>\d+)(?P<unit>[GMK]?)n?c?")

def parse_reqmem(mem, nnodes, ncpus):
    """
    Convert SLURM ReqMem to total megabytes.

    '4000Mn' -> 4000   (per node × nnodes)
    '2Gn'    -> 2048 × nnodes
    '8000'   -> 8000   (already total)
    ''/NaN   -> np.nan
    Handles both 'n' (per-node) and 'c' (per-cpu) scopes.
    """
    if pd.isna(mem) or mem == "" or mem == "0":
        return np.nan

    m = re.match(r"(?P<val>\d+)(?P<unit>[GMK]?)(?P<scope>[nc]?)", str(mem))
    if not m:
        return np.nan

    val = int(m["val"])
    unit = m["unit"].upper()
    scope = m["scope"].lower()

    mb = val
    if unit == "G":
        mb *= 1024
    elif unit == "K":
        mb = mb / 1024  # K → MB

    # scale if per-node or per-cpu
    if scope == "n":
        mb *= nnodes if pd.notna(nnodes) else 1
    elif scope == "c":
        mb *= ncpus if pd.notna(ncpus) else 1

    return float(mb)
