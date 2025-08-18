# HPC-MARWAN Job Analytics Dashboard

This Streamlit app analyzes usage of the CNRST HPC (MARWAN) cluster using SLURM job logs and SQL metadata.

## Features

- Interactive filtering (by date, user, partition, application, state)
- Job efficiency, memory and failure analysis
- Usage by institution and research topic
- Automated, data-driven recommendations
- Download filtered tables as CSV

## How to Run

1. Clone the repo or copy the folder.
2. Install requirements: `pip install -r requirements.txt`
3. Prepare data:
    - Place raw SLURM CSV in `data/raw/`
    - Run: `python src/make_dataset.py --raw-file data/raw/JOBS_2021_2025.csv --out-file data/processed/jobs_clean.parquet`
4. Run the app:
    - `streamlit run app/hpc_dashboard_app.py`
5. Connect to your local MySQL (see `hpc_dashboard_app.py` for connection details).

## Files

- `src/clean_jobs.py`: Time/memory parsing utilities
- `src/make_dataset.py`: Cleans raw SLURM logs
- `app/hpc_dashboard_app.py`: The dashboard
- `data/`: Input/output data
- `tests/`: Unit tests

## Authors

Internship team (your names here)
