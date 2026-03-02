import streamlit as st
import pandas as pd
from supabase import create_client
import os
from datetime import datetime, timedelta, UTC

# Connect to Supabase
supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

st.set_page_config(page_title="Job Intelligence Dashboard", layout="wide")

st.title("🌍 Job Intelligence Dashboard")

# Fetch data
response = supabase.table("jobs").select("*").execute()
data = response.data

df = pd.DataFrame(data)

if df.empty:
    st.warning("No jobs found.")
    st.stop()

# Convert last_seen_at to datetime
df["last_seen_at"] = pd.to_datetime(df["last_seen_at"])

# --------------------------
# NEW JOBS (Last 24 Hours)
# --------------------------

st.header("🆕 New Jobs (Last 24 Hours)")

cutoff = datetime.now(UTC) - timedelta(hours=24)
new_jobs = df[df["last_seen_at"] > cutoff]

st.write(f"Total new jobs: {len(new_jobs)}")

st.dataframe(
    new_jobs[["company", "title", "region", "remote_scope", "url"]],
    use_container_width=True
)

# --------------------------
# SEARCHABLE TABLE
# --------------------------

st.header("🔎 Search All Jobs")

search = st.text_input("Search by keyword (title or company)")

filtered_df = df.copy()

if search:
    filtered_df = df[
        df["title"].str.contains(search, case=False, na=False)
        | df["company"].str.contains(search, case=False, na=False)
    ]

st.write(f"Total results: {len(filtered_df)}")

st.dataframe(
    filtered_df[["company", "title", "region", "remote_scope", "url"]],
    use_container_width=True
)