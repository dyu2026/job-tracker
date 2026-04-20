# -------------------------
# Imports
# -------------------------

# Standard lib
import json
import random
import re
import time
import platform
import urllib.parse
from datetime import datetime, timedelta, timezone, UTC
from urllib.parse import urlparse

# Third-party
import requests
import feedparser
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# Local
from utils import classify_job, classify_location
from supabase_client import supabase

# -------------------------
# Constants / Helpers
# -------------------------

ALLOWED_REMOTE_SCOPES = {"global", "apac", "japan"}


def log(msg, company=None):
    prefix = f"[{company}] " if company else ""
    print(f"{prefix}{msg}")


def safe_request(method, url, **kwargs):
    MAX_RETRIES = 3

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.request(method, url, timeout=20, **kwargs)

            if response.status_code == 200:
                return response

            if response.status_code in {500, 502, 503}:
                wait = 3 * (attempt + 1)
                print(f"⚠️ Retry {attempt+1} ({response.status_code}), waiting {wait}s")
                time.sleep(wait)
                continue

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request error: {e}")

        time.sleep(2 * (attempt + 1))

    return None


def classify(title, location_name):
    seniority, function = classify_job(title)
    region, is_remote, is_japan, remote_scope = classify_location(location_name)
    return seniority, function, region, is_remote, is_japan, remote_scope


def is_target_job(is_japan, remote_scope):
    return is_japan or remote_scope in ALLOWED_REMOTE_SCOPES


def build_job_data(
    company,
    external_id,
    title,
    location,
    url,
    seniority,
    function,
    region,
    is_remote,
    is_japan,
    **extra
):
    return {
        "company": company,
        "external_id": str(external_id),
        "title": title,
        "location": location,
        "url": url,
        "seniority": seniority,
        "function": function,
        "region": region,
        "is_remote": is_remote,
        "is_japan": is_japan,
        **extra,
    }


def extract_location(location):
    if isinstance(location, dict):
        return location.get("name", "")
    if isinstance(location, str):
        return location
    return ""


# -------------------------
# Supabase Helpers
# -------------------------


def upsert_job(job_data):
    now = datetime.now(UTC).isoformat()

    existing = (
        supabase.table("jobs")
        .select("first_seen_at")
        .eq("company", job_data["company"])
        .eq("external_id", job_data["external_id"])
        .execute()
    )

    first_seen = existing.data[0]["first_seen_at"] if existing.data else now

    job_data["first_seen_at"] = first_seen
    job_data["last_seen_at"] = now
    job_data["is_active"] = True

    supabase.table("jobs").upsert(job_data, on_conflict="company,external_id").execute()


def mark_removed_jobs(company_name, seen_ids):
    if not seen_ids:
        log("No seen_ids, skipping removal detection", company_name)
        return

    response = (
        supabase.table("jobs")
        .select("external_id")
        .eq("company", company_name)
        .eq("is_active", True)
        .execute()
    )

    existing_ids = {row["external_id"] for row in response.data}
    removed_ids = existing_ids - seen_ids

    if removed_ids:
        log(f"Marking {len(removed_ids)} removed jobs", company_name)

        (
            supabase.table("jobs")
            .update({"is_active": False})
            .in_("external_id", list(removed_ids))
            .eq("company", company_name)
            .execute()
        )


# -------------------------
# Example Refactored Scraper (Greenhouse)
# -------------------------


def scrape_greenhouse(company_slug, company_name):
    url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs"
    response = safe_request("GET", url)
    if not response:
        return

    jobs = response.json().get("jobs", [])
    log(f"Found {len(jobs)} jobs", company_name)

    seen_ids = set()

    for job in jobs:
        title = job.get("title")
        external_id = job.get("id")

        if not title or not external_id:
            continue

        location_name = extract_location(job.get("location"))

        seniority, function, region, is_remote, is_japan, remote_scope = classify(
            title, location_name
        )

        if not is_target_job(is_japan, remote_scope):
            continue

        external_id = str(external_id)
        seen_ids.add(external_id)

        job_data = build_job_data(
            company_name,
            external_id,
            title,
            location_name,
            job.get("absolute_url"),
            seniority,
            function,
            region,
            is_remote,
            is_japan,
        )

        upsert_job(job_data)

    mark_removed_jobs(company_name, seen_ids)


# -------------------------
# Runner
# -------------------------


def run_task(func, *args):
    company_name = args[1] if len(args) >= 2 else func.__name__

    for attempt in range(2):
        try:
            log("Starting", company_name)
            func(*args)
            log("Success", company_name)
            return
        except Exception as e:
            log(f"Retry {attempt+1}/2: {e}", company_name)
            time.sleep(2 * (attempt + 1))

    log("Failed after retries", company_name)


if __name__ == "__main__":
    start_time = time.time()

    tasks = [
        (scrape_greenhouse, "figma", "Figma"),
    ]

    MAX_WORKERS = 1 if platform.system() == "Windows" else 3

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(run_task, *task) for task in tasks]

        for future in as_completed(futures):
            future.result()

    print(f"\n⏱ Runtime: {round(time.time() - start_time, 2)}s")
