from utils import classify_job, classify_location
from supabase_client import supabase

import requests
import json
import feedparser
import re
import time
import platform
import urllib.parse

from datetime import datetime, timedelta, timezone, UTC
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------------------
# Helper: Upsert With first_seen_at
# -----------------------------------

def upsert_job(job_data):
    now = datetime.now(UTC).isoformat()

    existing = supabase.table("jobs") \
        .select("first_seen_at") \
        .eq("company", job_data["company"]) \
        .eq("external_id", job_data["external_id"]) \
        .execute()

    if existing.data:
        first_seen = existing.data[0]["first_seen_at"]
    else:
        first_seen = now

    job_data["first_seen_at"] = first_seen
    job_data["last_seen_at"] = now
    job_data["is_active"] = True

    supabase.table("jobs").upsert(
        job_data,
        on_conflict="company,external_id"
    ).execute()


# -----------------------------------
# Helper: Mark Removed Jobs
# -----------------------------------

def mark_removed_jobs(company_name, seen_ids):
    if not seen_ids:
        print(f"⚠️ No seen_ids for {company_name}, skipping removal detection")
        return

    response = supabase.table("jobs") \
        .select("external_id") \
        .eq("company", company_name) \
        .eq("is_active", True) \
        .execute()

    existing_ids = {row["external_id"] for row in response.data}
    removed_ids = existing_ids - seen_ids

    if removed_ids:
        print(f"Marking {len(removed_ids)} removed jobs for {company_name}")

        supabase.table("jobs") \
            .update({"is_active": False}) \
            .in_("external_id", list(removed_ids)) \
            .eq("company", company_name) \
            .execute()

# -----------------------------------
# Universal Next.js Scraper (Auto Detect)
# -----------------------------------

def scrape_nextjs_company(
    company_name,
    careers_url,
    json_page_path,
    locale="en"
):

    print(f"\nScraping {company_name}...")

    # -------------------------
    # Step 1: Fetch careers page
    # -------------------------
    try:
        res = requests.get(careers_url, timeout=20)
        res.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to fetch careers page: {e}")
        return

    soup = BeautifulSoup(res.text, "html.parser")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})

    if not script_tag:
        print("❌ __NEXT_DATA__ not found")
        return

    next_data = json.loads(script_tag.string)
    build_id = next_data.get("buildId")

    if not build_id:
        print("❌ buildId missing")
        return

    print(f"✅ buildId found: {build_id}")

    # -------------------------
    # Step 2: Fetch JSON data
    # -------------------------
    parsed = urlparse(careers_url)
    root = f"{parsed.scheme}://{parsed.netloc}"

    # Special case for Miro
    if company_name.lower() == "miro":
        json_url = f"{root}/careers/_next/data/{build_id}/{locale}/{json_page_path}.json"
    else:
        # Generic for other Next.js companies
        # Remove trailing slash and last segment
        path = parsed.path.rstrip("/")
        base_path = "/".join(path.split("/")[:-1])
        json_url = f"{root}{base_path}/_next/data/{build_id}/{locale}/{json_page_path}.json"

    print(f"📡 Fetching JSON: {json_url}")

    try:
        data_res = requests.get(json_url, timeout=20)
        data_res.raise_for_status()
        data = data_res.json()
    except Exception as e:
        print(f"❌ Failed to fetch JSON: {e}")
        return

    # -------------------------
    # Step 3: Auto-detect job list
    # -------------------------

    def find_job_list(obj):
        if isinstance(obj, dict):
            for v in obj.values():
                result = find_job_list(v)
                if result:
                    return result
        elif isinstance(obj, list):
            if not obj:
                return None
            if isinstance(obj[0], dict):
                keys = obj[0].keys()
                if (
                    any(k in keys for k in ["title", "text", "name"])
                    and any(k in keys for k in ["id", "jobId", "requisition_id"])
                ):
                    return obj
            for item in obj:
                result = find_job_list(item)
                if result:
                    return result
        return None

    jobs = find_job_list(data)

    if not jobs:
        print("❌ No job list detected")
        return

    print(f"Found {len(jobs)} jobs for {company_name}")

    # -------------------------
    # Step 4: Process Jobs
    # -------------------------

    seen_ids = set()

    for job in jobs:

        # --- Auto-detect fields ---
        title = (
            job.get("title")
            or job.get("text")
            or job.get("name")
        )

        external_id = (
            job.get("id")
            or job.get("jobId")
            or job.get("requisition_id")
        )

        location_obj = job.get("location")

        if isinstance(location_obj, dict):
            location_name = location_obj.get("name", "")
        else:
            location_name = location_obj or ""

        # --- Determine job URL ---
        job_url = None

        # 1. Try top-level absolute_url
        if "absolute_url" in job and job["absolute_url"]:
            # Make full URL if relative
            if job["absolute_url"].startswith("/"):
                job_url = f"https://miro.com{job['absolute_url']}"
            else:
                job_url = job["absolute_url"]

        # 2. Try other known fields
        elif "url" in job and job["url"]:
            job_url = job["url"]
        elif "applyUrl" in job and job["applyUrl"]:
            job_url = job["applyUrl"]

        # 3. Fallback: if still None, build from external_id (Miro pattern)
        if not job_url:
            job_url = f"https://miro.com/careers/vacancy/{job.get('id')}"

        # Optional: print for debugging
        # print(f"{title} -> {job_url}")

        if not title or not external_id:
            continue

        # --- Classification ---
        seniority, function = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        if not (
            is_japan
            or remote_scope in ["global", "apac", "japan"]
        ):
            continue

        external_id = str(external_id)
        seen_ids.add(external_id)

        job_data = {
            "company": company_name,
            "external_id": external_id,
            "title": title,
            "location": location_name,
            "url": job_url,
            "seniority": seniority,
            "function": function,
            "region": region,
            "is_remote": is_remote,
            "is_japan": is_japan,
        }

        upsert_job(job_data)

    mark_removed_jobs(company_name, seen_ids)

    print(f"✅ Finished {company_name}")

# -----------------------------------
# Greenhouse
# -----------------------------------

def scrape_greenhouse(company_slug, company_name):
    url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs"
    response = requests.get(url)
    data = response.json()

    jobs = data.get("jobs", [])
    print(f"Found {len(jobs)} jobs for {company_name}")

    seen_ids = set()

    for job in jobs:
        location_name = ""

        if isinstance(job.get("location"), dict):
            location_name = job["location"].get("name", "")

        seniority, function = classify_job(job["title"])
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        if not (is_japan or remote_scope in ["global", "apac", "japan"]):
            continue

        external_id = str(job["id"])
        seen_ids.add(external_id)

        job_data = {
            "company": company_name,
            "external_id": external_id,
            "title": job["title"],
            "location": location_name,
            "url": job["absolute_url"],
            "seniority": seniority,
            "function": function,
            "region": region,
            "is_remote": is_remote,
            "is_japan": is_japan,
        }

        upsert_job(job_data)

    mark_removed_jobs(company_name, seen_ids)
    
# -----------------------------------
# Miro
# -----------------------------------
  
def scrape_miro():
    scrape_nextjs_company(
        company_name="Miro",
        careers_url="https://miro.com/careers/open-positions/",
        json_page_path="open-positions",
        locale="en"
    )

# -----------------------------------
# Ashby
# -----------------------------------

def scrape_ashby(company_slug, company_name):
    url = f"https://api.ashbyhq.com/posting-api/job-board/{company_slug}"
    response = requests.get(url)
    data = response.json()

    jobs = data.get("jobs", [])
    print(f"Found {len(jobs)} jobs for {company_name}")

    seen_ids = set()

    for job in jobs:
        title = job.get("title")
        location_name = ""

        # Ashby sometimes uses location
        if isinstance(job.get("location"), dict):
            location_name = job["location"].get("name", "")

        # Sometimes it's just a string
        elif isinstance(job.get("location"), str):
            location_name = job.get("location", "")

        # Many Ashby boards use locations (plural)
        elif job.get("locations"):
            location_list = [loc.get("name", "") for loc in job["locations"]]
            location_name = "; ".join(location_list)

        seniority, function = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        if not (
            is_japan
            or remote_scope in ["global", "apac", "japan"]
        ):
            continue


        external_id = str(job["id"])
        seen_ids.add(external_id)

        job_data = {
            "company": company_name,
            "external_id": external_id,
            "title": title,
            "location": location_name,
            "url": job.get("applyUrl"),
            "seniority": seniority,
            "function": function,
            "region": region,
            "is_remote": is_remote,
            "is_japan": is_japan,
        }

        upsert_job(job_data)

    mark_removed_jobs(company_name, seen_ids)


# -----------------------------------
# SmartRecruiters
# -----------------------------------

def scrape_smartrecruiters(company_slug, company_name):
    all_jobs = []
    offset = 0
    limit = 100

    # -----------------------------------
    # 1. PAGINATION (FIX)
    # -----------------------------------
    while True:
        url = f"https://api.smartrecruiters.com/v1/companies/{company_slug}/postings?limit={limit}&offset={offset}"
        response = requests.get(url)
        data = response.json()

        jobs = data.get("content", [])
        if not jobs:
            break

        all_jobs.extend(jobs)
        offset += limit

    print(f"Found {len(all_jobs)} jobs for {company_name}")

    seen_ids = set()

    # -----------------------------------
    # 2. PROCESS JOBS
    # -----------------------------------
    for job in all_jobs:
        title = job.get("name", "")

        loc = job.get("location") or {}

        city = loc.get("city", "")
        region_name = loc.get("region", "")
        country = loc.get("country", "")

        # Cleaner location formatting
        location_parts = [city, region_name, country]
        location_name = ", ".join([p for p in location_parts if p])

        seniority, function = classify_job(title)

        # -----------------------------------
        # 3. SAFER LOCATION CLASSIFICATION
        # -----------------------------------
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        # 🔥 TEMP DEBUG (you should keep this for now)
        # print(company_name, location_name, is_japan, remote_scope)

        # -----------------------------------
        # 4. RELAX FILTER (FIX FOR WISE)
        # -----------------------------------
        if not (
            is_japan
            or remote_scope in ["global", "apac", "japan"]
            or "japan" in location_name.lower()
            or "tokyo" in location_name.lower()
        ):
            continue

        job_id = job.get("id")
        external_id = str(job_id)
        seen_ids.add(external_id)

        job_data = {
            "company": company_name,
            "external_id": external_id,
            "title": title,
            "location": location_name,
            "url": f"https://jobs.smartrecruiters.com/{company_slug.capitalize()}/{job_id}",
            "seniority": seniority,
            "function": function,
            "region": region,
            "is_remote": is_remote,
            "is_japan": is_japan,
        }

        upsert_job(job_data)

    mark_removed_jobs(company_name, seen_ids)


# -----------------------------------
# Workday
# -----------------------------------

def scrape_workday(company_slug, company_name, location_ids=None, facet="locations"):

    parts = company_slug.split("|")

    if len(parts) == 3:
        subdomain, tenant, cluster = parts
    else:
        subdomain, tenant = parts
        cluster = "wd5"  # default for most companies

    url = f"https://{subdomain}.{cluster}.myworkdayjobs.com/wday/cxs/{subdomain}/{tenant}/jobs"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": f"https://{subdomain}.{cluster}.myworkdayjobs.com",
        "Referer": f"https://{subdomain}.{cluster}.myworkdayjobs.com/"
    }

    PAGE_SIZE = 20
    MAX_OFFSET = 200
    total_jobs = 0
    seen_ids = set()

    if location_ids and not isinstance(location_ids, list):
        location_ids = [location_ids]

    def is_japan_override(location_name, external_path):
        if location_name.lower() in ["2 locations", "multiple locations"]:
            if external_path and "japan" in external_path.lower():
                return True
        return False

    payload = {
        "limit": PAGE_SIZE,
        "offset": 0,
        "searchText": "",
        "appliedFacets": {}
    }

    if location_ids:
        payload["appliedFacets"][facet] = location_ids

    while True:

        if payload["offset"] > MAX_OFFSET:
            print("Safety break triggered")
            break

        response = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=15
        )

        if response.status_code != 200:
            print(f"Failed for {company_name}: {response.status_code}")
            break

        data = response.json()
        jobs = data.get("jobPostings", [])

        if not jobs:
            break

        print(f"{company_name}: fetched {len(jobs)} jobs at offset {payload['offset']}")

        for job in jobs:

            title = job.get("title", "")
            external_path = job.get("externalPath")

            if job.get("locations"):
                location_name = " / ".join(
                    loc.get("locationName", "") for loc in job["locations"]
                )
            else:
                location_name = job.get("locationsText", "")

            override_japan = is_japan_override(location_name, external_path)

            seniority, function = classify_job(title)
            region, is_remote, is_japan, remote_scope = classify_location(location_name)

            if override_japan:
                is_japan = True
                remote_scope = "japan"

            if not (is_japan or remote_scope in ["global", "apac", "japan"]):
                continue

            external_id = external_path
            seen_ids.add(external_id)

            job_data = {
                "company": company_name,
                "external_id": external_id,
                "title": title,
                "location": location_name,
                "url": f"https://{subdomain}.{cluster}.myworkdayjobs.com/en-US/{tenant}{external_path}",
                "seniority": seniority,
                "function": function,
                "region": region,
                "is_remote": is_remote,
                "is_japan": is_japan,
            }

            upsert_job(job_data)
            total_jobs += 1

        if len(jobs) < PAGE_SIZE:
            break

        payload["offset"] += PAGE_SIZE

    print(f"Total jobs found for {company_name}: {total_jobs}")
    mark_removed_jobs(company_name, seen_ids)
 
# -----------------------------------
# Lever/Spotify
# -----------------------------------
 
def scrape_lever(company_slug, company_name):
    url = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"

    response = requests.get(url, timeout=15)
    if response.status_code != 200:
        print(f"Failed Lever fetch for {company_name}")
        return

    jobs = response.json()
    seen_ids = set()

    for job in jobs:
        title = job.get("text", "")
        external_id = job.get("id")
        
        # Lever nesting for location
        categories = job.get("categories", {})
        location_name = categories.get("location", "") or ""
        workplace_type = job.get("workplaceType", "")

        # Combine workplaceType if remote to help classify_location
        if workplace_type == "remote" and "remote" not in location_name.lower():
            location_name = f"Remote, {location_name}".strip(", ")

        # --- 1. Use your classifiers ---
        seniority, function = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        # --- 2. Apply your standard eligibility filter ---
        if not (
            is_japan
            or remote_scope in ["global", "apac", "japan"]
        ):
            continue

        external_id = str(external_id)
        seen_ids.add(external_id)

        # --- 3. Build the standardized job_data dictionary ---
        job_data = {
            "company": company_name,
            "external_id": external_id,
            "title": title,
            "location": location_name,
            "url": job.get("hostedUrl"),
            "seniority": seniority,  # Now correctly classified
            "function": function,    # Now correctly classified
            "region": region,        # Extracted from classify_location
            "is_remote": is_remote,
            "is_japan": is_japan,
            "remote_scope": remote_scope,
        }

        # --- 4. Use the helper function to handle first_seen_at ---
        upsert_job(job_data)

    # --- 5. Mark removed jobs ---
    mark_removed_jobs(company_name, seen_ids)

    print(f"✅ Lever scrape complete for {company_name}")
    
# -----------------------------------
# Monday.com
# -----------------------------------
    
def scrape_monday(company_name="monday.com"):

    url = "https://monday.com/careers"

    print(f"Scraping {company_name}...")

    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to fetch {company_name}: {e}")
        return

    soup = BeautifulSoup(response.text, "html.parser")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})

    if not script_tag:
        print("❌ __NEXT_DATA__ not found.")
        return

    data = json.loads(script_tag.string)

    dynamic_data = data.get("props", {}).get("pageProps", {}).get("dynamicData", {})

    if not dynamic_data:
        print("❌ dynamicData not found.")
        return

    container_key = list(dynamic_data.keys())[0]
    positions = dynamic_data[container_key].get("positions", [])

    print(f"Found {len(positions)} jobs for {company_name}")

    seen_ids = set()

    for job in positions:
        title = job.get("name")
        external_id = job.get("uid")

        if not title or not external_id:
            continue

        location_name = job.get("location", {}).get("name", "")
        job_url = job.get("url_active_page")

        # --- Classification ---
        seniority, function = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        # --- Geography filter ---
        if not (
            is_japan
            or remote_scope in ["global", "apac", "japan"]
        ):
            continue

        external_id = str(external_id)
        seen_ids.add(external_id)

        job_data = {
            "company": company_name,
            "external_id": external_id,
            "title": title,
            "location": location_name,
            "url": job_url,
            "seniority": seniority,
            "function": function,
            "region": region,
            "is_remote": is_remote,
            "is_japan": is_japan,
        }

        upsert_job(job_data)

    mark_removed_jobs(company_name, seen_ids)

    print(f"✅ Finished {company_name}")
 
# -----------------------------------
# eightfold.ai
# -----------------------------------
 
def scrape_eightfold(company_slug, company_name, location, pid):

    import requests

    BASE_URL = f"https://{company_slug}.eightfold.ai/api/apply/v2/jobs"

    PAGE_SIZE = 10
    MAX_OFFSET = 200

    start = 0
    total_jobs = 0
    seen_ids = set()

    while True:

        if start > MAX_OFFSET:
            print("Safety break triggered")
            break

        params = {
            "domain": f"{company_slug}.com",
            "start": start,
            "num": PAGE_SIZE,
            "location": location,
            "pid": pid,
            "sort_by": "relevance",
            "hl": "en",
            "triggerGoButton": "false"
        }

        r = requests.get(
            BASE_URL,
            params=params,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30
        )

        if r.status_code != 200:
            print(f"Failed for {company_name}: {r.status_code}")
            break

        data = r.json()
        jobs = data.get("positions", [])

        if not jobs:
            break

        print(f"{company_name}: fetched {len(jobs)} jobs at offset {start}")

        for job in jobs:

            title = job.get("name")
            location_name = job.get("location")
            external_id = job.get("ats_job_id")
            url = job.get("canonicalPositionUrl")

            if not external_id:
                continue

            seen_ids.add(external_id)

            seniority, function = classify_job(title)
            region, is_remote, is_japan, remote_scope = classify_location(location_name)

            if not (is_japan or remote_scope in ["global", "apac", "japan"]):
                continue

            job_data = {
                "company": company_name,
                "external_id": external_id,
                "title": title,
                "location": location_name,
                "url": url,
                "seniority": seniority,
                "function": function,
                "region": region,
                "is_remote": is_remote,
                "is_japan": is_japan
            }

            upsert_job(job_data)
            total_jobs += 1

        if len(jobs) < PAGE_SIZE:
            break

        start += PAGE_SIZE

    print(f"Total jobs found for {company_name}: {total_jobs}")

    mark_removed_jobs(company_name, seen_ids)
    
# -----------------------------------
# BambooHR
# -----------------------------------

def scrape_bamboohr(subdomain, company_name):

    url = f"https://{subdomain}.bamboohr.com/careers/list"

    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()

    except Exception as e:
        print(f"❌ Failed BambooHR fetch for {company_name}: {e}")
        return

    # -------------------------
    # Detect job list format
    # -------------------------

    jobs = []

    if isinstance(data, list):
        jobs = data

    elif isinstance(data, dict):

        if "result" in data:
            jobs = data["result"]

        elif "jobs" in data:
            jobs = data["jobs"]

    print(f"Found {len(jobs)} jobs for {company_name}")

    seen_ids = set()

    for job in jobs:

        # -------------------------
        # Title detection
        # -------------------------

        title = (
            job.get("jobTitle")
            or job.get("jobOpeningName")
            or job.get("title")
            or job.get("name")
        )

        # -------------------------
        # ID detection
        # -------------------------

        external_id = (
            job.get("id")
            or job.get("jobId")
        )

        if not title or not external_id:
            continue

        external_id = str(external_id)

        # -------------------------
        # Location parsing
        # -------------------------

        location_name = ""

        location = job.get("location")

        if isinstance(location, dict):

            city = location.get("city")
            state = location.get("state")

            parts = [p for p in [city, state] if p]

            if parts:
                location_name = ", ".join(parts)

        elif isinstance(location, str):

            location_name = location

        if not location_name:
            location_name = "Remote / Unknown"

        # -------------------------
        # Job URL
        # -------------------------

        job_url = f"https://{subdomain}.bamboohr.com/careers/{external_id}"

        # -------------------------
        # Classification
        # -------------------------

        seniority, function = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        if not (
            is_japan
            or remote_scope in ["global", "apac", "japan"]
        ):
            continue

        seen_ids.add(external_id)

        job_data = {
            "company": company_name,
            "external_id": external_id,
            "title": title,
            "location": location_name,
            "url": job_url,
            "seniority": seniority,
            "function": function,
            "region": region,
            "is_remote": is_remote,
            "is_japan": is_japan,
        }

        upsert_job(job_data)

    mark_removed_jobs(company_name, seen_ids)

    print(f"✅ BambooHR scrape complete for {company_name}")

# -----------------------------------
# Netflix (Japan)
# -----------------------------------

def scrape_netflix():

    company_name = "Netflix"

    base_url = "https://explore.jobs.netflix.net/api/apply/v2/jobs"

    params = {
        "domain": "netflix.com",
        "pid": "790302851017",
        "location": "Tokyo, Japan",
        "num": 10,
        "sort_by": "relevance"
    }

    print(f"\nScraping {company_name}...")

    start = 0
    seen_ids = set()

    while True:

        params["start"] = start

        try:
            r = requests.get(base_url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"❌ Failed Netflix scrape: {e}")
            return

        jobs = data.get("positions", [])

        if not jobs:
            break

        print(f"Processing {len(jobs)} jobs (start={start})")

        for job in jobs:

            job_id = job.get("id")
            title = job.get("name")
            location_name = job.get("location", "")

            if not job_id or not title:
                continue

            external_id = str(job_id)
            seen_ids.add(external_id)

            seniority, function = classify_job(title)
            region, is_remote, is_japan, remote_scope = classify_location(location_name)

            job_url = f"https://explore.jobs.netflix.net/careers/apply?domain=netflix.com&pid={external_id}"

            job_data = {
                "company": company_name,
                "external_id": external_id,
                "title": title,
                "location": location_name,
                "url": job_url,
                "seniority": seniority,
                "function": function,
                "region": region,
                "is_remote": is_remote,
                "is_japan": is_japan,
            }

            upsert_job(job_data)

        start += params["num"]

    mark_removed_jobs(company_name, seen_ids)

    print(f"✅ Finished {company_name}")


# -----------------------------------
# LinkedIn RSS feed for posts
# -----------------------------------

INC_KEYWORDS = [
    "hiring","opportunity","job","recruitment","talent",
    "bilingual","director","career","positions","募集",
    "roles","role","we are hiring","join our team"
]

EXC_KEYWORDS = [
    "excited to announce","i’m happy to share","started a new position",
    "looking for a new role","i am looking","please help me find"
]

def matches_filters(text):
    text = text.lower()
    has_hiring_signal = any(k in text for k in INC_KEYWORDS)
    is_not_seeker = not any(k in text for k in EXC_KEYWORDS)
    return has_hiring_signal and is_not_seeker


def extract_linkedin_url(summary):
    match = re.search(r'href="(https://www.linkedin.com/posts/[^"]+)"', summary)
    return match.group(1) if match else None

def scrape_linkedin():

    DAYS_TO_PULL = 7

    QUERY = f'site:linkedin.com/posts (hiring OR recruiting OR "now hiring" OR "募集" OR "求人" OR newopportunity) Japan when:{DAYS_TO_PULL}d'
    ENCODED_QUERY = urllib.parse.quote(QUERY)

    RSS_URL = f"https://news.google.com/rss/search?q={ENCODED_QUERY}&hl=en-JP&gl=JP&ceid=JP:en"

    feed = feedparser.parse(RSS_URL)

    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_TO_PULL)

    print(f"LinkedIn feed entries: {len(feed.entries)}")

    # Clean old entries
    supabase.table("linkedin_posts") \
        .delete() \
        .lt("published_at", cutoff.isoformat()) \
        .execute()

    for entry in feed.entries:

        if not hasattr(entry, "published_parsed") or entry.published_parsed is None:
            continue

        published_utc = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)

        if published_utc < cutoff:
            continue

        title = entry.title
        summary = entry.get("summary", "")

        combined_text = title + " " + summary

        if not matches_filters(combined_text):
            continue

        url = extract_linkedin_url(summary) or entry.link
        snippet = re.sub("<.*?>", "", summary)

        data = {
            "title": title,
            "url": url,
            "snippet": snippet,
            "published_at": published_utc.isoformat(),
        }

        supabase.table("linkedin_posts") \
            .upsert(data, on_conflict="url") \
            .execute()

    print("LinkedIn RSS scrape complete")

# -----------------------------------
# Parallel Task Runner
# -----------------------------------

def run_task(func, *args):
    for attempt in range(2):
        try:
            func(*args)
            return
        except Exception as e:
            if attempt == 1:
                print(f"❌ Failed {func.__name__}: {e}")

# -----------------------------------
# MAIN
# -----------------------------------

if __name__ == "__main__":

    start_time = time.time()

    tasks = [

        # Next.js
        (scrape_miro,),
        (scrape_monday,),

        # Greenhouse
        (scrape_greenhouse, "brave", "Brave"),
        (scrape_greenhouse, "gitlab", "GitLab"),
        (scrape_greenhouse, "figma", "Figma"),
        (scrape_greenhouse, "stripe", "Stripe"),
        (scrape_greenhouse, "anthropic", "Anthropic"),
        (scrape_greenhouse, "nothing", "Nothing"),
        (scrape_greenhouse, "phrase", "Phrase"),
        (scrape_greenhouse, "okta", "Okta"),
        (scrape_greenhouse, "datadog", "Datadog"),
        (scrape_greenhouse, "goodnotes", "Goodnotes"),
        (scrape_greenhouse, "asana", "Asana"),
        (scrape_greenhouse, "workato", "Workato"),
        (scrape_greenhouse, "braze", "Braze"),
        (scrape_greenhouse, "hubspotjobs", "Hubspot"),
        (scrape_greenhouse, "automatticcareers", "Automattic"),
        (scrape_greenhouse, "unity3d", "Unity"),
        (scrape_greenhouse, "storyblok", "Storyblok"),
        (scrape_greenhouse, "speechify", "Speechify"),
        (scrape_greenhouse, "grafanalabs", "Grafana"),
        (scrape_greenhouse, "roblox", "Roblox"),
        (scrape_greenhouse, "airbnb", "Airbnb"),

        # Ashby
        (scrape_ashby, "notion", "Notion"),
        (scrape_ashby, "duck-duck-go", "DuckDuckGo"),
        (scrape_ashby, "deepl", "DeepL"),
        (scrape_ashby, "lilt-corporate", "Lilt"),
        (scrape_ashby, "perplexity", "Perplexity"),
        (scrape_ashby, "sierra", "Sierra"),
        (scrape_ashby, "zapier", "Zapier"),
        (scrape_ashby, "supabase", "Supabase"),
        (scrape_ashby, "substack", "Substack"),
        
        # SmartRecruiters
        (scrape_smartrecruiters, "Canva", "Canva"),
        (scrape_smartrecruiters, "wise", "Wise"),

        # Workday
        (
            scrape_workday,
            "disney|disneycareer",
            "Disney",
            [
                "4f84d9e8a09701011a72254a71290000",  # Tokyo
                "4f84d9e8a09701011a5995833ead0000"   # Chiba
            ]
        ),

        (
            scrape_workday,
            "workday|Workday",
            "Workday",
            "9248082dd0ba104584ac4b3d9356363b"
        ),

        (
            scrape_workday,
            "nvidia|NVIDIAExternalCareerSite",
            "NVIDIA",
            [
                "91336993fab910af6d6f9a47b91cc19e",
                "b00b3256ed551015d42d2bebe06b02b7"
            ]
        ),

        (
            scrape_workday,
            "mastercard|CorporateCareers|wd1",
            "Mastercard",
            "8eab563831bf10acbe1cda510e782135"
        ),
        
        (
            scrape_workday,
            "warnerbros|global|wd5",
            "Warner Bros",
            "8b705da2becf43cfaccc091da0988ab2",
            "locationCountry"
        ),
        
        (
            scrape_workday,
            "zoom|zoom",
            "Zoom",
            "8b705da2becf43cfaccc091da0988ab2",
            "locationCountry"
        ),
        
        (
            scrape_workday,
            "cisco|Cisco_Careers",
            "Cisco",
            [
                "1cf8ea09530d1001f4c5c40ec3720000",
                "f5adf8182d281001f842e5b0f10b0000",
                "662e524adea41001f4d1611409bb0000"
            ]
        ),
        
        # eightfold
        (
            scrape_eightfold,
            "aexp",
            "American Express",
            "Minato-ku, Tokyo, Japan",
            "39549064"
        ),
      
        # Lever
        (scrape_lever, "spotify", "Spotify"),
        (scrape_lever, "superside", "Superside"),
        
        # BambooHR
        (scrape_bamboohr, "lottiefiles", "LottieFiles"),

        # Netflix
        (scrape_netflix,), 

        # LinkedIn signals
        (scrape_linkedin,)
    ]

    system = platform.system()

    if system == "Windows":
        MAX_WORKERS = 1
    else:
        MAX_WORKERS = 4

    print(f"Detected OS: {system} | Using {MAX_WORKERS} worker(s)")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [
            executor.submit(run_task, *task)
            for task in tasks
        ]

        for future in as_completed(futures):
            future.result()

    end_time = time.time()
    runtime = round(end_time - start_time, 2)

    print(f"\n✅ All scrapers completed")
    print(f"⏱ Total runtime: {runtime} seconds")
    