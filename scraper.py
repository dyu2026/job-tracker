from utils import classify_job, classify_location
import requests
from supabase_client import supabase
from datetime import datetime, UTC


# -----------------------------------
# Helper: Upsert With first_seen_at
# -----------------------------------

def upsert_job(job_data):
    now = datetime.now(UTC).isoformat()

    # Check if job already exists
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

        if isinstance(job.get("location"), dict):
            location_name = job["location"].get("name", "")
        elif isinstance(job.get("location"), str):
            location_name = job.get("location", "")

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
    url = f"https://api.smartrecruiters.com/v1/companies/{company_slug}/postings"
    response = requests.get(url)
    data = response.json()

    jobs = data.get("content", [])
    print(f"Found {len(jobs)} jobs for {company_name}")

    seen_ids = set()

    for job in jobs:
        title = job.get("name")
        loc = job.get("location", {})

        city = loc.get("city", "")
        region_name = loc.get("region", "")
        country = loc.get("country", "")

        location_name = " ".join([city, region_name, country])

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
            "url": job.get("ref"),
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

def scrape_workday(company_slug, company_name, location_id):
    subdomain, tenant = company_slug.split("|")

    url = f"https://{subdomain}.wd5.myworkdayjobs.com/wday/cxs/{subdomain}/{tenant}/jobs"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": f"https://{subdomain}.wd5.myworkdayjobs.com",
        "Referer": f"https://{subdomain}.wd5.myworkdayjobs.com/"
    }

    PAGE_SIZE = 20

    payload = {
        "appliedFacets": {
            "locations": [location_id]
        },
        "count": PAGE_SIZE,
        "offset": 0,
        "searchText": ""
    }

    MAX_OFFSET = 200
    total_jobs = 0
    seen_ids = set()

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

        print(f"Fetched {len(jobs)} jobs at offset {payload['offset']}")

        for job in jobs:
            external_path = job.get("externalPath")
            if not external_path:
                continue

            location_name = job.get("locationsText", "")

            seniority, function = classify_job(job.get("title", ""))
            region, is_remote, is_japan, remote_scope = classify_location(location_name)

            if not (
                is_japan
                or remote_scope in ["global", "apac", "japan"]
            ):
                continue


            external_id = external_path
            seen_ids.add(external_id)

            job_data = {
                "company": company_name,
                "external_id": external_id,
                "title": job.get("title"),
                "location": location_name,
                "url": f"https://{subdomain}.wd5.myworkdayjobs.com/ja-JP/{tenant}{external_path}",
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
# MAIN
# -----------------------------------

if __name__ == "__main__":
    scrape_greenhouse("brave", "Brave")
    scrape_greenhouse("gitlab", "GitLab")
    scrape_greenhouse("figma", "Figma")
    scrape_greenhouse("stripe", "Stripe")
    scrape_greenhouse("anthropic", "Anthropic")
    scrape_greenhouse("nothing", "Nothing")

    scrape_ashby("notion", "Notion")
    scrape_ashby("duck-duck-go", "DuckDuckGo")
    scrape_ashby("deepl", "DeepL")
    scrape_ashby("lilt-corporate", "Lilt")

    scrape_smartrecruiters("Canva", "Canva")

    scrape_workday(
        "disney|disneycareer",
        "Disney",
        location_id="4f84d9e8a09701011a72254a71290000"
    )

    scrape_workday(
        "workday|Workday",
        "Workday",
        location_id="9248082dd0ba104584ac4b3d9356363b"
    )