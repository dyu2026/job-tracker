from utils import classify_job, classify_location
import requests
from supabase_client import supabase
from datetime import datetime, UTC


def scrape_greenhouse(company_slug, company_name):
    url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs"
    now = datetime.now(UTC)
    response = requests.get(url)
    data = response.json()

    jobs = data.get("jobs", [])
    print(f"Found {len(jobs)} jobs for {company_name}")

    for job in jobs:
        location_name = ""

        if isinstance(job.get("location"), dict):
            location_name = job["location"].get("name", "")

        seniority, function = classify_job(job["title"])
        region, is_remote, is_japan, remote_scope = classify_location(location_name)
        

        job_data = {
            "company": company_name,
            "external_id": str(job["id"]),
            "title": job["title"],
            "location": location_name,
            "url": job["absolute_url"],
            "seniority": seniority,
            "function": function,
            "region": region,
            "is_remote": is_remote,
            "is_japan": is_japan,
            "last_seen_at": now.isoformat()
        }

        supabase.table("jobs").upsert(
            job_data,
            on_conflict="company,external_id"
        ).execute()

def scrape_ashby(company_slug, company_name):
    url = f"https://api.ashbyhq.com/posting-api/job-board/{company_slug}"
    now = datetime.now(UTC)
    response = requests.get(url)
    data = response.json()

    jobs = data.get("jobs", [])

    print(f"Found {len(jobs)} jobs for {company_name}")

    for job in jobs:
        title = job.get("title")
        location_name = ""

        if isinstance(job.get("location"), dict):
            location_name = job["location"].get("name", "")
        elif isinstance(job.get("location"), str):
            location_name = job.get("location", "")

        seniority, function = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        job_data = {
            "company": company_name,
            "external_id": job["id"],
            "title": title,
            "location": location_name,
            "url": job.get("applyUrl"),
            "seniority": seniority,
            "function": function,
            "region": region,
            "is_remote": is_remote,
            "is_japan": is_japan,
            "last_seen_at": now.isoformat()
        }

        supabase.table("jobs").upsert(
            job_data,
            on_conflict="company,external_id"
        ).execute()

def scrape_smartrecruiters(company_slug, company_name):
    url = f"https://api.smartrecruiters.com/v1/companies/{company_slug}/postings"
    now = datetime.now(UTC)
    response = requests.get(url)
    data = response.json()

    jobs = data.get("content", [])

    print(f"Found {len(jobs)} jobs for {company_name}")

    for job in jobs:
        title = job.get("name")
        loc = job.get("location", {})

        city = loc.get("city", "")
        region_name = loc.get("region", "")
        country = loc.get("country", "")

        location_name = " ".join([city, region_name, country])

        seniority, function = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        job_data = {
            "company": company_name,
            "external_id": job["id"],
            "title": title,
            "location": location_name,
            "url": job.get("ref"),
            "seniority": seniority,
            "function": function,
            "region": region,
            "is_remote": is_remote,
            "is_japan": is_japan,
            "last_seen_at": now.isoformat()
        }

        supabase.table("jobs").upsert(
            job_data,
            on_conflict="company,external_id"
        ).execute()

def scrape_workday(company_slug, company_name, location_id):
    subdomain, tenant = company_slug.split("|")

    url = f"https://{subdomain}.wd5.myworkdayjobs.com/wday/cxs/{subdomain}/{tenant}/jobs"
    now = datetime.now(UTC)

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": f"https://{subdomain}.wd5.myworkdayjobs.com",
        "Referer": f"https://{subdomain}.wd5.myworkdayjobs.com/"
    }

    payload = {
        "appliedFacets": {
            "locations": [location_id]
        },
        "limit": 20,
        "offset": 0,
        "searchText": ""
    }

    MAX_OFFSET = 200
    total_jobs = 0

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
            print(response.text)
            break

        data = response.json()
        jobs = data.get("jobPostings", [])

        if not jobs:
            break

        print(f"Fetched {len(jobs)} jobs at offset {payload['offset']}")

        for job in jobs:
            location_name = job.get("locationsText", "")

            seniority, function = classify_job(job.get("title", ""))
            region, is_remote, is_japan, remote_scope = classify_location(location_name)

            job_data = {
                "company": company_name,
                "external_id": str(job.get("id")),
                "title": job.get("title"),
                "location": location_name,
                "url": f"https://{subdomain}.wd5.myworkdayjobs.com/ja-JP/{tenant}/job/{job.get('externalPath')}",
                "seniority": seniority,
                "function": function,
                "region": region,
                "is_remote": is_remote,
                "is_japan": is_japan,
                "last_seen_at": now.isoformat()
            }

            supabase.table("jobs").upsert(
                job_data,
                on_conflict="company,external_id"
            ).execute()

            total_jobs += 1

        payload["offset"] += 20

    print(f"Total jobs found for {company_name}: {total_jobs}")

if __name__ == "__main__":
    # Greenhouse
    scrape_greenhouse("brave", "Brave")
    scrape_greenhouse("gitlab", "GitLab")

    # Ashby
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