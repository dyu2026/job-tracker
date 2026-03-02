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