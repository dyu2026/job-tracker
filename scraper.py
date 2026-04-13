"""
Job scrapers → Supabase (jobs + linkedin_posts).

Drop-in companion to scraper.py: same external setup (utils, supabase_client, .env).
Run: python job_scraper_sync.py
"""

from __future__ import annotations

import json
import platform
import random
import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta, timezone
from urllib.parse import urlparse

import feedparser
import requests
from bs4 import BeautifulSoup

from supabase_client import supabase
from utils import classify_job, classify_location

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT_S = 20
SAFE_REQUEST_MAX_RETRIES = 3

REMOTE_SCOPES_INCLUDED = frozenset({"global", "apac", "asia", "japan"})

LINKEDIN_INC_KEYWORDS = [
    "hiring", "opportunity", "job", "recruitment", "talent",
    "bilingual", "director", "career", "positions", "募集",
    "roles", "role", "we are hiring", "join our team",
]

LINKEDIN_EXC_KEYWORDS = [
    "excited to announce", "i’m happy to share", "started a new position",
    "looking for a new role", "i am looking", "please help me find", "I'm joining",
]

# ---------------------------------------------------------------------------
# Scrape logging ([Company] lines). LinkedIn + main() OS line stay unchanged.
# ---------------------------------------------------------------------------


def log_start(company: str, source: str) -> None:
    print(f"[{company}] START ({source})")


def log_error(company: str, message: str) -> None:
    print(f"[{company}] ERROR: {message}")


def log_page(
    company: str,
    *,
    offset: int | None = None,
    start: int | None = None,
    raw_in_page: int | None = None,
    relevant_in_page: int | None = None,
    api_total: int | None = None,
    extra: str | None = None,
) -> None:
    parts = []
    if offset is not None:
        parts.append(f"offset={offset}")
    if start is not None:
        parts.append(f"start={start}")
    if api_total is not None:
        parts.append(f"api_total={api_total}")
    if raw_in_page is not None:
        parts.append(f"raw_in_page={raw_in_page}")
    if relevant_in_page is not None:
        parts.append(f"relevant_in_page={relevant_in_page}")
    if extra:
        parts.append(extra)
    print(f"[{company}] Page | " + " | ".join(parts))


def log_stop(company: str, reason: str) -> None:
    print(f"[{company}] Stop: {reason}")


def log_summary(company: str, raw_listings: int, relevant_stored: int) -> None:
    print(f"[{company}] Summary: raw_listings={raw_listings} | relevant_stored={relevant_stored}")


def log_success(company: str) -> None:
    print(f"[{company}] SUCCESS")


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def safe_request(method: str, url: str, **kwargs) -> requests.Response | None:
    """GET/POST with retries; returns None if all attempts fail."""
    kwargs.setdefault("timeout", REQUEST_TIMEOUT_S)

    for attempt in range(SAFE_REQUEST_MAX_RETRIES):
        try:
            response = requests.request(method, url, **kwargs)
            if response.status_code == 200:
                return response
            if response.status_code in (500, 502, 503):
                wait = 3 * (attempt + 1)
                print(f"[HTTP] status={response.status_code}, retry {attempt + 1}, waiting {wait}s")
                time.sleep(wait)
                continue
        except requests.exceptions.RequestException as e:
            print(f"[HTTP] Request error: {e}")

        time.sleep(2 * (attempt + 1))

    return None


# ---------------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------------


def upsert_job(job_data: dict) -> None:
    now = datetime.now(UTC).isoformat()

    existing = (
        supabase.table("jobs")
        .select("first_seen_at")
        .eq("company", job_data["company"])
        .eq("external_id", job_data["external_id"])
        .execute()
    )

    if existing.data:
        first_seen = existing.data[0]["first_seen_at"]
    else:
        first_seen = now

    job_data["first_seen_at"] = first_seen
    job_data["last_seen_at"] = now
    job_data["is_active"] = True

    supabase.table("jobs").upsert(job_data, on_conflict="company,external_id").execute()


def mark_removed_jobs(company_name: str, seen_ids: set) -> None:
    if not seen_ids:
        print(f"[{company_name}] Removal detection: skipped (no job IDs collected this run)")
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
        print(
            f"[{company_name}] Removal detection: marking {len(removed_ids)} "
            f"listing(s) inactive (no longer on board)"
        )
        supabase.table("jobs").update({"is_active": False}).in_(
            "external_id", list(removed_ids)
        ).eq("company", company_name).execute()
    else:
        print(f"[{company_name}] Removal detection: no change (all active DB IDs still on board)")


# ---------------------------------------------------------------------------
# Shared job row + geography filter (same rules as original scraper.py)
# ---------------------------------------------------------------------------


def job_row(
    company: str,
    external_id: str,
    title: str,
    location: str,
    url,
    seniority: str,
    role: str,
    region,
    is_remote: bool,
    is_japan: bool,
    **extra,
) -> dict:
    row = {
        "company": company,
        "external_id": external_id,
        "title": title,
        "location": location,
        "url": url,
        "seniority": seniority,
        "role": role,
        "region": region,
        "is_remote": is_remote,
        "is_japan": is_japan,
    }
    row.update(extra)
    return row


def include_job(
    is_japan: bool,
    remote_scope,
    location_name: str = "",
    *,
    allow_japan_tokyo_substring: bool = False,
) -> bool:
    if is_japan or remote_scope in REMOTE_SCOPES_INCLUDED:
        return True
    if allow_japan_tokyo_substring and location_name:
        low = location_name.lower()
        return "japan" in low or "tokyo" in low
    return False


# ---------------------------------------------------------------------------
# Next.js / __NEXT_DATA__
# ---------------------------------------------------------------------------


def _find_job_list_in_json(obj):
    if isinstance(obj, dict):
        for v in obj.values():
            result = _find_job_list_in_json(v)
            if result is not None:
                return result
    elif isinstance(obj, list):
        if not obj:
            return None
        if isinstance(obj[0], dict):
            keys = obj[0].keys()
            if (
                any(k in keys for k in ("title", "text", "name"))
                and any(k in keys for k in ("id", "jobId", "requisition_id"))
            ):
                return obj
        for item in obj:
            result = _find_job_list_in_json(item)
            if result is not None:
                return result
    return None


def scrape_nextjs_company(
    company_name: str,
    careers_url: str,
    json_page_path: str,
    locale: str = "en",
) -> None:
    log_start(company_name, "Next.js")

    try:
        res = requests.get(careers_url, timeout=REQUEST_TIMEOUT_S)
        res.raise_for_status()
    except Exception as e:
        log_error(company_name, f"failed to fetch careers page: {e}")
        return

    soup = BeautifulSoup(res.text, "html.parser")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not script_tag:
        log_error(company_name, "no __NEXT_DATA__ script on careers page")
        return

    next_data = json.loads(script_tag.string)
    build_id = next_data.get("buildId")
    if not build_id:
        log_error(company_name, "buildId missing in __NEXT_DATA__")
        return

    parsed = urlparse(careers_url)
    root = f"{parsed.scheme}://{parsed.netloc}"

    if company_name.lower() == "miro":
        json_url = f"{root}/careers/_next/data/{build_id}/{locale}/{json_page_path}.json"
    else:
        path = parsed.path.rstrip("/")
        base_path = "/".join(path.split("/")[:-1])
        json_url = f"{root}{base_path}/_next/data/{build_id}/{locale}/{json_page_path}.json"

    try:
        data_res = requests.get(json_url, timeout=REQUEST_TIMEOUT_S)
        data_res.raise_for_status()
        data = data_res.json()
    except Exception as e:
        log_error(company_name, f"failed to fetch or parse JSON ({json_url}): {e}")
        return

    jobs = _find_job_list_in_json(data)
    if not jobs:
        log_error(company_name, "no job list found in JSON after auto-detect")
        return

    raw_total = len(jobs)
    log_page(company_name, offset=0, raw_in_page=raw_total, extra="single JSON payload (no pagination offset)")
    seen_ids: set[str] = set()
    relevant = 0

    for job in jobs:
        title = job.get("title") or job.get("text") or job.get("name")
        external_id = job.get("id") or job.get("jobId") or job.get("requisition_id")

        location_obj = job.get("location")
        if isinstance(location_obj, dict):
            location_name = location_obj.get("name", "")
        else:
            location_name = location_obj or ""

        job_url = None
        if job.get("absolute_url"):
            if job["absolute_url"].startswith("/"):
                job_url = f"https://miro.com{job['absolute_url']}"
            else:
                job_url = job["absolute_url"]
        elif job.get("url"):
            job_url = job["url"]
        elif job.get("applyUrl"):
            job_url = job["applyUrl"]

        if not job_url:
            job_url = f"https://miro.com/careers/vacancy/{job.get('id')}"

        if not title or not external_id:
            continue

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        if not include_job(is_japan, remote_scope):
            continue

        external_id = str(external_id)
        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name,
                external_id,
                title,
                location_name,
                job_url,
                seniority,
                role,
                region,
                is_remote,
                is_japan,
            )
        )

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)


def scrape_miro() -> None:
    scrape_nextjs_company(
        company_name="Miro",
        careers_url="https://miro.com/careers/open-positions/",
        json_page_path="open-positions",
        locale="en",
    )


# ---------------------------------------------------------------------------
# Greenhouse
# ---------------------------------------------------------------------------


def scrape_greenhouse(company_slug: str, company_name: str) -> None:
    log_start(company_name, "Greenhouse")
    url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs"
    response = safe_request("GET", url)
    if not response:
        log_error(company_name, "no HTTP response from Greenhouse API after retries")
        return

    jobs = response.json().get("jobs", [])
    raw_total = len(jobs)
    log_page(company_name, offset=0, raw_in_page=raw_total, extra="single API response")
    seen_ids: set[str] = set()
    relevant = 0

    for job in jobs:
        location_name = ""
        if isinstance(job.get("location"), dict):
            location_name = job["location"].get("name", "")

        seniority, role = classify_job(job["title"])

        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        if not include_job(is_japan, remote_scope):
            continue

        external_id = str(job["id"])
        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name,
                external_id,
                job["title"],
                location_name,
                job["absolute_url"],
                seniority,
                role,
                region,
                is_remote,
                is_japan,
            )
        )

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)


# ---------------------------------------------------------------------------
# Ashby
# ---------------------------------------------------------------------------

def slugify(title: str) -> str:
    slug = title.lower()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"\s+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


def validate_url(url: str) -> bool:
    try:
        r = requests.get(url, timeout=5)

        if r.status_code != 200:
            return False

        content = r.text.lower()
        if "page not found" in content or "not found" in content:
            return False

        return True
    except:
        return False


def scrape_ashby(company_slug: str, company_name: str) -> None:
    log_start(company_name, "Ashby")
    url = f"https://api.ashbyhq.com/posting-api/job-board/{company_slug}"
    response = safe_request("GET", url)
    if not response:
        log_error(company_name, "no HTTP response from Ashby API after retries")
        return

    jobs = response.json().get("jobs", [])
    raw_total = len(jobs)
    log_page(company_name, offset=0, raw_in_page=raw_total, extra="single API response")
    seen_ids: set[str] = set()
    relevant = 0

    for job in jobs:
        title = job.get("title")
        location_name = ""

        if isinstance(job.get("location"), dict):
            location_name = job["location"].get("name", "")
        elif isinstance(job.get("location"), str):
            location_name = job.get("location", "")
        elif job.get("locations"):
            location_list = [loc.get("name", "") for loc in job["locations"]]
            location_name = "; ".join(location_list)

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        if not include_job(is_japan, remote_scope):
            continue

        external_id = str(job["id"])
        seen_ids.add(external_id)
        relevant += 1

        # --- URL LOGIC (Cursor-specific) ---
        apply_url = job.get("applyUrl")

        if company_slug == "cursor":
            # 1. Try Cursor slug page (PRIMARY)
            slug = slugify(title or "")
            cursor_url = f"https://cursor.com/ja/careers/{slug}"

            if validate_url(cursor_url):
                apply_url = cursor_url
            else:
                # 2. fallback to API applyUrl if valid
                if apply_url and validate_url(apply_url):
                    pass  # keep apply_url
                else:
                    # 3. final fallback (likely soft 404 but safe)
                    apply_url = f"https://jobs.ashbyhq.com/{company_slug}/{external_id}"

        # --- default behavior for other Ashby companies ---
        else:
            if not apply_url:
                apply_url = f"https://jobs.ashbyhq.com/{company_slug}/{external_id}"

        upsert_job(
            job_row(
                company_name,
                external_id,
                title,
                location_name,
                apply_url,
                seniority,
                role,
                region,
                is_remote,
                is_japan,
            )
        )

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)


# ---------------------------------------------------------------------------
# SmartRecruiters
# ---------------------------------------------------------------------------


def scrape_smartrecruiters(company_slug: str, company_name: str) -> None:
    log_start(company_name, "SmartRecruiters")
    all_jobs = []
    offset = 0
    limit = 100

    while True:
        url = (
            f"https://api.smartrecruiters.com/v1/companies/{company_slug}/postings"
            f"?limit={limit}&offset={offset}"
        )
        response = safe_request("GET", url)
        if not response:
            log_error(company_name, f"no HTTP response at API offset={offset} after retries")
            return

        jobs = response.json().get("content", [])
        if not jobs:
            log_stop(company_name, f"empty batch at offset={offset} (pagination end)")
            break

        log_page(company_name, offset=offset, raw_in_page=len(jobs))
        all_jobs.extend(jobs)
        offset += limit

    raw_total = len(all_jobs)
    seen_ids: set[str] = set()
    relevant = 0

    for job in all_jobs:
        title = job.get("name", "")
        loc = job.get("location") or {}
        city = loc.get("city", "")
        region_name = loc.get("region", "")
        country = loc.get("country", "")
        location_name = ", ".join(p for p in (city, region_name, country) if p)

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        if not include_job(
            is_japan,
            remote_scope,
            location_name,
            allow_japan_tokyo_substring=True,
        ):
            continue

        job_id = job.get("id")
        external_id = str(job_id)
        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name,
                external_id,
                title,
                location_name,
                f"https://jobs.smartrecruiters.com/{company_slug.capitalize()}/{job_id}",
                seniority,
                role,
                region,
                is_remote,
                is_japan,
            )
        )

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)


# ---------------------------------------------------------------------------
# Workday
# ---------------------------------------------------------------------------


def scrape_workday(
    company_slug: str,
    company_name: str,
    location_ids=None,
    facet: str = "locations",
) -> None:
    parts = company_slug.split("|")

    if len(parts) == 3:
        subdomain, tenant, cluster = parts
    else:
        subdomain, tenant = parts
        cluster = "wd5"

    api_url = (
        f"https://{subdomain}.{cluster}.myworkdayjobs.com/wday/cxs/"
        f"{subdomain}/{tenant}/jobs"
    )

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": f"https://{subdomain}.{cluster}.myworkdayjobs.com",
        "Referer": f"https://{subdomain}.{cluster}.myworkdayjobs.com/en-US/{tenant}",
    }

    page_size = 20
    max_offset = 200

    payload = {
        "limit": page_size,
        "offset": 0,
        "searchText": "",
        "appliedFacets": {},
    }

    if location_ids:
        if not isinstance(location_ids, list):
            location_ids = [location_ids]
        payload["appliedFacets"][facet] = location_ids

    seen_ids: set = set()
    total_jobs = 0
    raw_listings_total = 0
    exited_on_http_error = False

    log_start(company_name, "Workday")

    def is_japan_override(location_name: str, external_path) -> bool:
        if location_name.lower() in ("2 locations", "multiple locations"):
            if external_path and "japan" in external_path.lower():
                return True
        return False

    while True:
        cur_offset = payload["offset"]

        if cur_offset > max_offset:
            log_stop(company_name, f"safety cap offset>{max_offset}")
            break

        response = safe_request("POST", api_url, json=payload, headers=headers)
        if not response:
            log_error(company_name, "no HTTP response from Workday API after retries")
            exited_on_http_error = True
            break

        if response.status_code != 200:
            log_error(company_name, f"Workday API HTTP status {response.status_code}")
            exited_on_http_error = True
            break

        data = response.json()
        jobs = data.get("jobPostings", [])
        total = data.get("total")
        raw_listings_total += len(jobs)

        if not jobs:
            log_stop(company_name, f"no job postings at offset={cur_offset}")
            break

        new_ids = {job.get("externalPath") for job in jobs}

        if new_ids.issubset(seen_ids):
            log_stop(company_name, "duplicate page (same IDs as prior page)")
            break

        filtered_count = 0

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

            seniority, role = classify_job(title)
            region, is_remote, is_japan, remote_scope = classify_location(location_name)

            if override_japan:
                is_japan = True
                remote_scope = "japan"

            if not include_job(is_japan, remote_scope):
                continue

            filtered_count += 1

            upsert_job(
                job_row(
                    company_name,
                    external_path,
                    title,
                    location_name,
                    f"https://{subdomain}.{cluster}.myworkdayjobs.com/en-US/{tenant}{external_path}",
                    seniority,
                    role,
                    region,
                    is_remote,
                    is_japan,
                )
            )
            total_jobs += 1

        log_page(
            company_name,
            offset=cur_offset,
            api_total=total,
            raw_in_page=len(jobs),
            relevant_in_page=filtered_count,
        )

        seen_ids.update(new_ids)

        if len(jobs) < page_size:
            log_stop(company_name, "last page (batch smaller than page size)")
            break

        if filtered_count == 0:
            log_stop(company_name, "no geography-relevant jobs on this page")
            break

        if total and payload["offset"] >= total:
            log_stop(company_name, "offset reached API total count")
            break

        payload["offset"] += page_size
        time.sleep(random.uniform(1.0, 2.5))

    log_summary(company_name, raw_listings_total, total_jobs)
    mark_removed_jobs(company_name, seen_ids)
    if not exited_on_http_error:
        log_success(company_name)


# ---------------------------------------------------------------------------
# Lever
# ---------------------------------------------------------------------------


def scrape_lever(company_slug: str, company_name: str) -> None:
    log_start(company_name, "Lever")
    url = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"
    response = safe_request("GET", url)

    if not response:
        log_error(company_name, "no HTTP response from Lever API after retries")
        return

    jobs = response.json()
    raw_total = len(jobs)
    log_page(company_name, offset=0, raw_in_page=raw_total, extra="single API response")
    seen_ids: set[str] = set()
    relevant = 0

    for job in jobs:
        title = job.get("text", "")
        external_id = job.get("id")
        categories = job.get("categories", {})
        location_name = categories.get("location", "") or ""
        workplace_type = job.get("workplaceType", "")

        if workplace_type == "remote" and "remote" not in location_name.lower():
            location_name = f"Remote, {location_name}".strip(", ")

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        if not include_job(is_japan, remote_scope):
            continue

        external_id = str(external_id)
        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name,
                external_id,
                title,
                location_name,
                job.get("hostedUrl"),
                seniority,
                role,
                region,
                is_remote,
                is_japan,
                remote_scope=remote_scope,
            )
        )

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)


# ---------------------------------------------------------------------------
# Monday.com
# ---------------------------------------------------------------------------


def scrape_monday(company_name: str = "monday.com") -> None:
    log_start(company_name, "Monday.com / __NEXT_DATA__")
    url = "https://monday.com/careers"

    response = safe_request("GET", url)
    if not response:
        log_error(company_name, "no HTTP response for careers page after retries")
        return

    soup = BeautifulSoup(response.text, "html.parser")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not script_tag:
        log_error(company_name, "no __NEXT_DATA__ script on careers page")
        return

    data = json.loads(script_tag.string)
    dynamic_data = data.get("props", {}).get("pageProps", {}).get("dynamicData", {})
    if not dynamic_data:
        log_error(company_name, "dynamicData missing in pageProps")
        return

    container_key = list(dynamic_data.keys())[0]
    positions = dynamic_data[container_key].get("positions", [])

    raw_total = len(positions)
    log_page(company_name, offset=0, raw_in_page=raw_total, extra="single embedded payload")
    seen_ids: set[str] = set()
    relevant = 0

    for job in positions:
        title = job.get("name")
        external_id = job.get("uid")
        if not title or not external_id:
            continue

        location_name = job.get("location", {}).get("name", "")
        job_url = job.get("url_active_page")

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        if not include_job(is_japan, remote_scope):
            continue

        external_id = str(external_id)
        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name,
                external_id,
                title,
                location_name,
                job_url,
                seniority,
                role,
                region,
                is_remote,
                is_japan,
            )
        )

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)


# ---------------------------------------------------------------------------
# eightfold.ai
# ---------------------------------------------------------------------------


def scrape_eightfold(company_slug: str, company_name: str, location: str, pid: str) -> None:
    log_start(company_name, "Eightfold")
    base_url = f"https://{company_slug}.eightfold.ai/api/apply/v2/jobs"
    page_size = 10
    max_offset = 200
    start = 0
    total_jobs = 0
    raw_listings_total = 0
    seen_ids: set = set()
    exited_on_http_error = False

    while True:
        if start > max_offset:
            log_stop(company_name, f"safety cap start>{max_offset}")
            break

        params = {
            "domain": f"{company_slug}.com",
            "start": start,
            "num": page_size,
            "location": location,
            "pid": pid,
            "sort_by": "relevance",
            "hl": "en",
            "triggerGoButton": "false",
        }

        r = safe_request(
            "GET",
            base_url,
            params=params,
            headers={"User-Agent": "Mozilla/5.0"},
        )

        if not r or r.status_code != 200:
            log_error(
                company_name,
                f"Eightfold API: {getattr(r, 'status_code', 'no response')}",
            )
            exited_on_http_error = True
            break

        data = r.json()
        jobs = data.get("positions", [])
        if not jobs:
            log_stop(company_name, f"empty batch at start={start}")
            break

        raw_listings_total += len(jobs)
        page_relevant = 0

        for job in jobs:
            title = job.get("name")
            location_name = job.get("location")
            external_id = job.get("ats_job_id")
            job_url = job.get("canonicalPositionUrl")

            if not external_id:
                continue

            seen_ids.add(external_id)

            seniority, role = classify_job(title)
            region, is_remote, is_japan, remote_scope = classify_location(location_name)

            if not include_job(is_japan, remote_scope):
                continue

            upsert_job(
                job_row(
                    company_name,
                    external_id,
                    title,
                    location_name,
                    job_url,
                    seniority,
                    role,
                    region,
                    is_remote,
                    is_japan,
                )
            )
            total_jobs += 1
            page_relevant += 1

        log_page(
            company_name,
            start=start,
            raw_in_page=len(jobs),
            relevant_in_page=page_relevant,
        )

        if len(jobs) < page_size:
            log_stop(company_name, "last page (batch smaller than page size)")
            break

        start += page_size

    log_summary(company_name, raw_listings_total, total_jobs)
    mark_removed_jobs(company_name, seen_ids)
    if not exited_on_http_error:
        log_success(company_name)


# ---------------------------------------------------------------------------
# BambooHR
# ---------------------------------------------------------------------------


def scrape_bamboohr(subdomain: str, company_name: str) -> None:
    log_start(company_name, "BambooHR")
    url = f"https://{subdomain}.bamboohr.com/careers/list"

    response = safe_request("GET", url)
    if not response:
        log_error(company_name, "no HTTP response from BambooHR list endpoint after retries")
        return

    try:
        data = response.json()
    except Exception as e:
        log_error(company_name, f"invalid JSON from BambooHR: {e}")
        return

    jobs = []
    if isinstance(data, list):
        jobs = data
    elif isinstance(data, dict):
        if "result" in data:
            jobs = data["result"]
        elif "jobs" in data:
            jobs = data["jobs"]

    raw_total = len(jobs)
    log_page(company_name, offset=0, raw_in_page=raw_total, extra="single list response")
    seen_ids: set[str] = set()
    relevant = 0

    for job in jobs:
        title = (
            job.get("jobTitle")
            or job.get("jobOpeningName")
            or job.get("title")
            or job.get("name")
        )
        external_id = job.get("id") or job.get("jobId")
        if not title or not external_id:
            continue

        external_id = str(external_id)

        location_name = ""
        location = job.get("location")
        if isinstance(location, dict):
            city = location.get("city")
            state = location.get("state")
            parts = [p for p in (city, state) if p]
            if parts:
                location_name = ", ".join(parts)
        elif isinstance(location, str):
            location_name = location

        if not location_name:
            location_name = "Remote / Unknown"

        job_url = f"https://{subdomain}.bamboohr.com/careers/{external_id}"

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope = classify_location(location_name)

        if not include_job(is_japan, remote_scope):
            continue

        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name,
                external_id,
                title,
                location_name,
                job_url,
                seniority,
                role,
                region,
                is_remote,
                is_japan,
            )
        )

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)


# ---------------------------------------------------------------------------
# Netflix (Japan)
# ---------------------------------------------------------------------------


def scrape_netflix() -> None:
    company_name = "Netflix"
    log_start(company_name, "Netflix jobs API")
    base_url = "https://explore.jobs.netflix.net/api/apply/v2/jobs"
    params = {
        "domain": "netflix.com",
        "pid": "790302851017",
        "location": "Tokyo, Japan",
        "num": 10,
        "sort_by": "relevance",
    }

    start = 0
    seen_ids: set[str] = set()
    raw_listings_total = 0
    relevant_total = 0
    exited_on_http_error = False

    while True:
        params["start"] = start

        try:
            r = safe_request("GET", base_url, params=params)
            if not r:
                log_error(company_name, "no HTTP response from Netflix API after retries")
                exited_on_http_error = True
                break
            data = r.json()
        except Exception as e:
            log_error(company_name, f"Netflix request or JSON parse failed: {e}")
            log_summary(company_name, raw_listings_total, relevant_total)
            mark_removed_jobs(company_name, seen_ids)
            return

        jobs = data.get("positions", [])
        if not jobs:
            log_stop(company_name, f"empty batch at start={start}")
            break

        raw_listings_total += len(jobs)
        page_stored = 0

        for job in jobs:
            job_id = job.get("id")
            title = job.get("name")
            location_name = job.get("location", "")

            if not job_id or not title:
                continue

            external_id = str(job_id)
            seen_ids.add(external_id)

            seniority, role = classify_job(title)
            region, is_remote, is_japan, remote_scope = classify_location(location_name)

            job_url = (
                f"https://explore.jobs.netflix.net/careers/apply"
                f"?domain=netflix.com&pid={external_id}"
            )

            upsert_job(
                job_row(
                    company_name,
                    external_id,
                    title,
                    location_name,
                    job_url,
                    seniority,
                    role,
                    region,
                    is_remote,
                    is_japan,
                )
            )
            page_stored += 1

        log_page(
            company_name,
            start=start,
            raw_in_page=len(jobs),
            relevant_in_page=page_stored,
        )
        relevant_total += page_stored

        start += params["num"]

    log_summary(company_name, raw_listings_total, relevant_total)
    mark_removed_jobs(company_name, seen_ids)
    if not exited_on_http_error:
        log_success(company_name)

# ---------------------------------------------------------------------------
# Uber
# ---------------------------------------------------------------------------

def scrape_uber(company_name: str = "Uber") -> None:
    log_start(company_name, "Uber API")

    url = "https://www.uber.com/api/loadSearchJobsResults"
    params = {"localeCode": "ja-JP"}

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://www.uber.com",
        "Referer": "https://www.uber.com/jp/ja/careers/list/",
        "x-csrf-token": "x",
    }

    page = 0
    limit = 10
    raw_total = 0
    relevant_total = 0
    seen_ids: set[str] = set()

    while True:
        payload = {
            "limit": limit,
            "page": page,
            "params": {
                "location": [{"country": "JPN", "city": "Tokyo"}]
            },
        }

        response = safe_request(
            "POST",
            url,
            params=params,
            json=payload,
            headers=headers,
        )

        if not response:
            log_error(company_name, f"no response at page={page}")
            break

        try:
            data = response.json()
        except Exception as e:
            log_error(company_name, f"JSON parse failed at page={page}: {e}")
            break

        results = data.get("data", {}).get("results", [])
        if not results:
            log_stop(company_name, f"empty results at page={page}")
            break

        raw_total += len(results)
        page_relevant = 0

        for job in results:
            external_id = str(job.get("id"))
            title = job.get("title")
            location_name = job.get("location", {}).get("name", "Tokyo, Japan")

            if not external_id or not title:
                continue

            job_url = f"https://www.uber.com/global/en/careers/list/{external_id}/"

            seniority, role = classify_job(title)
            region, is_remote, is_japan, remote_scope = classify_location(location_name)

            if not include_job(is_japan, remote_scope):
                continue

            seen_ids.add(external_id)
            page_relevant += 1

            upsert_job(
                job_row(
                    company_name,
                    external_id,
                    title,
                    location_name,
                    job_url,
                    seniority,
                    role,
                    region,
                    is_remote,
                    is_japan,
                )
            )

        log_page(
            company_name,
            offset=page,
            raw_in_page=len(results),
            relevant_in_page=page_relevant,
        )

        relevant_total += page_relevant

        # stop if last page
        if len(results) < limit:
            log_stop(company_name, "last page (results < limit)")
            break

        page += 1
        time.sleep(random.uniform(1.0, 2.0))

# ---------------------------------------------------------------------------
# LinkedIn (Google News RSS)
# ---------------------------------------------------------------------------


def linkedin_matches_filters(text: str) -> bool:
    text = text.lower()
    has_hiring_signal = any(k in text for k in LINKEDIN_INC_KEYWORDS)
    is_not_seeker = not any(k in text for k in LINKEDIN_EXC_KEYWORDS)
    return has_hiring_signal and is_not_seeker


def extract_linkedin_url(summary: str):
    match = re.search(r'href="(https://www.linkedin.com/posts/[^"]+)"', summary)
    return match.group(1) if match else None


def scrape_linkedin() -> None:
    days_to_pull = 7
    query = (
        'site:linkedin.com/posts (hiring OR recruiting OR "now hiring" OR "募集" '
        f'OR "求人" OR newopportunity) Japan when:{days_to_pull}d'
    )
    encoded_query = urllib.parse.quote(query)
    rss_url = (
        f"https://news.google.com/rss/search?q={encoded_query}"
        f"&hl=en-JP&gl=JP&ceid=JP:en"
    )

    feed = feedparser.parse(rss_url)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_to_pull)

    print(f"LinkedIn feed entries: {len(feed.entries)}")

    supabase.table("linkedin_posts").delete().lt(
        "published_at", cutoff.isoformat()
    ).execute()

    for entry in feed.entries:
        if not hasattr(entry, "published_parsed") or entry.published_parsed is None:
            continue

        published_utc = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        if published_utc < cutoff:
            continue

        title = entry.title
        summary = entry.get("summary", "")
        combined_text = title + " " + summary

        if not linkedin_matches_filters(combined_text):
            continue

        url = extract_linkedin_url(summary) or entry.link
        snippet = re.sub("<.*?>", "", summary)

        supabase.table("linkedin_posts").upsert(
            {
                "title": title,
                "url": url,
                "snippet": snippet,
                "published_at": published_utc.isoformat(),
            },
            on_conflict="url",
        ).execute()

    print("LinkedIn RSS scrape complete")


# ---------------------------------------------------------------------------
# Task runner + main
# ---------------------------------------------------------------------------


def _task_label(func, args: tuple) -> str:
    """Human-readable name for logs (same rules as legacy scraper.py run_task)."""
    if len(args) >= 2:
        return args[1]
    if len(args) == 1 and isinstance(args[0], str):
        return args[0]
    return func.__name__


def run_task(func, *args) -> None:
    label = _task_label(func, args)

    for attempt in range(2):
        try:
            print(f"🚀 Starting {label}")
            func(*args)
            print(f"✅ Success {label}")
            return
        except Exception as e:
            print(f"⚠️ Retry {attempt + 1}/2 for {label}: {e}")
            time.sleep(2 * (attempt + 1))

    print(f"❌ Failed {label} after 2 attempts")


SCRAPER_TASKS: list[tuple] = [
    (scrape_miro,),
    (scrape_monday,),
    (scrape_greenhouse, "nansen", "Nansen"),
    (scrape_greenhouse, "brave", "Brave"),
    (scrape_greenhouse, "gitlab", "GitLab"),
    (scrape_greenhouse, "figma", "Figma"),
    (scrape_greenhouse, "stripe", "Stripe"),
    (scrape_greenhouse, "anthropic", "Anthropic"),
    (scrape_greenhouse, "nothing", "Nothing"),
    (scrape_greenhouse, "phrase", "Phrase"),
    (scrape_greenhouse, "okta", "Okta"),
    (scrape_greenhouse, "datadog", "Datadog"),
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
    (scrape_greenhouse, "wrike", "Wrike"),
    (scrape_greenhouse, "gumgum", "GumGum"),
    (scrape_greenhouse, "discord", "Discord"),
    (scrape_ashby, "notion", "Notion"),
    (scrape_ashby, "duck-duck-go", "DuckDuckGo"),
    (scrape_ashby, "deepl", "DeepL"),
    (scrape_ashby, "lilt-corporate", "Lilt"),
    (scrape_ashby, "perplexity", "Perplexity"),
    (scrape_ashby, "sierra", "Sierra"),
    (scrape_ashby, "zapier", "Zapier"),
    (scrape_ashby, "supabase", "Supabase"),
    (scrape_ashby, "substack", "Substack"),
    (scrape_ashby, "kraken.com", "Kraken"),
    (scrape_ashby, "snowflake", "Snowflake"),
    (scrape_ashby, "cursor", "Cursor"),
    (scrape_smartrecruiters, "Canva", "Canva"),
    (scrape_smartrecruiters, "wise", "Wise"),
    (
        scrape_workday,
        "disney|disneycareer",
        "Disney",
        [
            "4f84d9e8a09701011a72254a71290000",
            "4f84d9e8a09701011a5995833ead0000",
        ],
    ),
    (scrape_workday, "workday|Workday", "Workday", "9248082dd0ba104584ac4b3d9356363b"),
    (
        scrape_workday,
        "nvidia|NVIDIAExternalCareerSite",
        "NVIDIA",
        [
            "91336993fab910af6d6f9a47b91cc19e",
            "b00b3256ed551015d42d2bebe06b02b7",
        ],
    ),
    (
        scrape_workday,
        "mastercard|CorporateCareers|wd1",
        "Mastercard",
        "8eab563831bf10acbe1cda510e782135",
    ),
    (
        scrape_workday,
        "warnerbros|global|wd5",
        "Warner Bros",
        "8b705da2becf43cfaccc091da0988ab2",
        "locationCountry",
    ),
    (
        scrape_workday,
        "zoom|zoom",
        "Zoom",
        "8b705da2becf43cfaccc091da0988ab2",
        "locationCountry",
    ),
    (
        scrape_workday,
        "cisco|Cisco_Careers",
        "Cisco",
        [
            "1cf8ea09530d1001f4c5c40ec3720000",
            "f5adf8182d281001f842e5b0f10b0000",
            "662e524adea41001f4d1611409bb0000",
        ],
    ),
    (
        scrape_eightfold,
        "aexp",
        "American Express",
        "Minato-ku, Tokyo, Japan",
        "39549064",
    ),
    (scrape_lever, "spotify", "Spotify"),
    (scrape_lever, "superside", "Superside"),
    (scrape_lever, "kinsta", "Kinsta"),
    (scrape_bamboohr, "lottiefiles", "LottieFiles"),
    (scrape_netflix,),
    (scrape_uber,),
    (scrape_linkedin,),
]


def main() -> None:
    start_time = time.time()

    system = platform.system()
    max_workers = 1 if system == "Windows" else 3
    print(f"Detected OS: {system} | Using {max_workers} worker(s)")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(run_task, *task) for task in SCRAPER_TASKS]
        for future in as_completed(futures):
            future.result()

    runtime = round(time.time() - start_time, 2)
    print("\n✅ All scrapers completed")
    print(f"⏱ Total runtime: {runtime} seconds")


if __name__ == "__main__":
    main()
