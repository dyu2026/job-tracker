"""
Job scrapers → Supabase (jobs + linkedin_posts).

Drop-in companion to scraper.py: same external setup (utils, supabase_client, .env).
Run: python scraper.py
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
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT_S = 20
SAFE_REQUEST_MAX_RETRIES = 3
UNKNOWN_LOCATION = "Remote / Unknown"

REMOTE_SCOPES_INCLUDED = frozenset({"global", "apac", "asia", "japan"})

LINKEDIN_INC_KEYWORDS = [
    "hiring", "opportunity", "job", "recruitment", "talent",
    "bilingual", "director", "career", "positions", "募集",
    "roles", "role", "we are hiring", "join our team", "jobopportunity",
]

LINKEDIN_EXC_KEYWORDS = [
    "excited to announce", "i'm happy to share", "started a new position",
    "looking for a new role", "i am looking", "please help me find", "I'm joining",
]

# ---------------------------------------------------------------------------
# Logging helpers
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
# Supabase helpers
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

    first_seen = existing.data[0]["first_seen_at"] if existing.data else now

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
# Job row builder + geography filter
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
    remote_scope,
    is_valid: bool,
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
        "remote_scope": remote_scope,
        "is_valid": is_valid,
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


def enrich_and_validate_location(location_name: str) -> tuple:
    region, is_remote, is_japan, remote_scope = classify_location(location_name)
    is_valid = include_job(is_japan=is_japan, remote_scope=remote_scope, location_name=location_name)
    return region, is_remote, is_japan, remote_scope, is_valid


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

    res = safe_request("GET", careers_url)
    if not res:
        log_error(company_name, "failed to fetch careers page")
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
        location_name = location_obj.get("name", "") if isinstance(location_obj, dict) else str(location_obj or "")

        if job.get("absolute_url"):
            job_url = (
                f"https://miro.com{job['absolute_url']}"
                if job["absolute_url"].startswith("/")
                else job["absolute_url"]
            )
        elif job.get("url"):
            job_url = job["url"]
        elif job.get("applyUrl"):
            job_url = job["applyUrl"]
        else:
            job_url = f"https://miro.com/careers/vacancy/{job.get('id')}"

        if not title or not external_id:
            continue

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name)

        if not is_valid:
            continue

        external_id = str(external_id)
        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name, external_id, title, location_name, job_url,
                seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
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
        location_name = job["location"].get("name", "") if isinstance(job.get("location"), dict) else ""

        seniority, role = classify_job(job["title"])
        region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name)

        if not is_valid:
            continue

        external_id = str(job["id"])
        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name, external_id, job["title"], location_name, job["absolute_url"],
                seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
            )
        )

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)


# ---------------------------------------------------------------------------
# Ashby
# ---------------------------------------------------------------------------


def _slugify(title: str) -> str:
    slug = title.lower()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"\s+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


def _validate_url(url: str) -> bool:
    """HEAD-style check: returns True only if the URL resolves and isn't a soft 404."""
    try:
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            return False
        content = r.text.lower()
        return "page not found" not in content and "not found" not in content
    except Exception:
        return False


def _resolve_cursor_url(title: str, external_id: str, api_apply_url: str | None) -> str:
    """Return the best available URL for a Cursor job listing."""
    slug = _slugify(title or "")
    slug_url = f"https://cursor.com/ja/careers/{slug}"

    if _validate_url(slug_url):
        return slug_url
    if api_apply_url and _validate_url(api_apply_url):
        return api_apply_url
    return f"https://jobs.ashbyhq.com/cursor/{external_id}"


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
        external_id = str(job["id"])

        location_obj = job.get("location")
        if isinstance(location_obj, dict):
            location_name = location_obj.get("name", "")
        elif isinstance(location_obj, str):
            location_name = location_obj
        elif job.get("locations"):
            location_name = "; ".join(loc.get("name", "") for loc in job["locations"])
        else:
            location_name = ""

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name)

        if not is_valid:
            continue

        if company_slug == "cursor":
            apply_url = _resolve_cursor_url(title, external_id, job.get("applyUrl"))
        else:
            apply_url = job.get("applyUrl") or f"https://jobs.ashbyhq.com/{company_slug}/{external_id}"

        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name, external_id, title, location_name, apply_url,
                seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
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
        location_name = ", ".join(
            p for p in (loc.get("city", ""), loc.get("region", ""), loc.get("country", "")) if p
        )

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name)

        if not is_valid:
            continue

        external_id = str(job.get("id"))
        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name, external_id, title, location_name,
                f"https://jobs.smartrecruiters.com/{company_slug.capitalize()}/{external_id}",
                seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
            )
        )

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)


# ---------------------------------------------------------------------------
# Workday
# ---------------------------------------------------------------------------

def _parse_workday_slug(company_slug: str) -> tuple[str, str, str]:
    if "|" in company_slug:
        parts = [p.strip() for p in company_slug.split("|") if p.strip()]
    else:
        parts = [p.strip() for p in company_slug.split("-") if p.strip()]

    if len(parts) < 2:
        raise ValueError(f"Invalid Workday company_slug: {company_slug}")

    subdomain = parts[0]
    tenant = parts[1]
    cluster = parts[2] if len(parts) >= 3 else "wd5"
    return subdomain, tenant, cluster


def _extract_workday_locations_from_detail(detail_json: dict) -> list[str]:
    locations = []

    job = detail_json.get("jobPostingInfo", {})

    # Primary
    primary = job.get("location")
    if isinstance(primary, dict):
        name = primary.get("descriptor") or primary.get("name")
        if name:
            locations.append(name.strip())
    elif isinstance(primary, str):
        locations.append(primary.strip())

    # Additional
    extras = job.get("additionalLocations") or []
    for loc in extras:
        if isinstance(loc, dict):
            name = loc.get("descriptor") or loc.get("name")
            if name:
                locations.append(name.strip())
        elif isinstance(loc, str):
            locations.append(loc.strip())

    # Fallback
    if not locations:
        fallback = job.get("locationsText") or job.get("locationText")
        if fallback:
            locations.append(fallback.strip())

    return locations


def scrape_workday(
    company_slug: str,
    company_name: str,
    location_ids=None,
    facet: str = "locations",
) -> None:
    subdomain, tenant, cluster = _parse_workday_slug(company_slug)

    apiurl = f"https://{subdomain}.{cluster}.myworkdayjobs.com/wday/cxs/{subdomain}/{tenant}/jobs"

    session = requests.Session()

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": f"https://{subdomain}.{cluster}.myworkdayjobs.com",
        "Referer": f"https://{subdomain}.{cluster}.myworkdayjobs.com/en-US/{tenant}",
    }

    pagesize = 20
    maxoffset = 200

    payload = {
        "limit": pagesize,
        "offset": 0,
        "searchText": "",
        "appliedFacets": {}
    }

    if location_ids:
        payload["appliedFacets"][facet] = (
            location_ids if isinstance(location_ids, list) else [location_ids]
        )

    seenids: set[str] = set()
    totaljobs = 0
    rawlistingstotal = 0

    log_start(company_name, "Workday")

    no_relevant_pages = 0

    # 🔥 DEBUG counters
    detail_calls = 0

    while True:
        curoffset = payload["offset"]

        if curoffset > maxoffset:
            log_stop(company_name, f"safety cap offset>{maxoffset}")
            break

        # small jitter BEFORE request (important)
        time.sleep(random.uniform(0.2, 0.6))

        response = safe_request("POST", apiurl, json=payload, headers=headers)

        if not response:
            log_error(company_name, "no HTTP response from Workday API after retries")
            return

        data = response.json()
        jobs = data.get("jobPostings", [])
        total = data.get("total", 0)

        rawlistingstotal += len(jobs)

        if not jobs:
            log_stop(company_name, f"no job postings at offset={curoffset}")
            break

        # 🔥 EARLY PAGE SKIP (big win)
        if all(
            "japan" not in (job.get("locationsText") or "").lower()
            for job in jobs
        ):
            no_relevant_pages += 1
            payload["offset"] += pagesize
            continue

        # dedup
        page_ids = {
            str(job.get("externalPath"))
            for job in jobs
            if job.get("externalPath")
        }

        if page_ids.issubset(seenids):
            log_stop(company_name, "duplicate page detected")
            break

        seenids.update(page_ids)

        filteredcount = 0

        # 🔥 LIMIT detail calls per page (critical)
        detail_budget = 5

        for job in jobs:
            title = job.get("title")
            externalpath = job.get("externalPath")

            if not title or not externalpath:
                continue

            primary = job.get("location")
            extras = job.get("additionalLocations") or []
            locations_text = job.get("locationsText") or ""

            display_parts = []

            if isinstance(primary, dict):
                name = primary.get("descriptor") or primary.get("name")
                if name:
                    display_parts.append(name.strip())
            elif isinstance(primary, str):
                display_parts.append(primary.strip())

            for loc in extras:
                if isinstance(loc, str):
                    display_parts.append(loc.strip())
                elif isinstance(loc, dict):
                    name = loc.get("descriptor") or loc.get("name")
                    if name:
                        display_parts.append(name.strip())

            display_location = " / ".join(display_parts) if display_parts else locations_text.strip()
            filter_location = " ".join(display_parts).strip().lower()

            # 🔥 SMART detail trigger
            needs_detail = (
                not filter_location
                or "locations" in display_location.lower()
            )

            if needs_detail and detail_budget > 0:
                detail_budget -= 1
                detail_calls += 1

                detail = safe_request(
                    "GET",
                    f"https://{subdomain}.{cluster}.myworkdayjobs.com/wday/cxs/{subdomain}/{tenant}{externalpath}",
                    headers={"User-Agent": "Mozilla/5.0"},
                )

                if detail and detail.status_code == 200:
                    detail_json = detail.json()
                    detail_locations = _extract_workday_locations_from_detail(detail_json)

                    if detail_locations:
                        display_location = " / ".join(detail_locations)
                        filter_location = " ".join(detail_locations).lower()

            seniority, role = classify_job(title)

            region, is_remote, is_japan, remote_scope, is_valid = (
                enrich_and_validate_location(filter_location)
            )

            if not is_valid:
                continue

            filteredcount += 1
            totaljobs += 1

            upsert_job(
                job_row(
                    company_name,
                    str(externalpath),
                    title,
                    display_location,
                    f"https://{subdomain}.{cluster}.myworkdayjobs.com/en-US/{tenant}{externalpath}",
                    seniority,
                    role,
                    region,
                    is_remote,
                    is_japan,
                    remote_scope,
                    is_valid,
               )
            )

        log_page(
            company_name,
            offset=curoffset,
            api_total=total,
            raw_in_page=len(jobs),
            relevant_in_page=filteredcount,
            extra=f"detail_calls={detail_calls}"
        )

        # early exit
        if filteredcount == 0:
            no_relevant_pages += 1
        else:
            no_relevant_pages = 0

        if no_relevant_pages >= 2:
            log_stop(company_name, "2 consecutive irrelevant pages")
            break

        if len(jobs) < pagesize:
            log_stop(company_name, "last page")
            break

        if total and curoffset + pagesize >= total:
            log_stop(company_name, "reached total")
            break

        payload["offset"] += pagesize
    
    log_summary(company_name, rawlistingstotal, totaljobs)
    mark_removed_jobs(company_name, seenids)
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
        region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name)

        if not is_valid:
            continue

        external_id = str(external_id)
        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name, external_id, title, location_name, job.get("hostedUrl"),
                seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
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
        region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name)

        if not is_valid:
            continue

        external_id = str(external_id)
        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name, external_id, title, location_name, job_url,
                seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
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

        r = safe_request("GET", base_url, params=params, headers={"User-Agent": "Mozilla/5.0"})
        if not r or r.status_code != 200:
            log_error(company_name, f"Eightfold API: {getattr(r, 'status_code', 'no response')}")
            return

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
            region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name)

            if not is_valid:
                continue

            upsert_job(
                job_row(
                    company_name, external_id, title, location_name, job_url,
                    seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
                )
            )
            total_jobs += 1
            page_relevant += 1

        log_page(company_name, start=start, raw_in_page=len(jobs), relevant_in_page=page_relevant)

        if len(jobs) < page_size:
            log_stop(company_name, "last page (batch smaller than page size)")
            break

        start += page_size

    log_summary(company_name, raw_listings_total, total_jobs)
    mark_removed_jobs(company_name, seen_ids)
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

    if isinstance(data, list):
        jobs = data
    elif isinstance(data, dict):
        jobs = data.get("result") or data.get("jobs") or []
    else:
        jobs = []

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
        external_id = str(job.get("id") or job.get("jobId") or "")
        if not title or not external_id:
            continue

        location = job.get("location")
        if isinstance(location, dict):
            location_name = ", ".join(
                p for p in (location.get("city"), location.get("state")) if p
            )
        elif isinstance(location, str):
            location_name = location
        else:
            location_name = UNKNOWN_LOCATION

        if not location_name:
            location_name = UNKNOWN_LOCATION

        job_url = f"https://{subdomain}.bamboohr.com/careers/{external_id}"

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name)

        if not is_valid:
            continue

        seen_ids.add(external_id)
        relevant += 1

        upsert_job(
            job_row(
                company_name, external_id, title, location_name, job_url,
                seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
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

    while True:
        params["start"] = start

        try:
            r = safe_request("GET", base_url, params=params)
            if not r:
                log_error(company_name, "no HTTP response from Netflix API after retries")
                return
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
            region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name)

            if not is_valid:
                continue

            job_url = (
                f"https://explore.jobs.netflix.net/careers/apply"
                f"?domain=netflix.com&pid={external_id}"
            )

            upsert_job(
                job_row(
                    company_name, external_id, title, location_name, job_url,
                    seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
                )
            )
            page_stored += 1

        log_page(company_name, start=start, raw_in_page=len(jobs), relevant_in_page=page_stored)
        relevant_total += page_stored
        start += params["num"]

    log_summary(company_name, raw_listings_total, relevant_total)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)


# ---------------------------------------------------------------------------
# Uber
# ---------------------------------------------------------------------------


def scrape_uber(company_name: str = "Uber") -> None:
    log_start(company_name, "Uber API")

    url = "https://www.uber.com/api/loadSearchJobsResults"
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
            "params": {"location": [{"country": "JPN", "city": "Tokyo"}]},
        }

        response = safe_request(
            "POST", url,
            params={"localeCode": "ja-JP"},
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

            seniority, role = classify_job(title)
            region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name)

            if not is_valid:
                continue

            seen_ids.add(external_id)
            page_relevant += 1

            upsert_job(
                job_row(
                    company_name, external_id, title, location_name,
                    f"https://www.uber.com/global/en/careers/list/{external_id}/",
                    seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
                )
            )

        log_page(company_name, offset=page, raw_in_page=len(results), relevant_in_page=page_relevant)
        relevant_total += page_relevant

        if len(results) < limit:
            log_stop(company_name, "last page (results < limit)")
            break

        page += 1
        time.sleep(random.uniform(1.0, 2.0))

    log_summary(company_name, raw_total, relevant_total)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------

def scrape_meta(company_name: str = "Meta") -> None:

    log_start(company_name, "Meta GraphQL")

    seen_ids: set[str] = set()
    raw_total = 0
    relevant = 0

    # --- helpers ---
    def extract_meta_jobs(data: dict) -> list[dict]:
        jobs = []

        try:
            d = data.get("data", {})

            if "job_search_with_featured_jobs" not in d:
                return []

            all_jobs = d["job_search_with_featured_jobs"].get("all_jobs", [])

            for j in all_jobs:
                jobs.append({
                    "id": j.get("id"),
                    "title": j.get("title"),
                    "locations": j.get("locations", []),
                })

        except Exception as e:
            print(f"[Meta ERROR] extract failed: {e}")

        return jobs

    def normalize_meta_location(job: dict) -> str:
        locations = job.get("locations", [])

        if not locations:
            return ""

        names = []
        for loc in locations:
            if isinstance(loc, dict):
                name = loc.get("name", "")
            else:
                name = str(loc)

            if name:
                names.append(name)

        if not names:
            return ""

        # ✅ PRIORITY: return Japan location if present
        for name in names:
            if "Japan" in name:
                return name

        # fallback to first
        return names[0]

    # --- Playwright ---
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        def handle_response(response):
            nonlocal raw_total, relevant

            # Only care about GraphQL
            if "graphql" not in response.url:
                return

            try:
                data = response.json()
            except Exception:
                return

            if "job_search_with_featured_jobs" not in data.get("data", {}):
                return

            jobs = extract_meta_jobs(data)
            if not jobs:
                return

            print(f"[Meta DEBUG] Found {len(jobs)} jobs")

            raw_total += len(jobs)

            for job in jobs:
                title = job.get("title")
                external_id = job.get("id")

                if not title or not external_id:
                    continue

                external_id = str(external_id)

                if external_id in seen_ids:
                    continue
                seen_ids.add(external_id)

                location_name = normalize_meta_location(job)

                seniority, role = classify_job(title)
                region, is_remote, is_japan, remote_scope, is_valid = (
                    enrich_and_validate_location(location_name)
                )

                if not is_valid:
                    continue

                relevant += 1

                upsert_job(
                    job_row(
                        company_name,
                        external_id,
                        title,
                        location_name,
                        f"https://www.metacareers.com/jobs/{external_id}/",
                        seniority,
                        role,
                        region,
                        is_remote,
                        is_japan,
                        remote_scope,
                        is_valid,
                    )
                )

        page.on("response", handle_response)

        page.goto(
            "https://www.metacareers.com/jobsearch?offices[0]=Tokyo%2C%20Japan",
            wait_until="networkidle"
        )

        # wait for GraphQL responses
        page.wait_for_timeout(8000)

        browser.close()

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

# ---------------------------------------------------------------------------
# Wayve (FirstStage)
# ---------------------------------------------------------------------------

def scrape_wayve(company_name: str = "Wayve") -> None:

    log_start(company_name, "FirstStage")

    url = "https://wayve.firststage.co/jobs"

    response = safe_request(
        "GET",
        url,
        headers={"User-Agent": "Mozilla/5.0"},
    )

    if not response or response.status_code != 200:
        log_error(company_name, f"HTTP error: {getattr(response, 'status_code', 'no response')}")
        return

    soup = BeautifulSoup(response.text, "html.parser")

    links = soup.select("a[href*='/jobs/']")

    raw_total = len(links)
    relevant = 0
    seen_ids: set[str] = set()

    log_page(company_name, offset=0, raw_in_page=raw_total, extra="HTML parse")

    # --- parsing helper (from your tested minimal script) ---
    def split_title_location(text: str):
        text = " ".join(text.split())  # normalize whitespace

        # remove noise
        noise_patterns = [
            r"full time.*$",
            r"part time.*$",
            r"on-site.*$",
            r"remote.*$",
        ]
        for p in noise_patterns:
            text = re.sub(p, "", text, flags=re.IGNORECASE).strip()

        location_patterns = [
            r"(Tokyo, Japan)",
            r"(Yokohama, Japan)",
            r"(London, United Kingdom)",
            r"(Sunnyvale, California USA)",
            r"(Detroit, Michigan USA)",
            r"(Herzliya, Israel)",
            r"(Leonberg, Germany)",
            r"(Location Flexible)",
        ]

        for loc_pattern in location_patterns:
            match = re.search(loc_pattern + r"$", text)
            if match:
                location = match.group(1).strip()
                title = text[:match.start()].strip()
                title = title.rstrip(",- ").strip()
                return title, location

        return text.strip(), UNKNOWN_LOCATION

    # --- main loop ---
    for a in links:
        href = a.get("href")

        if not href or "/jobs/" not in href:
            continue

        # fix link
        if href.startswith("http"):
            job_url = href
        else:
            job_url = "https://wayve.firststage.co" + href

        # extract job id
        try:
            external_id = job_url.split("/jobs/")[1].split("/")[0]
        except Exception:
            continue

        if not external_id:
            continue

        raw_text = a.get_text(" ", strip=True)
        if not raw_text:
            continue

        title, location_name = split_title_location(raw_text)

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope, is_valid = (
            enrich_and_validate_location(location_name)
        )

        if not is_valid:
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
                remote_scope,
                is_valid,
            )
        )

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

# ---------------------------------------------------------------------------
# Waymo
# ---------------------------------------------------------------------------
    
def scrape_waymo(company_name: str = "Waymo") -> None:
    log_start(company_name, "Waymo HTML")

    url = "https://careers.withwaymo.com/jobs/search"
    params = [("country_codes[]", "JP")]

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://careers.withwaymo.com/",
        "Connection": "keep-alive",
    }

    # Warm session (avoids 202 bot response)
    session.get("https://careers.withwaymo.com/", headers=headers)

    response = session.get(url, params=params, headers=headers)

    if not response or response.status_code != 200:
        log_error(company_name, f"HTTP {getattr(response, 'status_code', 'no response')}")
        return

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(response.text, "html.parser")

    jobs = soup.select("article.job-search-results-card-col")

    raw_total = len(jobs)
    relevant = 0
    seen_ids: set[str] = set()

    log_page(company_name, offset=0, raw_in_page=raw_total, extra="HTML parse")

    for job in jobs:
        try:
            a = job.select_one("h3 a")
            if not a:
                continue

            title = a.get_text(strip=True)
            job_url = a["href"]

            # Use URL slug as stable ID
            external_id = job_url.rstrip("/").split("/")[-1]

            loc_el = job.select_one(".job-component-location span")
            location_name = loc_el.get_text(strip=True) if loc_el else "Tokyo, Japan"

            # Normalize for your pipeline
            if location_name == "Tokyo":
                location_name = "Tokyo, Japan"

            seniority, role = classify_job(title)
            region, is_remote, is_japan, remote_scope, is_valid = (
                enrich_and_validate_location(location_name)
            )

            if not is_valid:
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
                    remote_scope,
                    is_valid,
                )
            )

        except Exception as e:
            log_error(company_name, f"parse error: {e}")

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

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

    supabase.table("linkedin_posts").delete().lt("published_at", cutoff.isoformat()).execute()

    for entry in feed.entries:
        if not hasattr(entry, "published_parsed") or entry.published_parsed is None:
            continue

        published_utc = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        if published_utc < cutoff:
            continue

        title = entry.title
        summary = entry.get("summary", "")

        if not linkedin_matches_filters(title + " " + summary):
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
    """Human-readable name for logs."""
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
    (scrape_meta,),
    (scrape_wayve,),
    (scrape_waymo,),
    (scrape_monday,),
    (scrape_greenhouse, "canonical", "Canonical"),
    (scrape_greenhouse, "nansen", "Nansen"),
    (scrape_greenhouse, "brave", "Brave"),
    (scrape_greenhouse, "gitlab", "GitLab"),
    (scrape_greenhouse, "figma", "Figma"),
    (scrape_greenhouse, "stripe", "Stripe"),
    (scrape_greenhouse, "anthropic", "Anthropic"),
    (scrape_greenhouse, "nothing", "Nothing"),
    (scrape_greenhouse, "boxinc", "Box"),
    (scrape_greenhouse, "phrase", "Phrase"),
    (scrape_greenhouse, "okta", "Okta"),
    (scrape_greenhouse, "datadog", "Datadog"),
    (scrape_greenhouse, "asana", "Asana"),
    (scrape_greenhouse, "workato", "Workato"),
    (scrape_greenhouse, "braze", "Braze"),
    (scrape_greenhouse, "hubspotjobs", "Hubspot"),
    (scrape_greenhouse, "automatticcareers", "Automattic"),
    (scrape_greenhouse, "unity3d", "Unity"),
    (scrape_greenhouse, "scopely", "Scopely"),
    (scrape_greenhouse, "storyblok", "Storyblok"),
    (scrape_greenhouse, "speechify", "Speechify"),
    (scrape_greenhouse, "grafanalabs", "Grafana"),
    (scrape_greenhouse, "roblox", "Roblox"),
    (scrape_greenhouse, "airbnb", "Airbnb"),
    (scrape_greenhouse, "wrike", "Wrike"),
    (scrape_greenhouse, "gumgum", "GumGum"),
    (scrape_greenhouse, "discord", "Discord"),
    (scrape_ashby, "chromatic", "Chromatic"),
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
    (scrape_ashby, "cognition", "Cognition"),
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