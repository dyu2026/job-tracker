"""
Job scrapers → Supabase (jobs + linkedin_posts).

Run: python scraper.py
"""

from __future__ import annotations

import cloudscraper
import feedparser
import json
import platform
import random
import re
import requests
import threading
import time
import unicodedata
import urllib.parse
import queue
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta, timezone

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from supabase_client import supabase
from utils import classify_job, classify_location

# ---------------------------------------------------------------------------
# Constants & Globals
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

# The central queue for all DB operations
db_queue = queue.Queue(maxsize=5000)

DEBUG_FILTERS = True
FILTER_STATS = defaultdict(int)

TITLE_NEGATIVE_PATTERNS = [
    r"\bamer\b",
    r"\bemea\b",
    r"\blatam\b",
    r"\busa\b",
    r"\bunited states\b",
    r"\bu\.s\.a\.(?!\w)",
    r"\bu\.s\.(?!\w)",
    r"\bcanada\b",
    r"\beurope\b",
    r"\bgermany\b",
    r"\bmiddle east\b",
    r"\buae\b",
    r"\bindia\b",
    r"\bindonesia\b",
    r"\bvietnam\b",
    r"\bsingapore\b",
    r"\baustralia\b",
    r"\bnew york\b",
    r"\bsan francisco\b",
    r"\boregon\b",
    r"\bbrisbane\b",
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


def safe_request(method, url, **kwargs):
    scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})

    for attempt in range(SAFE_REQUEST_MAX_RETRIES):
        try:
            if attempt == 0:
                response = requests.request(method, url, timeout=REQUEST_TIMEOUT_S, **kwargs)
            else:
                response = scraper.request(method, url, timeout=REQUEST_TIMEOUT_S, **kwargs)

            if response.status_code == 200:
                return response

        except Exception as e:
            print(f"[HTTP] Attempt {attempt + 1}/{SAFE_REQUEST_MAX_RETRIES} failed for {url}: {e}")

        time.sleep(1.5 * (attempt + 1))

    print(f"[HTTP] All retries exhausted for {url}")
    return None


def safe_supabase_call(fn, retries=3):
    last_exception = None
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            last_exception = e
            print(f"[Supabase retry {i+1}] {e}")
            time.sleep(2 * (i + 1))
    raise last_exception


# ---------------------------------------------------------------------------
# Producer helpers
# ---------------------------------------------------------------------------


def upsert_jobs(company: str, jobs: list[dict]) -> None:
    now = datetime.now(UTC).isoformat()
    for job in jobs:
        job["_scraped_at"] = now

    db_queue.put(("JOB_BATCH", (company, jobs)))


def mark_removed_jobs(company_name: str, seen_ids: set) -> None:
    """Queues a cleanup signal for a specific company."""
    db_queue.put(("FLUSH_COMPANY", (company_name, seen_ids)))

# ---------------------------------------------------------------------------
# The Single Writer (Consumer)
# ---------------------------------------------------------------------------


def db_writer_worker():
    pending_jobs = defaultdict(list)
    print("[Writer] Thread started.")

    def flush_company(company):
        raw_batch = pending_jobs.pop(company, [])

        if not raw_batch:
            return

        unique_map = {j["external_id"]: j for j in raw_batch}
        batch = list(unique_map.values())

        ext_ids = [j["external_id"] for j in batch]
        if not ext_ids:
            return

        existing = safe_supabase_call(
            lambda c=company, ids=ext_ids: supabase.table("jobs")
            .select("external_id, first_seen_at")
            .eq("company", c)
            .in_("external_id", ids)
            .execute()
        )

        rows = existing.data or []
        first_seen_map = {r["external_id"]: r["first_seen_at"] for r in rows}

        for job in batch:
            scraped_at = job.pop("_scraped_at", datetime.now(UTC).isoformat())
            job["first_seen_at"] = first_seen_map.get(job["external_id"], scraped_at)
            job["last_seen_at"] = scraped_at
            job["is_active"] = True

        safe_supabase_call(
            lambda b=batch: supabase.table("jobs")
            .upsert(b, on_conflict="company,external_id")
            .execute()
        )

        print(f"[{company}] Writer: Bulk upserted {len(batch)} jobs.")

    while True:
        try:
            msg = db_queue.get()

            if msg is None:
                for company in list(pending_jobs.keys()):
                    flush_company(company)
                break

            action, payload = msg

            if action == "JOB_BATCH":
                company, jobs = payload
                pending_jobs[company].extend(jobs)

                if len(pending_jobs[company]) >= 100:
                    flush_company(company)

            elif action == "FLUSH_COMPANY":
                company, seen_ids = payload

                flush_company(company)

                result = safe_supabase_call(
                    lambda c=company: supabase.table("jobs")
                    .select("external_id")
                    .eq("company", c)
                    .eq("is_active", True)
                    .execute()
                )

                rows = result.data or []
                existing_ids = set(r["external_id"] for r in rows)
                removed_ids = existing_ids - seen_ids

                if removed_ids:
                    safe_supabase_call(
                        lambda c=company, ids=removed_ids: supabase.table("jobs")
                        .update({"is_active": False})
                        .in_("external_id", list(ids))
                        .eq("company", c)
                        .execute()
                    )

                    print(f"[{company}] Writer: Marked {len(removed_ids)} jobs inactive.")

            elif action == "LINKEDIN_CLEANUP":
                safe_supabase_call(
                    lambda p=payload: supabase.table("linkedin_posts")
                    .delete()
                    .lt("published_at", p)
                    .execute()
                )

            elif action == "LINKEDIN_UPSERT_BATCH":
                if payload:
                    unique_posts = {p["url"]: p for p in payload}
                    batch = list(unique_posts.values())

                    safe_supabase_call(
                        lambda b=batch: supabase.table("linkedin_posts")
                        .upsert(b, on_conflict="url")
                        .execute()
                    )

                    print(f"[LinkedIn] Writer: Bulk upserted {len(batch)} posts.")

            # logging
            if random.random() < 0.01:
                try:
                    size = db_queue.qsize()
                except NotImplementedError:
                    size = "N/A"
                print(f"[Writer] Queue size: {size}")

        except Exception as e:
            print(f"[Writer] ERROR: {e}")

        finally:
            db_queue.task_done()

    print("[Writer] Thread stopping.")


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


def title_indicates_non_japan(title: str) -> bool:
    t = (title or "").lower()
    return any(re.search(p, t) for p in TITLE_NEGATIVE_PATTERNS)


def enrich_and_validate_location(location_name: str, title: str = ""):
    region, is_remote, is_japan, remote_scope = classify_location(location_name)

    # reviews title for non-jp location names
    if location_name and location_name.lower().strip() == "remote":
        if title_indicates_non_japan(title):
            return region, is_remote, False, "restricted", False

    is_valid = include_job(
        is_japan=is_japan,
        remote_scope=remote_scope,
        location_name=location_name
    )

    return region, is_remote, is_japan, remote_scope, is_valid


# ---------------------------------------------------------------------------
# Next.js / __NEXT_DATA__
# ---------------------------------------------------------------------------


def slugify(text: str) -> str:
    """Converts title to URL-friendly slug for Revolut."""
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r'[^\w\s-]', '', text).lower().strip()
    return re.sub(r'[-\s]+', '-', text)


_KNOWN_JOB_LIST_KEYS = ("positions", "jobs", "roles", "openings", "postings", "listings")


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
    pageprops_key: str | None = None,
) -> bool:
    log_start(company_name, "Next.js unified")

    res = safe_request("GET", careers_url)
    if not res:
        log_error(company_name, "failed to fetch careers page")
        return False

    soup = BeautifulSoup(res.text, "html.parser")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})

    if not script_tag:
        # If the tag is missing, the layout has changed - this is a real error.
        log_error(company_name, "no __NEXT_DATA__ script (layout changed)")
        return False

    try:
        data = json.loads(script_tag.string)
    except Exception as e:
        log_error(company_name, f"JSON parse error: {e}")
        return False

    page_props = data.get("props", {}).get("pageProps", {})

    if pageprops_key is not None:
        # Direct key access — used when the pageProps key is known.
        # Avoids the DFS heuristic picking up the wrong list (e.g. locations
        # arrays whose items also have "name" and "id" keys).
        jobs = page_props.get(pageprops_key)
        if jobs is None:
            log_error(company_name, f"pageProps.{pageprops_key} not found (layout changed?)")
            return False
    else:
        # Generic DFS — scan for any list that looks like a job list.
        jobs = _find_job_list_in_json(data)
        if jobs is None:
            # The page parsed correctly but no job-list structure was found.
            # Check whether pageProps contains any known job-list key — if so,
            # treat it as a valid zero-result scrape (e.g. Monday with 0 Japan roles).
            found_empty = any(
                isinstance(page_props.get(k), list)
                for k in _KNOWN_JOB_LIST_KEYS
            )
            if found_empty or page_props:
                log_summary(company_name, 0, 0)
                mark_removed_jobs(company_name, set())
                log_success(company_name)
                return True
            log_error(company_name, "could not find job list structure in JSON")
            return False

    raw_total = len(jobs)
    log_page(company_name, offset=0, raw_in_page=raw_total, extra="__NEXT_DATA__ direct")

    if raw_total == 0:
        log_summary(company_name, 0, 0)
        mark_removed_jobs(company_name, set())
        log_success(company_name)
        return True

    seen_ids: set[str] = set()
    relevant = 0
    batch = []

    for job in jobs:
        title = job.get("title") or job.get("text") or job.get("name")
        external_id = job.get("id") or job.get("jobId") or job.get("uid")

        if not title or not external_id:
            continue

        # Location extraction logic...
        if "locations" in job and isinstance(job["locations"], list):
            location_parts = [loc.get("name", "") for loc in job["locations"] if loc.get("name")]
            location_name = " | ".join(location_parts)
        else:
            loc = job.get("location")
            location_name = loc.get("name", "") if isinstance(loc, dict) else (loc if isinstance(loc, str) else "Unknown")

        # Custom URL Builder
        if "revolut" in company_name.lower():
            job_url = f"https://www.revolut.com/en-JP/careers/position/{slugify(title)}-{external_id}/"
        elif "miro" in company_name.lower():
            job_url = f"https://miro.com/careers/vacancy/{external_id}/"
        elif "monday" in company_name.lower():
            job_url = f"https://monday.com/careers/{external_id}"
        else:
            job_url = job.get("url") or f"{careers_url.rstrip('/')}/{external_id}"

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope, is_valid = (
            enrich_and_validate_location(location_name, title=title)
        )

        if not is_valid:
            continue

        seen_ids.add(str(external_id))
        relevant += 1
        batch.append(job_row(company_name, str(external_id), title, location_name, job_url,
                             seniority, role, region, is_remote, is_japan, remote_scope, is_valid))

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant)

    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


# --- Wrappers ---


def scrape_monday() -> bool:
    return scrape_nextjs_company(
        company_name="Monday.com",
        careers_url="https://monday.com/careers",
    )


def scrape_miro() -> bool:
    return scrape_nextjs_company(
        company_name="Miro",
        careers_url="https://miro.com/careers/open-positions/",
    )


def scrape_revolut() -> bool:
    return scrape_nextjs_company(
        company_name="Revolut",
        careers_url="https://www.revolut.com/en-JP/careers/?city=Tokyo",
        pageprops_key="positions",
    )


# ---------------------------------------------------------------------------
# Greenhouse
# ---------------------------------------------------------------------------


def scrape_greenhouse(company_slug: str, company_name: str) -> bool:
    log_start(company_name, "Greenhouse")

    url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs"

    response = safe_request("GET", url)
    if not response:
        log_error(company_name, "no HTTP response from Greenhouse API after retries")
        return False

    # --- Safe JSON parsing ---
    try:
        data = response.json()
    except Exception as e:
        log_error(company_name, f"JSON parse error: {e}")
        return False

    jobs = data.get("jobs", [])
    raw_total = len(jobs)

    log_page(company_name, offset=0, raw_in_page=raw_total, extra="single API response")

    seen_ids: set[str] = set()
    relevant = 0
    batch = []

    for job in jobs:
        title = job.get("title", "")
        location_name = (
            job["location"].get("name", "")
            if isinstance(job.get("location"), dict)
            else ""
        )

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name, title=title)

        if not is_valid:
            continue

        external_id = str(job["id"])
        seen_ids.add(external_id)
        relevant += 1

        batch.append(
            job_row(
                company_name,
                external_id,
                title,
                location_name,
                job["absolute_url"],
                seniority,
                role,
                region,
                is_remote,
                is_japan,
                remote_scope,
                is_valid,
            )
        )

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


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


def scrape_ashby(company_slug: str, company_name: str) -> bool:
    log_start(company_name, "Ashby")

    url = f"https://api.ashbyhq.com/posting-api/job-board/{company_slug}"

    response = safe_request("GET", url)
    if not response:
        log_error(company_name, "no HTTP response from Ashby API after retries")
        return False

    # --- Safe JSON parsing ---
    try:
        data = response.json()
    except Exception as e:
        log_error(company_name, f"JSON parse error: {e}")
        return False

    jobs = data.get("jobs", [])
    raw_total = len(jobs)

    log_page(company_name, offset=0, raw_in_page=raw_total, extra="single API response")

    seen_ids: set[str] = set()
    relevant = 0
    batch = []

    for job in jobs:
        title = job.get("title")
        external_id = job.get("id")

        if not title or not external_id:
            continue

        external_id = str(external_id)

        # --- Location handling ---
        location_obj = job.get("location")

        if isinstance(location_obj, dict):
            location_name = location_obj.get("name", "")
        elif isinstance(location_obj, str):
            location_name = location_obj
        elif job.get("locations"):
            location_name = "; ".join(
                loc.get("name", "") for loc in job["locations"] if loc.get("name")
            )
        else:
            location_name = ""

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name, title=title)

        if not is_valid:
            continue

        # --- URL handling ---
        if company_slug == "cursor":
            apply_url = _resolve_cursor_url(title, external_id, job.get("applyUrl"))
        else:
            apply_url = job.get("applyUrl") or f"https://jobs.ashbyhq.com/{company_slug}/{external_id}"

        seen_ids.add(external_id)
        relevant += 1

        batch.append(
            job_row(
                company_name, external_id, title, location_name, apply_url,
                seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
            )
        )

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


# ---------------------------------------------------------------------------
# SmartRecruiters
# ---------------------------------------------------------------------------


def scrape_smartrecruiters(company_slug: str, company_name: str) -> bool:
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
            return False

        # --- Safe JSON parsing ---
        try:
            data = response.json()
        except Exception as e:
            log_error(company_name, f"JSON parse error at offset={offset}: {e}")
            return False

        jobs = data.get("content", [])

        if not jobs:
            log_stop(company_name, f"empty batch at offset={offset} (pagination end)")
            break

        log_page(company_name, offset=offset, raw_in_page=len(jobs))
        all_jobs.extend(jobs)

        offset += limit

        # --- Safety cap ---
        if offset > 2000:
            log_error(company_name, "pagination exceeded safety limit")
            return False

    raw_total = len(all_jobs)
    seen_ids: set[str] = set()
    relevant = 0
    batch = []

    for job in all_jobs:
        title = job.get("name", "")

        loc = job.get("location") or {}
        location_name = ", ".join(
            p for p in (
                loc.get("city", ""),
                loc.get("region", ""),
                loc.get("country", "")
            ) if p
        )

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name, title=title)

        if not is_valid:
            continue

        external_id = job.get("id")
        if not external_id:
            continue

        external_id = str(external_id)

        job_url = job.get("ref") or f"https://jobs.smartrecruiters.com/{company_slug}/{external_id}"

        seen_ids.add(external_id)
        relevant += 1

        batch.append(
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

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


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
) -> bool:
    subdomain, tenant, cluster = _parse_workday_slug(company_slug)
    if DEBUG_FILTERS:
        FILTER_STATS.clear()

    apiurl = f"https://{subdomain}.{cluster}.myworkdayjobs.com/wday/cxs/{subdomain}/{tenant}/jobs"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": f"https://{subdomain}.{cluster}.myworkdayjobs.com",
        "Referer": f"https://{subdomain}.{cluster}.myworkdayjobs.com/en-US/{tenant}",
    }

    # Configuration
    is_global_scrape = location_ids is None
    pagesize = 20
    maxoffset = 200
    max_irrelevant_pages = 10 if is_global_scrape else 2

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

    seen_ids: set[str] = set()
    relevant = 0
    raw_total = 0
    no_relevant_pages = 0

    log_start(company_name, "Workday")

    while True:
        curoffset = payload["offset"]
        if curoffset > maxoffset:
            log_stop(company_name, f"safety cap offset>{maxoffset}")
            break

        time.sleep(random.uniform(0.3, 0.7))
        response = safe_request("POST", apiurl, json=payload, headers=headers)

        if not response:
            log_error(company_name, "no HTTP response")
            return False

        data = response.json()
        jobs = data.get("jobPostings", [])
        total = data.get("total", 0)
        raw_total += len(jobs)

        if not jobs:
            log_stop(company_name, f"end of pagination at offset={curoffset}")
            break

        # Early page skip check includes externalPath keywords to prevent skipping
        # pages where locationText is generic but the URL is specific.
        keywords = ["japan", "tokyo", "osaka", "remote"]
        has_any_potential = any(
            any(k in (job.get("locationsText") or "").lower() for k in keywords) or
            any(k in str(job.get("externalPath") or "").lower() for k in keywords)
            for job in jobs
        )

        if not has_any_potential and not is_global_scrape:
            no_relevant_pages += 1
            payload["offset"] += pagesize
            continue

        # Dedup check
        page_ids = {str(job.get("externalPath")) for job in jobs if job.get("externalPath")}
        if page_ids.issubset(seen_ids):
            log_stop(company_name, "duplicate page detected")
            break
        seen_ids.update(page_ids)

        filteredcount = 0
        detail_budget = 20 if is_global_scrape else 5  # Higher budget for global/multi-location sites
        batch = []

        for job in jobs:
            title = job.get("title")
            externalpath = str(job.get("externalPath", ""))
            locations_text = (job.get("locationsText") or "").strip()

            if not title or not externalpath:
                continue
            if DEBUG_FILTERS:
                FILTER_STATS["raw_jobs_seen"] += 1

            # Identify Japan via URL even if location text says "2 Locations"
            url_path_lower = externalpath.lower()
            url_indicates_japan = any(k in url_path_lower for k in ["japan", "tokyo", "osaka"])
            is_multi = "location" in locations_text.lower() or not locations_text

            display_location = locations_text
            filter_location = locations_text.lower()

            # Trigger detail fetch for multi-location roles or high-confidence URL matches
            needs_detail = (
                is_multi
                or not locations_text
                or (url_indicates_japan and "japan" not in filter_location)
            )

            if needs_detail and detail_budget > 0:
                detail_budget -= 1
                detail = safe_request(
                    "GET",
                    f"https://{subdomain}.{cluster}.myworkdayjobs.com/wday/cxs/{subdomain}/{tenant}{externalpath}",
                    headers={"User-Agent": "Mozilla/5.0"},
                )

                if detail and detail.status_code == 200:
                    try:
                        detail_json = detail.json()
                        detail_locations = _extract_workday_locations_from_detail(detail_json)
                        if detail_locations:
                            display_location = " / ".join(detail_locations)
                            filter_location = " ".join(detail_locations).lower()
                    except Exception:
                        pass

            # Classification & Validation
            seniority, role = classify_job(title)
            region, is_remote, is_japan, remote_scope, is_valid = (
                enrich_and_validate_location(display_location, title=title)
            )

            # Final safety net: if the URL explicitly contains Japan/Tokyo keywords,
            # force-include the job even if string-based validation was unsure.
            if not is_valid and url_indicates_japan:
                is_valid = True
                is_japan = True
                if "location" in display_location.lower():
                    display_location = "Tokyo, Japan (Multiple Locations)"

            if not is_valid:
                continue

            filteredcount += 1
            relevant += 1
            batch.append(
                job_row(
                    company_name, externalpath, title, display_location,
                    f"https://{subdomain}.{cluster}.myworkdayjobs.com/en-US/{tenant}{externalpath}",
                    seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
                )
            )

        if batch:
            upsert_jobs(company_name, batch)

        log_page(company_name, offset=curoffset, api_total=total,
                 raw_in_page=len(jobs), relevant_in_page=filteredcount)

        # Update irrelevant page counter
        if filteredcount == 0:
            no_relevant_pages += 1
        else:
            no_relevant_pages = 0

        # Stop if we hit too many consecutive empty pages
        if no_relevant_pages >= max_irrelevant_pages:
            log_stop(company_name, f"{no_relevant_pages} consecutive irrelevant pages")
            break

        if len(jobs) < pagesize:
            log_stop(company_name, "last page")
            break

        payload["offset"] += pagesize

    log_summary(company_name, raw_total, relevant)
    if DEBUG_FILTERS and FILTER_STATS:
        print(f"[{company_name}] Filter stats: { dict(FILTER_STATS) }")
    mark_removed_jobs(company_name, seen_ids)
    return True


# ---------------------------------------------------------------------------
# Lever
# ---------------------------------------------------------------------------


def scrape_lever(company_slug: str, company_name: str) -> bool:
    log_start(company_name, "Lever")
    url = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"

    response = safe_request("GET", url)
    if not response:
        log_error(company_name, "no HTTP response from Lever API after retries")
        return False

    try:
        data = response.json()
    except Exception as e:
        log_error(company_name, f"JSON parse error: {e}")
        return False

    # Lever returns a list of postings directly
    jobs = data if isinstance(data, list) else []
    raw_total = len(jobs)

    log_page(company_name, offset=0, raw_in_page=raw_total, extra="single API response")

    seen_ids: set[str] = set()
    relevant = 0
    batch = []

    for job in jobs:
        title = job.get("text", "")
        external_id = job.get("id")

        if not title or not external_id:
            continue

        categories = job.get("categories", {})
        location_name = categories.get("location", "") or ""
        workplace_type = job.get("workplaceType", "")

        # Normalize location for remote roles
        if workplace_type == "remote" and "remote" not in location_name.lower():
            location_name = f"Remote, {location_name}".strip(", ")

        # Ensure we have the full URL for the validator "URL Hint" logic
        external_id = str(external_id)
        job_url = job.get("hostedUrl") or f"https://jobs.lever.co/{company_slug}/{external_id}"

        seniority, role = classify_job(title)

        region, is_remote, is_japan, remote_scope, is_valid = (
            enrich_and_validate_location(location_name, title=title)
        )

        if not is_valid:
            continue

        seen_ids.add(external_id)
        relevant += 1

        batch.append(
            job_row(
                company_name, external_id, title, location_name, job_url,
                seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
            )
        )

    # Process the batch if any jobs passed the filter
    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant)

    # Only return False (triggering retry) if the API literally returned nothing (potential block/fail)
    if raw_total == 0:
        log_error(company_name, "API returned zero jobs (possible structure change or block)")
        return False

    # If raw_total > 0 but relevant == 0, it means the filter worked perfectly (e.g., Kinsta).
    # We proceed to mark_removed_jobs to clear any old Japan roles that are now closed.
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True

# ---------------------------------------------------------------------------
# eightfold.ai
# ---------------------------------------------------------------------------


def scrape_eightfold(company_slug: str, company_name: str, location: str, pid: str) -> bool:
    log_start(company_name, "Eightfold")

    base_url = f"https://{company_slug}.eightfold.ai/api/apply/v2/jobs"
    page_size = 10
    max_offset = 200
    start = 0

    total_jobs = 0
    raw_listings_total = 0
    seen_ids: set[str] = set()

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

        if not r:
            log_error(company_name, "Eightfold API: no response after retries")
            return False

        try:
            data = r.json()
        except Exception as e:
            log_error(company_name, f"JSON parse error: {e}")
            return False

        jobs = data.get("positions", [])
        if not jobs:
            log_stop(company_name, f"empty batch at start={start}")
            break

        raw_listings_total += len(jobs)
        page_relevant = 0
        batch = []

        for job in jobs:
            title = job.get("name")
            external_id = job.get("ats_job_id")

            if not title or not external_id:
                continue

            external_id = str(external_id)

            location_name = job.get("location", "")
            job_url = job.get("canonicalPositionUrl") or f"https://{company_slug}.eightfold.ai/careers/job/{external_id}"

            seniority, role = classify_job(title)
            region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name, title=title)

            if not is_valid:
                continue

            seen_ids.add(external_id)

            batch.append(
                job_row(
                    company_name, external_id, title, location_name, job_url,
                    seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
                )
            )

            total_jobs += 1
            page_relevant += 1

        if batch:
            upsert_jobs(company_name, batch)

        log_page(company_name, start=start, raw_in_page=len(jobs), relevant_in_page=page_relevant)

        if len(jobs) < page_size:
            log_stop(company_name, "last page (batch smaller than page size)")
            break

        start += page_size

    log_summary(company_name, raw_listings_total, total_jobs)

    # soft failure - continue to next page
    if raw_listings_total > 0 and total_jobs == 0:
        log_error(company_name, "no relevant jobs after filtering")
        return False

    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


# ---------------------------------------------------------------------------
# BambooHR
# ---------------------------------------------------------------------------


def scrape_bamboohr(subdomain: str, company_name: str) -> bool:
    log_start(company_name, "BambooHR")

    url = f"https://{subdomain}.bamboohr.com/careers/list"

    response = safe_request("GET", url)
    if not response:
        log_error(company_name, "no HTTP response from BambooHR list endpoint after retries")
        return False

    try:
        data = response.json()
    except Exception as e:
        log_error(company_name, f"invalid JSON from BambooHR: {e}")
        return False

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
    batch = []

    for job in jobs:
        title = (
            job.get("jobTitle")
            or job.get("jobOpeningName")
            or job.get("title")
            or job.get("name")
        )

        external_id_raw = job.get("id") or job.get("jobId")

        if not title or not external_id_raw:
            continue

        external_id = str(external_id_raw)

        location = job.get("location")

        if isinstance(location, dict):
            location_name = ", ".join(
                p for p in (
                    location.get("city"),
                    location.get("state"),
                    location.get("country"),
                ) if p
            )
        elif isinstance(location, str):
            location_name = location
        else:
            location_name = UNKNOWN_LOCATION

        if not location_name:
            location_name = UNKNOWN_LOCATION

        job_url = (
            job.get("jobUrl")
            or job.get("applyUrl")
            or f"https://{subdomain}.bamboohr.com/careers/{external_id}"
        )

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name, title=title)

        if not is_valid:
            continue

        seen_ids.add(external_id)
        relevant += 1

        batch.append(
            job_row(
                company_name, external_id, title, location_name, job_url,
                seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
            )
        )

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant)

    # soft failure - continue to next page
    if raw_total > 0 and relevant == 0:
        log_error(company_name, "no relevant jobs after filtering")
        return False

    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


# ---------------------------------------------------------------------------
# Netflix (Japan)
# ---------------------------------------------------------------------------


def scrape_netflix() -> bool:
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

        r = safe_request("GET", base_url, params=params)
        if not r:
            log_error(company_name, "no HTTP response from Netflix API after retries")
            return False

        try:
            data = r.json()
        except Exception as e:
            log_error(company_name, f"Netflix JSON parse failed: {e}")
            return False

        jobs = data.get("positions", [])

        if not jobs:
            if start == 0:
                log_error(company_name, "no jobs found on first page")
                return False
            log_stop(company_name, f"empty batch at start={start}")
            break

        raw_listings_total += len(jobs)
        page_stored = 0

        batch = []

        for job in jobs:
            job_id = job.get("id")
            title = job.get("name")
            location_name = job.get("location", "")

            if not job_id or not title:
                continue

            external_id = str(job_id)
            seen_ids.add(external_id)

            seniority, role = classify_job(title)
            region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name, title=title)

            if not is_valid:
                continue

            job_url = (
                f"https://explore.jobs.netflix.net/careers/apply"
                f"?domain=netflix.com&pid={external_id}"
            )

            batch.append(
                job_row(
                    company_name, external_id, title, location_name, job_url,
                    seniority, role, region, is_remote, is_japan, remote_scope, is_valid,
                )
            )
            page_stored += 1

        if batch:
            upsert_jobs(company_name, batch)

        log_page(company_name, start=start, raw_in_page=len(jobs), relevant_in_page=page_stored)

        relevant_total += page_stored
        start += params["num"]

    log_summary(company_name, raw_listings_total, relevant_total)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


# ---------------------------------------------------------------------------
# Uber
# ---------------------------------------------------------------------------


def extract_uber_location(job):
    """
    Build a clean 'City, Country' string.
    Prioritizes Japan locations, including multi-location roles.
    """

    # 1. Try primary location
    loc = job.get("location") or {}
    if loc.get("country") == "JPN":
        city = loc.get("city")
        country = loc.get("countryName", "Japan")
        if city:
            return f"{city}, {country}"
        return country  # fallback if no city

    # 2. Fallback: check allLocations
    for loc in job.get("allLocations", []):
        if loc.get("country") == "JPN":
            city = loc.get("city")
            country = loc.get("countryName", "Japan")
            if city:
                return f"{city}, {country}"
            return country

    return None  # no Japan location found


def scrape_uber(company_name: str = "Uber") -> bool:
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
            "params": {
                "location": [{"country": "JPN"}],
            },
        }

        response = safe_request(
            "POST",
            url,
            params={"localeCode": "ja-JP"},
            json=payload,
            headers=headers,
        )

        # hard failure
        if not response:
            log_error(company_name, f"no response at page={page}")
            return False

        try:
            data = response.json()
        except Exception as e:
            log_error(company_name, f"JSON parse failed at page={page}: {e}")
            return False

        results = data.get("data", {}).get("results")

        # Uber returns null (not []) when pagination is exhausted.
        # Only treat it as a hard failure on the very first page.
        if results is None:
            if page == 0:
                log_error(company_name, "no results on first page (null response)")
                return False
            log_stop(company_name, f"null results at page={page} — end of pagination")
            break

        # Any other non-list value is a genuine unexpected response.
        if not isinstance(results, list):
            log_error(company_name, f"invalid results at page={page}: {results}")
            return False

        # empty results handling
        if not results:
            if page == 0:
                log_error(company_name, "no jobs found on first page")
                return False
            log_stop(company_name, f"empty results at page={page}")
            break

        raw_total += len(results)
        page_relevant = 0

        batch = []

        for job in results:
            external_id = str(job.get("id"))
            title = job.get("title")

            if not external_id or not title:
                continue

            location_name = extract_uber_location(job)
            if not location_name:
                continue  # expected filter

            seniority, role = classify_job(title)
            region, is_remote, is_japan, remote_scope, is_valid = (
                enrich_and_validate_location(location_name, title=title)
            )

            if not is_valid:
                continue  # expected filter

            seen_ids.add(external_id)
            page_relevant += 1

            batch.append(
                job_row(
                    company_name,
                    external_id,
                    title,
                    location_name,
                    f"https://www.uber.com/global/en/careers/list/{external_id}/",
                    seniority,
                    role,
                    region,
                    is_remote,
                    is_japan,
                    remote_scope,
                    is_valid,
                )
            )

        if batch:
            upsert_jobs(company_name, batch)

        log_page(
            company_name,
            offset=page,
            raw_in_page=len(results),
            relevant_in_page=page_relevant,
        )

        relevant_total += page_relevant

        # pagination stop
        if len(results) < limit:
            log_stop(company_name, "last page (results < limit)")
            break

        page += 1
        time.sleep(random.uniform(1.0, 2.0))

    log_summary(company_name, raw_total, relevant_total)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------


def scrape_meta(company_name: str = "Meta") -> bool:
    log_start(company_name, "Meta GraphQL")

    seen_ids: set[str] = set()
    raw_total = 0
    relevant = 0
    got_any_response = False

    # --- helpers ---
    def extract_meta_jobs(data: dict) -> list[dict]:
        try:
            d = data.get("data", {})

            if "job_search_with_featured_jobs" not in d:
                return []

            all_jobs = d["job_search_with_featured_jobs"].get("all_jobs", [])

            return [
                {
                    "id": j.get("id"),
                    "title": j.get("title"),
                    "locations": j.get("locations", []),
                }
                for j in all_jobs
            ]

        except Exception as e:
            log_error(company_name, f"extract failed: {e}")
            return []

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

        # prioritize Japan
        for name in names:
            if "Japan" in name:
                return name

        return names[0]

    # --- Playwright ---
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            def handle_response(response):
                nonlocal raw_total, relevant, got_any_response

                if "graphql" not in response.url:
                    return

                try:
                    data = response.json()
                except Exception:
                    return

                if "job_search_with_featured_jobs" not in data.get("data", {}):
                    return

                got_any_response = True

                jobs = extract_meta_jobs(data)
                if not jobs:
                    return

                print(f"[Meta DEBUG] Found {len(jobs)} jobs")

                raw_total += len(jobs)

                batch = []

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
                        enrich_and_validate_location(location_name, title=title)
                    )

                    if not is_valid:
                        continue

                    relevant += 1

                    batch.append(
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

                if batch:
                    upsert_jobs(company_name, batch)

            page.on("response", handle_response)

            page.goto(
                "https://www.metacareers.com/jobsearch?offices[0]=Tokyo%2C%20Japan",
                wait_until="networkidle"
            )

            page.wait_for_timeout(8000)
            browser.close()

    except Exception as e:
        log_error(company_name, f"Playwright failure: {e}")
        return False

    # Critical failure conditions
    if not got_any_response:
        log_error(company_name, "no GraphQL job data captured")
        return False

    if raw_total == 0:
        log_error(company_name, "no jobs found from API")
        return False

    # Success path
    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


# ---------------------------------------------------------------------------
# Wayve (FirstStage)
# ---------------------------------------------------------------------------


def scrape_wayve(company_name: str = "Wayve") -> bool:
    log_start(company_name, "FirstStage")

    url = "https://wayve.firststage.co/jobs"

    response = safe_request(
        "GET",
        url,
        headers={"User-Agent": "Mozilla/5.0"},
    )

    if not response or response.status_code != 200:
        log_error(company_name, f"HTTP error: {getattr(response, 'status_code', 'no response')}")
        return False

    soup = BeautifulSoup(response.text, "html.parser")

    links = soup.select("a[href*='/jobs/']")

    raw_total = len(links)
    relevant = 0
    seen_ids: set[str] = set()

    log_page(company_name, offset=0, raw_in_page=raw_total, extra="HTML parse")

    # --- parsing helper ---
    def split_title_location(text: str):
        text = " ".join(text.split())

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

    batch = []

    for a in links:
        href = a.get("href")

        if not href or "/jobs/" not in href:
            continue

        job_url = href if href.startswith("http") else f"https://wayve.firststage.co{href}"

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
            enrich_and_validate_location(location_name, title=title)
        )

        if not is_valid:
            continue

        seen_ids.add(external_id)
        relevant += 1

        batch.append(
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

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


# ---------------------------------------------------------------------------
# Waymo
# ---------------------------------------------------------------------------


def scrape_waymo(company_name: str = "Waymo") -> bool:
    log_start(company_name, "Waymo HTML")

    url = "https://careers.withwaymo.com/jobs/search"
    params = [("country_codes[]", "JP")]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://careers.withwaymo.com/",
    }

    # Warm session
    safe_request("GET", "https://careers.withwaymo.com/", headers=headers)
    response = safe_request("GET", url, params=params, headers=headers)

    if not response or response.status_code != 200:
        log_error(company_name, f"HTTP {getattr(response, 'status_code', 'no response')}")
        return False

    soup = BeautifulSoup(response.text, "html.parser")
    jobs = soup.select("article.job-search-results-card-col")

    raw_total = len(jobs)
    relevant = 0
    seen_ids: set[str] = set()

    log_page(company_name, offset=0, raw_in_page=raw_total, extra="HTML parse")

    if raw_total == 0:
        log_error(company_name, "no job cards found (HTML structure or dynamic loading changed)")
        return False

    batch = []

    for job in jobs:
        try:
            a = job.select_one("h3 a")
            if not a:
                continue

            title = a.get_text(strip=True)
            job_url = a["href"]

            # Waymo URLs can be relative or absolute; ensure we have a string for the validator
            full_url = job_url if job_url.startswith("http") else f"https://careers.withwaymo.com{job_url}"
            external_id = full_url.rstrip("/").split("/")[-1]

            loc_el = job.select_one(".job-component-location span")
            location_name = loc_el.get_text(strip=True) if loc_el else "Tokyo, Japan"

            if location_name == "Tokyo":
                location_name = "Tokyo, Japan"

            seniority, role = classify_job(title)
            region, is_remote, is_japan, remote_scope, is_valid = (
                enrich_and_validate_location(location_name, title=title)
            )

            if not is_valid:
                continue

            seen_ids.add(external_id)
            relevant += 1

            batch.append(
                job_row(
                    company_name,
                    external_id,
                    title,
                    location_name,
                    full_url,
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
            log_error(company_name, f"parse error on job: {e}")
            continue

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)

    # Optional strictness: if we found HTML but no valid Japan roles, check the validator logic
    if raw_total > 0 and relevant == 0:
        log_error(company_name, "found roles but all were filtered out as invalid")
        return False

    log_success(company_name)
    return True


# ---------------------------------------------------------------------------
# Wiz
# ---------------------------------------------------------------------------


def scrape_wiz(company_name: str = "Wiz") -> bool:
    log_start(company_name, "Wiz API")

    url = "https://www.wiz.io/api/fetch-jobs-data"

    response = safe_request("GET", url)
    if not response:
        log_error(company_name, "no HTTP response from Wiz API after retries")
        return False

    # --- Safe JSON parsing ---
    try:
        data = response.json()
    except Exception as e:
        log_error(company_name, f"JSON parse error: {e}")
        return False

    # --- Extract jobs safely (Wiz returns nested structure) ---
    if isinstance(data, dict):
        jobs = (
            data.get("jobs")
            or data.get("data")
            or data.get("positions")
            or data.get("items")
            or (list(data.values())[0] if data else [])
        )
    elif isinstance(data, list):
        jobs = data
    else:
        log_error(company_name, "Unexpected API response format")
        return False

    raw_total = len(jobs)

    log_page(company_name, offset=0, raw_in_page=raw_total, extra="single API response")

    seen_ids: set[str] = set()
    relevant = 0
    batch = []

    for job in jobs:
        if not isinstance(job, dict):
            continue

        title = job.get("title")
        external_id = job.get("id")

        if not title:
            continue

        # --- Normalize ID ---
        external_id = str(external_id) if external_id else None

        # --- Location handling ---
        location_name = (
            job.get("location")
            or job.get("locations")
            or job.get("country")
            or ""
        )

        # --- URL handling ---
        apply_url = job.get("url") or job.get("applyUrl")

        if not external_id or not apply_url:
            continue

        seniority, role = classify_job(title)
        region, is_remote, is_japan, remote_scope, is_valid = enrich_and_validate_location(location_name, title=title)

        if not is_valid:
            continue

        seen_ids.add(external_id)
        relevant += 1

        batch.append(
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
                remote_scope,
                is_valid,
            )
        )

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True

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


def scrape_linkedin() -> bool:
    days_to_pull = 7
    query = (
        'site:linkedin.com/posts (hiring OR recruiting OR "now hiring" OR "募集" '
        f'OR "求人" OR newopportunity) Japan when:{days_to_pull}d'
    )
    encoded_query = urllib.parse.quote(query)
    rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-JP&gl=JP&ceid=JP:en"

    feed = feedparser.parse(rss_url)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_to_pull)

    db_queue.put(("LINKEDIN_CLEANUP", cutoff.isoformat()))

    # Use a dictionary to deduplicate URLs before they even hit the queue
    unique_posts = {}
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
        if not url:
            continue

        unique_posts[url] = {
            "title": title,
            "url": url,
            "snippet": re.sub("<.*?>", "", summary),
            "published_at": published_utc.isoformat(),
        }

    db_queue.put(("LINKEDIN_UPSERT_BATCH", list(unique_posts.values())))
    log_summary("LinkedIn", len(feed.entries), len(unique_posts))
    log_success("LinkedIn")
    return True


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


def run_task(func, *args) -> dict:
    label = _task_label(func, args)

    for attempt in range(2):
        start = time.time()

        try:
            if attempt == 0:
                print(f"🚀 Starting {label}")
            else:
                print(f"🔁 Retry {attempt + 1}/2 for {label}")

            result = func(*args)

            duration = round(time.time() - start, 2)

            # Explicit failure from scraper
            if result is False:
                raise RuntimeError("scraper returned False")

            print(f"✅ {label} ({duration}s)")
            return {
                "label": label,
                "success": True,
                "duration": duration,
                "error": None,
            }

        except Exception as e:
            duration = round(time.time() - start, 2)
            error_msg = str(e)

            print(f"⚠️  Attempt {attempt + 1}/2 failed for {label} ({duration}s): {error_msg}")

            if attempt == 1:
                # final failure
                print(f"❌ {label} after 2 attempts")
                return {
                    "label": label,
                    "success": False,
                    "duration": duration,
                    "error": error_msg,
                }

            time.sleep(2 * (attempt + 1))


SCRAPER_TASKS: list[tuple] = [
    (scrape_wiz,),
    (scrape_monday,),
    (scrape_miro,),
    (scrape_revolut,),
    (scrape_uber,),
    (scrape_meta,),
    (scrape_wayve,),
    (scrape_waymo,),
    (scrape_smartrecruiters, "linkedin3", "LinkedIn"),
    (scrape_smartrecruiters, "Canva", "Canva"),
    (scrape_smartrecruiters, "wise", "Wise"),
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
    (scrape_linkedin,),
]

SCRAPER_STATS = {"total": 0, "success": 0, "failed": 0}


def main() -> None:
    start_time = time.time()

    # Reset stats (important if reused)
    SCRAPER_STATS["total"] = 0
    SCRAPER_STATS["success"] = 0
    SCRAPER_STATS["failed"] = 0

    failed_tasks = []
    durations = []

    system = platform.system()
    max_workers = 1 if system == "Windows" else 3
    print(f"Detected OS: {system} | Using {max_workers} scraper worker(s)")

    writer_thread = threading.Thread(target=db_writer_worker, daemon=True)
    writer_thread.start()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(run_task, *task) for task in SCRAPER_TASKS]

        for future in as_completed(futures):
            SCRAPER_STATS["total"] += 1

            try:
                result = future.result()

                durations.append((result["label"], result["duration"]))

                if result["success"]:
                    SCRAPER_STATS["success"] += 1
                else:
                    SCRAPER_STATS["failed"] += 1
                    failed_tasks.append((result["label"], result["error"]))

            except Exception as e:
                SCRAPER_STATS["failed"] += 1
                failed_tasks.append(("unknown", str(e)))
                print(f"❌ Unhandled exception in future: {e}")

    print("\nScraping done. Finalizing DB operations...")
    db_queue.put(None)
    db_queue.join()
    writer_thread.join()

    runtime = round(time.time() - start_time, 2)

    # Summary
    print("\n" + "=" * 50)
    print("📊 SCRAPER RUN SUMMARY")
    print("=" * 50)
    print(f"Total scrapers : {SCRAPER_STATS['total']}")
    print(f"Successful     : {SCRAPER_STATS['success']}")
    print(f"Failed         : {SCRAPER_STATS['failed']}")

    if SCRAPER_STATS["total"] > 0:
        rate = SCRAPER_STATS["success"] / SCRAPER_STATS["total"]
        print(f"Success rate   : {rate:.1%}")

    print(f"Total runtime  : {runtime} seconds")

    # Failed scraper details
    if failed_tasks:
        print("\n❌ Failed scrapers:")
        for label, error in failed_tasks:
            print(f" - {label}: {error}")

    # Slowest scrapers
    if durations:
        durations_sorted = sorted(durations, key=lambda x: x[1], reverse=True)

        print("\n⏱  Slowest scrapers:")
        for label, duration in durations_sorted[:5]:
            print(f" - {label}: {duration}s")

    print("=" * 50)


if __name__ == "__main__":
    main()