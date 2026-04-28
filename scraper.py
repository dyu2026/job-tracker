"""
Job scrapers → Supabase (jobs + linkedin_posts).

Run: python scraper.py
"""

from __future__ import annotations

# --- Standard library ---
import json
import platform
import random
import re
import threading
import time
import unicodedata
import urllib.parse
import queue
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta, timezone

# --- Third-party ---
import cloudscraper
import feedparser
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# --- Local ---
from supabase_client import supabase
from utils import classify_job, classify_location

# ---------------------------------------------------------------------------
# Constants & Globals
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT_S = 20
SAFE_REQUEST_MAX_RETRIES = 3
UNKNOWN_LOCATION = "Remote / Unknown"

REMOTE_SCOPES_INCLUDED = frozenset({"global", "apac", "asia", "japan"})

JOB_TITLE_KEYS = (
    "title",
    "text",
    "name",
    "jobOpeningName",   # BambooHR
    "jobTitle",         # (some BambooHR variants)
)
JOB_ID_KEYS = ("id", "jobId", "uid", "requisition_id")

LINKEDIN_INC_KEYWORDS = [
    "hiring",
    "opportunity",
    "job",
    "recruitment",
    "talent",
    "bilingual",
    "director",
    "career",
    "positions",
    "募集",
    "roles",
    "role",
    "we are hiring",
    "join our team",
    "jobopportunity",
]

LINKEDIN_EXC_KEYWORDS = [
    "excited to announce",
    "i'm happy to share",
    "started a new position",
    "looking for a new role",
    "i am looking",
    "please help me find",
    "I'm joining",
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
    r"\bcanada\b",
    r"\beurope\b",
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
    print(
        f"[{company}] Summary: raw_listings={raw_listings} | relevant_stored={relevant_stored}"
    )


def log_success(company: str) -> None:
    print(f"[{company}] SUCCESS")


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def safe_request(method, url, **kwargs):
    print(f"[DEBUG] safe_request → {method} {url}")

    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows"}
    )

    for attempt in range(1, SAFE_REQUEST_MAX_RETRIES + 1):
        client = requests if attempt == 1 else scraper
        client_name = "requests" if attempt == 1 else "cloudscraper"

        try:
            print(f"[DEBUG] Attempt {attempt}: {client_name}")

            response = client.request(
                method, url, timeout=REQUEST_TIMEOUT_S, **kwargs
            )

            print(f"[DEBUG] Attempt {attempt} status: {response.status_code}")

            if response.status_code == 200:
                return response

        except Exception as e:
            print(f"[DEBUG] Attempt {attempt} failed: {e}")

        time.sleep(1.5 * attempt)

    print("[DEBUG] safe_request FAILED")
    return None


def safe_supabase_call(fn, retries=3):

    last_exception = None
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            last_exception = e
            print(f"[Supabase retry {i + 1}] {e}")
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

    def run(query_fn):
        return safe_supabase_call(lambda: query_fn().execute())

    def flush_company(company):
        raw_batch = pending_jobs.pop(company, [])
        if not raw_batch:
            return

        batch = list({j["external_id"]: j for j in raw_batch}.values())
        ext_ids = [j["external_id"] for j in batch]
        if not ext_ids:
            return

        existing = run(
            lambda: (
                supabase.table("jobs")
                .select("external_id, first_seen_at")
                .eq("company", company)
                .in_("external_id", ext_ids)
            )
        )

        rows = (existing.data if existing else []) or []
        first_seen_map = {r["external_id"]: r["first_seen_at"] for r in rows}

        now_iso = datetime.now(UTC).isoformat()

        for job in batch:
            scraped_at = job.pop("_scraped_at", now_iso)
            job["first_seen_at"] = first_seen_map.get(job["external_id"], scraped_at)
            job["last_seen_at"] = scraped_at
            job["is_active"] = True

        run(
            lambda: (
                supabase.table("jobs")
                .upsert(batch, on_conflict="company,external_id")
            )
        )

        print(f"[{company}] Writer: Bulk upserted {len(batch)} jobs.")

    while True:
        try:
            msg = db_queue.get()

            if msg is None:
                for company in list(pending_jobs):
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

                result = run(
                    lambda: (
                        supabase.table("jobs")
                        .select("external_id")
                        .eq("company", company)
                        .eq("is_active", True)
                    )
                )

                rows = (result.data if result else []) or []
                existing_ids = {r["external_id"] for r in rows}
                removed_ids = existing_ids - seen_ids

                if removed_ids:
                    run(
                        lambda: (
                            supabase.table("jobs")
                            .update({"is_active": False})
                            .in_("external_id", list(removed_ids))
                            .eq("company", company)
                        )
                    )

                    print(f"[{company}] Writer: Marked {len(removed_ids)} jobs inactive.")

            elif action == "LINKEDIN_CLEANUP":
                run(
                    lambda: (
                        supabase.table("linkedin_posts")
                        .delete()
                        .lt("published_at", payload)
                    )
                )

            elif action == "LINKEDIN_UPSERT_BATCH":
                if payload:
                    batch = list({p["url"]: p for p in payload}.values())

                    run(
                        lambda: (
                            supabase.table("linkedin_posts")
                            .upsert(batch, on_conflict="url")
                        )
                    )

                    print(f"[LinkedIn] Writer: Bulk upserted {len(batch)} posts.")

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
# Job builder + geography filter
# ---------------------------------------------------------------------------

def _get_first(job, keys):
    for k in keys:
        v = job.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

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

def parse_job_common(
    *,
    job: dict,
    company_name: str,
    title: str,
    external_id: str,
    location_name: str,
    job_url: str,
):
    seniority, role = classify_job(title)

    region, is_remote, is_japan, remote_scope, is_valid = (
        enrich_and_validate_location(
            location_name,
            company_name,
            external_id,
            title,
        )
    )

    if not is_valid:
        return None

    return job_row(
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


def enrich_and_validate_location(
    location_name: str,
    company: str = "",
    job_id: str = "",
    title: str = "",
):
    region, is_remote, is_japan, remote_scope = classify_location(location_name)

    loc = (location_name or "").lower().strip()

    # FIX 1: Remote roles with hidden region in title → exclude
    if loc == "remote":
        if title_indicates_non_japan(title):
            return region, is_remote, False, "restricted", False


    # FIX 2: Unknown / missing location fallback
    if not loc or loc in ("unknown", "n/a"):
        combined = f"{title} {job_id}".lower()

        # Only allow if NOT clearly tied to a restricted region
        if not title_indicates_non_japan(combined):
            return None, False, False, "unknown", True

        return None, False, False, "restricted", False

    # Default validation
    is_valid = include_job(
        is_japan=is_japan,
        remote_scope=remote_scope,
        location_name=location_name,
    )

    return region, is_remote, is_japan, remote_scope, is_valid


# ---------------------------------------------------------------------------
# Next.js / __NEXT_DATA__
# ---------------------------------------------------------------------------


def slugify(text: str) -> str:
    """Converts title to URL-friendly slug for Revolut."""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text).lower().strip()
    return re.sub(r"[-\s]+", "-", text)


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
            if any(k in keys for k in ("title", "text", "name")) and any(
                k in keys for k in ("id", "jobId", "requisition_id")
            ):
                return obj
        for item in obj:
            result = _find_job_list_in_json(item)
            if result is not None:
                return result
    return None


def scrape_nextjs_company(company_name: str, careers_url: str) -> bool:
    log_start(company_name, "Next.js unified")

    res = safe_request("GET", careers_url)
    if not res:
        log_error(company_name, "failed to fetch careers page")
        return False

    soup = BeautifulSoup(res.text, "html.parser")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})

    if not script_tag:
        log_error(company_name, "no __NEXT_DATA__ script (layout changed)")
        return False

    try:
        data = json.loads(script_tag.string)
    except Exception as e:
        log_error(company_name, f"JSON parse error: {e}")
        return False

    jobs = _find_job_list_in_json(data)
    if jobs is None:
        log_error(company_name, "could not find job list structure in JSON")
        return False

    raw_total = len(jobs)
    log_page(company_name, offset=0, raw_in_page=raw_total, extra="__NEXT_DATA__")

    seen_ids: set[str] = set()
    batch = []

    company_key = company_name.lower()
    base_url = careers_url.rstrip("/")

    for job in jobs:
        title = _get_first(job, JOB_TITLE_KEYS)
        external_id = _get_first(job, JOB_ID_KEYS)

        if not title or not external_id:
            continue

        external_id = str(external_id)

        locations = job.get("locations")
        if isinstance(locations, list):
            location_name = " | ".join(
                loc.get("name", "") for loc in locations if loc.get("name")
            )
        else:
            loc = job.get("location")
            if isinstance(loc, dict):
                location_name = loc.get("name", "")
            elif isinstance(loc, str):
                location_name = loc
            else:
                location_name = ""

        if "revolut" in company_key:
            job_url = f"https://www.revolut.com/en-JP/careers/position/{slugify(title)}-{external_id}/"
        elif "miro" in company_key:
            job_url = f"https://miro.com/careers/vacancy/{external_id}/"
        elif "monday" in company_key:
            job_url = f"https://monday.com/careers/{external_id}"
        else:
            job_url = job.get("url") or f"{base_url}/{external_id}"

        parsed = parse_job_common(
            job=job,
            company_name=company_name,
            title=title,
            external_id=external_id,
            location_name=location_name,
            job_url=job_url,
        )

        if not parsed:
            continue

        seen_ids.add(external_id)
        batch.append(parsed)

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, len(batch))
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


# --- Wrappers to call scrape_nextjs_company ---


def scrape_monday() -> None:
    scrape_nextjs_company(
        company_name="Monday.com", careers_url="https://monday.com/careers"
    )


def scrape_miro() -> None:
    scrape_nextjs_company(
        company_name="Miro", careers_url="https://miro.com/careers/open-positions/"
    )


def scrape_revolut() -> None:
    scrape_nextjs_company(
        company_name="Revolut",
        careers_url="https://www.revolut.com/en-JP/careers/?city=Tokyo",
    )


# ---------------------------------------------------------------------------
# Greenhouse
# ---------------------------------------------------------------------------


def scrape_greenhouse(company_slug: str, company_name: str) -> bool:
    log_start(company_name, "Greenhouse")

    url = f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs"

    response = safe_request("GET", url)
    if not response:
        log_error(company_name, "no HTTP response from Greenhouse API")
        return False

    try:
        data = response.json()
    except Exception as e:
        log_error(company_name, f"JSON parse error: {e}")
        return False

    jobs = data.get("jobs", [])
    raw_total = len(jobs)

    log_page(company_name, offset=0, raw_in_page=raw_total)

    seen_ids: set[str] = set()
    batch = []

    for job in jobs:
        title = _get_first(job, JOB_TITLE_KEYS)
        external_id = _get_first(job, JOB_ID_KEYS)

        if not title or not external_id:
            continue

        external_id = str(external_id)

        loc = job.get("location")
        if isinstance(loc, dict):
            location_name = loc.get("name", "")
        elif isinstance(loc, str):
            location_name = loc
        else:
            location_name = ""

        job_url = job.get("absolute_url")

        parsed = parse_job_common(
            job=job,
            company_name=company_name,
            title=title,
            external_id=external_id,
            location_name=location_name,
            job_url=job_url,
        )

        if not parsed:
            continue

        seen_ids.add(external_id)
        batch.append(parsed)

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, len(batch))
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
        log_error(company_name, "no HTTP response from Ashby API")
        return False

    try:
        data = response.json()
    except Exception as e:
        log_error(company_name, f"JSON parse error: {e}")
        return False

    jobs = data.get("jobs", [])
    raw_total = len(jobs)

    log_page(company_name, offset=0, raw_in_page=raw_total)

    seen_ids: set[str] = set()
    batch = []

    for job in jobs:
        title = _get_first(job, JOB_TITLE_KEYS)
        external_id = _get_first(job, JOB_ID_KEYS)

        if not title or not external_id:
            continue

        external_id = str(external_id)

        locations = job.get("locations")
        if isinstance(locations, list):
            location_name = "; ".join(
                loc.get("name", "") for loc in locations if loc.get("name")
            )
        else:
            loc = job.get("location")
            if isinstance(loc, dict):
                location_name = loc.get("name", "")
            elif isinstance(loc, str):
                location_name = loc
            else:
                location_name = ""

        apply_url = job.get("applyUrl")

        if company_slug == "cursor":
            apply_url = _resolve_cursor_url(title, external_id, apply_url)
        else:
            apply_url = apply_url or f"https://jobs.ashbyhq.com/{company_slug}/{external_id}"

        parsed = parse_job_common(
            job=job,
            company_name=company_name,
            title=title,
            external_id=external_id,
            location_name=location_name,
            job_url=apply_url,
        )

        if not parsed:
            continue

        seen_ids.add(external_id)
        batch.append(parsed)

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, len(batch))
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


# ---------------------------------------------------------------------------
# SmartRecruiters
# ---------------------------------------------------------------------------


def scrape_smartrecruiters(company_slug: str, company_name: str) -> bool:
    log_start(company_name, "SmartRecruiters")

    offset = 0
    limit = 100
    all_jobs = []

    while True:
        url = (
            f"https://api.smartrecruiters.com/v1/companies/{company_slug}/postings"
            f"?limit={limit}&offset={offset}"
        )

        response = safe_request("GET", url)
        if not response:
            log_error(company_name, f"no HTTP response at offset={offset}")
            return False

        try:
            data = response.json()
        except Exception as e:
            log_error(company_name, f"JSON parse error at offset={offset}: {e}")
            return False

        jobs = data.get("content", [])
        if not jobs:
            log_stop(company_name, f"pagination end at offset={offset}")
            break

        log_page(company_name, offset=offset, raw_in_page=len(jobs))
        all_jobs.extend(jobs)

        offset += limit
        if offset > 2000:
            log_error(company_name, "pagination exceeded safety limit")
            return False

    raw_total = len(all_jobs)
    seen_ids: set[str] = set()
    batch = []

    for job in all_jobs:
        title = _get_first(job, JOB_TITLE_KEYS)
        external_id = _get_first(job, JOB_ID_KEYS)

        if not title or not external_id:
            continue

        external_id = str(external_id)

        # --- location ---
        loc = job.get("location") or {}
        if isinstance(loc, dict):
            location_name = ", ".join(
                p for p in (loc.get("city"), loc.get("region"), loc.get("country")) if p
            )
        else:
            location_name = ""

        job_url = job.get("ref") or f"https://jobs.smartrecruiters.com/{company_slug}/{external_id}"

        parsed = parse_job_common(
            job=job,
            company_name=company_name,
            title=title,
            external_id=external_id,
            location_name=location_name,
            job_url=job_url,
        )

        if not parsed:
            continue

        seen_ids.add(external_id)
        batch.append(parsed)

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, len(batch))
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

    apiurl = f"https://{subdomain}.{cluster}.myworkdayjobs.com/wday/cxs/{subdomain}/{tenant}/jobs"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": f"https://{subdomain}.{cluster}.myworkdayjobs.com",
        "Referer": f"https://{subdomain}.{cluster}.myworkdayjobs.com/en-US/{tenant}",
    }

    is_global_scrape = location_ids is None
    pagesize = 20
    maxoffset = 200
    max_irrelevant_pages = 10 if is_global_scrape else 2

    payload = {"limit": pagesize, "offset": 0, "searchText": "", "appliedFacets": {}}

    if location_ids:
        payload["appliedFacets"][facet] = (
            location_ids if isinstance(location_ids, list) else [location_ids]
        )

    seen_ids: set[str] = set()
    raw_total = 0
    relevant_total = 0
    no_relevant_pages = 0

    log_start(company_name, "Workday")

    base_url = f"https://{subdomain}.{cluster}.myworkdayjobs.com/en-US/{tenant}"

    while True:
        offset = payload["offset"]

        if offset > maxoffset:
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

        if not jobs:
            log_stop(company_name, f"end of pagination at offset={offset}")
            break

        raw_total += len(jobs)

        # --- Early page skip ---
        keywords = ("japan", "tokyo", "osaka", "remote")
        if not is_global_scrape:
            has_signal = any(
                any(k in (job.get("locationsText") or "").lower() for k in keywords)
                or any(k in str(job.get("externalPath") or "").lower() for k in keywords)
                for job in jobs
            )
            if not has_signal:
                no_relevant_pages += 1
                payload["offset"] += pagesize
                continue

        # --- Dedup page ---
        page_ids = {
            str(job.get("externalPath")) for job in jobs if job.get("externalPath")
        }
        if page_ids.issubset(seen_ids):
            log_stop(company_name, "duplicate page detected")
            break
        seen_ids.update(page_ids)

        batch = []
        filtered_count = 0
        detail_budget = 20 if is_global_scrape else 5

        for job in jobs:
            title = _get_first(job, JOB_TITLE_KEYS)
            external_path = job.get("externalPath")

            if not title or not external_path:
                continue

            external_id = str(external_path)

            locations_text = (job.get("locationsText") or "").strip()
            url_lower = external_path.lower()

            url_indicates_japan = any(k in url_lower for k in ("japan", "tokyo", "osaka"))
            is_multi = "location" in locations_text.lower() or not locations_text

            display_location = locations_text
            filter_location = locations_text.lower()

            # --- Detail fetch ---
            needs_detail = (
                is_multi
                or not locations_text
                or (url_indicates_japan and "japan" not in filter_location)
            )

            if needs_detail and detail_budget > 0:
                detail_budget -= 1

                detail = safe_request(
                    "GET",
                    f"https://{subdomain}.{cluster}.myworkdayjobs.com/wday/cxs/{subdomain}/{tenant}{external_path}",
                    headers={"User-Agent": "Mozilla/5.0"},
                )

                if detail and detail.status_code == 200:
                    try:
                        detail_json = detail.json()
                        locs = _extract_workday_locations_from_detail(detail_json)
                        if locs:
                            display_location = " / ".join(locs)
                    except Exception:
                        pass

            job_url = f"{base_url}{external_path}"

            parsed = parse_job_common(
                job=job,
                company_name=company_name,
                title=title,
                external_id=external_id,
                location_name=display_location,
                job_url=job_url,
            )

            # --- URL-based override (critical fix) ---
            if not parsed and url_indicates_japan:
                parsed = parse_job_common(
                    job=job,
                    company_name=company_name,
                    title=title,
                    external_id=external_id,
                    location_name="Tokyo, Japan (Multiple Locations)",
                    job_url=job_url,
                )

            if not parsed:
                continue

            filtered_count += 1
            relevant_total += 1
            batch.append(parsed)

        if batch:
            upsert_jobs(company_name, batch)

        log_page(
            company_name,
            offset=offset,
            api_total=total,
            raw_in_page=len(jobs),
            relevant_in_page=filtered_count,
        )

        # --- page stopping logic ---
        if filtered_count == 0:
            no_relevant_pages += 1
        else:
            no_relevant_pages = 0

        if no_relevant_pages >= max_irrelevant_pages:
            log_stop(company_name, f"{no_relevant_pages} irrelevant pages")
            break

        if len(jobs) < pagesize:
            log_stop(company_name, "last page")
            break

        payload["offset"] += pagesize

    log_summary(company_name, raw_total, relevant_total)
    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

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

    jobs = data if isinstance(data, list) else []
    raw_total = len(jobs)

    log_page(company_name, offset=0, raw_in_page=raw_total, extra="single API response")

    seen_ids: set[str] = set()
    batch = []

    for job in jobs:
        title = _get_first(job, JOB_TITLE_KEYS)
        external_id = _get_first(job, JOB_ID_KEYS)

        if not title or not external_id:
            continue

        external_id = str(external_id)

        # --- location ---
        categories = job.get("categories") or {}
        location_name = categories.get("location") or ""

        workplace_type = job.get("workplaceType") or ""
        if workplace_type.lower() == "remote" and "remote" not in location_name.lower():
            location_name = f"Remote, {location_name}".strip(", ")

        # --- url ---
        job_url = job.get("hostedUrl") or f"https://jobs.lever.co/{company_slug}/{external_id}"

        parsed = parse_job_common(
            job=job,
            company_name=company_name,
            title=title,
            external_id=external_id,
            location_name=location_name,
            job_url=job_url,
        )

        if not parsed:
            continue

        seen_ids.add(external_id)
        batch.append(parsed)

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, len(batch))

    # --- failure condition ---
    if raw_total == 0:
        log_error(company_name, "API returned zero jobs (possible structure change or block)")
        return False

    mark_removed_jobs(company_name, seen_ids)
    log_success(company_name)

    return True


# ---------------------------------------------------------------------------
# eightfold.ai
# ---------------------------------------------------------------------------


def scrape_eightfold(
    company_slug: str,
    company_name: str,
    location: str,
    pid: str,
) -> bool:
    log_start(company_name, "Eightfold")

    base_url = f"https://{company_slug}.eightfold.ai/api/apply/v2/jobs"

    page_size = 10
    max_offset = 200
    offset = 0

    raw_total = 0
    relevant_total = 0
    seen_ids: set[str] = set()

    while True:
        if offset > max_offset:
            log_stop(company_name, f"safety cap offset>{max_offset}")
            break

        params = {
            "domain": f"{company_slug}.com",
            "start": offset,
            "num": page_size,
            "location": location,
            "pid": pid,
            "sort_by": "relevance",
            "hl": "en",
            "triggerGoButton": "false",
        }

        response = safe_request(
            "GET",
            base_url,
            params=params,
            headers={"User-Agent": "Mozilla/5.0"},
        )

        if not response:
            log_error(company_name, "Eightfold API: no response after retries")
            return False

        if response.status_code != 200:
            log_error(company_name, f"Eightfold API: status {response.status_code}")
            return False

        try:
            data = response.json()
        except Exception as e:
            log_error(company_name, f"JSON parse error: {e}")
            return False

        jobs = data.get("positions") or []

        if not jobs:
            log_stop(company_name, f"empty batch at offset={offset}")
            break

        raw_total += len(jobs)
        batch = []

        for job in jobs:
            title = _get_first(job, JOB_TITLE_KEYS)
            external_id = _get_first(job, JOB_ID_KEYS)

            if not title or not external_id:
                continue

            external_id = str(external_id)

            location_name = job.get("location") or ""

            job_url = (
                job.get("canonicalPositionUrl")
                or f"https://{company_slug}.eightfold.ai/careers/job/{external_id}"
            )

            parsed = parse_job_common(
                job=job,
                company_name=company_name,
                title=title,
                external_id=external_id,
                location_name=location_name,
                job_url=job_url,
            )

            if not parsed:
                continue

            seen_ids.add(external_id)
            batch.append(parsed)

        if batch:
            upsert_jobs(company_name, batch)
            relevant_total += len(batch)

        log_page(
            company_name,
            start=offset,
            raw_in_page=len(jobs),
            relevant_in_page=len(batch),
        )

        if len(jobs) < page_size:
            log_stop(company_name, "last page (batch < page_size)")
            break

        offset += page_size

    log_summary(company_name, raw_total, relevant_total)

    # --- soft failure ---
    if raw_total > 0 and relevant_total == 0:
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

    # --- normalize job list ---
    if isinstance(data, list):
        jobs = data
    elif isinstance(data, dict):
        jobs = data.get("result") or data.get("jobs") or []
    else:
        jobs = []

    raw_total = len(jobs)

    log_page(company_name, offset=0, raw_in_page=raw_total, extra="single list response")

    seen_ids: set[str] = set()
    batch = []
    relevant = 0

    for job in jobs:
        title = _get_first(job, JOB_TITLE_KEYS)
        external_id = _get_first(job, JOB_ID_KEYS)

        if not title or not external_id:
            continue

        external_id = str(external_id)

        # --- location normalization ---
        loc = job.get("location")

        if isinstance(loc, dict):
            location_name = ", ".join(
                p for p in (loc.get("city"), loc.get("state"), loc.get("country")) if p
            )
        elif isinstance(loc, str):
            location_name = loc
        else:
            location_name = ""

        location_name = location_name.strip() if location_name else UNKNOWN_LOCATION

        # --- BambooHR location fallback ---
        if location_name == UNKNOWN_LOCATION:
            if job.get("isRemote") or job.get("locationType") == "1":
                location_name = "Remote"
            else:
                location_name = "Remote / Unknown"

        # --- url ---
        job_url = (
            job.get("jobUrl")
            or job.get("applyUrl")
            or f"https://{subdomain}.bamboohr.com/careers/{external_id}"
        )

        # track seen BEFORE filtering
        seen_ids.add(external_id)

        parsed = parse_job_common(
            job=job,
            company_name=company_name,
            title=title,
            external_id=external_id,
            location_name=location_name,
            job_url=job_url,
        )

        # fallback: keep job if parsing fails but it's the only job
        if not parsed and raw_total == 1:
            parsed = job_row(
                company_name,
                external_id,
                title,
                location_name,
                job_url,
                *classify_job(title),
                None,  # region
                True if "remote" in location_name.lower() else False,
                False,
                "global",
                True,
            )

        if not parsed:
            continue

        batch.append(parsed)
        relevant += 1

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant)

    if raw_total == 0:
        log_error(company_name, "no jobs returned from API")
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
    max_offset = 200

    seen_ids: set[str] = set()
    seen_pages: set[tuple] = set()

    raw_total = 0
    relevant_total = 0

    while start <= max_offset:

        params["start"] = start

        try:
            response = safe_request("GET", base_url, params=params)

            if not response:
                log_error(company_name, "no HTTP response after retries")
                break

            data = response.json()

        except Exception as e:
            log_error(company_name, f"request/JSON failure: {e}")
            break

        jobs = data.get("positions") or []

        if not isinstance(jobs, list):
            log_error(company_name, "unexpected response shape")
            break

        if not jobs:
            if start == 0:
                log_error(company_name, "no jobs found on first page")
                return False
            log_stop(company_name, f"empty batch at start={start}")
            break

        # --- page dedup safety ---
        page_sig = tuple(sorted(str(j.get("id")) for j in jobs if j.get("id")))
        if page_sig in seen_pages:
            log_stop(company_name, "duplicate page detected")
            break
        seen_pages.add(page_sig)

        raw_total += len(jobs)
        batch = []
        page_relevant = 0

        for job in jobs:
            external_id = _get_first(job, JOB_ID_KEYS)
            title = _get_first(job, JOB_TITLE_KEYS)
            location_name = job.get("location", "")

            if not external_id or not title:
                continue

            external_id = str(external_id)

            region, is_remote, is_japan, remote_scope, is_valid = (
                enrich_and_validate_location(
                    location_name,
                    company_name,
                    external_id,
                    title,
                )
            )

            if not is_valid:
                continue

            job_url = (
                "https://explore.jobs.netflix.net/careers/apply"
                f"?domain=netflix.com&pid={external_id}"
            )

            parsed = job_row(
                company_name,
                external_id,
                title,
                location_name,
                job_url,
                *classify_job(title),
                region,
                is_remote,
                is_japan,
                remote_scope,
                is_valid,
            )

            if not parsed:
                continue

            seen_ids.add(external_id)
            batch.append(parsed)
            page_relevant += 1

        if batch:
            upsert_jobs(company_name, batch)

        log_page(
            company_name,
            start=start,
            raw_in_page=len(jobs),
            relevant_in_page=page_relevant,
        )

        relevant_total += page_relevant

        # --- stopping logic aligned with ecosystem ---
        if len(jobs) < params["num"]:
            log_stop(company_name, "last page (results < num)")
            break

        if page_relevant == 0:
            log_stop(company_name, "no relevant jobs on page")
            break

        start += params["num"]
        time.sleep(random.uniform(1.0, 2.0))

    log_summary(company_name, raw_total, relevant_total)

    if raw_total == 0:
        log_error(company_name, "no jobs returned from Netflix API")
        return False

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

    limit = 10
    page = 0
    max_pages = 200

    raw_total = 0
    relevant_total = 0

    seen_ids: set[str] = set()
    seen_pages: set[tuple] = set()

    base_url = "https://www.uber.com/global/en/careers/list"

    while page <= max_pages:

        payload = {
            "limit": limit,
            "page": page,
            "params": {"location": [{"country": "JPN"}]},
        }

        response = safe_request(
            "POST",
            url,
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
            log_error(company_name, f"JSON parse error at page={page}: {e}")
            break

        results = (data.get("data") or {}).get("results")

        if not isinstance(results, list):
            log_error(company_name, f"unexpected response shape at page={page}")
            break

        if not results:
            log_stop(company_name, f"empty page at page={page}")
            break

        # --- page dedup safety ---
        page_sig = tuple(sorted(str(j.get("id")) for j in results if j.get("id")))
        if page_sig in seen_pages:
            log_stop(company_name, "duplicate page detected")
            break
        seen_pages.add(page_sig)

        raw_total += len(results)

        batch = []
        page_relevant = 0

        for job in results:
            external_id = _get_first(job, JOB_ID_KEYS)
            title = _get_first(job, JOB_TITLE_KEYS)

            if not external_id or not title:
                continue

            external_id = str(external_id)
            if external_id == "None":
                continue

            location_name = extract_uber_location(job)
            if not location_name:
                continue

            # --- unified validation layer (single source of truth) ---
            region, is_remote, is_japan, remote_scope, is_valid = (
                enrich_and_validate_location(
                    location_name,
                    company_name,
                    external_id,
                    title,
                )
            )

            if not is_valid:
                continue

            job_url = f"{base_url}/{external_id}/"

            parsed = job_row(
                company_name,
                external_id,
                title,
                location_name,
                job_url,
                classify_job(title)[0],  # seniority
                classify_job(title)[1],  # role
                region,
                is_remote,
                is_japan,
                remote_scope,
                is_valid,
            )

            if not parsed:
                continue

            seen_ids.add(external_id)
            batch.append(parsed)
            page_relevant += 1

        if batch:
            upsert_jobs(company_name, batch)

        log_page(
            company_name,
            offset=page,
            raw_in_page=len(results),
            relevant_in_page=page_relevant,
        )

        relevant_total += page_relevant

        # --- stopping logic aligned with ecosystem ---
        if len(results) < limit:
            log_stop(company_name, "last page (results < limit)")
            break

        if page_relevant == 0:
            log_stop(company_name, "no relevant jobs on page")
            break

        page += 1
        time.sleep(random.uniform(1.0, 2.0))

    log_summary(company_name, raw_total, relevant_total)

    if raw_total == 0:
        log_error(company_name, "no jobs returned from Uber API")
        return False

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
    relevant_total = 0
    got_any_response = False

    # --- helpers ---
    def extract_meta_jobs(data: dict) -> list[dict]:
        try:
            d = data.get("data", {})
            node = d.get("job_search_with_featured_jobs", {})
            all_jobs = node.get("all_jobs", []) if node else []

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
        locations = job.get("locations", []) or []

        names = []
        for loc in locations:
            if isinstance(loc, dict):
                name = loc.get("name")
            else:
                name = str(loc)

            if name:
                names.append(name.strip())

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
                nonlocal raw_total, relevant_total, got_any_response

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

                    parsed = parse_job_common(
                        job=job,
                        company_name=company_name,
                        title=title,
                        external_id=external_id,
                        location_name=location_name,
                        job_url=f"https://www.metacareers.com/jobs/{external_id}/",
                    )

                    if not parsed:
                        continue

                    batch.append(parsed)
                    relevant_total += 1

                if batch:
                    upsert_jobs(company_name, batch)

            page.on("response", handle_response)

            page.goto(
                "https://www.metacareers.com/jobsearch?offices[0]=Tokyo%2C%20Japan",
                wait_until="networkidle",
            )

            page.wait_for_timeout(8000)
            browser.close()

    except Exception as e:
        log_error(company_name, f"Playwright failure: {e}")
        return False

    # --- failure conditions ---
    if not got_any_response:
        log_error(company_name, "no GraphQL job data captured")
        return False

    if raw_total == 0:
        log_error(company_name, "no jobs found from API")
        return False

    # --- success ---
    log_summary(company_name, raw_total, relevant_total)
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
        log_error(
            company_name,
            f"HTTP error: {getattr(response, 'status_code', 'no response')}",
        )
        return False

    soup = BeautifulSoup(response.text, "html.parser")

    links = soup.select("a[href*='/jobs/']")

    raw_total = len(links)
    relevant_total = 0

    seen_ids: set[str] = set()

    log_page(company_name, raw_in_page=raw_total, extra="HTML scrape")

    # --- helper ---
    def parse_wayve_text(text: str):
        text = " ".join(text.split())

        # remove noise suffixes
        text = re.sub(
            r"(full time|part time|on-site|remote).*?$",
            "",
            text,
            flags=re.IGNORECASE,
        ).strip()

        # extract trailing location if present
        match = re.search(r"\(([^)]+)\)\s*$", text)
        if match:
            location = match.group(1).strip()
            title = text[: match.start()].strip().rstrip(",- ").strip()
            return title, location

        return text.strip(), ""

    batch = []

    for a in links:
        href = a.get("href")
        if not href or "/jobs/" not in href:
            continue

        job_url = (
            href
            if href.startswith("http")
            else f"https://wayve.firststage.co{href}"
        )

        try:
            external_id = job_url.split("/jobs/")[1].split("/")[0]
        except Exception:
            continue

        if not external_id:
            continue

        raw_text = a.get_text(" ", strip=True)
        if not raw_text:
            continue

        title, location_name = parse_wayve_text(raw_text)

        if not title:
            continue

        region, is_remote, is_japan, remote_scope, is_valid = (
            enrich_and_validate_location(location_name, title=title)
        )

        if not is_valid:
            continue

        parsed = parse_job_common(
            job={},
            company_name=company_name,
            title=title,
            external_id=external_id,
            location_name=location_name,
            job_url=job_url,
        )

        if not parsed:
            continue

        seen_ids.add(external_id)
        batch.append(parsed)
        relevant_total += 1

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant_total)
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
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://careers.withwaymo.com/",
    }

    # warm session
    safe_request("GET", "https://careers.withwaymo.com/", headers=headers)

    response = safe_request("GET", url, params=params, headers=headers)

    if not response or response.status_code != 200:
        log_error(company_name, f"HTTP {getattr(response, 'status_code', 'no response')}")
        return False

    soup = BeautifulSoup(response.text, "html.parser")
    jobs = soup.select("article.job-search-results-card-col")

    raw_total = len(jobs)
    relevant_total = 0

    seen_ids: set[str] = set()

    log_page(company_name, raw_in_page=raw_total, extra="HTML parse")

    if raw_total == 0:
        log_error(company_name, "no job cards found (possible HTML change)")
        return False

    batch = []

    for job in jobs:
        try:
            a = job.select_one("h3 a")
            if not a:
                continue

            title = a.get_text(strip=True)
            href = a.get("href")

            if not title or not href:
                continue

            full_url = (
                href
                if href.startswith("http")
                else f"https://careers.withwaymo.com{href}"
            )

            external_id = full_url.rstrip("/").split("/")[-1]
            if not external_id:
                continue

            # --- location ---
            loc_el = job.select_one(".job-component-location span")
            location_name = loc_el.get_text(strip=True) if loc_el else ""

            # normalize lightweight (no hardcoding "Tokyo, Japan")
            if location_name == "Tokyo":
                location_name = "Tokyo, Japan"

            # --- unified pipeline ---
            parsed = parse_job_common(
                job={},
                company_name=company_name,
                title=title,
                external_id=external_id,
                location_name=location_name,
                job_url=full_url,
            )

            if not parsed:
                continue

            seen_ids.add(external_id)
            batch.append(parsed)
            relevant_total += 1

        except Exception as e:
            log_error(company_name, f"parse error: {e}")
            continue

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant_total)
    mark_removed_jobs(company_name, seen_ids)

    if raw_total > 0 and relevant_total == 0:
        log_error(company_name, "all jobs filtered out by validation")
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

    try:
        data = response.json()
    except Exception as e:
        log_error(company_name, f"JSON parse error: {e}")
        return False

    # --- safe extraction (no fragile dict assumptions) ---
    jobs = []

    if isinstance(data, dict):
        for key in ("jobs", "data", "positions", "items"):
            if isinstance(data.get(key), list):
                jobs = data.get(key)
                break

    elif isinstance(data, list):
        jobs = data

    if not isinstance(jobs, list):
        log_error(company_name, "unexpected API response format")
        return False

    raw_total = len(jobs)

    log_page(company_name, raw_in_page=raw_total, extra="single API response")

    seen_ids: set[str] = set()
    batch = []
    relevant_total = 0

    for job in jobs:
        if not isinstance(job, dict):
            continue

        title = job.get("title")
        external_id = job.get("id")

        if not title:
            continue

        external_id = str(external_id) if external_id else None

        apply_url = job.get("url") or job.get("applyUrl")

        if not external_id or not apply_url:
            continue

        location_name = (
            job.get("location")
            or job.get("locations")
            or job.get("country")
            or ""
        )

        parsed = parse_job_common(
            job=job,
            company_name=company_name,
            title=title,
            external_id=external_id,
            location_name=location_name,
            job_url=apply_url,
        )

        if not parsed:
            continue

        seen_ids.add(external_id)
        batch.append(parsed)
        relevant_total += 1

    if batch:
        upsert_jobs(company_name, batch)

    log_summary(company_name, raw_total, relevant_total)
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


def scrape_linkedin() -> None:
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
    print(f"LinkedIn RSS: {len(unique_posts)} unique posts queued.")


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

            print(f"✅ SUCCESS {label} ({duration}s)")
            return {
                "label": label,
                "success": True,
                "duration": duration,
                "error": None,
            }

        except Exception as e:
            duration = round(time.time() - start, 2)
            error_msg = str(e)

            print(
                f"⚠️ Attempt {attempt + 1}/2 failed for {label} ({duration}s): {error_msg}"
            )

            if attempt == 1:
                # final failure
                print(f"❌ FAILED {label} after 2 attempts")
                return {
                    "label": label,
                    "success": False,
                    "duration": duration,
                    "error": error_msg,
                }

            time.sleep(2 * (attempt + 1))


SCRAPER_TASKS: list[tuple] = [
    (scrape_bamboohr, "lottiefiles", "LottieFiles"),
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

    # ✅ Summary
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

    # ❌ Failed scraper details
    if failed_tasks:
        print("\n❌ Failed scrapers:")
        for label, error in failed_tasks:
            print(f" - {label}: {error}")

    # ⏱ Slowest scrapers
    if durations:
        durations_sorted = sorted(durations, key=lambda x: x[1], reverse=True)

        print("\n⏱ Slowest scrapers:")
        for label, duration in durations_sorted[:5]:
            print(f" - {label}: {duration}s")

    print("=" * 50)


if __name__ == "__main__":
    main()
