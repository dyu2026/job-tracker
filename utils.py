"""Derive seniority, role, and geography hints from job/location text."""

from __future__ import annotations
import re

# --- Location: Japan substring hints ---

JAPAN_LOCATION_TERMS = (
    "japan",
    "jp",
    "jpn",
    "tokyo",
    "osaka",
    "yokohama",
    "kanagawa",
    "chiba",
)

# --- Location: remote string suggests "work from anywhere" style ---

GLOBAL_REMOTE_TERMS = ("anywhere", "worldwide", "global")

# --- Location: if remote but text looks tied to these regions, scope = restricted ---

RESTRICTED_REGION_KEYWORDS = (
    "united states", "usa", "us",
    "new york", "nyc", "san francisco", "seattle", "chicago", "atlanta",
    "north america", "americas", "amers", "california",
    "canada", "toronto", "vancouver", "british columbia", "montreal",
    "latam", "mexico", "brazil", "chile", "argentina",
    "buenos aires", "santiago",
    "colombia", "peru", "ecuador",
    "costa rica", "el salvador", "alberta",
    "europe", "emea",
    "united kingdom", "london",
    "france", "germany", "ireland", "netherlands",
    "spain", "sweden", "italy", "norway", "finland",
    "belgium", "denmark", "lithuania", "lisbon",
    "czech republic", "prague",
    "berlin", "amsterdam", "stockholm", "milan",
    "switzerland", "poland", "portugal",
    "united arab emirates", "uae", "saudi arabia",
    "singapore", "australia", "sydney",
    "bangkok", "thailand", "vietnam", "kuala lumpur",
    "south korea", "india", "bangalore",
    "sea", "philippines", 
    "south africa", "israel", "serbia",
    "baku", "cis",
)

ROLE_KEYWORDS = [
    ("solutions architect and engineer", [
        "solutions architect", "solution architect", "solutions engineer",
        "solution engineer"
    ]),
    
    ("customer solution", [
        "customer solutions", "customer solution", "solutions consultant",
        "solution consultant", "solutions consulting", "implementation consultant", 
        "tam", "technical account manager", "consultant", "consulting",
        "implementation", "professional services", "presales", "technical solutions",
        "technical deployment", "functional consultant"
    ]),

    ("Communications and PR", [
        "communications", "public relations", "pr", "media relations",
        "publicity"
    ]),

    ("product management", [
        "product manager", "product management", "product owner", "product lead",
        "cpo", "product merchandising", "product solutions"
    ]),

    ("engineering", [
        "engineer", "developer", "software", "backend", "frontend", "full stack",
        "devops", "platform", "mobile", "ios", "android", "cto", "engineering",
        "tech lead", "technical architect"
    ]),

    ("design", [
        "designer", "ux", "ui", "product design", "visual", "design", "creative director",
        "copywriter"
    ]),

    ("data and analytics", [
        "data", "analytics", "analyst", "machine learning", "ml", "ai", "insights"
    ]),

    ("marketing", [
        "marketing", "growth", "seo", "content", "brand", "market", "events", "field marketer",
        "community manager"
    ]),

    ("business development", [
        "business development", "bd", "partner manager", "partner development",
        "partner business", "partner relations", "partnerships", "alliances", "channel",
        "managing partner", "strategy", "strategic", "expansion"
    ]),

    ("sales", [
        "sales", "account executive", "account manager", "cro", "account management",
        "gtm", "partnerships", "account director"
    ]),

    ("customer success and experience", [
        "customer success", "customer support", "customer experience", "csm",
        "account manager", "onboarding", "renewals", "customer performance",
        "customer care", "claims experience"
    ]),

    ("HR and recruiting", [
        "recruiter", "talent", "hr", "people"
    ]),

    ("finance and accounting", [
        "finance", "accounting", "fp&a", "controller", "accountant"
    ]),

    ("operations and support", [
        "operations", "ops", "support", "administrative", "clerk",
        "health keeper"
    ]),

    ("program and project management", [
        "program manager", "project manager", "engagement manager",
        "delivery manager", "engagement lead", "program management",
        "project management"
    ]),

    ("Information Technology", [
        "it", "information technology", "systems administrator",
        "technology"
    ]),

    ("security", [
        "security", "infosec", "cybersecurity"
    ]),

    ("legal", [
        "legal", "counsel", "compliance", "IP relations", "crime",
        "sanctions", "auditor", "public policy"
    ]),

    ("research and development", [
        "research", "scientist", "r&d"
    ]),

    ("supply chain and procurement", [
        "supply chain", "procurement", "purchasing"
    ]),
]

def classify_role(title: str) -> str:
    t = title.lower()

    for role, keywords in ROLE_KEYWORDS:
        for kw in keywords:
            pattern = rf"\b{re.escape(kw)}\b"
            if re.search(pattern, t):
                return role

    return "other"


def classify_job(title: str) -> tuple[str, str]:
    """Map title text to (seniority_bucket, role_bucket)."""
    t = title.lower()

    # --- Seniority ---
    if "director" in t or "vp" in t:
        seniority = "Director+"
    elif "senior" in t or "sr" in t:
        seniority = "Senior"
    else:
        seniority = "Mid/Other"

    role = classify_role(title)
    return seniority, role


def classify_location(
    location_name: str | None,
) -> tuple[str | None, bool, bool, str | None]:
    """
    From a single location string, return:
        region_label, is_remote, is_japan, remote_scope

    remote_scope is None unless the text implies remote; then one of
    global / apac / restricted / japan (japan only when is_japan).
    """
    if not location_name:
        return None, False, False, None

    loc = location_name.lower().strip()

    is_japan = any(term in loc for term in JAPAN_LOCATION_TERMS)
    is_remote = "remote" in loc
    remote_scope: str | None = None

    if is_japan:
        return "Japan", is_remote, True, "japan"

    if is_remote:
        if any(term in loc for term in GLOBAL_REMOTE_TERMS):
            remote_scope = "global"
        elif "apac" in loc:
            remote_scope = "apac"
        elif "asia" in loc:
            remote_scope = "apac"
        elif any(term in loc for term in RESTRICTED_REGION_KEYWORDS):
            remote_scope = "restricted"
        else:
            remote_scope = "global"
    else:
        remote_scope = "restricted"

    return None, is_remote, False, remote_scope
