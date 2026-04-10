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
    # Americas
    "united states",
    "usa",
    "us-",
    "-us",
    "us ",
    " us",
    "new york",
    "san francisco",
    "seattle",
    "chicago",
    "atlanta",
    "west coast",
    "north america",
    "us/ca",
    "americas",
    "nyc",
    "amers",
    "canada",
    "toronto",
    "british columbia",
    "latam",
    "buenos aires",
    "santiago",
    "mexico",
    "chile",
    "columbia",
    "colombia",
    "peru",
    "ecuador",
    "costa rica",
    "el salvador",
    # Europe / MENA
    "europe",
    "emea",
    "france",
    "germany",
    "ireland",
    "netherlands",
    "spain",
    "sweden",
    "italy",
    "united kingdom",
    "amsterdam",
    "london",
    "stockholm",
    "belgium",
    "denmark",
    "lithuania",
    "berlin",
    "czech republic",
    "prague",
    "milan",
    # Asia / Pacific (non-Japan focus for restriction)
    "singapore",
    "sydney",
    "bangkok",
    "thailand",
    "sea",
    "vietnam",
    "south korea",
    "bangalore",
    "india",
    # Other
    "brazil",
    "australia",
    "united arab emirates",
    "uae",
    "switzerland",
    "poland",
    "south africa",
    "portugal",
    "vancouver",
)

ROLE_KEYWORDS = [
    ("solutions architect and engineer", [
        "solutions architect",
        "solution architect",
        "solutions engineer",
        "solution engineer"
    ]),
    
    ("customer solution", [
        "customer solutions",
        "customer solution",
        "solutions consultant",
        "solution consultant",
        "implementation consultant"
    ]),

    ("Communications and PR", [
        "communications",
        "public relations",
        "pr",
        "media relations",
        "publicity"
    ]),

    ("product management", [
        "product manager", "product management", "product owner", "product lead",
        "cpo"
    ]),

    ("engineering", [
        "engineer", "developer", "software", "backend", "frontend", "full stack",
        "devops", "platform", "mobile", "ios", "android", "cto"
    ]),

    ("design", [
        "designer", "ux", "ui", "product design", "visual", "design"
    ]),

    ("data and analytics", [
        "data", "analytics", "analyst", "machine learning", "ml", "ai"
    ]),

    ("marketing", [
        "marketing", "growth", "seo", "content", "brand", "market"
    ]),

    ("sales", [
        "sales", "account executive", "account manager", "cro"
    ]),

    ("business development", [
        "business development", "bd"
    ]),

    ("customer success and experience", [
        "customer success", "customer support", "customer experience", "csm"
    ]),

    ("HR and recruiting", [
        "recruiter", "talent", "hr", "people"
    ]),

    ("finance and accounting", [
        "finance", "accounting", "fp&a", "controller"
    ]),

    ("operations and support", [
        "operations", "ops", "support"
    ]),

    ("program and project management", [
        "program manager", "project manager"
    ]),

    ("Information Technology", [
        "it", "information technology", "systems administrator"
    ]),

    ("security", [
        "security", "infosec", "cybersecurity"
    ]),

    ("legal", [
        "legal", "counsel", "compliance"
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
