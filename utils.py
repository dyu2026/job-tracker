"""Derive seniority, function, and geography hints from job title and location text."""

from __future__ import annotations

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


def classify_job(title: str) -> tuple[str, str]:
    """Map title text to (seniority_bucket, function_bucket)."""
    t = title.lower()

    if "director" in t or "vp" in t:
        seniority = "Director+"
    elif "senior" in t or "sr" in t:
        seniority = "Senior"
    else:
        seniority = "Mid/Other"

    if "product" in t:
        function = "Product"
    elif "engineer" in t or "engineering" in t:
        function = "Engineering"
    elif "design" in t:
        function = "Design"
    else:
        function = "Other"

    return seniority, function


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
