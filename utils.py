def classify_job(title):
    title_lower = title.lower()

    # Seniority
    if "director" in title_lower or "vp" in title_lower:
        seniority = "Director+"
    elif "senior" in title_lower or "sr" in title_lower:
        seniority = "Senior"
    else:
        seniority = "Mid/Other"

    # Function
    if "product" in title_lower:
        function = "Product"
    elif "engineer" in title_lower or "engineering" in title_lower:
        function = "Engineering"
    elif "design" in title_lower:
        function = "Design"
    else:
        function = "Other"

    return seniority, function
    
def classify_location(location_name):
    if not location_name:
        return None, False, False, None

    loc = location_name.lower().strip()

    # Split multiple locations
    loc_parts = [part.strip() for part in loc.replace("/", ",").split(",")]

    is_japan = any(any(term in part for term in ["japan", "tokyo", "osaka", "yokohama"]) for part in loc_parts)
    is_remote = "remote" in loc
    remote_scope = None

    restricted_keywords = [
        "united states", "usa", "us-", "us ", " us",
        "new york", "san francisco", "seattle",
        "chicago", "atlanta", "west coast",
        "canada", "toronto", "british columbia",
        "europe", "emea",
        "singapore", "sydney", "bangkok",
        "france", "germany", "ireland", "netherlands",
        "spain", "sweden", "italy", "india",
        "brazil", "australia", "united kingdom",
        "south korea", "united arab emirates", "uae"
    ]

    # If any location is Japan, allow immediately
    if is_japan:
        return "Japan", is_remote, True, "japan"

    if is_remote:
        # Explicitly allowed global indicators
        if any(term in loc for term in ["anywhere", "worldwide", "global"]):
            remote_scope = "global"
        elif "apac" in loc:
            remote_scope = "apac"
        # If restricted keyword appears in any part → restrict
        elif any(any(term in part for term in restricted_keywords) for part in loc_parts):
            remote_scope = "restricted"
        else:
            remote_scope = "global"
    else:
        # Non-remote and non-Japan → check if any part is APAC
        if any(term in part for part in loc_parts for term in ["apac", "asia", "south korea", "singapore", "hong kong"]):
            remote_scope = "apac"
        else:
            remote_scope = "restricted"

    return None, is_remote, False, remote_scope