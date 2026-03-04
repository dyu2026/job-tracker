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

    loc_lower = location_name.lower().strip()

    # Split on '/' and ',' and strip spaces
    loc_parts = [part.strip() for part in loc_lower.replace("/", ",").split(",")]

    # Detect Japan
    japan_terms = ["japan", "tokyo", "osaka", "yokohama"]
    is_japan = any(any(term in part for term in japan_terms) for part in loc_parts)

    # Detect Remote
    is_remote = "remote" in loc_lower
    remote_scope = None

    # Restricted locations
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

    # If Japan is anywhere, allow
    if is_japan:
        return "Japan", is_remote, True, "japan"

    # Handle remote jobs
    if is_remote:
        if any(term in loc_lower for term in ["anywhere", "worldwide", "global"]):
            remote_scope = "global"
        elif "apac" in loc_lower:
            remote_scope = "apac"
        elif any(any(term in part for term in restricted_keywords) for part in loc_parts):
            remote_scope = "restricted"
        else:
            remote_scope = "global"
    else:
        # Non-remote, non-Japan → check APAC countries
        apac_terms = ["apac", "asia", "hong kong", "singapore", "south korea"]
        if any(any(term in part for term in apac_terms) for part in loc_parts):
            remote_scope = "apac"
        else:
            remote_scope = "restricted"

    return None, is_remote, False, remote_scope