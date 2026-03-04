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

    # -------------------------
    # JAPAN DETECTION
    # -------------------------
    japan_terms = [
        "japan", "tokyo", "osaka", "yokohama"
    ]
    is_japan = any(term in loc for term in japan_terms)

    # -------------------------
    # REMOTE DETECTION
    # -------------------------
    is_remote = "remote" in loc
    remote_scope = None

    # -------------------------
    # HARD RESTRICTED KEYWORDS
    # -------------------------
    restricted_keywords = [
        # United States variations
        "united states", "usa", "us-", "us ", " us",
        "new york", "san francisco", "seattle",
        "chicago", "atlanta",
        "west coast",

        # Canada
        "canada", "toronto", "british columbia",

        # Europe
        "europe", "emea",

        # APAC countries you want excluded
        "singapore", "sydney", "bangkok",

        # Other countries
        "france", "germany", "ireland", "netherlands",
        "spain", "sweden", "italy", "india",
        "brazil", "australia", "united kingdom",
        "south korea", "united arab emirates", "uae"
    ]

    # If Japan, allow immediately
    if is_japan:
        return "Japan", is_remote, True, "japan"

    # If remote
    if is_remote:

        # Explicitly allowed global indicators
        if any(term in loc for term in ["anywhere", "worldwide", "global"]):
            remote_scope = "global"

        elif "apac" in loc:
            remote_scope = "apac"

        # If restricted keyword appears → block
        elif any(term in loc for term in restricted_keywords):
            remote_scope = "restricted"

        # If contains comma or slash after remote,
        # assume region-qualified (e.g., Remote, US / US-Remote)
        elif "," in loc or "/" in loc or "-" in loc:
            remote_scope = "restricted"

        # Plain "Remote"
        else:
            remote_scope = "global"

    else:
        # Non-remote and non-Japan → restricted
        remote_scope = "restricted"

    return None, is_remote, False, remote_scope