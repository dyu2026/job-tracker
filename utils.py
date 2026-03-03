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

    # ------------------------
    # Japan detection
    # ------------------------
    is_japan = any(term in loc for term in [
        "japan", "tokyo", "osaka", "yokohama"
    ])

    # ------------------------
    # Remote detection
    # ------------------------
    is_remote = "remote" in loc

    remote_scope = None

    # Regions we EXCLUDE
    restricted_terms = [
        "us", "united states",
        "canada",
        "america", "americas",
        "north america",
        "emea",
        "ireland",
        "netherlands",
        "united arab emirates", "uae",
        "south korea",
        "italy",
        "spain",
        "sweden",
        "brazil",
        "australia"
    ]

    if is_remote:
        # Exclude restricted region remotes
        if any(term in loc for term in restricted_terms):
            remote_scope = "restricted"

        # Explicit APAC
        elif "apac" in loc:
            remote_scope = "apac"

        # Explicit Japan
        elif "japan" in loc:
            remote_scope = "japan"

        # Explicit global indicators
        elif any(term in loc for term in [
            "anywhere", "worldwide", "global"
        ]):
            remote_scope = "global"

        # Plain "Remote" with no region
        else:
            remote_scope = "global"

    region = "Japan" if is_japan else None

    return region, is_remote, is_japan, remote_scope