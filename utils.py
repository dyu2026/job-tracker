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
    japan_terms = ["japan", "tokyo", "osaka", "yokohama"]
    is_japan = any(term in loc for term in japan_terms)

    # ------------------------
    # Remote detection
    # ------------------------
    is_remote = "remote" in loc
    remote_scope = None

    # Explicit allowed global indicators
    global_terms = ["anywhere", "worldwide", "global"]

    # Explicit allowed regional indicator
    apac_terms = ["apac"]

    # If remote
    if is_remote:

        # Explicit allowed cases first
        if any(term in loc for term in global_terms):
            remote_scope = "global"

        elif any(term in loc for term in apac_terms):
            remote_scope = "apac"

        elif "japan" in loc:
            remote_scope = "japan"

        # If location contains a comma after "remote",
        # assume region-qualified and therefore restricted
        elif "," in loc:
            remote_scope = "restricted"

        # Plain "Remote" with no qualifier
        else:
            remote_scope = "global"

    region = "Japan" if is_japan else None

    return region, is_remote, is_japan, remote_scope