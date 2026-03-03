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

    loc = location_name.lower()

    # Detect Japan
    is_japan = "japan" in loc or "tokyo" in loc or "osaka" in loc

    # Detect Remote
    is_remote = "remote" in loc

    remote_scope = None

    if is_remote:
        if any(term in loc for term in ["anywhere", "worldwide", "global"]):
            remote_scope = "global"
        elif "apac" in loc:
            remote_scope = "apac"
        elif "japan" in loc:
            remote_scope = "japan"
        elif any(term in loc for term in [
            "us", "united states",
            "canada",
            "uk", "united kingdom",
            "germany",
            "france",
            "india",
            "singapore",
            "australia"
        ]):
            remote_scope = "country_specific"
        else:
            # Plain "Remote" with no qualifier
            remote_scope = "global"

    # Region (optional classification)
    region = "Japan" if is_japan else None

    return region, is_remote, is_japan, remote_scope