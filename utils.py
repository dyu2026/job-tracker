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

    location_lower = location_name.lower()

    is_remote = False
    is_japan = False
    region = "Other"
    remote_scope = None

    # --- Remote detection ---
    if "remote" in location_lower or "anywhere" in location_lower:
        is_remote = True

        if "anywhere" in location_lower:
            remote_scope = "Global"
        elif "japan" in location_lower:
            remote_scope = "Japan"
        elif "us" in location_lower or "united states" in location_lower:
            remote_scope = "US"
        elif "emea" in location_lower:
            remote_scope = "EMEA"
        else:
            remote_scope = "Unspecified"

    # --- Japan detection ---
    if "japan" in location_lower or "tokyo" in location_lower:
        is_japan = True
        region = "Japan"

    elif "north america" in location_lower:
        region = "North America"

    elif "europe" in location_lower:
        region = "Europe"

    elif is_remote:
        region = "Remote"

    return region, is_remote, is_japan, remote_scope