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

    # JAPAN DETECTION
    japan_terms = [
    "japan",
    "jp",
    "jpn",
    "tokyo",
    "osaka",
    "yokohama",
    "kanagawa",
    "chiba"
    ]
    is_japan = any(term in loc for term in japan_terms)

    # REMOTE DETECTION
    is_remote = "remote" in loc
    remote_scope = None

    restricted_keywords = [
        "united states", "usa", "us-", "-us", "us ", " us", "new york", "san francisco", "seattle",
        "chicago", "atlanta", "west coast", "north america", "us/ca", "americas", "nyc",
        "canada", "toronto", "british columbia",
        "europe", "emea",
        "singapore", "sydney", "bangkok", "thailand", "sea", "vietnam",
        "france", "germany", "ireland", "netherlands",
        "spain", "sweden", "italy", "india", "brazil", "australia",
        "united kingdom", "south korea", "united arab emirates", "uae", "switzerland",
        "poland", "south africa", "mexico", "portugal", "chile", "columbia", "colombia",
        "latam", "amsterdam", "buenos aires", "santiago", "london", "stockholm", "belgium",
        "denmark", "lithuania", "peru", "ecuador", "costa rica", "el salvador", "berlin"
    ]

    if is_japan:
        return "Japan", is_remote, True, "japan"

    if is_remote:
        if any(term in loc for term in ["anywhere", "worldwide", "global"]):
            remote_scope = "global"
        elif "apac" in loc:
            remote_scope = "apac"
        elif any(term in loc for term in restricted_keywords):
            remote_scope = "restricted"
#        elif "," in loc or "/" in loc or "-":
#            remote_scope = "restricted"
        else:
            remote_scope = "global"
    else:
        remote_scope = "restricted"

    return None, is_remote, False, remote_scope