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
REMOTE_SYNONYMS = ("remote", "home based", "home-based", "wfh", "work from home")


# --- Location: if remote but text looks tied to these regions, scope = restricted ---

RESTRICTED_REGION_KEYWORDS = (
    "u.s.a.", "u.s.", "united states", "usa", "us",
    "new york", "nyc", "san francisco", "seattle", "chicago", "atlanta",
    "north america", "americas", "amers", "california", "wa",
    "canada", "toronto", "vancouver", "british columbia", "montreal",
    "can-on",
    "latam", "mexico", "brazil", "chile", "argentina",
    "buenos aires", "santiago",
    "colombia", "peru", "ecuador", "bra",
    "costa rica", "el salvador", "alberta",
    "europe", "emea",
    "united kingdom", "london",
    "france", "germany", "ireland", "netherlands", "turkey",
    "dublin", "deu", "che", "porto",
    "spain", "sweden", "italy", "norway", "finland", "greece", "swe",
    "belgium", "denmark", "lithuania", "lisbon", "hungary",
    "czech republic", "prague", "barcelona", "luxembourg",
    "berlin", "amsterdam", "stockholm", "milan", "romania",
    "switzerland", "poland", "portugal", "budapest",
    "united arab emirates", "uae", "saudi arabia", "sau", "dubai",
    "singapore", "australia", "sydney", "new zealand",
    "bangkok", "thailand", "vietnam", "kuala lumpur", "th",
    "south korea", "india", "bangalore", "ind", "korea", "kr-south",
    "sea", "philippines", "az",
    "beijing", "china",
    "south africa", "israel", "serbia",
    "baku", "cis", "bogota",
)

ROLE_KEYWORDS = [
    ("solutions architect and engineer", [
        "solutions architect", "solution architect", "solutions engineer",
        "solution engineer",
    ]),
    
    ("customer solution", [
        "customer solutions", "customer solution", "solutions consultant",
        "solution consultant", "solutions consulting", "implementation consultant", 
        "tam", "technical account manager", "consultant", "consulting",
        "implementation", "professional services", "presales", "technical solutions",
        "technical deployment", "functional consultant",
    ]),

    ("Communications and PR", [
        "communications", "public relations", "pr", "media relations",
        "publicity",
    ]),

    ("product management", [
        "product manager", "product management", "product owner", "product lead",
        "cpo", "product merchandising", "product solutions",
        "プロダクトマネージャー", "プロダクト管理", "商品企画", "企画担当",
        "プロダクト企画", "サービス企画",
    ]),

    ("engineering", [
        "engineer", "developer", "software", "backend", "frontend", "full stack",
        "devops", "platform", "mobile", "ios", "android", "cto", "engineering",
        "tech lead", "technical architect",
        "エンジニア", "開発", "ソフトウェア", "フルスタック", "モバイル", "サーバー",
        "クライアント", "ネットワーク", "sdk", "システムソフトウェア",
        "低レイヤー", "組み込み", "ファームウェア",
    ]),
    
    ("quality assurance", [
        "qa", "test", "tester", "quality assurance",
        "テスト", "テスター", "品質保証", "品質エンジニア", "ローカライズテスター",
    ]),
    
    ("hardware engineering", [
        "hardware", "firmware", "embedded", "systems",
        "ハードウェア", "半導体", "lsi", "ssd", "組み込み", "回路設計", "チップ", "デバイス",
    ]),

    ("design", [
        "designer", "ux", "ui", "product design", "visual", "design", "creative director",
        "copywriter", 
        "デザイナー", "デザイン", "アーティスト", "3d", "アニメーター", "グラフィック", "ビジュアル",
    ]),

    ("data and analytics", [
        "data", "analytics", "analyst", "machine learning", "ml", "ai", "insights", 
        "measurement",
        "データ", "分析", "アナリスト",
    ]),

    ("marketing", [
        "marketing", "growth", "seo", "content", "brand", "market", "events", "field marketer",
        "community manager",
        "マーケティング", "ブランド", "プロモーション",
    ]),

    ("business development", [
        "business development", "bd", "partner manager", "partner development",
        "partner business", "partner relations", "partnerships", "alliances", "channel",
        "managing partner", "strategy", "strategic", "expansion",
        "事業開発", "ビジネス開発", "戦略", "事業企画",
    ]),

    ("sales", [
        "sales", "account executive", "account manager", "cro", "account management",
        "gtm", "partnerships", "account director", "smb", "deal management",
        "partner success", "deal desk", "lead generation",
    ]),

    ("customer success and experience", [
        "customer success", "customer support", "customer experience", "csm",
        "account manager", "onboarding", "renewals", "customer performance",
        "customer care", "claims experience", "renewal",
    ]),

    ("HR and recruiting", [
        "recruiter", "talent", "hr", "people",
        "人事", "採用", "採用担当", "人材",
    ]),

    ("finance and accounting", [
        "finance", "accounting", "fp&a", "controller", "accountant",
    ]),

    ("Information Technology", [
        "it", "information technology", "systems administrator",
        "technology",
        "情報システム", "社内インフラ", "インフラエンジニア", "システム管理", "社内it",
    ]),

    ("operations and support", [
        "operations", "ops", "support", "administrative", "clerk",
        "health keeper", "workplace experience", "lead diag tech", "fleet readiness",
        "services liason", "operational safety",
        "オペレーション", "運用", "サポート", "事務",
    ]),

    ("security", [
        "security", "infosec", "cybersecurity",
        "セキュリティ", "情報セキュリティ", "サイバーセキュリティ",
    ]),

    ("legal", [
        "legal", "counsel", "compliance", "IP relations", "crime",
        "sanctions", "auditor", "public policy", "fincrime",
    ]),

    ("research and development", [
        "research", "scientist", "r&d",
    ]),

    ("supply chain and procurement", [
        "supply chain", "procurement", "purchasing",
    ]),
]

def _is_ascii_keyword(kw: str) -> bool:
    """Return True if the keyword contains only ASCII characters."""
    return all(ord(c) < 128 for c in kw)


def classify_role(title: str) -> str:
    """Return the most specific matching role for the given title.

    Strategy:
    - ASCII keywords: use \\b word boundaries; first match in ROLE_KEYWORDS
      order wins (role ordering encodes priority).
    - Japanese / non-ASCII keywords: use plain substring match (\\b is
      meaningless for CJK); the *longest* matching keyword wins across all
      roles, so 品質エンジニア beats エンジニア regardless of list order.
    """
    t = title.lower()

    # --- Pass 1: ASCII keywords, first-role-order wins ---
    ascii_match_role: str | None = None
    ascii_match_len: int = 0
    for role, keywords in ROLE_KEYWORDS:
        for kw in keywords:
            if not _is_ascii_keyword(kw):
                continue
            pattern = rf"\b{re.escape(kw)}\b"
            if re.search(pattern, t):
                if ascii_match_role is None:
                    # Record first (highest-priority) ASCII match, but keep
                    # scanning this role's keywords to find the longest match
                    # within the same role so we can compare fairly if needed.
                    ascii_match_role = role
                    ascii_match_len = len(kw)
                break  # first matching role wins; no need to check others
        if ascii_match_role is not None:
            break

    # --- Pass 2: Japanese keywords, longest-match wins across all roles ---
    jp_match_role: str | None = None
    jp_match_len: int = 0
    for role, keywords in ROLE_KEYWORDS:
        for kw in keywords:
            if _is_ascii_keyword(kw):
                continue
            if kw in t and len(kw) > jp_match_len:
                jp_match_role = role
                jp_match_len = len(kw)

    # If both matched, prefer the more specific one (longer keyword).
    if ascii_match_role and jp_match_role:
        return jp_match_role if jp_match_len > ascii_match_len else ascii_match_role
    return jp_match_role or ascii_match_role or "other"


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

def classify_location(location_name: str | None) -> tuple[str | None, bool, bool, str | None]:
    if not location_name:
        return None, False, False, None

    loc = location_name.lower().strip()

    is_japan = any(term in loc for term in JAPAN_LOCATION_TERMS)
    
    is_remote = any(term in loc for term in REMOTE_SYNONYMS)
    
    remote_scope: str | None = None

    if is_japan:
        # If it's Japan, we usually want to track it regardless of remote status
        return "Japan", is_remote, True, "japan"

    if is_remote:
        # Check for global terms (e.g., "worldwide")
        if any(term in loc for term in GLOBAL_REMOTE_TERMS):
            remote_scope = "global"
        # Check for APAC/Asia
        elif "apac" in loc or "asia" in loc:
            remote_scope = "apac"
        # Check for specific restricted regions
        elif any(term in loc for term in RESTRICTED_REGION_KEYWORDS):
            remote_scope = "restricted"
        else:
            # Default remote scope if none of the above match
            remote_scope = "global"
    else:
        # If not remote and not Japan, it's a specific local office elsewhere
        remote_scope = "restricted"

    return None, is_remote, False, remote_scope