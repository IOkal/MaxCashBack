"""Shared retailer identity and sanitization helpers."""

import html
import re
import unicodedata
from typing import Dict

COMMON_SUFFIXES = (
    ".ca",
    ".com",
    " canada",
    " inc",
    " inc.",
    " ltd",
    " ltd.",
    " llc",
    " company",
    " co.",
)

# Trailing standalone word tokens to drop (as opposed to COMMON_SUFFIXES,
# which strips a substring regardless of word boundaries). "CA" shows up as
# its own word from some sources ("Psycho Bunny CA") rather than glued to
# the name as ".ca".
COMMON_TRAILING_WORDS = ("ca",)

# Cross-source brand-identity merges: the same real-world brand is reported
# under a different display name by different sources. Keyed by the merged
# source's match-key (not scoped to one source slug) so a NEW source that
# starts reporting the same alternate name is covered automatically instead
# of silently creating a duplicate retailer the way "Marriott International"
# did for Air Miles before this was global (it was previously only wired up
# for Rakuten).
RAW_GLOBAL_CANONICAL_NAME_OVERRIDES: Dict[str, str] = {
    "Marriott International": "Marriott Bonvoy",
    "Homes & Villas by Marriott": "Homes & Villas by Marriott Bonvoy",
    "Kate Spade New York": "Kate Spade",
    "OWC-Other World Computing": "Other World Computing",
    "Psycho Bunny CA": "Psycho Bunny",
    "Bobbi Brown Cosmetics": "Bobbi Brown",
    "Chatters Hair Salon": "Chatters Hair",
    "Dell Refurbished Computers": "Dell Refurbished",
    "Canadian Down & Feather Company": "Canadian Down and Feather",
}

# Overrides that are genuinely specific to how one source formats its names
# (e.g. GCR's French-language credit card titles) rather than a cross-source
# brand-identity collision — these stay scoped to a single source_slug.
RAW_CANONICAL_NAME_OVERRIDES: Dict[str, Dict[str, str]] = {
    "gcr": {
        "Amex : Carte AéroplanMD* American ExpressMD": "American Express® Aeroplan®* Card",
        "Amex : Carte CobaltMDAmerican Express": "American Express® Cobalt Card",
        "Amex : Carte Marriott BonvoyMDAmerican ExpressMD*": "The Marriott Bonvoy® American Express®* Card",
        "Amex : Carte Or avec primes American ExpressMD": "American Express® Gold Rewards Card",
        "Amex : Carte Prestige AéroplanMD": "American Express® Aeroplan®* Reserve Card",
        "Amex : Carte RemiseSimpleMD": "SimplyCash® Card from American Express",
        "Amex : Carte sélecte RemiseSimpleMD": "SimplyCash® Preferred Card from American Express",
        "Amex : Carte Verte American ExpressMD": "American Express® Green Card",
        "Amex : La Carte de PlatineMD": "The Platinum Card® by American Express",
        "Scotia : Carte American ExpressMDde la Banque ScotiaMD*": "Scotiabank American Express® Card",
        "Scotia : Carte American ExpressMDOr de la Banque ScotiaMD*": "Scotiabank Gold American Express® Card",
        "Scotia : Carte American ExpressMDPlatine": "Scotiabank Platinum American Express® Card",
    },
}


def has_html_markup(value: str) -> bool:
    return bool(re.search(r"<[^>]+>", value or ""))


def get_domain_variant(value: str) -> str:
    lowered = (value or "").lower()
    match = re.search(r"\.(ca|com)\b", lowered)
    return match.group(1) if match else ""


PROMO_SENTENCE_PREFIX_RE = re.compile(
    r"^Earn (up to )?[0-9.]+ points? per dollar on ", re.IGNORECASE
)


def sanitize_store_name(value: str) -> str:
    """Convert source names into safe, plain-text retailer names."""
    text = html.unescape(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"(?i)<br\s*/?>", " ", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("’", "'")

    # A past Aeroplan scraper bug used a promo-banner aria-label sentence
    # ("Earn 10 points per dollar on Kate Spade") as the store name itself,
    # creating a new phantom retailer every time the promo's point value
    # changed. Strip it down to the plain store name if it leaks through.
    text = PROMO_SENTENCE_PREFIX_RE.sub("", text, count=1)

    # Split common trademark markers from adjoining words.
    text = re.sub(r"(?<=[a-z])(?=(MD|MC)(?=$|[^a-z]))", " ", text)
    text = re.sub(r"(MD|MC)(?=[A-Z])", r"\1 ", text)
    text = re.sub(r"([®™℠*])(?=[A-Za-z0-9])", r"\1 ", text)

    # Normalize visible punctuation/spacing artifacts.
    text = re.sub(r"\s*:\s*", ": ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    return text


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_match_key(value: str) -> str:
    """Create a stable cross-source identity key."""
    text = sanitize_store_name(value).lower()
    text = _strip_accents(text)
    text = text.replace("&", " and ")

    if text.startswith("the "):
        text = text[4:]

    # Strip trailing qualifiers (".ca", " Canada", " Inc", " Company", a
    # standalone "CA" word, ...), repeatedly — a name can stack several
    # ("Store Canada Inc." needs both " inc" and " canada" removed before
    # it matches the plain "Store"). Anchored to the END of the string only:
    # a prior bug here did a plain substring replace anywhere, which could
    # mangle a suffix-like fragment appearing mid-name.
    changed = True
    while changed:
        changed = False
        for suffix in COMMON_SUFFIXES:
            if text.endswith(suffix):
                text = text[: -len(suffix)].strip()
                changed = True
        for word in COMMON_TRAILING_WORDS:
            if text == word:
                break
            if text.endswith(" " + word):
                text = text[: -(len(word) + 1)].strip()
                changed = True

    text = re.sub(r"[^a-z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def slugify(value: str) -> str:
    slug = normalize_match_key(value)
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")


def build_global_canonical_name_overrides() -> Dict[str, str]:
    return {
        normalize_match_key(alias_name): sanitize_store_name(canonical_name)
        for alias_name, canonical_name in RAW_GLOBAL_CANONICAL_NAME_OVERRIDES.items()
    }


def build_canonical_name_overrides() -> Dict[str, Dict[str, str]]:
    overrides: Dict[str, Dict[str, str]] = {}
    for source_slug, mapping in RAW_CANONICAL_NAME_OVERRIDES.items():
        overrides[source_slug] = {
            normalize_match_key(alias_name): sanitize_store_name(canonical_name)
            for alias_name, canonical_name in mapping.items()
        }
    return overrides


GLOBAL_CANONICAL_NAME_OVERRIDES = build_global_canonical_name_overrides()
CANONICAL_NAME_OVERRIDES = build_canonical_name_overrides()


def resolve_canonical_name(source_slug: str, value: str) -> str:
    clean_name = sanitize_store_name(value)
    match_key = normalize_match_key(clean_name)
    if match_key in GLOBAL_CANONICAL_NAME_OVERRIDES:
        return GLOBAL_CANONICAL_NAME_OVERRIDES[match_key]
    return CANONICAL_NAME_OVERRIDES.get(source_slug, {}).get(match_key, clean_name)
