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
)

RAW_CANONICAL_NAME_OVERRIDES: Dict[str, Dict[str, str]] = {
    "rakuten": {
        "Marriott International": "Marriott Bonvoy",
    },
    "aeroplan": {
        "Homes & Villas by Marriott": "Homes & Villas by Marriott Bonvoy",
    },
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


def sanitize_store_name(value: str) -> str:
    """Convert source names into safe, plain-text retailer names."""
    text = html.unescape(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"(?i)<br\s*/?>", " ", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("’", "'")

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

    for suffix in COMMON_SUFFIXES:
        text = text.replace(suffix, "")

    if text.startswith("the "):
        text = text[4:]

    text = re.sub(r"[^a-z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def slugify(value: str) -> str:
    slug = normalize_match_key(value)
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")


def build_canonical_name_overrides() -> Dict[str, Dict[str, str]]:
    overrides: Dict[str, Dict[str, str]] = {}
    for source_slug, mapping in RAW_CANONICAL_NAME_OVERRIDES.items():
        overrides[source_slug] = {
            normalize_match_key(alias_name): sanitize_store_name(canonical_name)
            for alias_name, canonical_name in mapping.items()
        }
    return overrides


CANONICAL_NAME_OVERRIDES = build_canonical_name_overrides()


def resolve_canonical_name(source_slug: str, value: str) -> str:
    clean_name = sanitize_store_name(value)
    match_key = normalize_match_key(clean_name)
    return CANONICAL_NAME_OVERRIDES.get(source_slug, {}).get(match_key, clean_name)
