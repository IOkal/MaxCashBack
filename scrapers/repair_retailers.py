"""One-time retailer cleanup and merge repair.

Usage:
  python3 repair_retailers.py --dry-run
  python3 repair_retailers.py --apply
"""

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

from db import get_connection
from retailer_identity import (
    get_domain_variant,
    has_html_markup,
    normalize_match_key,
    resolve_canonical_name,
    sanitize_store_name,
)


@dataclass
class AliasRecord:
    alias_id: int
    retailer_id: int
    retailer_name: str
    normalized_name: str
    source_id: int
    source_slug: str
    alias_name: str
    source_url: str


@dataclass
class RetailerRecord:
    retailer_id: int
    name: str
    normalized_name: str


def fetch_alias_records(conn) -> List[AliasRecord]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ra.id,
                r.id,
                r.name,
                r.normalized_name,
                s.id,
                s.slug,
                ra.alias_name,
                ra.source_url
            FROM retailer_aliases ra
            JOIN retailers r ON r.id = ra.retailer_id
            JOIN sources s ON s.id = ra.source_id
            ORDER BY ra.id
            """
        )
        return [AliasRecord(*row) for row in cur.fetchall()]


def choose_target_candidate(candidates: List[RetailerRecord], canonical_name: str) -> RetailerRecord:
    canonical_clean = sanitize_store_name(canonical_name)
    return sorted(
        candidates,
        key=lambda candidate: (
            0 if sanitize_store_name(candidate.name) == canonical_clean else 1,
            0 if not has_html_markup(candidate.name) else 1,
            candidate.retailer_id,
        ),
    )[0]


def build_merge_plan(alias_records: List[AliasRecord]):
    retailer_records: Dict[int, RetailerRecord] = {}
    retailer_key_counts: Dict[int, Counter] = defaultdict(Counter)
    candidates_by_key: Dict[str, Dict[int, RetailerRecord]] = defaultdict(dict)
    canonical_name_by_key: Dict[str, str] = {}

    for record in alias_records:
        retailer_records.setdefault(
            record.retailer_id,
            RetailerRecord(record.retailer_id, record.retailer_name, record.normalized_name),
        )

        clean_alias = sanitize_store_name(record.alias_name)
        canonical_name = resolve_canonical_name(record.source_slug, clean_alias)
        canonical_key = normalize_match_key(canonical_name)
        domain_variant = (
            get_domain_variant(clean_alias)
            or get_domain_variant(record.retailer_name)
            or get_domain_variant(canonical_name)
        )
        if domain_variant:
            canonical_key = f"{canonical_key}::{domain_variant}"

        retailer_key_counts[record.retailer_id][canonical_key] += 1
        candidates_by_key[canonical_key][record.retailer_id] = retailer_records[record.retailer_id]

        existing = canonical_name_by_key.get(canonical_key)
        if existing is None or (
            sanitize_store_name(existing) != canonical_name
            and len(canonical_name) > len(existing)
        ):
            canonical_name_by_key[canonical_key] = canonical_name

    retailer_target_key: Dict[int, str] = {}
    for retailer_id, counts in retailer_key_counts.items():
        retailer_target_key[retailer_id] = sorted(
            counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[0][0]

    target_retailer_by_key: Dict[str, int] = {}
    for canonical_key, candidates in candidates_by_key.items():
        canonical_name = canonical_name_by_key[canonical_key]
        target = choose_target_candidate(list(candidates.values()), canonical_name)
        target_retailer_by_key[canonical_key] = target.retailer_id

    retailer_target_map: Dict[int, int] = {}
    target_display_names: Dict[int, str] = {}
    for retailer_id, canonical_key in retailer_target_key.items():
        target_id = target_retailer_by_key[canonical_key]
        retailer_target_map[retailer_id] = target_id
        target_display_names[target_id] = canonical_name_by_key[canonical_key]

    return retailer_target_map, target_display_names


def merge_store_visits(cur, source_retailer_id: int, target_retailer_id: int):
    cur.execute(
        """
        INSERT INTO store_visits (retailer_id, visit_count, last_visited_at)
        SELECT %s, visit_count, last_visited_at
        FROM store_visits
        WHERE retailer_id = %s
        ON CONFLICT (retailer_id) DO UPDATE SET
            visit_count = store_visits.visit_count + EXCLUDED.visit_count,
            last_visited_at = GREATEST(store_visits.last_visited_at, EXCLUDED.last_visited_at)
        """,
        (target_retailer_id, source_retailer_id),
    )
    cur.execute("DELETE FROM store_visits WHERE retailer_id = %s", (source_retailer_id,))


def dedupe_alias_rows(conn):
    alias_records = fetch_alias_records(conn)
    grouped: Dict[Tuple[int, str], List[AliasRecord]] = defaultdict(list)
    for record in alias_records:
        clean_alias = sanitize_store_name(record.alias_name)
        grouped[(record.source_id, clean_alias)].append(record)

    alias_updates = 0
    alias_deletes = 0

    with conn.cursor() as cur:
        for (_, clean_alias), rows in grouped.items():
            keeper = sorted(
                rows,
                key=lambda record: (
                    0 if record.alias_name == clean_alias else 1,
                    0 if record.alias_name == record.retailer_name else 1,
                    0 if not has_html_markup(record.alias_name) else 1,
                    0 if record.source_url else 1,
                    record.alias_id,
                ),
            )[0]

            donor_urls = [record.source_url for record in rows if record.alias_id != keeper.alias_id and record.source_url]
            merged_source_url = keeper.source_url or (donor_urls[0] if donor_urls else None)

            if len(rows) > 1:
                donor_ids = [record.alias_id for record in rows if record.alias_id != keeper.alias_id]
                cur.execute(
                    "DELETE FROM retailer_aliases WHERE id = ANY(%s)",
                    (donor_ids,),
                )
                alias_deletes += len(donor_ids)

            if keeper.alias_name != clean_alias or keeper.source_url != merged_source_url:
                cur.execute(
                    """
                    UPDATE retailer_aliases
                    SET alias_name = %s,
                        source_url = %s
                    WHERE id = %s
                    """,
                    (clean_alias, merged_source_url, keeper.alias_id),
                )
                alias_updates += 1

    return alias_updates, alias_deletes


def normalize_current_rows(cur):
    cur.execute(
        """
        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY retailer_id, source_id
                    ORDER BY scraped_at DESC, id DESC
                ) AS rn
            FROM cashback_rates
            WHERE is_current = true
        )
        UPDATE cashback_rates cr
        SET is_current = false
        FROM ranked
        WHERE cr.id = ranked.id
          AND ranked.rn > 1
        """
    )


def delete_orphan_retailers(cur):
    cur.execute(
        """
        DELETE FROM retailers r
        WHERE NOT EXISTS (
            SELECT 1 FROM retailer_aliases ra WHERE ra.retailer_id = r.id
        )
          AND NOT EXISTS (
            SELECT 1 FROM cashback_rates cr WHERE cr.retailer_id = r.id
        )
          AND NOT EXISTS (
            SELECT 1 FROM store_visits sv WHERE sv.retailer_id = r.id
        )
        """
    )


def apply_repairs(conn):
    alias_records = fetch_alias_records(conn)
    retailer_target_map, target_display_names = build_merge_plan(alias_records)

    merge_pairs = sorted(
        (source_id, target_id)
        for source_id, target_id in retailer_target_map.items()
        if source_id != target_id
    )

    retailer_name_updates = 0
    merged_retailers = 0

    with conn.cursor() as cur:
        for source_retailer_id, target_retailer_id in merge_pairs:
            cur.execute(
                "UPDATE cashback_rates SET retailer_id = %s WHERE retailer_id = %s",
                (target_retailer_id, source_retailer_id),
            )
            cur.execute(
                "UPDATE retailer_aliases SET retailer_id = %s WHERE retailer_id = %s",
                (target_retailer_id, source_retailer_id),
            )
            merge_store_visits(cur, source_retailer_id, target_retailer_id)
            merged_retailers += 1

        alias_updates, alias_deletes = dedupe_alias_rows(conn)

        for retailer_id, canonical_name in sorted(target_display_names.items()):
            clean_name = sanitize_store_name(canonical_name)
            normalized_name = normalize_match_key(clean_name)
            cur.execute(
                """
                UPDATE retailers
                SET name = %s,
                    normalized_name = %s,
                    updated_at = NOW()
                WHERE id = %s
                  AND (name <> %s OR normalized_name <> %s)
                """,
                (clean_name, normalized_name, retailer_id, clean_name, normalized_name),
            )
            retailer_name_updates += cur.rowcount

        normalize_current_rows(cur)
        delete_orphan_retailers(cur)

    return {
        "merged_retailers": merged_retailers,
        "updated_retailer_names": retailer_name_updates,
        "updated_aliases": alias_updates,
        "deleted_aliases": alias_deletes,
    }


def dry_run(conn):
    alias_records = fetch_alias_records(conn)
    retailer_target_map, target_display_names = build_merge_plan(alias_records)

    merge_pairs = [
        (source_id, target_id)
        for source_id, target_id in retailer_target_map.items()
        if source_id != target_id
    ]
    html_alias_count = sum(1 for record in alias_records if has_html_markup(record.alias_name))
    html_retailer_count = len(
        {
            record.retailer_id
            for record in alias_records
            if has_html_markup(record.retailer_name)
        }
    )

    print(f"retailers_to_merge={len(merge_pairs)}")
    for source_id, target_id in merge_pairs[:20]:
        print(f"merge retailer {source_id} -> {target_id}")

    print(f"target_retailer_names_to_normalize={len(target_display_names)}")
    print(f"html_alias_rows={html_alias_count}")
    print(f"html_retailer_rows={html_retailer_count}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Apply the repair to the database")
    args = parser.parse_args()

    with get_connection() as conn:
        if args.apply:
            summary = apply_repairs(conn)
            print(summary)
        else:
            dry_run(conn)


if __name__ == "__main__":
    main()
