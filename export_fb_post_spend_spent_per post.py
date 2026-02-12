#!/usr/bin/env python3
"""Export Facebook Page post spend by attributing ad spend to page posts."""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from openpyxl import Workbook

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GRAPH_BASE_URL = "https://graph.facebook.com"
PAGE_ID = "101275806400438"
DEFAULT_AD_ACCOUNT_ID = "<AD_ACCOUNT_ID>"
DEFAULT_SINCE = "2026-01-01"  # YYYY-MM-DD
DEFAULT_UNTIL = "2026-01-31"  # YYYY-MM-DD
DEFAULT_GRAPH_VERSION = "v23.0"
DEFAULT_OUTPUT_PATH = os.path.join(SCRIPT_DIR, "fb_post_spend_report_spent.xlsx")
DEFAULT_DEBUG_PATH = os.path.join(SCRIPT_DIR, "spend_debug.json")

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 6
BASE_BACKOFF_SECONDS = 2
MAX_BACKOFF_SECONDS = 120
THROTTLE_SECONDS = 0.25

FATAL_GRAPH_ERROR_CODES = {10, 190, 200}
RATE_LIMIT_GRAPH_ERROR_CODES = {4, 17, 32, 613}


class GraphAPIError(RuntimeError):
    """Raised for non-fatal Graph API errors."""

    def __init__(self, message: str, code: int | None = None, payload: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.payload = payload or {}


class FatalGraphAPIError(GraphAPIError):
    """Raised for fatal Graph API auth/permission errors."""


@dataclass
class RunStats:
    posts_fetched: int = 0
    ads_scanned: int = 0
    ads_with_story_id: int = 0
    posts_matched_to_ads: int = 0


def configure_logging(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export spent-per-post for Facebook Page posts into XLSX."
    )
    parser.add_argument("--page-id", default=PAGE_ID)
    parser.add_argument("--ad-account-id", default=os.getenv("AD_ACCOUNT_ID", DEFAULT_AD_ACCOUNT_ID))
    parser.add_argument("--since", default=DEFAULT_SINCE)
    parser.add_argument("--until", default=DEFAULT_UNTIL)
    parser.add_argument("--graph-version", default=DEFAULT_GRAPH_VERSION)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def parse_date_to_unix(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def normalize_ad_account_id(value: str | None) -> str | None:
    if not value:
        return None
    v = value.strip()
    if not v or v == DEFAULT_AD_ACCOUNT_ID:
        return None
    if v.startswith("act_"):
        v = v[4:]
    if not re.fullmatch(r"\d+", v):
        raise ValueError(f"Invalid ad account id: {value!r}. Expected '123' or 'act_123'.")
    return v


def _compute_backoff(attempt: int) -> float:
    exp = min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
    jitter = random.uniform(0, 1)
    return min(MAX_BACKOFF_SECONDS, exp + jitter)


def graph_get(url: str, params: dict[str, Any], session: requests.Session) -> dict[str, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise GraphAPIError(f"Request failed after retries: {exc}") from exc
            wait = _compute_backoff(attempt)
            logging.warning("Request exception (%s). Retry %s/%s in %.2fs", exc, attempt, MAX_RETRIES, wait)
            time.sleep(wait)
            continue

        retriable_http = response.status_code == 429 or 500 <= response.status_code < 600

        payload: dict[str, Any]
        try:
            payload = response.json()
        except ValueError:
            payload = {}

        if "error" in payload:
            err = payload.get("error") or {}
            code = err.get("code")
            message = err.get("message", "Graph API returned an error")

            if code in FATAL_GRAPH_ERROR_CODES:
                raise FatalGraphAPIError(f"Fatal Graph API error {code}: {message}", code=code, payload=payload)

            if code in RATE_LIMIT_GRAPH_ERROR_CODES and attempt < MAX_RETRIES:
                wait = _compute_backoff(attempt)
                logging.warning("Rate limited (Graph code %s). Retry %s/%s in %.2fs", code, attempt, MAX_RETRIES, wait)
                time.sleep(wait)
                continue

            raise GraphAPIError(f"Graph API error {code}: {message}", code=code, payload=payload)

        if retriable_http:
            if attempt == MAX_RETRIES:
                raise GraphAPIError(
                    f"HTTP {response.status_code} after retries for URL {url}",
                    payload=payload,
                )
            wait = _compute_backoff(attempt)
            logging.warning("HTTP %s. Retry %s/%s in %.2fs", response.status_code, attempt, MAX_RETRIES, wait)
            time.sleep(wait)
            continue

        if not response.ok:
            raise GraphAPIError(f"HTTP {response.status_code} for URL {url}", payload=payload)

        time.sleep(THROTTLE_SECONDS)
        return payload

    raise GraphAPIError("Unreachable retry loop exit")


def fetch_posts(
    session: requests.Session,
    graph_version: str,
    page_id: str,
    access_token: str,
    since_unix: int,
    until_unix: int,
) -> list[dict[str, Any]]:
    url = f"{GRAPH_BASE_URL}/{graph_version}/{page_id}/posts"
    params: dict[str, Any] = {
        "fields": "id,created_time",
        "since": since_unix,
        "until": until_unix,
        "limit": 100,
        "access_token": access_token,
    }

    posts: list[dict[str, Any]] = []
    while True:
        payload = graph_get(url, params, session)
        posts.extend(payload.get("data", []))
        next_url = ((payload.get("paging") or {}).get("next"))
        if not next_url:
            break
        url = next_url
        params = {}

    return posts


def extract_story_id(ad: dict[str, Any]) -> str | None:
    adcreatives = ad.get("adcreatives") or {}
    for creative in adcreatives.get("data", []) or []:
        story_id = creative.get("effective_object_story_id")
        if story_id:
            return str(story_id)

    creative = ad.get("creative") or {}
    story_id = creative.get("effective_object_story_id")
    if story_id:
        return str(story_id)

    return None


def fetch_post_to_ads_map(
    session: requests.Session,
    graph_version: str,
    normalized_ad_account_id: str,
    access_token: str,
    post_ids: set[str],
    stats: RunStats,
) -> dict[str, list[str]]:
    url = f"{GRAPH_BASE_URL}/{graph_version}/act_{normalized_ad_account_id}/ads"
    params: dict[str, Any] = {
        "fields": "id,adcreatives{effective_object_story_id},creative{effective_object_story_id,id},created_time,updated_time,status",
        "limit": 100,
        "access_token": access_token,
    }

    mapping: dict[str, list[str]] = {}
    while True:
        payload = graph_get(url, params, session)
        ads = payload.get("data", [])
        for ad in ads:
            stats.ads_scanned += 1
            ad_id = str(ad.get("id", "")).strip()
            if not ad_id:
                continue

            story_id = extract_story_id(ad)
            if not story_id:
                continue

            stats.ads_with_story_id += 1
            if story_id in post_ids:
                mapping.setdefault(story_id, []).append(ad_id)

        next_url = ((payload.get("paging") or {}).get("next"))
        if not next_url:
            break
        url = next_url
        params = {}

    stats.posts_matched_to_ads = len(mapping)
    return mapping


def parse_spend(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return 0.0
    return 0.0


def fetch_ad_spend(
    session: requests.Session,
    graph_version: str,
    ad_id: str,
    access_token: str,
    since: str,
    until: str,
    cache: dict[str, float],
    spend_sample: dict[str, Any],
) -> float:
    if ad_id in cache:
        return cache[ad_id]

    url = f"{GRAPH_BASE_URL}/{graph_version}/{ad_id}/insights"
    params = {
        "fields": "spend",
        "level": "ad",
        "time_range[since]": since,
        "time_range[until]": until,
        "access_token": access_token,
    }
    payload = graph_get(url, params, session)
    data = payload.get("data", [])
    spend = 0.0
    if data:
        spend = parse_spend((data[0] or {}).get("spend"))

    cache[ad_id] = spend
    if len(spend_sample) < 10:
        spend_sample[ad_id] = payload
    return spend


def write_xlsx(
    output_path: str,
    rows: list[dict[str, Any]],
) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "FB Post Spend"

    headers = [
        "Post ID",
        "Spent per post",
        "Ad IDs",
        "Ads matched",
        "Since",
        "Until",
        "Graph version",
    ]
    ws.append(headers)

    for row in rows:
        ws.append([
            row["Post ID"],
            row["Spent per post"],
            row["Ad IDs"],
            row["Ads matched"],
            row["Since"],
            row["Until"],
            row["Graph version"],
        ])

    wb.save(output_path)


def write_debug_json(
    path: str,
    graph_version: str,
    page_id: str,
    normalized_ad_account_id: str | None,
    stats: RunStats,
    post_to_ads: dict[str, list[str]],
    spend_sample: dict[str, Any],
) -> None:
    sample_mappings = {k: v for k, v in list(post_to_ads.items())[:10]}
    payload = {
        "graph_version": graph_version,
        "page_id": page_id,
        "ad_account_id": normalized_ad_account_id,
        "counts": {
            "posts_fetched": stats.posts_fetched,
            "ads_scanned": stats.ads_scanned,
            "ads_with_story_id": stats.ads_with_story_id,
            "posts_matched_to_ads": stats.posts_matched_to_ads,
        },
        "sample_mappings": sample_mappings,
        "sample_spend_responses": spend_sample,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def build_rows(
    posts: list[dict[str, Any]],
    post_to_ads: dict[str, list[str]],
    spend_cache: dict[str, float],
    since: str,
    until: str,
    graph_version: str,
) -> list[dict[str, Any]]:
    def _sort_key(post: dict[str, Any]) -> tuple[int, str]:
        created_time = post.get("created_time", "")
        try:
            dt = datetime.fromisoformat(created_time.replace("Z", "+00:00"))
            ts = int(dt.timestamp())
        except ValueError:
            ts = 0
        return ts, str(post.get("id", ""))

    sorted_posts = sorted(posts, key=_sort_key)

    rows: list[dict[str, Any]] = []
    for post in sorted_posts:
        post_id = str(post.get("id", ""))
        ad_ids = post_to_ads.get(post_id, [])
        total_spend = round(sum(spend_cache.get(ad_id, 0.0) for ad_id in ad_ids), 2)
        rows.append(
            {
                "Post ID": post_id,
                "Spent per post": total_spend,
                "Ad IDs": ",".join(ad_ids),
                "Ads matched": len(ad_ids),
                "Since": since,
                "Until": until,
                "Graph version": graph_version,
            }
        )

    return rows


def main() -> int:
    args = parse_args()
    configure_logging(args.debug)

    access_token = os.getenv("FB_PAGE_ACCESS_TOKEN")
    if not access_token:
        logging.error("Fatal: FB_PAGE_ACCESS_TOKEN environment variable is required.")
        return 2

    try:
        since_unix = parse_date_to_unix(args.since)
        until_unix = parse_date_to_unix(args.until)
    except ValueError as exc:
        logging.error("Invalid date format for --since/--until. Expected YYYY-MM-DD: %s", exc)
        return 2

    try:
        normalized_ad_account_id = normalize_ad_account_id(args.ad_account_id)
    except ValueError as exc:
        logging.error("%s", exc)
        return 2

    stats = RunStats()
    spend_cache: dict[str, float] = {}
    spend_sample: dict[str, Any] = {}

    with requests.Session() as session:
        try:
            posts = fetch_posts(
                session=session,
                graph_version=args.graph_version,
                page_id=args.page_id,
                access_token=access_token,
                since_unix=since_unix,
                until_unix=until_unix,
            )
        except FatalGraphAPIError as exc:
            logging.error("Fatal Graph error while fetching posts: %s", exc)
            return 1
        except GraphAPIError as exc:
            logging.error("Graph API error while fetching posts: %s", exc)
            return 1

        stats.posts_fetched = len(posts)
        post_ids = {str(p.get("id", "")) for p in posts if p.get("id")}
        logging.info("Fetched %s posts for page %s", len(posts), args.page_id)

        post_to_ads: dict[str, list[str]] = {}

        if normalized_ad_account_id is None:
            logging.warning("AD_ACCOUNT_ID missing or placeholder value; all posts will have 0.0 spend.")
        else:
            try:
                post_to_ads = fetch_post_to_ads_map(
                    session=session,
                    graph_version=args.graph_version,
                    normalized_ad_account_id=normalized_ad_account_id,
                    access_token=access_token,
                    post_ids=post_ids,
                    stats=stats,
                )
                logging.info("Matched %s posts to ads", len(post_to_ads))
            except FatalGraphAPIError as exc:
                logging.error("Fatal Graph error while fetching ads: %s", exc)
                return 1
            except GraphAPIError as exc:
                logging.error("Graph API error while fetching ads: %s", exc)
                return 1

            unique_ad_ids = sorted({ad_id for ad_ids in post_to_ads.values() for ad_id in ad_ids})
            for ad_id in unique_ad_ids:
                try:
                    fetch_ad_spend(
                        session=session,
                        graph_version=args.graph_version,
                        ad_id=ad_id,
                        access_token=access_token,
                        since=args.since,
                        until=args.until,
                        cache=spend_cache,
                        spend_sample=spend_sample,
                    )
                except FatalGraphAPIError as exc:
                    logging.error("Fatal Graph error while fetching insights for ad %s: %s", ad_id, exc)
                    return 1
                except GraphAPIError as exc:
                    logging.error("Graph API error while fetching insights for ad %s: %s", ad_id, exc)
                    return 1

        rows = build_rows(
            posts=posts,
            post_to_ads=post_to_ads,
            spend_cache=spend_cache,
            since=args.since,
            until=args.until,
            graph_version=args.graph_version,
        )

        write_xlsx(args.output, rows)
        logging.info("Wrote XLSX report: %s (%s rows)", args.output, len(rows))

        if args.debug:
            write_debug_json(
                path=DEFAULT_DEBUG_PATH,
                graph_version=args.graph_version,
                page_id=args.page_id,
                normalized_ad_account_id=normalized_ad_account_id,
                stats=stats,
                post_to_ads=post_to_ads,
                spend_sample=spend_sample,
            )
            logging.info("Wrote debug artifact: %s", DEFAULT_DEBUG_PATH)

    return 0


if __name__ == "__main__":
    sys.exit(main())
