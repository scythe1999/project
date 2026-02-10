#!/usr/bin/env python3
"""
Facebook Page posts + insights exporter (read-only).

Run:
  pip install requests
  export FB_PAGE_ACCESS_TOKEN="..."                # macOS/Linux
  setx FB_PAGE_ACCESS_TOKEN "..."                  # Windows (new terminal)
  python export_fb_posts.py

Notes:
- Meta may deprecate insights metrics across Graph versions.
- This script discovers valid metrics once per run and skips unsupported metrics safely.
"""

import csv
import datetime
import logging
import os
import random
import time
from typing import Any, Dict, List, Optional, Set

import requests

# =========================
# Configuration
# =========================
GRAPH_VERSION = "v23.0"
PAGE_ID = "101275806400438"
ACCESS_TOKEN_PLACEHOLDER = "<ACCESS_TOKEN>"
ACCESS_TOKEN = os.getenv("FB_PAGE_ACCESS_TOKEN", ACCESS_TOKEN_PLACEHOLDER)
SINCE = "2026-01-01"  # YYYY-MM-DD
UNTIL = "2026-01-31"  # YYYY-MM-DD
OUTPUT_FILE = "fb_page_posts_report.csv"
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 6
BASE_BACKOFF_SECONDS = 2
MAX_BACKOFF_SECONDS = 120
THROTTLE_SECONDS = 0.25
DRY_RUN = False
DRY_RUN_LIMIT = 25

GRAPH_BASE_URL = "https://graph.facebook.com"
RATE_LIMIT_CODES = {4, 17, 32, 613}
FATAL_AUTH_PERMISSION_CODES = {10, 190, 200}
INVALID_METRIC_CODE = 100

POST_FIELDS_CANDIDATES = [
    # Preferred shape with richer metadata.
    "id,created_time,permalink_url,message,story,status_type,type",
    # Fallback for pages/accounts that reject post aggregated attachment fields.
    "id,created_time,permalink_url,message,story,type",
    # Minimal baseline fallback.
    "id,created_time,permalink_url,message,story",
]

INSIGHT_METRICS_CANDIDATES = [
    "post_impressions",
    "post_impressions_unique",
    "post_impressions_organic",
    "post_impressions_paid",
    "post_reach",
    "post_reach_organic",
    "post_reach_paid",
    "post_media_view",  # fallback in newer Graph changes for some pages
    "post_engaged_users",
    "post_clicks",
    "post_clicks_unique",
    "post_clicks_by_type",
    "post_reactions_by_type_total",
    "post_comments",
    "post_shares",
    "post_negative_feedback",
    "post_negative_feedback_unique",
    "post_video_views_3s",
    "post_video_views_1m",
    "post_video_view_time",
    "post_video_avg_time_watched",
    "post_video_views_3s_by_age_bucket_and_gender",
]

CSV_COLUMNS = [
    "Post ID",
    "Page name",
    "Title",
    "Publish time",
    "Permalink",
    "Post type",
    "Reach",
    "Reach (Organic)",
    "Reach (Paid/Boosted)",
    "Impressions",
    "Impressions (Unique)",
    "Impressions (Organic)",
    "Impressions (Paid/Boosted)",
    "Engaged users",
    "Reactions (Total)",
    "Reactions (like)",
    "Reactions (love)",
    "Reactions (wow)",
    "Reactions (haha)",
    "Reactions (sad)",
    "Reactions (angry)",
    "Comments",
    "Shares",
    "Total clicks",
    "Link Clicks",
    "Other Clicks",
    "Negative feedback",
    "Negative feedback (Unique)",
    "3-second video views",
    "1-minute video views",
    "Seconds viewed (video view time)",
    "Average seconds viewed (video avg time watched)",
    "3s_views_M_18_24",
    "3s_views_M_25_34",
    "3s_views_M_35_44",
    "3s_views_M_45_54",
    "3s_views_M_55_64",
    "3s_views_M_65_plus",
    "3s_views_F_18_24",
    "3s_views_F_25_34",
    "3s_views_F_35_44",
    "3s_views_F_45_54",
    "3s_views_F_55_64",
    "3s_views_F_65_plus",
]

BASE_COLUMNS = {
    "Post ID",
    "Page name",
    "Title",
    "Publish time",
    "Permalink",
    "Post type",
}

_VALID_METRICS_CACHE: Optional[List[str]] = None


class FatalGraphAPIError(RuntimeError):
    """Fatal Graph API issue that should stop script execution."""


class GraphAPIError(RuntimeError):
    """Graph API error with parsed metadata for smarter retry behavior."""

    def __init__(self, message: str, code: Optional[int] = None) -> None:
        super().__init__(message)
        self.code = code


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_date_to_unix(date_str: str) -> int:
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())


def _safe_error_summary(error_obj: Dict[str, Any]) -> str:
    return (
        f"message={error_obj.get('message', 'unknown')}; "
        f"type={error_obj.get('type', 'unknown')}; "
        f"code={error_obj.get('code', 'unknown')}; "
        f"subcode={error_obj.get('error_subcode', 'unknown')}; "
        f"fbtrace_id={error_obj.get('fbtrace_id', 'unknown')}"
    )


def graph_get(
    url: str,
    params: Dict[str, Any],
    non_retryable_graph_codes: Optional[Set[int]] = None,
) -> Dict[str, Any]:
    """Safe GET helper with timeout, retry, backoff+jitter and Graph error handling."""
    non_retryable_graph_codes = non_retryable_graph_codes or set()

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            status = response.status_code

            # Retry common transient HTTP statuses.
            if status in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {status}", response=response)

            payload = response.json()

            if "error" in payload:
                error_obj = payload.get("error", {})
                code = error_obj.get("code")
                summary = _safe_error_summary(error_obj)

                if code in FATAL_AUTH_PERMISSION_CODES:
                    raise FatalGraphAPIError(
                        "Fatal token/permission error from Graph API. "
                        "Check token validity/scopes/page roles. "
                        f"{summary}"
                    )

                if code in RATE_LIMIT_CODES:
                    raise requests.HTTPError(
                        f"Rate limit Graph error code={code}: {summary}",
                        response=response,
                    )

                raise GraphAPIError(f"Graph API error: {summary}", code=code)

            time.sleep(THROTTLE_SECONDS)
            return payload

        except FatalGraphAPIError:
            raise
        except GraphAPIError as exc:
            if exc.code in non_retryable_graph_codes:
                raise

            if attempt >= MAX_RETRIES:
                raise RuntimeError(
                    f"Request failed after {MAX_RETRIES} attempts for {url}: {exc}"
                ) from exc

            exp_delay = min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
            jitter = random.uniform(0, 1)
            delay = min(MAX_BACKOFF_SECONDS, exp_delay + jitter)
            logging.warning(
                "Attempt %s/%s failed for %s; retrying in %.2fs. Error: %s",
                attempt,
                MAX_RETRIES,
                url,
                delay,
                exc,
            )
            time.sleep(delay)
        except (requests.RequestException, ValueError) as exc:
            if attempt >= MAX_RETRIES:
                raise RuntimeError(
                    f"Request failed after {MAX_RETRIES} attempts for {url}: {exc}"
                ) from exc

            exp_delay = min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
            jitter = random.uniform(0, 1)
            delay = min(MAX_BACKOFF_SECONDS, exp_delay + jitter)
            logging.warning(
                "Attempt %s/%s failed for %s; retrying in %.2fs. Error: %s",
                attempt,
                MAX_RETRIES,
                url,
                delay,
                exc,
            )
            time.sleep(delay)

    raise RuntimeError(f"Unexpected request flow for {url}")


def fetch_page_name() -> str:
    url = f"{GRAPH_BASE_URL}/{GRAPH_VERSION}/{PAGE_ID}"
    params = {"fields": "name", "access_token": ACCESS_TOKEN}
    data = graph_get(url, params)
    return data.get("name", "")


def fetch_posts() -> List[Dict[str, Any]]:
    url = f"{GRAPH_BASE_URL}/{GRAPH_VERSION}/{PAGE_ID}/posts"
    fields_index = 0
    params: Dict[str, Any] = {
        "fields": POST_FIELDS_CANDIDATES[fields_index],
        "since": parse_date_to_unix(SINCE),
        "until": parse_date_to_unix(UNTIL),
        "limit": 100,
        "access_token": ACCESS_TOKEN,
    }

    posts: List[Dict[str, Any]] = []

    while True:
        try:
            payload = graph_get(url, params, non_retryable_graph_codes={12})
        except GraphAPIError as exc:
            is_attachment_aggregation_deprecation = (
                exc.code == 12
                and "deprecate_post_aggregated_fields_for_attachement" in str(exc)
                and url.endswith("/posts")
                and "fields" in params
            )
            if (
                is_attachment_aggregation_deprecation
                and fields_index + 1 < len(POST_FIELDS_CANDIDATES)
            ):
                fields_index += 1
                params["fields"] = POST_FIELDS_CANDIDATES[fields_index]
                logging.warning(
                    "Posts field set rejected by Graph API; retrying with fallback fields: %s",
                    params["fields"],
                )
                continue
            raise

        batch = payload.get("data", [])

        for post in batch:
            posts.append(post)
            if DRY_RUN and len(posts) >= DRY_RUN_LIMIT:
                logging.info("Dry-run reached limit of %d posts.", DRY_RUN_LIMIT)
                return posts

        paging = payload.get("paging", {})
        next_url = paging.get("next")
        if not next_url:
            break

        url = next_url
        params = {}

    return posts


def _value_from_insight(item: Dict[str, Any]) -> Any:
    values = item.get("values", [])
    if not values:
        return 0
    first = values[0]
    if not isinstance(first, dict):
        return 0
    return first.get("value", 0)


def _zero_insights() -> Dict[str, Any]:
    return {col: 0 for col in CSV_COLUMNS if col not in BASE_COLUMNS}


def parse_insights(payload: Dict[str, Any]) -> Dict[str, Any]:
    result = _zero_insights()
    items = payload.get("data", [])

    metrics: Dict[str, Any] = {}
    for item in items:
        name = item.get("name")
        if not name:
            continue
        metrics[name] = _value_from_insight(item)

    impressions = metrics.get("post_impressions", 0) or metrics.get("post_media_view", 0)

    result["Impressions"] = int(impressions or 0)
    result["Impressions (Unique)"] = int(metrics.get("post_impressions_unique", 0) or 0)
    result["Impressions (Organic)"] = int(metrics.get("post_impressions_organic", 0) or 0)
    result["Impressions (Paid/Boosted)"] = int(metrics.get("post_impressions_paid", 0) or 0)
    result["Reach"] = int(metrics.get("post_reach", 0) or 0)
    result["Reach (Organic)"] = int(metrics.get("post_reach_organic", 0) or 0)
    result["Reach (Paid/Boosted)"] = int(metrics.get("post_reach_paid", 0) or 0)
    result["Engaged users"] = int(metrics.get("post_engaged_users", 0) or 0)
    result["Comments"] = int(metrics.get("post_comments", 0) or 0)
    result["Shares"] = int(metrics.get("post_shares", 0) or 0)
    result["Negative feedback"] = int(metrics.get("post_negative_feedback", 0) or 0)
    result["Negative feedback (Unique)"] = int(metrics.get("post_negative_feedback_unique", 0) or 0)

    reactions = metrics.get("post_reactions_by_type_total", {})
    if isinstance(reactions, dict):
        result["Reactions (like)"] = int(reactions.get("like", 0) or 0)
        result["Reactions (love)"] = int(reactions.get("love", 0) or 0)
        result["Reactions (wow)"] = int(reactions.get("wow", 0) or 0)
        result["Reactions (haha)"] = int(reactions.get("haha", 0) or 0)
        result["Reactions (sad)"] = int(reactions.get("sad", 0) or 0)
        result["Reactions (angry)"] = int(reactions.get("angry", 0) or 0)
        result["Reactions (Total)"] = int(sum(int(v or 0) for v in reactions.values()))

    clicks_breakdown = metrics.get("post_clicks_by_type", {})
    total_clicks = metrics.get("post_clicks", 0)
    link_clicks = 0
    other_clicks = 0
    if isinstance(clicks_breakdown, dict):
        link_clicks = int(
            clicks_breakdown.get("link clicks", clicks_breakdown.get("link_clicks", 0)) or 0
        )
        breakdown_total = int(sum(int(v or 0) for v in clicks_breakdown.values()))
        other_clicks = max(0, breakdown_total - link_clicks)
        if not total_clicks:
            total_clicks = breakdown_total

    result["Total clicks"] = int(total_clicks or 0)
    result["Link Clicks"] = link_clicks
    result["Other Clicks"] = other_clicks

    result["3-second video views"] = int(metrics.get("post_video_views_3s", 0) or 0)
    result["1-minute video views"] = int(metrics.get("post_video_views_1m", 0) or 0)
    result["Seconds viewed (video view time)"] = int(metrics.get("post_video_view_time", 0) or 0)
    result["Average seconds viewed (video avg time watched)"] = int(
        metrics.get("post_video_avg_time_watched", 0) or 0
    )

    video_age_gender = metrics.get("post_video_views_3s_by_age_bucket_and_gender", {})
    if isinstance(video_age_gender, dict):
        mapping = {
            "M.18-24": "3s_views_M_18_24",
            "M.25-34": "3s_views_M_25_34",
            "M.35-44": "3s_views_M_35_44",
            "M.45-54": "3s_views_M_45_54",
            "M.55-64": "3s_views_M_55_64",
            "M.65+": "3s_views_M_65_plus",
            "F.18-24": "3s_views_F_18_24",
            "F.25-34": "3s_views_F_25_34",
            "F.35-44": "3s_views_F_35_44",
            "F.45-54": "3s_views_F_45_54",
            "F.55-64": "3s_views_F_55_64",
            "F.65+": "3s_views_F_65_plus",
        }
        for src_key, out_key in mapping.items():
            result[out_key] = int(video_age_gender.get(src_key, 0) or 0)

    return result


def resolve_valid_metrics_for_run(sample_post_id: str) -> List[str]:
    """
    Detect valid metrics once by testing each candidate individually.
    Invalid metric code (#100) is treated as non-retryable for fast skipping.
    """
    global _VALID_METRICS_CACHE
    if _VALID_METRICS_CACHE is not None:
        return _VALID_METRICS_CACHE

    url = f"{GRAPH_BASE_URL}/{GRAPH_VERSION}/{sample_post_id}/insights"
    valid: List[str] = []

    logging.info("Discovering valid insights metrics (this happens once per run)...")

    for metric in INSIGHT_METRICS_CANDIDATES:
        try:
            graph_get(
                url,
                {
                    "metric": metric,
                    "period": "lifetime",
                    "access_token": ACCESS_TOKEN,
                },
                non_retryable_graph_codes={INVALID_METRIC_CODE},
            )
            valid.append(metric)
        except FatalGraphAPIError:
            raise
        except GraphAPIError as exc:
            if exc.code == INVALID_METRIC_CODE:
                logging.info("Metric unsupported/deprecated; skipping: %s", metric)
                continue
            raise

    _VALID_METRICS_CACHE = valid
    logging.info("Valid metrics selected for run: %s", ",".join(valid) if valid else "(none)")
    return valid


def fetch_post_insights(post_id: str, valid_metrics: List[str]) -> Dict[str, Any]:
    if not valid_metrics:
        return _zero_insights()

    url = f"{GRAPH_BASE_URL}/{GRAPH_VERSION}/{post_id}/insights"
    params = {
        "metric": ",".join(valid_metrics),
        "period": "lifetime",
        "access_token": ACCESS_TOKEN,
    }
    data = graph_get(url, params)
    return parse_insights(data)


def post_to_base_row(post: Dict[str, Any], page_name: str) -> Dict[str, Any]:
    return {
        "Post ID": post.get("id", ""),
        "Page name": page_name,
        "Title": post.get("message") or post.get("story") or "",
        "Publish time": post.get("created_time", ""),
        "Permalink": post.get("permalink_url", ""),
        "Post type": post.get("status_type") or post.get("type") or "",
    }


def build_rows(posts: List[Dict[str, Any]], page_name: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    total = len(posts)

    sample_post_id = ""
    for post in posts:
        pid = post.get("id")
        if pid:
            sample_post_id = pid
            break

    valid_metrics: List[str] = []
    if sample_post_id:
        valid_metrics = resolve_valid_metrics_for_run(sample_post_id)
    else:
        logging.warning("No post IDs found; insight fields will be zeroed.")

    for idx, post in enumerate(posts, start=1):
        post_id = post.get("id", "")
        logging.info("Processing post %d/%d: %s", idx, total, post_id)

        row = post_to_base_row(post, page_name)

        try:
            insights = fetch_post_insights(post_id, valid_metrics)
        except FatalGraphAPIError:
            raise
        except Exception as exc:
            logging.warning(
                "Insights failed for post %s after retries; using zeroed metrics. Error: %s",
                post_id,
                exc,
            )
            insights = _zero_insights()

        row.update(insights)
        normalized_row = {
            col: row.get(col, "" if col in BASE_COLUMNS else 0)
            for col in CSV_COLUMNS
        }
        rows.append(normalized_row)

    return rows


def write_csv(rows: List[Dict[str, Any]]) -> None:
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def validate_config() -> None:
    if PAGE_ID == "<PAGE_ID>":
        raise FatalGraphAPIError("PAGE_ID is not configured. Set PAGE_ID constant.")
    if ACCESS_TOKEN in ("", "<ACCESS_TOKEN>"):
        raise FatalGraphAPIError(
            "Access token missing. Set FB_PAGE_ACCESS_TOKEN environment variable."
        )


def main() -> None:
    setup_logging()
    validate_config()

    logging.info("Starting Facebook Page export (read-only mode).")

    page_name = fetch_page_name()
    logging.info("Resolved page name: %s", page_name)

    posts = fetch_posts()
    total_posts = len(posts)

    if DRY_RUN:
        logging.info(
            "Dry-run enabled: would export %d posts to %s.",
            total_posts,
            OUTPUT_FILE,
        )
        return

    rows = build_rows(posts, page_name)
    write_csv(rows)

    logging.info("Export complete.")
    logging.info("Total posts fetched: %d", total_posts)
    logging.info("Total rows written: %d", len(rows))
    logging.info("Output file: %s", OUTPUT_FILE)


if __name__ == "__main__":
    try:
        main()
    except FatalGraphAPIError as exc:
        logging.error("Fatal error: %s", exc)
        raise SystemExit(1)
    except Exception as exc:
        logging.exception("Unexpected unhandled error: %s", exc)
        raise SystemExit(1)
