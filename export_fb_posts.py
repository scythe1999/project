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

import datetime
import json
import logging
import os
import random
import re
import time
import argparse
from typing import Any, Dict, List, Optional, Set

import requests
from openpyxl import Workbook

# =========================
# Configuration
# =========================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GRAPH_VERSION = "v23.0"
PAGE_ID = "101275806400438"
ACCESS_TOKEN_PLACEHOLDER = "<ACCESS_TOKEN>"
ACCESS_TOKEN = os.getenv("FB_PAGE_ACCESS_TOKEN", ACCESS_TOKEN_PLACEHOLDER)
SINCE = "2026-01-01"  # YYYY-MM-DD
UNTIL = "2026-01-31"  # YYYY-MM-DD
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "fb_page_posts_report.xlsx")
METRICS_DEBUG_FILE = os.path.join(SCRIPT_DIR, "metrics_debug.json")
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
INVALID_QUERY_CODE = 3001

POST_FIELDS_CANDIDATES = [
    # Include public engagement counters when available.
    "id,created_time,permalink_url,message,status_type,shares,comments.limit(0).summary(true)",
    "id,created_time,permalink_url,message,story,status_type,shares,comments.limit(0).summary(true)",
    "id,created_time,permalink_url,message,story,status_type,type,shares,comments.limit(0).summary(true)",
    # Stable minimal baseline first.
    "id,created_time,permalink_url,message,status_type",
    # Fallback with story if message-only payload is constrained.
    "id,created_time,permalink_url,message,story,status_type",
    # Richer metadata (can fail on some pages/apps).
    "id,created_time,permalink_url,message,story,status_type,type",
    # Last-resort fallback.
    "id,created_time,permalink_url,message",
]

INSIGHT_METRICS_CANDIDATES = [
    "post_reach",
    "post_reach_organic",
    "post_reach_paid",
    "post_impressions",
    "post_impressions_organic",
    "post_impressions_paid",
    "post_impressions_organic_unique",
    "post_impressions_paid_unique",
    "post_engaged_users",
    "post_comments",
    "post_shares",
    "post_negative_feedback",
    "post_negative_feedback_unique",
    "post_impressions_unique",
    "post_media_view",
    "post_media_views",
    "post_views",
    "post_video_views",
    "post_total_views",
    "post_clicks",
    "post_clicks_unique",
    "post_clicks_by_type",
    "post_reactions_by_type_total",
    "post_video_view_time",
    "post_video_avg_time_watched",
    "post_video_views_3s",
    "post_video_views_1m",
    "post_video_views_3s_clicked_to_play",
    "post_video_views_3s_autoplayed",
    "post_video_views_60s_excludes_shorter_views",
    "post_video_complete_views_30s",
    "post_video_complete_views_30s_organic",
    "post_video_complete_views_30s_paid",
    "post_video_views_3s_by_age_bucket_and_gender",
    "post_impressions_by_age_and_gender_unique",
]

VIDEO_3S_METRIC_PRIORITY = [
    "post_video_views_3s",
    "post_video_views_3s_clicked_to_play",
    "post_video_views_3s_autoplayed",
]

VIDEO_1M_METRIC_PRIORITY = [
    "post_video_views_1m",
    "post_video_views_60s_excludes_shorter_views",
    "post_video_complete_views_30s",
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
    "reach_M_18_24",
    "reach_M_25_34",
    "reach_M_35_44",
    "reach_M_45_54",
    "reach_M_55_64",
    "reach_M_65_plus",
    "reach_F_18_24",
    "reach_F_25_34",
    "reach_F_35_44",
    "reach_F_45_54",
    "reach_F_55_64",
    "reach_F_65_plus",
]

BASE_COLUMNS = {
    "Post ID",
    "Page name",
    "Title",
    "Publish time",
    "Permalink",
    "Post type",
}

_METRIC_DISCOVERY_CACHE: Dict[str, List[str]] = {}
_METRICS_DEBUG: Dict[str, Any] = {"graph_version": GRAPH_VERSION, "groups": {}}
_METRIC_USAGE_COUNTS: Dict[str, int] = {}
_DEBUG_ENABLED = False


class FatalGraphAPIError(RuntimeError):
    """Fatal Graph API issue that should stop script execution."""


class GraphAPIError(RuntimeError):
    """Graph API error with parsed metadata for smarter retry behavior."""

    def __init__(self, message: str, code: Optional[int] = None) -> None:
        super().__init__(message)
        self.code = code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Facebook page posts and compatible insights metrics (read-only)."
    )
    parser.add_argument("--since", default=SINCE, help="Start date YYYY-MM-DD")
    parser.add_argument("--until", default=UNTIL, help="End date YYYY-MM-DD")
    parser.add_argument("--page-id", default=PAGE_ID, help="Facebook Page ID")
    parser.add_argument("--graph-version", default=GRAPH_VERSION, help="Graph API version")
    parser.add_argument("--output", default=OUTPUT_FILE, help="XLSX output file path")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging and write metrics_debug.json",
    )
    return parser.parse_args()


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_date_to_unix(date_str: str) -> int:
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _looks_like_invalid_fields_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "field" in text
        and (
            "invalid" in text
            or "unsupported" in text
            or "unknown" in text
            or "cannot be accessed" in text
        )
    )


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
            is_field_related_error = (
                url.endswith("/posts")
                and "fields" in params
                and _looks_like_invalid_fields_error(exc)
            )
            if (
                is_field_related_error
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
    extracted_values: List[Any] = []
    for value_item in values:
        if isinstance(value_item, dict) and "value" in value_item:
            extracted_values.append(value_item.get("value", 0))

    if not extracted_values:
        return 0
    if len(extracted_values) == 1:
        return extracted_values[0]
    return extracted_values


def _normalize_breakdown_value(raw_value: Any) -> Dict[str, Any]:
    """Normalize Graph API breakdown payloads to a flat dict shape."""
    if isinstance(raw_value, list):
        merged: Dict[str, Any] = {}
        for item in raw_value:
            normalized = _normalize_breakdown_value(item)
            if not isinstance(normalized, dict):
                continue
            for key, value in normalized.items():
                merged[key] = _safe_int(merged.get(key, 0)) + _safe_int(value)
        return merged

    if isinstance(raw_value, dict):
        if "value" in raw_value and isinstance(raw_value["value"], dict):
            return raw_value["value"]
        if "total_value" in raw_value and isinstance(raw_value["total_value"], dict):
            nested_total = raw_value["total_value"]
            if "value" in nested_total and isinstance(nested_total["value"], dict):
                return nested_total["value"]
            if "breakdowns" in nested_total:
                return _normalize_breakdown_value(nested_total["breakdowns"])
            return nested_total
        if "breakdowns" in raw_value:
            return _normalize_breakdown_value(raw_value["breakdowns"])
        if "results" in raw_value and isinstance(raw_value["results"], list):
            normalized: Dict[str, Any] = {}
            for result in raw_value["results"]:
                if not isinstance(result, dict):
                    continue
                dimensions = result.get("dimension_values", [])
                value = _safe_int(result.get("value", 0))
                normalized_key = _normalize_age_gender_from_dimensions(dimensions)
                if not normalized_key:
                    if _DEBUG_ENABLED:
                        logging.debug(
                            "Skipping unrecognized age/gender dimensions: %s",
                            dimensions,
                        )
                    continue
                normalized[normalized_key] = normalized.get(normalized_key, 0) + value
            if normalized:
                return normalized
        return raw_value
    return {}


def _normalize_age_gender_key(raw_key: Any) -> Optional[str]:
    key = str(raw_key or "").strip()
    if not key:
        return None

    key_upper = key.upper().replace("_", ".")
    key_upper = re.sub(r"\s+", "", key_upper)
    match = re.search(r"\b(M|F|MALE|FEMALE)\b", key_upper)
    age_match = re.search(r"(18[-.]?24|25[-.]?34|35[-.]?44|45[-.]?54|55[-.]?64|65\+)", key_upper)
    if not match or not age_match:
        return None

    gender_token = match.group(1)
    gender = "M" if gender_token in {"M", "MALE"} else "F"
    age = age_match.group(1).replace(".", "-")
    if age == "65+":
        return f"{gender}.65+"
    return f"{gender}.{age}"


def _normalize_age_gender_from_dimensions(raw_dimensions: Any) -> Optional[str]:
    """Normalize age+gender values from Graph breakdown dimension arrays."""
    if not isinstance(raw_dimensions, list) or not raw_dimensions:
        return None

    cleaned_parts = [str(part or "").strip() for part in raw_dimensions if str(part or "").strip()]
    if not cleaned_parts:
        return None

    # Graph can return either ["18-24", "female"] or ["female", "18-24"] depending on version/metric.
    combined = ".".join(cleaned_parts)
    return _normalize_age_gender_key(combined)


def _normalize_scalar_value(raw_value: Any) -> int:
    """Normalize scalar insight values across Graph API response variants."""
    if isinstance(raw_value, (int, float, str)):
        return _safe_int(raw_value)

    if isinstance(raw_value, dict):
        # Common wrappers observed across Graph versions.
        for key in ("value", "total_value", "count", "total", "sum"):
            if key in raw_value:
                normalized = _normalize_scalar_value(raw_value[key])
                if normalized:
                    return normalized

        # If a dict has only numeric values, sum them as a fallback.
        numeric_values = [_safe_int(v) for v in raw_value.values()]
        if any(numeric_values):
            return int(sum(numeric_values))

    if isinstance(raw_value, list):
        # Keep best non-zero number in case Graph returns a short series.
        numeric_values = [_normalize_scalar_value(v) for v in raw_value]
        non_zero = [v for v in numeric_values if v > 0]
        if non_zero:
            return max(non_zero)
        return 0

    return 0


def _zero_insights() -> Dict[str, Any]:
    return {col: 0 for col in CSV_COLUMNS if col not in BASE_COLUMNS}


def _first_positive_metric(metrics: Dict[str, Any], metric_names: List[str]) -> int:
    """Return first positive metric value from a prioritized list of metric names."""
    for metric_name in metric_names:
        value = _safe_int(metrics.get(metric_name, 0))
        if value > 0:
            return value
    return 0


def _sum_metrics(metrics: Dict[str, Any], metric_names: List[str]) -> int:
    """Return the sum of metrics for scenarios where Graph splits one KPI into multiple fields."""
    return int(sum(_safe_int(metrics.get(metric_name, 0)) for metric_name in metric_names))


def _extract_post_counter(post: Dict[str, Any], key: str) -> int:
    """Best-effort extraction for counters returned directly on /posts edge."""
    raw_value = post.get(key)
    if key == "comments" and isinstance(raw_value, dict):
        summary = raw_value.get("summary", {})
        return _safe_int(summary.get("total_count", 0))
    if key == "shares" and isinstance(raw_value, dict):
        return _safe_int(raw_value.get("count", 0))
    return _safe_int(raw_value)


def parse_insights(payload: Dict[str, Any]) -> Dict[str, Any]:
    result = _zero_insights()
    items = payload.get("data", [])

    breakdown_metrics = {
        "post_reactions_by_type_total",
        "post_clicks_by_type",
        "post_video_views_3s_by_age_bucket_and_gender",
        "post_impressions_by_age_and_gender_unique",
    }

    metrics: Dict[str, Any] = {}
    for item in items:
        name = item.get("name")
        if not name:
            continue
        raw_value = _value_from_insight(item)
        if name in breakdown_metrics:
            metrics[name] = _normalize_breakdown_value(raw_value)
        else:
            metrics[name] = _normalize_scalar_value(raw_value)

    impression_fallback_metric_names = [
        "post_impressions",
        "post_media_view",
        "post_media_views",
        "post_views",
        "post_total_views",
        "post_video_views",
    ]
    impressions = 0
    for metric_name in impression_fallback_metric_names:
        value = metrics.get(metric_name, 0)
        if _safe_int(value) > 0:
            impressions = value
            break

    result["Impressions"] = _safe_int(impressions)

    impressions_organic = _safe_int(metrics.get("post_impressions_organic", 0))
    if impressions_organic <= 0:
        impressions_organic = _safe_int(metrics.get("post_impressions_organic_unique", 0))

    impressions_paid = _safe_int(metrics.get("post_impressions_paid", 0))
    if impressions_paid <= 0:
        impressions_paid = _safe_int(metrics.get("post_impressions_paid_unique", 0))

    impressions_unique = _safe_int(metrics.get("post_impressions_unique", 0))
    if impressions_unique <= 0 and (impressions_organic > 0 or impressions_paid > 0):
        impressions_unique = impressions_organic + impressions_paid

    result["Impressions (Unique)"] = impressions_unique
    result["Impressions (Organic)"] = impressions_organic
    result["Impressions (Paid/Boosted)"] = impressions_paid

    reach_organic = _safe_int(metrics.get("post_reach_organic", 0))
    if reach_organic <= 0:
        reach_organic = _safe_int(metrics.get("post_impressions_organic_unique", 0))
    if reach_organic <= 0:
        reach_organic = _safe_int(metrics.get("post_impressions_organic", 0))

    reach_paid = _safe_int(metrics.get("post_reach_paid", 0))
    if reach_paid <= 0:
        reach_paid = _safe_int(metrics.get("post_impressions_paid_unique", 0))
    if reach_paid <= 0:
        reach_paid = _safe_int(metrics.get("post_impressions_paid", 0))

    reach_total = _safe_int(metrics.get("post_reach", 0))
    if reach_total <= 0:
        reach_total = _safe_int(metrics.get("post_impressions_unique", 0))
    if reach_total <= 0 and (reach_organic > 0 or reach_paid > 0):
        reach_total = reach_organic + reach_paid
    if reach_total <= 0:
        reach_total = _safe_int(result.get("Impressions", 0))

    if reach_total > 0 and reach_organic > 0 and reach_paid <= 0:
        reach_paid = max(0, reach_total - reach_organic)
    if reach_total > 0 and reach_paid > 0 and reach_organic <= 0:
        reach_organic = max(0, reach_total - reach_paid)

    result["Reach"] = reach_total
    result["Reach (Organic)"] = reach_organic
    result["Reach (Paid/Boosted)"] = reach_paid
    engaged_users = _safe_int(metrics.get("post_engaged_users", 0))
    if engaged_users <= 0:
        engaged_users = _safe_int(metrics.get("post_clicks_unique", 0))
    if engaged_users <= 0:
        engaged_users = max(
            _safe_int(metrics.get("post_clicks", 0)),
            _safe_int(metrics.get("post_comments", 0)) + _safe_int(metrics.get("post_shares", 0)),
        )
    result["Engaged users"] = engaged_users
    result["Comments"] = _safe_int(metrics.get("post_comments", 0))
    result["Shares"] = _safe_int(metrics.get("post_shares", 0))
    result["Negative feedback"] = _safe_int(metrics.get("post_negative_feedback", 0))
    result["Negative feedback (Unique)"] = _safe_int(metrics.get("post_negative_feedback_unique", 0))

    reactions = metrics.get("post_reactions_by_type_total", {})
    if isinstance(reactions, dict):
        result["Reactions (like)"] = _safe_int(reactions.get("like", 0))
        result["Reactions (love)"] = _safe_int(reactions.get("love", 0))
        result["Reactions (wow)"] = _safe_int(reactions.get("wow", 0))
        result["Reactions (haha)"] = _safe_int(reactions.get("haha", 0))
        result["Reactions (sad)"] = _safe_int(reactions.get("sad", 0))
        result["Reactions (angry)"] = _safe_int(reactions.get("angry", 0))
        result["Reactions (Total)"] = int(sum(_safe_int(v) for v in reactions.values()))

    clicks_breakdown = metrics.get("post_clicks_by_type", {})
    total_clicks = metrics.get("post_clicks", 0)
    link_clicks = 0
    other_clicks = 0
    if isinstance(clicks_breakdown, dict):
        link_clicks = _safe_int(
            clicks_breakdown.get("link clicks", clicks_breakdown.get("link_clicks", 0))
        )
        breakdown_total = int(sum(_safe_int(v) for v in clicks_breakdown.values()))
        other_clicks = max(0, breakdown_total - link_clicks)
        if not total_clicks:
            total_clicks = breakdown_total

    result["Total clicks"] = _safe_int(total_clicks)
    result["Link Clicks"] = link_clicks
    result["Other Clicks"] = other_clicks

    three_second_views = _first_positive_metric(metrics, VIDEO_3S_METRIC_PRIORITY)
    if three_second_views <= 0:
        three_second_views = _sum_metrics(
            metrics,
            ["post_video_views_3s_clicked_to_play", "post_video_views_3s_autoplayed"],
        )
    if three_second_views <= 0:
        video_age_gender = metrics.get("post_video_views_3s_by_age_bucket_and_gender", {})
        if isinstance(video_age_gender, dict):
            three_second_views = int(sum(_safe_int(v) for v in video_age_gender.values()))
    if three_second_views <= 0:
        # Backward-compatible fallback in cases where Graph exposes only aggregate video views.
        three_second_views = _safe_int(metrics.get("post_video_views", 0))

    one_minute_views = _first_positive_metric(metrics, VIDEO_1M_METRIC_PRIORITY)
    if one_minute_views <= 0:
        one_minute_views = _safe_int(metrics.get("post_video_complete_views_30s_organic", 0)) + _safe_int(
            metrics.get("post_video_complete_views_30s_paid", 0)
        )

    # 1-minute views are a strict subset of 3-second views; keep values logically consistent.
    if one_minute_views > 0 and three_second_views < one_minute_views:
        three_second_views = one_minute_views

    result["3-second video views"] = three_second_views
    result["1-minute video views"] = one_minute_views
    result["Seconds viewed (video view time)"] = _safe_int(metrics.get("post_video_view_time", 0))
    result["Average seconds viewed (video avg time watched)"] = _safe_int(
        metrics.get("post_video_avg_time_watched", 0)
    )

    reach_age_gender = metrics.get("post_impressions_by_age_and_gender_unique", {})
    if not isinstance(reach_age_gender, dict) or not reach_age_gender:
        # Fallback to previous 3s video breakdown only when reach breakdown is unavailable.
        reach_age_gender = metrics.get("post_video_views_3s_by_age_bucket_and_gender", {})

    if isinstance(reach_age_gender, dict):
        normalized_breakdown: Dict[str, int] = {}
        for raw_key, raw_value in reach_age_gender.items():
            normalized_key = _normalize_age_gender_key(raw_key)
            if not normalized_key:
                logging.debug(
                    "Unable to map age/gender key '%s' from reach age/gender breakdown metric.",
                    raw_key,
                )
                continue
            normalized_breakdown[normalized_key] = normalized_breakdown.get(normalized_key, 0) + _safe_int(raw_value)

        if _DEBUG_ENABLED:
            logging.debug("Normalized reach age/gender breakdown: %s", normalized_breakdown)

        mapping = {
            "M.18-24": "reach_M_18_24",
            "M.25-34": "reach_M_25_34",
            "M.35-44": "reach_M_35_44",
            "M.45-54": "reach_M_45_54",
            "M.55-64": "reach_M_55_64",
            "M.65+": "reach_M_65_plus",
            "F.18-24": "reach_F_18_24",
            "F.25-34": "reach_F_25_34",
            "F.35-44": "reach_F_35_44",
            "F.45-54": "reach_F_45_54",
            "F.55-64": "reach_F_55_64",
            "F.65+": "reach_F_65_plus",
        }
        for src_key, out_key in mapping.items():
            result[out_key] = _safe_int(normalized_breakdown.get(src_key, 0))
    elif _DEBUG_ENABLED:
        logging.debug(
            "Expected dict for post_impressions_by_age_and_gender_unique/post_video_views_3s_by_age_bucket_and_gender but got %s",
            type(reach_age_gender).__name__,
        )

    return result


def _derive_post_group_key(post: Dict[str, Any]) -> str:
    status_type = post.get("status_type") or "unknown_status"
    post_type = post.get("type") or "unknown_type"
    return f"status_type={status_type}|type={post_type}"


def _probe_metrics_from_candidates(url: str) -> List[str]:
    valid: List[str] = []
    for metric in INSIGHT_METRICS_CANDIDATES:
        try:
            graph_get(
                url,
                {
                    "metric": metric,
                    "period": "lifetime",
                    "access_token": ACCESS_TOKEN,
                },
                non_retryable_graph_codes={INVALID_METRIC_CODE, INVALID_QUERY_CODE},
            )
            valid.append(metric)
        except GraphAPIError as exc:
            if exc.code == INVALID_METRIC_CODE:
                continue
            if exc.code == INVALID_QUERY_CODE:
                # Endpoint is not queryable for this post/context; skip quickly.
                return []
            raise
    return valid


def resolve_valid_metrics_for_group(sample_post_id: str, group_key: str) -> List[str]:
    if group_key in _METRIC_DISCOVERY_CACHE:
        return _METRIC_DISCOVERY_CACHE[group_key]

    url = f"{GRAPH_BASE_URL}/{GRAPH_VERSION}/{sample_post_id}/insights"
    valid: List[str] = []
    raw_payload: Optional[Dict[str, Any]] = None

    # First preference: discover available metrics from Graph directly.
    try:
        payload = graph_get(
            url,
            {
                "period": "lifetime",
                "access_token": ACCESS_TOKEN,
            },
            non_retryable_graph_codes={INVALID_METRIC_CODE, INVALID_QUERY_CODE},
        )
        names = [item.get("name") for item in payload.get("data", []) if item.get("name")]
        valid = [name for name in names if isinstance(name, str)]
        raw_payload = payload
    except GraphAPIError as exc:
        # Some versions require an explicit metric parameter.
        if exc.code not in {INVALID_METRIC_CODE, INVALID_QUERY_CODE}:
            raise

    if not valid:
        valid = _probe_metrics_from_candidates(url)

    _METRIC_DISCOVERY_CACHE[group_key] = valid
    _METRICS_DEBUG["groups"][group_key] = {
        "sample_post_id": sample_post_id,
        "valid_metrics": valid,
        "posts_processed": 0,
        "raw_insights_payload": raw_payload,
    }
    logging.info("Metrics for %s: %s", group_key, ",".join(valid) if valid else "(none)")
    return valid


def _extract_invalid_metric_name(error_message: str) -> Optional[str]:
    marker = "The value must be a valid insights metric"
    if marker not in error_message:
        return None
    if ":" not in error_message:
        return None
    return error_message.split(":", 1)[-1].strip().strip(".'\"")


def fetch_post_insights(post_id: str, valid_metrics: List[str], group_key: str) -> Dict[str, Any]:
    if not valid_metrics:
        return _zero_insights()

    url = f"{GRAPH_BASE_URL}/{GRAPH_VERSION}/{post_id}/insights"
    metrics_to_request = list(valid_metrics)
    while metrics_to_request:
        params = {
            "metric": ",".join(metrics_to_request),
            "period": "lifetime",
            "access_token": ACCESS_TOKEN,
        }
        try:
            data = graph_get(
                url,
                params,
                non_retryable_graph_codes={INVALID_METRIC_CODE, INVALID_QUERY_CODE},
            )
            group_info = _METRICS_DEBUG["groups"].get(group_key, {})
            if group_info.get("raw_insights_payload") is None:
                group_info["raw_insights_payload"] = data
            if _DEBUG_ENABLED:
                logging.debug("Raw insights payload for post %s: %s", post_id, data)
            return parse_insights(data)
        except GraphAPIError as exc:
            if exc.code == INVALID_QUERY_CODE:
                # Common for posts where insights are not available via this edge.
                _METRIC_DISCOVERY_CACHE[group_key] = []
                group_info = _METRICS_DEBUG["groups"].get(group_key, {})
                group_info["valid_metrics"] = []
                return _zero_insights()

            if exc.code != INVALID_METRIC_CODE:
                raise

            invalid_name = _extract_invalid_metric_name(str(exc))
            if invalid_name and invalid_name in metrics_to_request:
                logging.debug(
                    "Removing invalid metric '%s' for group '%s' while fetching post %s.",
                    invalid_name,
                    group_key,
                    post_id,
                )
                metrics_to_request.remove(invalid_name)
                cached = _METRIC_DISCOVERY_CACHE.get(group_key, [])
                _METRIC_DISCOVERY_CACHE[group_key] = [m for m in cached if m != invalid_name]
                group_info = _METRICS_DEBUG["groups"].get(group_key, {})
                group_info["valid_metrics"] = _METRIC_DISCOVERY_CACHE[group_key]
                continue
            break

    return _zero_insights()


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

    for idx, post in enumerate(posts, start=1):
        post_id = post.get("id", "")
        logging.info("Processing post %d/%d: %s", idx, total, post_id)

        row = post_to_base_row(post, page_name)
        group_key = _derive_post_group_key(post)
        _METRIC_USAGE_COUNTS[group_key] = _METRIC_USAGE_COUNTS.get(group_key, 0) + 1

        valid_metrics: List[str] = []
        if post_id:
            try:
                valid_metrics = resolve_valid_metrics_for_group(post_id, group_key)
            except Exception as exc:
                logging.warning(
                    "Metric discovery failed for post %s (group %s); using zeroed insights. Error: %s",
                    post_id,
                    group_key,
                    exc,
                )
                _METRIC_DISCOVERY_CACHE[group_key] = []
                group_info = _METRICS_DEBUG["groups"].setdefault(
                    group_key,
                    {
                        "sample_post_id": post_id,
                        "valid_metrics": [],
                        "posts_processed": 0,
                        "raw_insights_payload": None,
                    },
                )
                group_info["valid_metrics"] = []
        else:
            logging.warning("Post missing id; insight fields will be zeroed.")

        try:
            insights = fetch_post_insights(post_id, valid_metrics, group_key)
        except FatalGraphAPIError:
            raise
        except Exception as exc:
            logging.warning(
                "Insights failed for post %s after retries; using zeroed metrics. Error: %s",
                post_id,
                exc,
            )
            insights = _zero_insights()

        fallback_comments = _extract_post_counter(post, "comments")
        fallback_shares = _extract_post_counter(post, "shares")
        if _safe_int(insights.get("Comments", 0)) <= 0 and fallback_comments > 0:
            insights["Comments"] = fallback_comments
        if _safe_int(insights.get("Shares", 0)) <= 0 and fallback_shares > 0:
            insights["Shares"] = fallback_shares

        if group_key in _METRICS_DEBUG["groups"]:
            _METRICS_DEBUG["groups"][group_key]["posts_processed"] += 1

        row.update(insights)
        normalized_row = {
            col: row.get(col, "" if col in BASE_COLUMNS else 0)
            for col in CSV_COLUMNS
        }
        rows.append(normalized_row)

    return rows


def write_xlsx(rows: List[Dict[str, Any]]) -> None:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "FB Posts"

    worksheet.append(CSV_COLUMNS)
    for row in rows:
        worksheet.append([row.get(column, "") for column in CSV_COLUMNS])

    workbook.save(OUTPUT_FILE)


def write_metrics_debug() -> None:
    with open(METRICS_DEBUG_FILE, "w", encoding="utf-8") as debug_file:
        json.dump(_METRICS_DEBUG, debug_file, indent=2, ensure_ascii=False)


def print_metric_summary() -> None:
    logging.info("Insights metrics summary by post group:")
    for group_key, count in sorted(_METRIC_USAGE_COUNTS.items()):
        metrics = _METRIC_DISCOVERY_CACHE.get(group_key, [])
        logging.info(
            "Group=%s | posts=%d | valid_metrics=%s",
            group_key,
            count,
            ",".join(metrics) if metrics else "(none)",
        )


def validate_config() -> None:
    if PAGE_ID == "<PAGE_ID>":
        raise FatalGraphAPIError("PAGE_ID is not configured. Set PAGE_ID constant.")
    if ACCESS_TOKEN in ("", "<ACCESS_TOKEN>"):
        raise FatalGraphAPIError(
            "Access token missing. Set FB_PAGE_ACCESS_TOKEN environment variable."
        )


def main() -> None:
    global PAGE_ID, SINCE, UNTIL, OUTPUT_FILE, GRAPH_VERSION, _DEBUG_ENABLED, _METRICS_DEBUG

    args = parse_args()
    PAGE_ID = args.page_id
    SINCE = args.since
    UNTIL = args.until
    OUTPUT_FILE = args.output
    GRAPH_VERSION = args.graph_version
    _DEBUG_ENABLED = args.debug
    _METRICS_DEBUG = {"graph_version": GRAPH_VERSION, "groups": {}}

    setup_logging()
    if _DEBUG_ENABLED:
        logging.getLogger().setLevel(logging.DEBUG)
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
    write_xlsx(rows)
    print_metric_summary()

    if _DEBUG_ENABLED:
        write_metrics_debug()
        logging.info("Metrics debug output: %s", METRICS_DEBUG_FILE)

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

