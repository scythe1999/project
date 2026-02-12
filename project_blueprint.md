You are a senior Python engineer. Create a NEW standalone Python script named:

  export_fb_post_spend.py

This script must export "Spent per post" for Facebook Page posts by attributing ad spend to each post via the Marketing API, using Graph API v23.0. The output must be an XLSX file that includes a Post ID join key column so it can be merged with my existing per-post insights export.

DO NOT modify my existing script. This is a new script.

=========================
GOALS / OUTPUT
=========================
1) Produce an XLSX file (openpyxl) with a single sheet "FB Post Spend".
2) Each row must represent a POST (by post_id), and include:
   - "Post ID"                     (join key; must match post ids from /{page_id}/posts)
   - "Spent per post"              (float, rounded to 2 decimals)
   - "Ad IDs"                      (optional: comma-separated list; helpful for debugging)
   - "Ads matched"                 (integer count of ads mapped to the post)
   - "Since"                       (YYYY-MM-DD)
   - "Until"                       (YYYY-MM-DD)
   - "Graph version"               (e.g., v23.0)
3) If a post has no matched spend, still include it with "Spent per post" = 0.0.
4) This script must fetch the posts itself (same date window as spend). It must not require the user to supply post ids.

=========================
INPUTS / CLI
=========================
Implement argparse with flags:
- --page-id              default from PAGE_ID constant
- --ad-account-id         default from env AD_ACCOUNT_ID or "<AD_ACCOUNT_ID>"
- --since                default "YYYY-MM-DD"
- --until                default "YYYY-MM-DD"
- --graph-version        default "v23.0"
- --output               default "fb_post_spend_report.xlsx"
- --debug                enable debug logging + write JSON debug file

Token:
- Use env var FB_PAGE_ACCESS_TOKEN as ACCESS_TOKEN (same token used for post insights; assume it has ads permissions).
- If token missing, exit with a clear fatal error.

=========================
API / ATTRIBUTION LOGIC
=========================
Use Graph base URL: https://graph.facebook.com

A) Fetch posts for the Page within the date range:
   Endpoint: /{graph_version}/{page_id}/posts
   Params:
     fields=id,created_time,permalink_url,message,story,status_type,type
     since=<unix>
     until=<unix>
     limit=100
     access_token=ACCESS_TOKEN
   Handle paging.next until done.
   Store a set of post_ids and also a list of post dicts (keep created_time/permalink for context columns optionally).

B) Fetch ads for the Ad Account and map ads -> effective_object_story_id:
   Endpoint: /{graph_version}/act_{ad_account_id}/ads
   Params:
     fields=id,adcreatives{effective_object_story_id},creative{effective_object_story_id,id},created_time,updated_time,status
     limit=100
     access_token=ACCESS_TOKEN
   Handle paging.
   For each ad:
     - Extract ad_id
     - Extract story_id = effective_object_story_id from adcreatives or creative
     - If story_id exists AND story_id is in the post_ids set, store mapping: post_id -> list[ad_id]
   If story_id is missing for an ad, ignore it.

C) Fetch spend per ad_id using Marketing API insights:
   Endpoint: /{graph_version}/{ad_id}/insights
   Params:
     fields=spend
     level=ad
     time_range[since]=SINCE
     time_range[until]=UNTIL
     access_token=ACCESS_TOKEN
   Parse spend as float.
   Implement caching per ad_id to avoid re-fetch.
   NOTE: Handle that insights data may be empty -> spend=0.0.
   Sum spend across all ads mapped to a post.

D) Output rows:
   For each post in the posts list (to ensure 0.0 rows exist), compute:
     spend = round(sum(spend_by_ad_id), 2)
   Write XLSX with the required columns.
   Keep deterministic order: sort by Publish time ascending (created_time) or keep fetched order.

=========================
RELIABILITY / SAFETY
=========================
Implement a robust `graph_get()` helper like my existing script:
- requests.get with timeout
- retry up to MAX_RETRIES with exponential backoff + jitter
- retry on HTTP 429/5xx
- detect Graph errors in payload.error:
   - fatal auth/permission codes: {10, 190, 200} => raise FatalGraphAPIError
   - rate limit codes: {4, 17, 32, 613} => retry
   - other codes => raise GraphAPIError(code=...)
- throttle between successful requests (e.g., 0.25s)

Use the same constants:
REQUEST_TIMEOUT_SECONDS=30, MAX_RETRIES=6, BASE_BACKOFF_SECONDS=2, MAX_BACKOFF_SECONDS=120, THROTTLE_SECONDS=0.25

Include logging (INFO by default, DEBUG if --debug).

=========================
DEBUG ARTIFACT
=========================
If --debug:
- Write a JSON file (e.g., spend_debug.json) that includes:
  - graph_version, page_id, ad_account_id (normalized)
  - counts: posts_fetched, ads_scanned, ads_with_story_id, posts_matched_to_ads
  - sample mappings: first N post_id -> ad_ids
  - sample spend responses (first N ad_ids)
Be mindful not to log the access token.

=========================
EDGE CASES
=========================
- ad-account-id may be provided as "act_123" or "123"; normalize to numeric, but build URL as act_<id>.
- If AD_ACCOUNT_ID is missing ("<AD_ACCOUNT_ID>"), do not crash; output spend=0.0 for all posts and log a warning.
- If a post was boosted via multiple ads, sum all relevant ad spends.
- If API returns spend strings, parse safely as float.

=========================
DELIVERABLE
=========================
Return the COMPLETE Python script (single file) ready to run.
No diffs. No placeholders beyond the constants. Do not include any extraneous commentary.
