Write a production-ready Python 3 script that uses the Facebook Graph API to export a Facebook Page’s posts + insights into a single CSV file.

GOAL
- Fetch all posts from a given Facebook Page within a date range
- For each post, fetch insights (reach, impressions, reactions, comments, shares, clicks, negative feedback, video metrics, and a 3-second video views breakdown by age/gender if available)
- Output one row per post to a CSV file (UTF-8)
- IMPORTANT: Build strong error handling so the script is “safe” and does not break the Page or cause issues. It must:
  - Never attempt to modify Page data (READ ONLY)
  - Avoid aggressive request bursts (throttle requests)
  - Implement robust retries with exponential backoff for transient errors
  - Handle rate limits (Facebook error codes like 4, 17, 32, 613) by backing off
  - Handle token/permission errors gracefully and stop with a clear message (no endless loops)
  - Continue processing other posts even if one post’s insights fail (write zeros/blanks for missing metrics)
  - Log actions in a clear way (progress + warnings), but DO NOT print the access token

CONSTRAINTS
- Use only: requests + Python standard library (csv, json, time, datetime, logging)
- Do NOT use the Facebook SDK.
- Do NOT store or output the access token anywhere except reading it from an environment variable by default.
- Provide a “dry-run” mode that only fetches the first N posts and prints how many would be exported.

CONFIG (constants at top of file)
- GRAPH_VERSION = "v19.0" (allow easy change)
- PAGE_ID = "<PAGE_ID>"
- ACCESS_TOKEN = read from env var "FB_PAGE_ACCESS_TOKEN" (fallback to a placeholder constant if needed)
- SINCE = "2026-01-01"   (YYYY-MM-DD)
- UNTIL = "2026-01-31"   (YYYY-MM-DD)
- OUTPUT_FILE = "fb_page_posts_report.csv"
- REQUEST_TIMEOUT_SECONDS = 30
- MAX_RETRIES = 6
- BASE_BACKOFF_SECONDS = 2
- MAX_BACKOFF_SECONDS = 120
- THROTTLE_SECONDS = 0.25  (sleep between requests)
- DRY_RUN = False
- DRY_RUN_LIMIT = 25

STEP 1 — FETCH PAGE NAME (once)
GET https://graph.facebook.com/{GRAPH_VERSION}/{PAGE_ID}
Params: fields=name, access_token=...

STEP 2 — FETCH POSTS (with pagination)
GET https://graph.facebook.com/{GRAPH_VERSION}/{PAGE_ID}/posts
Params:
  fields = "id,created_time,permalink_url,message,story,status_type,type"
  since = SINCE (convert to unix timestamp)
  until = UNTIL (convert to unix timestamp)
  limit = 100
  access_token = ...

Paginate using paging.next until no more data.
Collect posts into a list of dicts.
If DRY_RUN = True: stop after DRY_RUN_LIMIT posts.

Base columns per post:
- Post ID (id)
- Publish time (created_time)
- Permalink (permalink_url)
- Title (prefer message; else story; else empty)
- Post type (prefer status_type; else type; else empty)
- Page name

STEP 3 — FETCH INSIGHTS PER POST
For each post:
GET https://graph.facebook.com/{GRAPH_VERSION}/{POST_ID}/insights
Params:
  metric = comma-separated list below
  period = lifetime
  access_token = ...

Metrics to request (ignore unknown metrics gracefully):
Core:
- post_impressions
- post_impressions_unique
- post_impressions_organic
- post_impressions_paid
- post_reach
- post_reach_organic
- post_reach_paid
- post_engaged_users
- post_clicks
- post_clicks_unique
- post_clicks_by_type
- post_reactions_by_type_total
- post_comments
- post_shares
- post_negative_feedback
- post_negative_feedback_unique

Video (may be missing for non-video posts):
- post_video_views_3s
- post_video_views_1m
- post_video_view_time
- post_video_avg_time_watched
- post_video_views_3s_by_age_bucket_and_gender  (breakdown object keys like "M.18-24")

PARSING RULES
- Insights response is a list of objects with fields: name, values.
- For lifetime: use values[0].value (if missing, treat as 0 or {}).
- Breakdown parsing:
  A) post_reactions_by_type_total:
     - value is an object with keys like: like, love, wow, haha, sad, angry (sometimes “sorry” or others)
     - Create:
       - Reactions (Total) = sum of all reaction types
       - Also output each of: like, love, wow, haha, sad, angry (0 if missing)
  B) post_clicks_by_type:
     - value is an object with keys like "link clicks" and other click categories
     - Create:
       - Total clicks = post_clicks if available else sum of breakdown
       - Link Clicks = breakdown["link clicks"] if present (also accept "link_clicks")
       - Other Clicks = sum(breakdown) - Link Clicks
  C) post_video_views_3s_by_age_bucket_and_gender:
     - value is an object with keys like:
       "M.18-24", "M.25-34", "M.35-44", "M.45-54", "M.55-64", "M.65+",
       "F.18-24", "F.25-34", "F.35-44", "F.45-54", "F.55-64", "F.65+"
     - Flatten into these CSV columns (0 if missing):
       - 3s_views_M_18_24
       - 3s_views_M_25_34
       - 3s_views_M_35_44
       - 3s_views_M_45_54
       - 3s_views_M_55_64
       - 3s_views_M_65_plus
       - 3s_views_F_18_24
       - 3s_views_F_25_34
       - 3s_views_F_35_44
       - 3s_views_F_45_54
       - 3s_views_F_55_64
       - 3s_views_F_65_plus

STEP 4 — CSV OUTPUT (one row per post)
Write UTF-8 CSV with EXACT column order:
- Post ID
- Page name
- Title
- Publish time
- Permalink
- Post type
- Reach
- Reach (Organic)
- Reach (Paid/Boosted)
- Impressions
- Impressions (Unique)
- Impressions (Organic)
- Impressions (Paid/Boosted)
- Engaged users
- Reactions (Total)
- Reactions (like)
- Reactions (love)
- Reactions (wow)
- Reactions (haha)
- Reactions (sad)
- Reactions (angry)
- Comments
- Shares
- Total clicks
- Link Clicks
- Other Clicks
- Negative feedback
- Negative feedback (Unique)
- 3-second video views
- 1-minute video views
- Seconds viewed (video view time)
- Average seconds viewed (video avg time watched)
- 3s_views_M_18_24
- 3s_views_M_25_34
- 3s_views_M_35_44
- 3s_views_M_45_54
- 3s_views_M_55_64
- 3s_views_M_65_plus
- 3s_views_F_18_24
- 3s_views_F_25_34
- 3s_views_F_35_44
- 3s_views_F_45_54
- 3s_views_F_55_64
- 3s_views_F_65_plus

ERROR HANDLING (MUST IMPLEMENT)
- Create a helper function: graph_get(url, params) that:
  - Adds timeout
  - Retries on network errors and Facebook transient errors
  - Uses exponential backoff with jitter
  - Detects Graph API error structure: {"error": {"message":..., "type":..., "code":..., "error_subcode":..., "fbtrace_id":...}}
  - For rate limit codes (4, 17, 32, 613) -> backoff and retry
  - For token/permission errors (e.g., code 190, 10, 200) -> raise a fatal exception with a clear message and stop
- Throttle between successful requests: sleep(THROTTLE_SECONDS)
- If an insights request fails for a post after retries:
  - Log warning and proceed
  - Fill all insight fields with 0/blank for that post

LOGGING / SAFETY
- Use logging module
- Never log or print the access token
- Print progress like: "Processing post 12/345: <post_id>"
- End summary: total posts fetched, total rows written, output file path

DELIVERABLE
- Output the complete Python script as a single file content.
- Include brief run instructions at top:
  - pip install requests
  - export FB_PAGE_ACCESS_TOKEN="..."
  - python export_fb_posts.py
