"""
═══════════════════════════════════════════════════════════════
PX DASHBOARD PIPELINE
═══════════════════════════════════════════════════════════════
Processes an Intercom CSV export and writes structured data
to Supabase for the dashboard to read.

Usage:
  python pipeline.py data/export.csv              # Full run
  python pipeline.py data/export.csv --skip-categorize  # Skip API calls, just aggregate
  python pipeline.py --publish-insights           # Publish all draft insights
  python pipeline.py --review-insights            # View and approve draft insights

Requirements: anthropic, python-dotenv, requests
"""

import csv
import html
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from statistics import median

import anthropic
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), override=True)

# ═══ CONFIG ═══

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
CATEGORIZATION_MODEL = "claude-haiku-4-5-20251001"
INSIGHTS_MODEL = "claude-sonnet-4-6"

HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

HEADERS_RETURN = {
    **HEADERS,
    "Prefer": "return=representation",
}

# Automated senders to exclude from conversation counts
AUTOMATED_SENDERS = {"Alphie", "Alpha Anywhere Bot"}

# Only these agents appear in Team stats/insights
TEAM_AGENTS = {"Mary Betz", "Nene Addico", "David Ionescu"}

# Non-parent email domains / senders — NOT real conversations
JUNK_EMAIL_PATTERNS = [
    "intercom.io",                       # Bot/operator tickets (320 of them)
    "support@2hr-learning-support.kayako.com",
    "no-reply@zoom.us",
    "noreply@zoom.us",
    "no-reply@kayako.com",
    "mailer-daemon",
    "noreply@",
    "no-reply@",
    "notifications@",
    "postmaster@",
]

# Subject patterns that indicate ads, system notifications, or non-parent messages
JUNK_SUBJECT_PATTERNS = [
    r"^\d{10,}$",               # Pure numeric IDs like 215473472240938
    r"^undeliverable:",
    r"^out of office",
    r"^automatic reply",
    r"^auto-reply",
    r"delivery status notification",
    r"mailer.daemon",
    r"^unsubscribe",
]

# Business hours for response time calculation (EST)
BIZ_START_HOUR = 6   # 6am EST
BIZ_END_HOUR = 18    # 6pm EST


# ═══ CATEGORIZATION PROMPT ═══

CATEGORIZATION_SYSTEM = """You are a support conversation categorizer for Alpha Anywhere, an online K-12 education platform (2hourlearning.com). Your job is to read a parent/family support conversation and classify it accurately.

## CRITICAL RULES

1. READ THE FULL CONVERSATION before categorizing. The first message is often vague.
2. Categorize based on INTENT (what the parent actually needs) not KEYWORDS.
3. A conversation can have up to 2 categories: a PRIMARY (the core reason they reached out) and an optional SECONDARY (if a meaningfully different issue also comes up).
4. If you're unsure between two categories, choose the one that would determine which team member or resource should handle it.

## MACRO CATEGORIES (10 domains)

1. Curriculum & Learning - Questions about lessons, coursework, apps/software used for learning (StudyReel, etc.), bracketing/placement levels, subject content
2. Platform & Technical - Dashboard/portal bugs or confusion about UI elements, login/credential issues, outages, technical errors, device compatibility
3. Testing & Assessment - MAP Growth testing, MAP Diagnostic/Screener, test scheduling, test results interpretation, testing technical issues
4. Progress & Reporting - Student progress questions, mastery metrics, ring/level explanations when asking "how is my child doing", reports, transcripts
5. Coaching & Scheduling - Booking coaching/tutoring sessions, rescheduling calls, tutor assignments, coaching content questions
6. Enrollment & Onboarding - New family setup, first-week questions, program information inquiries, pricing questions from prospective families
7. Account & Billing - Billing, subscription changes, cancellation requests, refund requests, payment issues
8. Funding & Transfers - ESA/state funding questions, Alpha School transfers, high school program inquiries, international inquiries
9. Follow-ups - Parent checking back on a previously raised issue ("any update?", "is this resolved?", "still waiting")
10. Rewards - Alpha Rewards program, points, tiers, redemption
11. General & Other - Positive feedback/thank-yous with no actionable issue, general inquiries that don't fit any category above, miscellaneous messages

## SUB-CATEGORIES (assign one)

Under Curriculum & Learning: Lessons & Coursework, Apps & Software, Bracketing & Placement
Under Platform & Technical: Dash & Portal, Login & Credentials, Outages & Downtime
Under Testing & Assessment: MAP Diagnostic/Screener, MAP Growth Testing
Under Progress & Reporting: Student Progress, Reports & Transcripts
Under Coaching & Scheduling: Coaching & Tutoring, Scheduling & Calls
Under Enrollment & Onboarding: Onboarding, New Inquiry & Program Info, Pricing
Under Account & Billing: Billing & Subscription, Cancellation, Refunds & Credits
Under Funding & Transfers: High School Inquiry, Alpha School Transfer, ESA & Funding, International
Under Follow-ups: Follow-up & Status
Under Rewards: Alpha Rewards
Under General & Other: Positive Feedback, General Inquiry, Misc

## DISAMBIGUATION RULES

### Dashboard mentions
- Parent says "the dashboard shows X about my child's progress" -> Progress & Reporting > Student Progress (asking about CHILD, not PLATFORM)
- Parent says "the dashboard won't load / looks wrong / I can't find X" -> Platform & Technical > Dash & Portal (PLATFORM issue)
- Parent says "what do the rings/colors on the dashboard mean?" -> Platform & Technical > Dash & Portal (UNDERSTANDING THE UI)

### MAP/Testing mentions
- Parent asks about MAP scores in context of "why is my child placed at this level" -> Curriculum & Learning > Bracketing & Placement (PLACEMENT, not test)
- Parent asks "when is the next MAP test" or "my child's MAP test froze" -> Testing & Assessment (TEST itself)
- Parent asks "what do my child's MAP scores mean for their progress" -> Progress & Reporting > Student Progress (PROGRESS)

### Cancellation mentions
- Parent says "I want to cancel" or "how do I cancel" -> Account & Billing > Cancellation
- Parent says "I want to cancel the MAP test" or "cancel my coaching session" -> Testing or Coaching respectively (canceling a SPECIFIC THING)

### Login mentions
- Parent can't log in to the main platform -> Platform & Technical > Login & Credentials
- Parent can't log in to StudyReel or a specific learning app -> Curriculum & Learning > Apps & Software (LEARNING TOOL access)
- New family can't log in during first week -> Enrollment & Onboarding > Onboarding (ONBOARDING issue)

### Follow-up detection
- If the parent's ENTIRE message is checking status on a prior issue -> Follow-ups > Follow-up & Status
- If the parent follows up BUT adds new information or a new question -> categorize based on the NEW content

### Billing vs Pricing
- Existing family asking about charges, payments, plan changes -> Account & Billing > Billing & Subscription
- Prospective family asking "how much does it cost" -> Enrollment & Onboarding > Pricing

### Transfer vs Cancellation
- Family leaving Alpha Anywhere for Alpha School -> Funding & Transfers > Alpha School Transfer (POSITIVE movement)
- Family leaving entirely -> Account & Billing > Cancellation

## FEW-SHOT EXAMPLES

### Example 1: Dashboard + Progress
Conversation: "Hi, I noticed on the dashboard that my daughter's math ring went from green to yellow. Is she falling behind?"
CORRECT: Primary: Progress & Reporting > Student Progress
WRONG: Platform & Technical > Dash & Portal
WHY: Parent is asking about PERFORMANCE. Dashboard is just where they saw it.

### Example 2: MAP + Placement
Conversation: "My son just took the MAP screener and got placed in Level 2 math. He was doing Level 4 at his old school."
CORRECT: Primary: Curriculum & Learning > Bracketing & Placement
WHY: Core concern is PLACEMENT LEVEL, not the test itself.

### Example 3: Multi-issue
Conversation: "Two things - first, we can't get StudyReel to load on our iPad. Second, when is the next MAP Growth test?"
CORRECT: Primary: Curriculum & Learning > Apps & Software, Secondary: Testing & Assessment > MAP Growth Testing

### Example 4: New family login
Conversation: "We just signed up yesterday and the login from the welcome email isn't working."
CORRECT: Primary: Enrollment & Onboarding > Onboarding
WHY: BRAND NEW family. This is onboarding, not a platform bug.

### Example 5: Follow-up with new info
Conversation: "Following up on my ticket from last week about the billing error. Also, can we switch to annual billing?"
CORRECT: Primary: Account & Billing > Billing & Subscription
WHY: Has a NEW request (switching to annual), not just checking status.

### Example 6: Cancellation of a specific thing
Conversation: "I need to cancel my daughter's coaching session for Thursday."
CORRECT: Primary: Coaching & Scheduling > Scheduling & Calls
WHY: Rescheduling an APPOINTMENT, not canceling their account.

## OUTPUT FORMAT

Respond with ONLY valid JSON. No markdown, no explanation, no preamble.

{
  "primary_macro": "exact macro category name",
  "primary_sub": "exact sub-category name",
  "secondary_macro": "exact macro category name or null",
  "secondary_sub": "exact sub-category name or null",
  "confidence": "high|medium|low",
  "reasoning": "1 sentence explaining why"
}"""


# ═══ SUPABASE HELPERS ═══

def sb_url(table):
    return f"{SUPABASE_URL}/rest/v1/{table}"


def sb_select(table, params=None):
    """Read from Supabase."""
    r = requests.get(sb_url(table), headers=HEADERS, params=params or {})
    r.raise_for_status()
    return r.json()


def sb_upsert(table, data, on_conflict=None):
    """Insert or update rows in Supabase."""
    headers = {**HEADERS_RETURN, "Prefer": "return=minimal,resolution=merge-duplicates"}
    url = sb_url(table)
    if on_conflict:
        url += f"?on_conflict={on_conflict}"
    r = requests.post(url, headers=headers, json=data)
    if r.status_code not in (200, 201, 204):
        print(f"  ERROR writing to {table}: {r.status_code} {r.text[:300]}")
        return False
    return True


def sb_insert(table, data):
    """Insert rows (no upsert)."""
    r = requests.post(sb_url(table), headers=HEADERS, json=data)
    if r.status_code not in (200, 201, 204):
        print(f"  ERROR inserting to {table}: {r.status_code} {r.text[:300]}")
        return False
    return True


def sb_update(table, match_params, data):
    """Update rows matching params."""
    url = sb_url(table)
    params = {f"{k}": f"eq.{v}" for k, v in match_params.items()}
    r = requests.patch(url, headers=HEADERS, params=params, json=data)
    if r.status_code not in (200, 204):
        print(f"  ERROR updating {table}: {r.status_code} {r.text[:300]}")
        return False
    return True


# ═══ CSV PARSING ═══

def parse_csv(filepath):
    """Read Intercom CSV export and return list of conversation dicts."""
    rows = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    print(f"  Read {len(rows)} rows from CSV")
    return rows


def is_real_conversation(row):
    """Filter out bot-only threads, test messages, internal tickets, ads, and automation noise."""
    conv = row.get("full_conversation", "")
    subject = row.get("subject", "").lower()
    user_email = row.get("user_email", "").lower().strip()

    # Skip if no real content (lowered from 20 to 10)
    if len(conv.strip()) < 10:
        return False

    # Skip Breakthrough Coaching automation alerts
    if "breakthrough coaching" in subject:
        return False

    # Skip junk senders — automated systems, bots, not real parents
    if user_email:
        for junk in JUNK_EMAIL_PATTERNS:
            if junk in user_email:
                return False

    # Skip junk subjects — numeric IDs, auto-replies, delivery failures
    for pattern in JUNK_SUBJECT_PATTERNS:
        if re.search(pattern, subject):
            return False

    # Skip internal tickets / admin-only threads where all messages are "None"
    if all(re.search(r":\s*None\s*$", line.strip()) or not line.strip()
           for line in conv.split("\n")):
        return False

    # Skip bot-only conversations (only automated senders)
    # Use the timestamp-aware pattern to extract real sender names
    ts_sender_pattern = re.compile(r"^\[.*?\]\s+(.+?)\s*(?:\(admin\)|\(bot\))?:\s")
    senders = set()
    has_bot_only = True
    for line in conv.split("\n"):
        m = ts_sender_pattern.match(line.strip())
        if m:
            sender = m.group(1).strip()
            if sender:
                senders.add(sender)
                # Check if this line is NOT a bot/automated sender
                if "(bot)" not in line and sender not in AUTOMATED_SENDERS:
                    has_bot_only = False

    # If we found senders and ALL are bots/automated, skip
    if senders and has_bot_only:
        return False

    return True


def assign_week_label(date_str):
    """Convert a date string to a week label like 'Jan 12' (Monday of that week)."""
    try:
        # Strip timezone offset if present (e.g., +00:00)
        cleaned = date_str.strip()
        if "+" in cleaned and cleaned.index("+") > 10:
            cleaned = cleaned[:cleaned.rindex("+")]
        elif cleaned.endswith("Z"):
            cleaned = cleaned[:-1]

        # Try common Intercom date formats
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d",
                     "%m/%d/%Y %H:%M", "%m/%d/%Y"]:
            try:
                dt = datetime.strptime(cleaned, fmt)
                break
            except ValueError:
                continue
        else:
            return "Unknown"

        # Find the Monday of that week
        monday = dt - timedelta(days=dt.weekday())
        # Use %d and lstrip("0") for cross-platform compat (%-d fails on Windows)
        return f"{monday.strftime('%b')} {monday.day}"
    except Exception:
        return "Unknown"


def extract_agent(row):
    """Extract the first human agent (admin) who replied, from the conversation text."""
    conv = row.get("full_conversation", "")

    # Look for lines like: [2026-03-13 16:22:50] Mary Betz (admin): ... or (admin|comment):
    admin_pattern = re.compile(r"\[.*?\]\s+(.+?)\s+\(admin(?:\|[^)]*)?\):")
    for match in admin_pattern.finditer(conv):
        name = match.group(1).strip()
        if name and name not in AUTOMATED_SENDERS:
            return name

    return "Unassigned"


def business_hours_between(start, end):
    """Calculate minutes between two datetimes counting ONLY business hours.

    Business hours: 6:00am–6:00pm EST, Monday–Friday.
    Timestamps from Intercom are assumed to be in UTC; we convert to EST (UTC-5).
    """
    EST_OFFSET = timedelta(hours=-5)
    start_est = start + EST_OFFSET
    end_est = end + EST_OFFSET

    total_minutes = 0.0
    current = start_est

    while current < end_est:
        # Skip weekends (Mon=0, Sun=6)
        if current.weekday() >= 5:
            # Jump to next Monday 6am
            days_until_monday = 7 - current.weekday()
            current = current.replace(hour=BIZ_START_HOUR, minute=0, second=0) + timedelta(days=days_until_monday)
            continue

        # Before business hours -> jump to start of business
        if current.hour < BIZ_START_HOUR:
            current = current.replace(hour=BIZ_START_HOUR, minute=0, second=0)
            continue

        # After business hours -> jump to next day start
        if current.hour >= BIZ_END_HOUR:
            current = (current + timedelta(days=1)).replace(hour=BIZ_START_HOUR, minute=0, second=0)
            continue

        # We're in business hours — count until end of this biz day or end time
        biz_end_today = current.replace(hour=BIZ_END_HOUR, minute=0, second=0)
        chunk_end = min(end_est, biz_end_today)
        delta = (chunk_end - current).total_seconds() / 60.0
        if delta > 0:
            total_minutes += delta

        # Move past this chunk
        if chunk_end >= biz_end_today:
            current = (current + timedelta(days=1)).replace(hour=BIZ_START_HOUR, minute=0, second=0)
        else:
            break

    return round(total_minutes, 1)


def extract_first_response_minutes(row):
    """Calculate business-hours minutes from first customer message to first admin reply.

    Only counts time within 6am–6pm EST, Monday–Friday.

    Parses timestamps from conversation lines like:
      [2026-03-13 15:36:45] Sadie Young: ...
      [2026-03-13 16:22:50] Mary Betz (admin): ...

    Returns minutes as float, or None if no admin reply found.
    """
    conv = row.get("full_conversation", "")
    lines = conv.split("\n")

    ts_pattern = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+(.+?):\s")
    admin_pattern = re.compile(r"\(admin(?:\|[^)]*)?\)")  # matches (admin) or (admin|comment)
    bot_pattern = re.compile(r"\(bot(?:\|[^)]*)?\)")      # matches (bot) or (bot|assignment)

    first_customer_ts = None
    first_admin_ts = None

    for line in lines:
        m = ts_pattern.match(line.strip())
        if not m:
            continue

        ts_str = m.group(1)
        sender_part = m.group(2).strip()

        try:
            ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        # Skip bots
        if bot_pattern.search(sender_part):
            continue

        if admin_pattern.search(sender_part):
            # First admin reply
            if first_admin_ts is None:
                first_admin_ts = ts
                break  # We only need the first one
        else:
            # Customer message
            if first_customer_ts is None:
                first_customer_ts = ts

    if first_customer_ts and first_admin_ts and first_admin_ts > first_customer_ts:
        minutes = business_hours_between(first_customer_ts, first_admin_ts)
        # Discard sub-1-minute RTs — these are auto-assignments, bot handoffs,
        # or forwarded conversations, not real human response times.
        if minutes < 1:
            return None
        return minutes

    return None


def extract_exchanges(row):
    """Parse all real message exchanges from a conversation.

    Returns dict with:
      parent_messages  — count of real parent/user messages
      team_replies     — count of real admin replies
      exchange_count   — number of user→admin paired exchanges
      all_response_times — list of RT in minutes for each exchange
      exchanges        — list of dicts with user_ts, admin_ts, admin_name, rt_minutes
    """
    conv = row.get("full_conversation", "")
    lines = conv.split("\n")

    # Regex handles both old and new formats:
    #   Old: [ts] Author (admin): body    or   [ts] Author: body (no tag)
    #   New: [ts] Author (admin|comment): body
    ts_pattern = re.compile(
        r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+(.+?)(?:\s+\(([^)]*)\))?:\s*(.*)"
    )

    # Collect classified messages in order
    messages = []
    for line in lines:
        line = line.strip()
        m = ts_pattern.match(line)
        if not m:
            continue

        ts_str, sender, type_info, body_start = m.group(1), m.group(2).strip(), m.group(3), m.group(4)

        try:
            ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        # Determine body emptiness (body_start is just the first line after colon)
        body_text = body_start.strip() if body_start else ""
        if not body_text or body_text == "None":
            continue

        type_info = type_info or ""

        if "|" in type_info:
            # New format: (author_type|part_type)
            author_type, part_type = type_info.split("|", 1)
            # Only comment and source are real messages
            if part_type not in ("comment", "source"):
                continue
            if author_type == "bot":
                continue
            if author_type == "admin":
                if sender in AUTOMATED_SENDERS:
                    continue
                messages.append({"ts": ts, "role": "admin", "sender": sender})
            else:
                messages.append({"ts": ts, "role": "user", "sender": sender})
        else:
            # Old format: (admin), (bot), or no tag
            if "bot" in type_info:
                continue
            if "admin" in type_info:
                if sender in AUTOMATED_SENDERS:
                    continue
                messages.append({"ts": ts, "role": "admin", "sender": sender})
            else:
                messages.append({"ts": ts, "role": "user", "sender": sender})

    # State machine to pair exchanges
    parent_messages = 0
    team_replies = 0
    exchanges = []
    waiting_for_admin = False
    user_ts = None

    for msg in messages:
        if msg["role"] == "user":
            parent_messages += 1
            if not waiting_for_admin:
                user_ts = msg["ts"]
                waiting_for_admin = True
            # If already waiting, keep original user_ts (RT from when they started waiting)
        elif msg["role"] == "admin":
            team_replies += 1
            if waiting_for_admin and user_ts is not None:
                rt = business_hours_between(user_ts, msg["ts"])
                if rt >= 1:  # Same threshold as first-response
                    exchanges.append({
                        "user_ts": user_ts.isoformat(),
                        "admin_ts": msg["ts"].isoformat(),
                        "admin_name": msg["sender"],
                        "rt_minutes": rt,
                    })
                waiting_for_admin = False
                user_ts = None

    return {
        "parent_messages": parent_messages,
        "team_replies": team_replies,
        "exchange_count": len(exchanges),
        "all_response_times": [e["rt_minutes"] for e in exchanges],
        "exchanges": exchanges,
    }


# ═══ CONVERSATION CLEANING ═══

def clean_conversation(raw_text):
    """Strip HTML, remove None messages, email signatures, and image tags.
    Returns clean plain text that Sonnet can categorize accurately."""

    lines = raw_text.split("\n")
    cleaned_lines = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Skip lines that are just "None" bot/admin actions
        # Match patterns like "[timestamp] Sender (role): None"
        if re.search(r":\s*None\s*$", stripped):
            continue

        # Strip all HTML tags
        text = re.sub(r"<[^>]+>", " ", stripped)

        # Decode HTML entities (&amp; -> &, etc.)
        text = html.unescape(text)

        # Remove image URLs and tracking links
        text = re.sub(r"https?://\S+\.(png|jpg|jpeg|gif|svg|ico)\S*", "", text)
        text = re.sub(r"https?://ci\d+\.googleusercontent\.com/\S+", "", text)

        # Remove email signature markers and common signature content
        if text.strip() == "--":
            break  # Everything after -- is signature

        # Collapse multiple spaces and clean up
        text = re.sub(r"\s+", " ", text).strip()

        # Skip if nothing meaningful left after cleaning
        if len(text) < 3:
            continue

        cleaned_lines.append(text)

    result = "\n".join(cleaned_lines)

    # Final cleanup: collapse multiple blank lines
    result = re.sub(r"\n{3,}", "\n\n", result)

    return result.strip()


# ═══ CATEGORIZATION ═══

def categorize_conversation(client, conversation_text, retries=5):
    """Call Sonnet API to categorize a single conversation. Handles 529 overloaded with backoff."""
    # Truncate very long conversations to save tokens
    if len(conversation_text) > 3000:
        conversation_text = conversation_text[:1500] + "\n...[truncated]...\n" + conversation_text[-1500:]

    for attempt in range(retries + 1):
        try:
            response = client.messages.create(
                model=CATEGORIZATION_MODEL,
                max_tokens=200,
                system=[{
                    "type": "text",
                    "text": CATEGORIZATION_SYSTEM,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{
                    "role": "user",
                    "content": f"Categorize this support conversation:\n\n{conversation_text}",
                }],
            )

            # Track cache usage across the full run
            usage = response.usage
            cache_read = getattr(usage, "cache_read_input_tokens", 0)
            cache_create = getattr(usage, "cache_creation_input_tokens", 0)
            if not hasattr(categorize_conversation, "_cache_stats"):
                categorize_conversation._cache_stats = {"reads": 0, "creates": 0, "calls": 0}
            categorize_conversation._cache_stats["calls"] += 1
            categorize_conversation._cache_stats["reads"] += cache_read
            categorize_conversation._cache_stats["creates"] += cache_create
            # Log first call (to confirm caching works) and every 50th
            n = categorize_conversation._cache_stats["calls"]
            if n == 1 or n == 2:
                print(f"    Cache call #{n}: create={cache_create} read={cache_read} input={usage.input_tokens} output={usage.output_tokens}")
            elif n % 50 == 0:
                s = categorize_conversation._cache_stats
                print(f"    Cache after {n} calls: total_read={s['reads']} total_create={s['creates']}")

            text = response.content[0].text.strip()
            # Clean up any markdown formatting
            text = re.sub(r"^```json\s*", "", text)
            text = re.sub(r"\s*```$", "", text)

            result = json.loads(text)

            # Validate required fields
            if "primary_macro" not in result or "primary_sub" not in result:
                raise ValueError("Missing required fields in response")

            return result

        except anthropic.APIStatusError as e:
            # 529 Overloaded — exponential backoff
            if e.status_code == 529:
                wait = min(2 ** attempt * 5, 120)  # 5s, 10s, 20s, 40s, 80s, 120s
                if attempt < retries:
                    print(f"    API overloaded (529), waiting {wait}s before retry {attempt + 1}...")
                    time.sleep(wait)
                    continue
            if attempt < retries:
                print(f"    Retry {attempt + 1}: {e}")
                time.sleep(3)
            else:
                return {
                    "primary_macro": "UNCATEGORIZED",
                    "primary_sub": "UNCATEGORIZED",
                    "secondary_macro": None,
                    "secondary_sub": None,
                    "confidence": "failed",
                    "reasoning": f"API error after {retries + 1} attempts: {str(e)[:100]}",
                }

        except (json.JSONDecodeError, ValueError) as e:
            if attempt < retries:
                print(f"    Retry {attempt + 1}: {e}")
                time.sleep(2)
            else:
                return {
                    "primary_macro": "UNCATEGORIZED",
                    "primary_sub": "UNCATEGORIZED",
                    "secondary_macro": None,
                    "secondary_sub": None,
                    "confidence": "failed",
                    "reasoning": f"Parse error after {retries + 1} attempts: {str(e)[:100]}",
                }


def categorize_all(conversations):
    """Categorize all conversations via Sonnet API.

    - Resumes from where it left off (checks Supabase for already-categorized IDs)
    - Saves to Supabase every 50 conversations so progress survives crashes
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    total = len(conversations)

    # Check what's already categorized in Supabase (resume support)
    existing = sb_select("categorizations", {"select": "conversation_id"})
    already_done = {str(r["conversation_id"]) for r in existing} if existing else set()

    if already_done:
        print(f"\n  Found {len(already_done)} already categorized — resuming from where we left off")

    to_categorize = [(i, c) for i, c in enumerate(conversations) if str(c.get("id", "")) not in already_done]
    skipped = total - len(to_categorize)

    print(f"\n  Categorizing {len(to_categorize)} conversations via Haiku (skipping {skipped} already done)...")
    print(f"  (Estimated cost: ~${len(to_categorize) * 0.003:.2f})")

    results = []
    batch_buffer = []  # Buffer for Supabase writes

    for idx, (i, convo) in enumerate(to_categorize):
        conv_text = convo.get("full_conversation", "")

        # Clean HTML, remove None messages, strip signatures
        conv_text = clean_conversation(conv_text)

        if not conv_text.strip():
            result = {
                "conversation_id": convo["id"],
                "primary_macro": "UNCATEGORIZED",
                "primary_sub": "UNCATEGORIZED",
                "confidence": "failed",
                "reasoning": "Empty conversation after cleaning HTML/None messages",
            }
        else:
            result = categorize_conversation(client, conv_text)
            result["conversation_id"] = convo["id"]

        results.append(result)
        batch_buffer.append({
            "conversation_id": result["conversation_id"],
            "primary_macro": result.get("primary_macro", "UNCATEGORIZED"),
            "primary_sub": result.get("primary_sub", "UNCATEGORIZED"),
            "secondary_macro": result.get("secondary_macro"),
            "secondary_sub": result.get("secondary_sub"),
            "confidence": result.get("confidence", "medium"),
            "reasoning": result.get("reasoning", ""),
        })

        # Save to Supabase every 50 conversations (crash-safe)
        if len(batch_buffer) >= 50:
            sb_upsert("categorizations", batch_buffer)
            print(f"    [SAVED] {len(batch_buffer)} categorizations to Supabase")
            batch_buffer = []

        # Progress indicator
        done = idx + 1 + skipped
        if (idx + 1) % 10 == 0 or idx == len(to_categorize) - 1:
            pct = round(done / total * 100)
            failed = sum(1 for r in results if r.get("confidence") == "failed")
            low = sum(1 for r in results if r.get("confidence") == "low")
            print(f"    [{pct:3d}%] {done}/{total} done | {failed} failed | {low} low confidence")

        # Small delay to avoid rate limits
        if (idx + 1) % 5 == 0:
            time.sleep(0.5)

    # Flush remaining buffer
    if batch_buffer:
        sb_upsert("categorizations", batch_buffer)
        print(f"    [SAVED] {len(batch_buffer)} categorizations to Supabase")

    # Print final cache summary
    if hasattr(categorize_conversation, "_cache_stats"):
        s = categorize_conversation._cache_stats
        print(f"\n  CACHE SUMMARY: {s['calls']} API calls | {s['reads']:,} tokens read from cache | {s['creates']:,} tokens created")
        if s['reads'] > 0:
            print(f"  Caching is WORKING -- saved ~90% on cached system prompt tokens")
        else:
            print(f"  WARNING: No cache reads detected -- caching may not be working")

    # Also load back the previously-done categorizations for downstream use
    if already_done:
        all_cats = sb_select("categorizations", {"select": "*"})
        return all_cats if all_cats else results
    return results


# ═══ STATS AGGREGATION ═══

def compute_weekly_stats(conversations, categorizations):
    """Aggregate conversations into weekly stats."""
    weeks = defaultdict(lambda: {
        "conversations": [],
        "agents": defaultdict(lambda: {
            "conversations": 0, "messages": 0, "response_times": [],
            "reply_count": 0, "exchange_rts": [],
        }),
        "categories": defaultdict(int),
    })

    # Index categorizations by conversation_id
    cat_by_id = {c["conversation_id"]: c for c in categorizations}

    for convo in conversations:
        week = convo.get("_week_label", "Unknown")
        weeks[week]["conversations"].append(convo)

        # Agent stats — only track team members (Mary, Nene, David)
        agent = convo.get("_agent", "Unassigned")
        if agent not in TEAM_AGENTS:
            continue
        weeks[week]["agents"][agent]["conversations"] += 1

        # First-response time (existing metric)
        resp_min = convo.get("_response_minutes")
        if resp_min is not None:
            weeks[week]["agents"][agent]["response_times"].append(resp_min)

        # Exchange-level stats: attribute each reply to the week it HAPPENED in,
        # not the week the conversation was created. A reply on March 31 goes into
        # the "Mar 30" week even if the conversation started in "Mar 23".
        exch = convo.get("_exchanges", {})
        for ex in exch.get("exchanges", []):
            ex_agent = ex.get("admin_name", "")
            if ex_agent not in TEAM_AGENTS:
                continue
            # Determine the week this reply belongs to (by admin reply timestamp)
            reply_week = assign_week_label(ex["admin_ts"])
            if reply_week not in weeks:
                # Initialize the week entry if it doesn't exist yet
                weeks[reply_week]["conversations"]  # triggers defaultdict
            weeks[reply_week]["agents"][ex_agent]["exchange_rts"].append(ex["rt_minutes"])
            weeks[reply_week]["agents"][ex_agent]["reply_count"] += 1

        # Use actual message_count from CSV
        msg_count = convo.get("message_count", "")
        try:
            msg_count = int(msg_count)
        except (ValueError, TypeError):
            conv_text = convo.get("full_conversation", "")
            msg_count = len(re.findall(r"^\[\d{4}-\d{2}-\d{2}", conv_text, re.MULTILINE))
            msg_count = max(1, msg_count)
        weeks[week]["agents"][agent]["messages"] += msg_count

        # Category stats
        cat = cat_by_id.get(convo["id"])
        if cat and cat.get("primary_macro") != "UNCATEGORIZED":
            weeks[week]["categories"][cat["primary_macro"]] += 1
            if cat.get("secondary_macro"):
                weeks[week]["categories"][cat["secondary_macro"]] += 1

    # Build weekly_stats rows
    stats_rows = []
    for week_label, data in sorted(weeks.items()):
        convos = data["conversations"]
        total = len(convos)

        # Agent stats as JSON
        agent_stats = []
        for agent_name, agent_data in data["agents"].items():
            rt = agent_data["response_times"]
            entry = {
                "name": agent_name,
                "conversations": agent_data["conversations"],
                "messages": agent_data["messages"],
            }
            if rt:
                sorted_rt = sorted(rt)
                n = len(sorted_rt)
                entry["median_response_min"] = round(median(sorted_rt), 1)
                entry["mean_response_min"] = round(sum(sorted_rt) / n, 1)
                entry["p25"] = round(sorted_rt[max(0, int(n * 0.25) - 1)], 1) if n >= 2 else round(sorted_rt[0], 1)
                entry["p75"] = round(sorted_rt[min(n - 1, int(n * 0.75))], 1) if n >= 2 else round(sorted_rt[0], 1)
                entry["p90"] = round(sorted_rt[min(n - 1, int(n * 0.90))], 1) if n >= 2 else round(sorted_rt[0], 1)
                entry["response_count"] = n

            # Exchange-level RT stats (all replies, not just first response)
            ex_rts = agent_data["exchange_rts"]
            entry["reply_count"] = agent_data["reply_count"]
            if ex_rts:
                sorted_ex = sorted(ex_rts)
                ne = len(sorted_ex)
                entry["exchange_median_rt"] = round(median(sorted_ex), 1)
                entry["exchange_mean_rt"] = round(sum(sorted_ex) / ne, 1)
                entry["exchange_p90_rt"] = round(sorted_ex[min(ne - 1, int(ne * 0.90))], 1) if ne >= 2 else round(sorted_ex[0], 1)

            agent_stats.append(entry)

        # Determine the top volume driver category for this week
        sorted_cats = sorted(data["categories"].items(), key=lambda x: -x[1])
        volume_driver = None
        if sorted_cats:
            top_cat, top_count = sorted_cats[0]
            top_pct = round(top_count / total * 100) if total > 0 else 0
            second_info = ""
            if len(sorted_cats) > 1:
                s_cat, s_count = sorted_cats[1]
                s_pct = round(s_count / total * 100) if total > 0 else 0
                second_info = f", followed by {s_cat} ({s_count}, {s_pct}%)"
            volume_driver = f"{top_cat} ({top_count}, {top_pct}%){second_info}"

        stats_rows.append({
            "week_label": week_label,
            "total_conversations": total,
            "total_messages": sum(a["messages"] for a in agent_stats),
            "agent_stats": json.dumps(agent_stats),
            "category_breakdown": json.dumps(dict(data["categories"])),
            "volume_driver": volume_driver,
            "is_partial_week": total < 30,  # heuristic for partial weeks
            "computed_at": datetime.utcnow().isoformat(),
        })

    return stats_rows


def compute_hourly_patterns(conversations):
    """Compute inbound volume by hour of day."""
    hourly = defaultdict(int)

    for convo in conversations:
        date_str = convo.get("created_at", "")
        try:
            cleaned = date_str.strip()
            if "+" in cleaned and cleaned.index("+") > 10:
                cleaned = cleaned[:cleaned.rindex("+")]
            elif cleaned.endswith("Z"):
                cleaned = cleaned[:-1]
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %H:%M"]:
                try:
                    dt = datetime.strptime(cleaned, fmt)
                    hourly[dt.hour] += 1
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    rows = []
    for hour in range(5, 22):  # 5am to 9pm
        label = f"{hour}am" if hour < 12 else f"{12 if hour == 12 else hour - 12}pm"
        rows.append({
            "week_label": "all",  # aggregate across all weeks
            "hour_label": label,
            "hour_num": hour,
            "inbound_count": hourly.get(hour, 0),
        })

    return rows


# ═══ INSIGHTS GENERATION ═══

INSIGHTS_SYSTEM = """You are a VP of Customer Experience and Chief of Staff analyst. You are analyzing Intercom conversation data for a company called Alpha Anywhere (also known as 2hr Learning), which is a parent-facing education support operation.

Team members to analyze (ignore all others entirely):
* David
* Nene
* Mary

Time calculation rule: When calculating any median response time or time-based metric, only count time that falls within 6:00am to 6:00pm EST, Monday through Friday. Do not count evenings, weekends, or holidays in any time delta calculations.

Exclusion rule: Ignore all "Breakthrough Coaching Alerts" assigned to or sent by David. These are fully automated notifications -- David does not personally act on them or assign them to others. Including them would skew his metrics unfairly.

Your task: Analyze the data provided and produce executive-level insights designed specifically for CEO and COO decision-making.

## MANDATORY ANALYSIS AREAS

### 1. TEAM SLA PERFORMANCE REVIEW (24 business-hour SLA)
For EACH team member (Mary, Nene, David), provide a frank performance assessment:
- Their median RT vs the 24hr SLA. Are they meeting it? By how much margin?
- Week-over-week RT trend — improving, degrading, or stable?
- Their conversation volume share — are they carrying their weight?
- Their message volume — are they writing thorough responses or just quick replies?
- Specific praise or concern with data. "Nene averaged X min RT across Y conversations" not vague platitudes.
- If someone is struggling, say so directly with the numbers. If someone is excelling, celebrate with specifics.

### 2. TEAM CAPACITY & VOLUME GROWTH
- Is volume growing faster than the team can handle? Compare week-over-week volume growth rate vs team capacity.
- Project forward: at current growth rate, when does the team hit capacity?
- Which agent is closest to being overloaded? Which has headroom?

### 3. CATEGORY & VOLUME DRIVERS
- For each week, identify what drove volume — was it a cohort start, MAP testing, or an organic spike?
- Which categories are growing fastest? Any emerging problems?

### 4. OPERATIONAL RECOMMENDATIONS
- Specific, assignable actions for Monday morning.
- Staffing recommendations if warranted by the data.

Write with the clarity and confidence of a senior executive presenting to a board. Be direct. Do not hedge unnecessarily. Back every claim with a number from the data.

## VOLUME DRIVERS — use these to explain what drove volume up or down

Known cohort start dates (new families onboard, expect Enrollment & Onboarding spikes 0-2 weeks after):
- Jan 5, Jan 26, Feb 16, Mar 9, Mar 30, Apr 20, May 11, Jun 1, Jun 22, Jul 13, Aug 3, Aug 24, Sep 14, Oct 5, Oct 26, Nov 16, Dec 7
(Every 3 weeks starting Jan 5, 2026)

MAP Growth testing windows (expect Testing & Assessment spikes during these weeks):
- Feb 2 – Feb 15 (spring window)

When you see volume spikes, ALWAYS check if they align with a cohort start or MAP testing window and call that out explicitly. If volume rises outside these windows, flag it as an anomaly worth investigating.

## OUTPUT FORMAT

Output ONLY valid JSON:
{
  "signals": [
    {
      "severity": "critical|warning|info|positive",
      "icon": "single emoji",
      "title": "short punchy title (max 10 words)",
      "body": "2-3 sentences. Reference specific numbers. Explain the 'so what' — what does this mean for ops capacity, parent experience, or team load. No generic observations.",
      "actions": ["specific, assignable action with owner or team implied"],
      "priority": "P0|P1|P2|P3"
    }
  ],
  "noise": [
    {
      "icon": "single emoji",
      "title": "short title",
      "body": "1-2 sentences explaining why this metric looks off but is actually fine. Cite the structural reason."
    }
  ],
  "actions": [
    {
      "priority": "P0|P1|P2",
      "label": "crisp action label (imperative verb)",
      "why": "1 sentence: what changes if this ships vs. doesn't"
    }
  ]
}

## QUALITY BAR

- Signals: 8-12 items. MUST include at least one signal per team member with specific performance data. Lead with severity.
- Every signal must have a "so what" — if a leadership team read this, they should know what to do next.
- MUST include a signal about volume growth rate vs team capacity.
- MUST include a signal with an overall team performance review against the 24hr SLA.
- Noise: 2-4 items. Preempt the wrong conclusions. If a number looks bad, explain the structural reason.
- Actions: 5-8 items. Each action should be specific enough to assign to someone Monday morning. "Improve response time" is bad. "Pre-build MAP Growth FAQ and deploy via Atlas before May window" is good.
- Reference actual numbers from the data. Don't say "increased" — say "went from X to Y, a Z% change."
- Name specific weeks when citing trends. "Volume spiked w/c Feb 16" not "volume spiked recently."
- Name specific categories and sub-categories. "Curriculum & Learning > Bracketing & Placement drove 40% of the Feb 16 spike" not "category X went up."
- Name specific agents when relevant. "Mary's median RT went from 45min to 112min w/c Mar 9" not "response times increased."
- Always explain WHY. Correlate against cohort starts, MAP windows, or call out that it's unexplained.
- If a week was tough, break down exactly what made it tough: which categories surged, which agents were overloaded, how RT shifted.
- If something went well, celebrate it with specifics.
- Think about what a board-level CX review would focus on: unit economics of support, deflection opportunities, capacity planning, and quality signals.
- Do NOT include any signals, noise, or actions about "Unassigned" conversations — those are tickets routed to other teams and are not relevant to this team's performance.
- Do NOT write generic platitudes like "the team is doing a great job." Every statement needs data backing it.
"""


def generate_insights(weekly_stats, categorizations):
    """Generate deep insights via Sonnet API with full week-by-week data."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    all_weeks = sorted(weekly_stats, key=lambda x: x.get("week_label", ""))
    if not all_weeks:
        print("  No weekly data to generate insights from.")
        return []

    current = all_weeks[-1]

    # ── Cohort & event calendar ──
    cohort_dates = []
    d = datetime(2026, 1, 5)
    while d.year <= 2027:
        cohort_dates.append(d.strftime("%b %d"))
        d += timedelta(weeks=3)
    map_windows = "Feb 2 – Feb 15 (spring window)"

    # ── Full week-by-week breakdown ──
    week_details = []
    for w in all_weeks:
        cats = w.get("category_breakdown", "{}")
        if isinstance(cats, str):
            cats = json.loads(cats)
        agents = w.get("agent_stats", "[]")
        if isinstance(agents, str):
            agents = json.loads(agents)

        # Per-agent summary for this week
        agent_lines = []
        for a in agents:
            rt_str = f", median RT: {a['median_response_min']}min biz-hrs" if a.get("median_response_min") is not None else ""
            agent_lines.append(f"    {a['name']}: {a.get('conversations', 0)} convos, {a.get('messages', 0)} msgs{rt_str}")

        # Top categories this week sorted by volume
        sorted_cats = sorted(cats.items(), key=lambda x: -x[1])
        cat_lines = [f"    {name}: {count}" for name, count in sorted_cats[:6]]

        week_details.append(
            f"Week of {w.get('week_label', '?')} — {w.get('total_conversations', 0)} conversations, {w.get('total_messages', 0)} messages"
            + (f" [PARTIAL WEEK]" if w.get("is_partial_week") else "")
            + f"\n  Categories:\n" + "\n".join(cat_lines)
            + f"\n  Agents:\n" + "\n".join(agent_lines)
        )

    # ── Overall category distribution with sub-categories ──
    macro_counts = defaultdict(int)
    sub_counts = defaultdict(lambda: defaultdict(int))
    for cat in categorizations:
        pm = cat.get("primary_macro")
        ps = cat.get("primary_sub", "Other")
        if pm and pm != "UNCATEGORIZED":
            macro_counts[pm] += 1
            sub_counts[pm][ps] += 1

    category_detail = []
    for macro, count in sorted(macro_counts.items(), key=lambda x: -x[1]):
        subs = sorted(sub_counts[macro].items(), key=lambda x: -x[1])
        sub_str = ", ".join(f"{s}: {c}" for s, c in subs[:5])
        category_detail.append(f"  {macro}: {count} total → [{sub_str}]")

    # ── Week-over-week deltas ──
    wow_lines = []
    for i in range(1, len(all_weeks)):
        prev = all_weeks[i - 1]
        curr = all_weeks[i]
        pv = prev.get("total_conversations", 0)
        cv = curr.get("total_conversations", 0)
        delta = cv - pv
        pct = round(delta / pv * 100) if pv > 0 else 0
        arrow = "↑" if delta > 0 else "↓" if delta < 0 else "→"
        wow_lines.append(f"  {prev.get('week_label', '?')} → {curr.get('week_label', '?')}: {pv} → {cv} ({arrow}{abs(delta)}, {pct:+d}%)")

    context = f"""
═══ FULL PERIOD DATA ({len(all_weeks)} weeks, {len(categorizations)} conversations) ═══

WEEK-BY-WEEK BREAKDOWN (every week, every category, every agent):
{"".join(chr(10) + wd for wd in week_details)}

WEEK-OVER-WEEK VOLUME CHANGES:
{"".join(chr(10) + wl for wl in wow_lines)}

CATEGORY DISTRIBUTION WITH SUB-CATEGORIES (full period):
{chr(10).join(category_detail)}

CONFIDENCE: {sum(1 for c in categorizations if c.get('confidence') == 'high')} high, {sum(1 for c in categorizations if c.get('confidence') == 'medium')} medium, {sum(1 for c in categorizations if c.get('confidence') == 'low')} low, {sum(1 for c in categorizations if c.get('confidence') == 'failed')} failed

═══ KNOWN EVENTS CALENDAR (correlate volume against these) ═══
Cohort start dates (new families onboard → expect Enrollment & Onboarding spikes 0-2 weeks after):
  {', '.join(cohort_dates[:12])}

MAP Growth testing window (expect Testing & Assessment spikes):
  {map_windows}

═══ KEY QUESTIONS YOU MUST ANSWER ═══
1. What drove volume each week? Was it a cohort start, MAP testing, or something else?
2. Which weeks were the hardest and why? What category surged? Did any agent get disproportionately loaded?
3. Which categories are growing week-over-week vs shrinking? Any emerging trends?
4. Where are the response time wins and losses? Which agent improved, which got slower, and why?
5. What went well this period that leadership should celebrate?
6. What structural problems are building that will get worse if not addressed?
"""

    print(f"\n  Generating insights across {len(all_weeks)} weeks...")
    print(f"  Context size: {len(context)} chars")

    try:
        response = client.messages.create(
            model=INSIGHTS_MODEL,
            max_tokens=8192,
            system=[{
                "type": "text",
                "text": INSIGHTS_SYSTEM,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{
                "role": "user",
                "content": f"Analyze this full PX operations dataset and generate insights:\n{context}",
            }],
        )

        text = response.content[0].text.strip()
        text = re.sub(r"^```json\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        data = json.loads(text)

        # Convert to database rows
        rows = []
        week = current.get("week_label", "Unknown")

        for signal in data.get("signals", []):
            rows.append({
                "week_label": week,
                "insight_type": "signal",
                "severity": signal.get("severity", "info"),
                "icon": signal.get("icon", ""),
                "title": signal["title"],
                "body": signal["body"],
                "actions": json.dumps(signal.get("actions", [])),
                "priority": signal.get("priority"),
                "status": "draft",
            })

        for noise in data.get("noise", []):
            rows.append({
                "week_label": week,
                "insight_type": "noise",
                "severity": "info",
                "icon": noise.get("icon", ""),
                "title": noise["title"],
                "body": noise["body"],
                "actions": json.dumps([]),
                "priority": None,
                "status": "draft",
            })

        for action in data.get("actions", []):
            rows.append({
                "week_label": week,
                "insight_type": "action",
                "severity": "info",
                "icon": "",
                "title": action["label"],
                "body": action["why"],
                "actions": json.dumps([]),
                "priority": action.get("priority"),
                "status": "draft",
            })

        print(f"  Generated {len(rows)} insight items (all as DRAFT)")
        return rows

    except Exception as e:
        print(f"  ERROR generating insights: {e}")
        return []


# ═══ REVIEW FLOW ═══

def review_insights():
    """Interactive review of draft insights."""
    drafts = sb_select("weekly_insights", {
        "status": "eq.draft",
        "order": "week_label.desc,insight_type,priority",
    })

    if not drafts:
        print("\n  No draft insights to review.")
        return

    print(f"\n  Found {len(drafts)} draft insights:\n")

    for i, insight in enumerate(drafts):
        print(f"  [{i + 1}] [{insight.get('insight_type', '').upper()}] "
              f"[{insight.get('severity', '')}] "
              f"{insight.get('priority', '  ')} "
              f"{insight.get('icon', '')} {insight['title']}")
        print(f"      {insight['body'][:120]}...")
        print()

    print("  Options:")
    print("    'a'     - Approve ALL drafts (publish them)")
    print("    '1,3,5' - Approve specific items by number")
    print("    'r 2'   - Reject item 2")
    print("    'q'     - Quit without changes")

    choice = input("\n  > ").strip().lower()

    if choice == "q":
        print("  No changes made.")
        return

    if choice == "a":
        for insight in drafts:
            sb_update("weekly_insights",
                      {"id": str(insight["id"])},
                      {"status": "published", "reviewed_at": datetime.utcnow().isoformat()})
        print(f"  Published all {len(drafts)} insights.")
        return

    if choice.startswith("r "):
        try:
            idx = int(choice[2:]) - 1
            insight = drafts[idx]
            sb_update("weekly_insights",
                      {"id": str(insight["id"])},
                      {"status": "rejected", "reviewed_at": datetime.utcnow().isoformat()})
            print(f"  Rejected: {insight['title']}")
        except (ValueError, IndexError):
            print("  Invalid selection.")
        return

    # Approve specific items
    try:
        indices = [int(x.strip()) - 1 for x in choice.split(",")]
        for idx in indices:
            insight = drafts[idx]
            sb_update("weekly_insights",
                      {"id": str(insight["id"])},
                      {"status": "published", "reviewed_at": datetime.utcnow().isoformat()})
            print(f"  Published: {insight['title']}")
    except (ValueError, IndexError):
        print("  Invalid selection.")


def publish_all_insights():
    """Publish all draft insights without review."""
    drafts = sb_select("weekly_insights", {"status": "eq.draft"})
    if not drafts:
        print("\n  No draft insights to publish.")
        return

    for insight in drafts:
        sb_update("weekly_insights",
                  {"id": str(insight["id"])},
                  {"status": "published", "reviewed_at": datetime.utcnow().isoformat()})

    print(f"\n  Published {len(drafts)} insights.")


# ═══ MAIN PIPELINE ═══

def run_pipeline(csv_path, skip_categorize=False, skip_insights=False):
    """Full pipeline: CSV -> categorize -> aggregate -> write to Supabase."""

    print("\n" + "=" * 55)
    print("  PX DASHBOARD PIPELINE")
    print("=" * 55)

    # ── Step 1: Parse CSV ──
    print("\n[1/6] Reading CSV...")
    raw_rows = parse_csv(csv_path)

    # ── Step 2: Filter real conversations ──
    print("\n[2/6] Filtering real conversations...")
    START_DATE = datetime(2026, 2, 16)  # Only process from Feb 16 onwards
    conversations = []
    skipped_before_start = 0
    for row in raw_rows:
        if is_real_conversation(row):
            # Skip conversations before Jan 5
            date_str = row.get("created_at", "").strip()
            # Strip timezone offset if present
            cleaned_date = date_str
            if "+" in cleaned_date and cleaned_date.index("+") > 10:
                cleaned_date = cleaned_date[:cleaned_date.rindex("+")]
            elif cleaned_date.endswith("Z"):
                cleaned_date = cleaned_date[:-1]
            try:
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d",
                             "%m/%d/%Y %H:%M", "%m/%d/%Y"]:
                    try:
                        dt = datetime.strptime(cleaned_date, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    dt = None
                if dt and dt < START_DATE:
                    skipped_before_start += 1
                    continue
            except Exception:
                pass

            row["_week_label"] = assign_week_label(row.get("created_at", ""))
            # Use CSV admin_name as primary agent source; fall back to text parsing
            csv_admin = row.get("admin_name", "").strip()
            row["_agent"] = csv_admin if csv_admin else extract_agent(row)
            row["_response_minutes"] = extract_first_response_minutes(row)
            row["_exchanges"] = extract_exchanges(row)
            conversations.append(row)

    filtered_out = len(raw_rows) - len(conversations) - skipped_before_start
    print(f"  {len(conversations)} real conversations ({filtered_out} filtered out, {skipped_before_start} before Jan 5)")

    # ── Step 3: Write conversations to Supabase ──
    print("\n[3/6] Writing conversations to Supabase...")
    conv_rows = []
    for c in conversations:
        # Use CSV message_count if available, otherwise count timestamped lines
        msg_count = c.get("message_count", "")
        try:
            msg_count = int(msg_count)
        except (ValueError, TypeError):
            # Fallback: count timestamped message lines in conversation
            conv_text = c.get("full_conversation", "")
            msg_count = len(re.findall(r"^\[\d{4}-\d{2}-\d{2}", conv_text, re.MULTILINE))
            msg_count = max(1, msg_count)

        conv_rows.append({
            "id": c.get("id", str(hash(c.get("full_conversation", "")[:100]))),
            "created_at": c.get("created_at"),
            "user_name": c.get("user_name", ""),
            "user_email": c.get("user_email", ""),
            "assigned_agent": c.get("_agent", ""),
            "state": c.get("state", ""),
            "subject": c.get("subject", ""),
            "full_conversation": c.get("full_conversation", ""),
            "source_channel": c.get("inbox_name", c.get("source_channel", c.get("channel", ""))),
            "week_label": c.get("_week_label", ""),
            "message_count": msg_count,
            "first_response_min": c.get("_response_minutes"),
        })

    # Batch insert in chunks of 50
    for i in range(0, len(conv_rows), 50):
        batch = conv_rows[i:i + 50]
        sb_upsert("conversations", batch, on_conflict="id")
        print(f"  Wrote conversations {i + 1}-{min(i + 50, len(conv_rows))}")

    # ── Step 4: Categorize ──
    if skip_categorize:
        print("\n[4/6] Skipping categorization (--skip-categorize flag)")
        # Load existing categorizations from Supabase
        categorizations = sb_select("categorizations", {"order": "categorized_at.desc"})
        print(f"  Loaded {len(categorizations)} existing categorizations from Supabase")
    else:
        print("\n[4/6] Categorizing conversations...")
        categorizations = categorize_all(conversations)
        # categorize_all now saves to Supabase in batches as it goes (crash-safe)

        # Print categorization summary
        print("\n  Categorization Summary:")
        macro_counts = defaultdict(int)
        for cat in categorizations:
            macro_counts[cat.get("primary_macro", "UNCATEGORIZED")] += 1
        for macro, count in sorted(macro_counts.items(), key=lambda x: -x[1]):
            pct = round(count / len(categorizations) * 100)
            print(f"    {macro}: {count} ({pct}%)")

        confidence_counts = defaultdict(int)
        for cat in categorizations:
            confidence_counts[cat.get("confidence", "unknown")] += 1
        print(f"\n  Confidence: {dict(confidence_counts)}")

    # ── Step 5: Aggregate weekly stats ──
    print("\n[5/6] Computing weekly stats...")
    weekly_stats = compute_weekly_stats(conversations, categorizations)
    hourly = compute_hourly_patterns(conversations)

    sb_upsert("weekly_stats", weekly_stats, on_conflict="week_label")
    print(f"  Wrote {len(weekly_stats)} weekly stat rows")

    sb_upsert("hourly_patterns", hourly, on_conflict="week_label,hour_num")
    print(f"  Wrote {len(hourly)} hourly pattern rows")

    # ── Step 6: Generate insights ──
    if skip_insights:
        print("\n[6/6] Skipping insights (--skip-insights flag)")
        insights = []
    else:
        print("\n[6/6] Generating insights...")
        insights = generate_insights(weekly_stats, categorizations)

        if insights:
            sb_insert("weekly_insights", insights)
            print(f"  Wrote {len(insights)} insight drafts")
            print("\n  Run 'python pipeline.py --review-insights' to review and publish them.")

    # ── Done ──
    print("\n" + "=" * 55)
    print("  PIPELINE COMPLETE")
    print("=" * 55)
    print(f"  Conversations processed: {len(conversations)}")
    print(f"  Weekly stat rows: {len(weekly_stats)}")
    print(f"  Insight drafts: {len(insights)}")
    print(f"  Dashboard will show published insights only.")
    print(f"  Review drafts: python pipeline.py --review-insights")
    print("=" * 55 + "\n")


# ═══ CLI ═══

if __name__ == "__main__":
    if not ANTHROPIC_API_KEY:
        print("ERROR: Set ANTHROPIC_API_KEY in your .env file")
        sys.exit(1)
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY in your .env file")
        sys.exit(1)

    args = sys.argv[1:]

    if "--review-insights" in args:
        review_insights()
    elif "--publish-insights" in args:
        publish_all_insights()
    elif len(args) >= 1 and not args[0].startswith("--"):
        csv_path = args[0]
        if not os.path.exists(csv_path):
            print(f"ERROR: File not found: {csv_path}")
            sys.exit(1)
        skip = "--skip-categorize" in args
        skip_ins = "--skip-insights" in args
        run_pipeline(csv_path, skip_categorize=skip, skip_insights=skip_ins)
    else:
        print("Usage:")
        print("  python pipeline.py <csv_file>              Full pipeline run")
        print("  python pipeline.py <csv_file> --skip-categorize  Skip API categorization")
        print("  python pipeline.py --review-insights       Review and publish draft insights")
        print("  python pipeline.py --publish-insights      Auto-publish all drafts")
