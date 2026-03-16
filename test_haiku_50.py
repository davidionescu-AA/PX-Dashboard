"""
Test: Load Feb 16+ conversations into Supabase, categorize 50 with Haiku, print results.
"""
import csv
import json
import os
import re
import time
from datetime import datetime
from collections import Counter
from dotenv import load_dotenv
import anthropic
import requests

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), override=True)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}
HEADERS_UPSERT = {
    **HEADERS,
    "Prefer": "return=minimal,resolution=merge-duplicates",
}

CSV_PATH = "intercom_conversations_68days_20260313.csv"
START_DATE = datetime(2026, 2, 16)
HAIKU_MODEL = "claude-haiku-4-5-20251001"
TEST_LIMIT = 50

# Import from pipeline
from pipeline import (CATEGORIZATION_SYSTEM, clean_conversation,
                      is_real_conversation, extract_agent,
                      extract_first_response_minutes, assign_week_label)


def sb_upsert(table, data):
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS_UPSERT, json=data)
    if r.status_code not in (200, 201, 204):
        print(f"  ERROR {table}: {r.status_code} {r.text[:200]}")
        return False
    return True


def parse_date(date_str):
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y"]:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def categorize_one(client, conv_text, retries=3):
    if len(conv_text) > 3000:
        conv_text = conv_text[:1500] + "\n...[truncated]...\n" + conv_text[-1500:]

    for attempt in range(retries + 1):
        try:
            response = client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=200,
                system=[{
                    "type": "text",
                    "text": CATEGORIZATION_SYSTEM,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": f"Categorize this support conversation:\n\n{conv_text}"}],
            )
            usage = response.usage
            cr = getattr(usage, "cache_read_input_tokens", 0)
            cc = getattr(usage, "cache_creation_input_tokens", 0)

            text = response.content[0].text.strip()
            text = re.sub(r"^```json\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
            result = json.loads(text)

            if "primary_macro" not in result:
                raise ValueError("Missing primary_macro")

            return result, cr, cc

        except anthropic.APIStatusError as e:
            if e.status_code == 529 and attempt < retries:
                wait = min(2 ** attempt * 5, 60)
                print(f"    529 overloaded, waiting {wait}s...")
                time.sleep(wait)
                continue
            if attempt >= retries:
                return {"primary_macro": "UNCATEGORIZED", "primary_sub": "UNCATEGORIZED",
                        "confidence": "failed", "reasoning": str(e)[:100]}, 0, 0

        except (json.JSONDecodeError, ValueError) as e:
            if attempt < retries:
                time.sleep(1)
                continue
            return {"primary_macro": "UNCATEGORIZED", "primary_sub": "UNCATEGORIZED",
                    "confidence": "failed", "reasoning": str(e)[:100]}, 0, 0


def main():
    print("\n" + "=" * 60)
    print("  HAIKU TEST — Feb 16+ conversations, 50 sample")
    print("=" * 60)

    # 1. Read CSV & filter
    print("\n[1] Reading CSV and filtering to Feb 16+...")
    rows = []
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    print(f"  Total CSV rows: {len(rows)}")

    clean = []
    for row in rows:
        if not is_real_conversation(row):
            continue
        dt = parse_date(row.get("created_at", ""))
        if not dt or dt < START_DATE:
            continue
        row["_week_label"] = assign_week_label(row.get("created_at", ""))
        row["_agent"] = extract_agent(row)
        row["_response_minutes"] = extract_first_response_minutes(row)
        clean.append(row)

    print(f"  After cleaning + Feb 16 filter: {len(clean)} conversations")

    # 2. Write ALL clean conversations to Supabase
    print(f"\n[2] Writing {len(clean)} conversations to Supabase...")
    conv_rows = []
    for c in clean:
        conv_rows.append({
            "id": c.get("id", str(hash(c.get("full_conversation", "")[:100]))),
            "created_at": c.get("created_at"),
            "user_name": c.get("user_name", ""),
            "user_email": c.get("user_email", ""),
            "assigned_agent": c.get("_agent", ""),
            "state": c.get("state", ""),
            "subject": c.get("subject", ""),
            "full_conversation": c.get("full_conversation", ""),
            "source_channel": c.get("inbox_name", c.get("source_channel", "")),
            "week_label": c.get("_week_label", ""),
        })
    for i in range(0, len(conv_rows), 50):
        batch = conv_rows[i:i + 50]
        sb_upsert("conversations", batch)
    print(f"  Done — {len(conv_rows)} conversations in Supabase")

    # 3. Categorize first 50 with Haiku
    print(f"\n[3] Categorizing first {TEST_LIMIT} with Haiku...")
    print(f"  Model: {HAIKU_MODEL}")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    sample = clean[:TEST_LIMIT]

    total_cache_read = 0
    total_cache_create = 0
    results = []
    cat_rows = []

    for idx, convo in enumerate(sample):
        raw = convo.get("full_conversation", "")
        cleaned = clean_conversation(raw)
        if not cleaned.strip():
            result = {"primary_macro": "UNCATEGORIZED", "primary_sub": "UNCATEGORIZED",
                      "confidence": "failed", "reasoning": "Empty after cleaning"}
            cr, cc = 0, 0
        else:
            result, cr, cc = categorize_one(client, cleaned)

        total_cache_read += cr
        total_cache_create += cc
        result["conversation_id"] = convo.get("id", "")
        results.append(result)

        cat_rows.append({
            "conversation_id": result["conversation_id"],
            "primary_macro": result.get("primary_macro", "UNCATEGORIZED"),
            "primary_sub": result.get("primary_sub", "UNCATEGORIZED"),
            "secondary_macro": result.get("secondary_macro"),
            "secondary_sub": result.get("secondary_sub"),
            "confidence": result.get("confidence", "medium"),
            "reasoning": result.get("reasoning", ""),
        })

        # Print each result with preview
        subject = convo.get("subject", "")[:60].encode("ascii", "replace").decode()
        preview = cleaned[:200].replace("\n", " ").encode("ascii", "replace").decode()
        pm = result.get("primary_macro", "?")
        ps = result.get("primary_sub", "?")
        sm = result.get("secondary_macro")
        conf = result.get("confidence", "?")

        print(f"\n--- [{idx+1}/{TEST_LIMIT}] ID: {convo.get('id','')} ---")
        print(f"  Subject: {subject}")
        print(f"  Preview: {preview}")
        print(f"  >> {pm} > {ps} (conf: {conf})")
        if sm:
            print(f"  >> Secondary: {sm} > {result.get('secondary_sub', '?')}")
        reasoning = result.get('reasoning', 'n/a')
        if reasoning:
            reasoning = reasoning.encode("ascii", "replace").decode()
        print(f"  Reasoning: {reasoning}")

        if (idx + 1) % 5 == 0:
            time.sleep(0.3)

    # 4. Save categorizations to Supabase
    print(f"\n[4] Saving {len(cat_rows)} categorizations to Supabase...")
    sb_upsert("categorizations", cat_rows)
    print("  Done")

    # 5. Summary
    print(f"\n{'=' * 60}")
    print(f"  SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Total clean convos (Feb 16+): {len(clean)}")
    print(f"  Categorized: {len(results)}")

    macro_counts = Counter(r.get("primary_macro") for r in results)
    conf_counts = Counter(r.get("confidence") for r in results)

    print(f"\n  Category breakdown:")
    for cat, count in macro_counts.most_common():
        print(f"    {cat}: {count}")

    print(f"\n  Confidence: {dict(conf_counts)}")
    print(f"  Cache: {total_cache_read:,} tokens read, {total_cache_create:,} tokens created")
    if total_cache_read > 0:
        print(f"  Caching is WORKING")
    else:
        print(f"  WARNING: No cache reads — check if Haiku supports prompt caching")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
