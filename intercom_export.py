"""
═══════════════════════════════════════════════════════════════
INTERCOM CONVERSATION EXPORTER
═══════════════════════════════════════════════════════════════
Pulls conversations from Intercom API and saves as CSV.
Designed to feed directly into pipeline.py.

Usage:
  python intercom_export.py                    # Last 7 days (default)
  python intercom_export.py 60                 # Last 60 days
  python intercom_export.py 60 --run-pipeline  # Export + auto-run pipeline
  python intercom_export.py --discover         # Dump raw API fields from 1 convo

Reads INTERCOM_API_KEY from .env (no interactive prompt needed).
Saves CSV to this folder, ready for pipeline.py.
"""

import csv
import json
import os
import sys
import time
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

# Load .env from the same folder as this script
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), override=True)

INTERCOM_API_KEY = os.getenv("INTERCOM_API_KEY")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
API_VERSION = "2.11"

HEADERS = {
    "Authorization": f"Bearer {INTERCOM_API_KEY}",
    "Content-Type": "application/json",
    "Intercom-Version": API_VERSION,
}

# Known team inbox mappings (from Intercom Teams API)
INBOX_MAP = {
    8854449: "Customers (VirtualSupport)",
    8880501: "Prospects (AlphaAnywhere)",
    9764963: "Students",
}

# Admin ID → name cache (populated at runtime)
ADMIN_CACHE = {}


def fetch_conversations(days_back):
    """Search Intercom for conversations created in the last N days."""
    cutoff = int((datetime.now() - timedelta(days=days_back)).timestamp())
    all_conversations = []
    starting_after = None
    page = 0

    while True:
        page += 1
        print(f"  Fetching page {page}...")

        search_body = {
            "query": {
                "operator": "AND",
                "value": [
                    {"field": "created_at", "operator": ">", "value": cutoff},
                    {"field": "source.author.type", "operator": "=", "value": "user"},
                ],
            },
        }

        if starting_after:
            search_body["pagination"] = {"starting_after": starting_after}
        else:
            search_body["pagination"] = {"per_page": 150}

        response = requests.post(
            "https://api.intercom.io/conversations/search",
            headers=HEADERS,
            json=search_body,
        )

        if response.status_code != 200:
            print(f"\n  ERROR: {response.status_code}")
            print(f"  {response.text[:500]}")
            sys.exit(1)

        data = response.json()
        batch = data.get("conversations", [])
        all_conversations.extend(batch)
        print(f"  Found {len(all_conversations)} conversations so far...")

        next_cursor = data.get("pages", {}).get("next", {}).get("starting_after")
        if next_cursor:
            starting_after = next_cursor
            time.sleep(0.5)
        else:
            break

    return all_conversations


def fetch_full_conversation(conv_id):
    """Fetch a single conversation with all message parts."""
    r = requests.get(
        f"https://api.intercom.io/conversations/{conv_id}",
        headers=HEADERS,
    )
    if r.status_code == 200:
        return r.json()
    return None


def fetch_admin_name(admin_id):
    """Resolve admin ID to name (cached)."""
    if not admin_id:
        return ""
    admin_id = str(admin_id)
    if admin_id in ADMIN_CACHE:
        return ADMIN_CACHE[admin_id]
    try:
        r = requests.get(f"https://api.intercom.io/admins/{admin_id}", headers=HEADERS)
        if r.status_code == 200:
            name = r.json().get("name", "")
            ADMIN_CACHE[admin_id] = name
            return name
    except Exception:
        pass
    ADMIN_CACHE[admin_id] = ""
    return ""


def extract_inbox_fields(conv):
    """Extract all inbox/channel/routing fields from a conversation object."""
    source = conv.get("source", {})
    team_id = conv.get("team_assignee_id")
    stats = conv.get("statistics", {})
    return {
        "delivered_as": source.get("delivered_as", ""),
        "source_type": source.get("type", ""),
        "team_assignee_id": team_id or "",
        "inbox_name": INBOX_MAP.get(team_id, "Unknown") if team_id else "Unassigned",
        "admin_assignee_id": conv.get("admin_assignee_id", ""),
        "time_to_admin_reply": stats.get("time_to_admin_reply", ""),
        "median_time_to_reply": stats.get("median_time_to_reply", ""),
        "first_close_at": stats.get("first_close_at", ""),
        "count_assignments": stats.get("count_assignments", ""),
        "tags": ",".join(t.get("name", "") for t in conv.get("tags", {}).get("tags", [])),
    }


def build_conversation_text(conv):
    """Build full conversation text from source + parts."""
    parts = []

    # Initial message
    if conv.get("source", {}).get("body"):
        author = (
            conv["source"].get("author", {}).get("name")
            or conv["source"].get("author", {}).get("email")
            or "User"
        )
        ts = datetime.fromtimestamp(conv.get("created_at", 0)).strftime("%Y-%m-%d %H:%M:%S")
        parts.append(f"[{ts}] {author} (user|source): {conv['source']['body']}")

    # All conversation parts (replies)
    for part in conv.get("conversation_parts", {}).get("conversation_parts", []):
        author_info = part.get("author", {})
        author_type = author_info.get("type", "unknown")
        author_name = (
            author_info.get("name")
            or author_info.get("email")
            or f"{author_type}_user"
        )
        ts = datetime.fromtimestamp(part.get("created_at", 0)).strftime("%Y-%m-%d %H:%M:%S")
        body = part.get("body", "")
        part_type = part.get("part_type", "unknown")
        parts.append(f"[{ts}] {author_name} ({author_type}|{part_type}): {body}")

    return "\n\n".join(parts)


def discover_fields():
    """Fetch one conversation and dump all available fields for inspection."""
    print("\n  Fetching 1 conversation to discover available fields...\n")

    cutoff = int((datetime.now() - timedelta(days=7)).timestamp())
    search_body = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "created_at", "operator": ">", "value": cutoff},
                {"field": "source.author.type", "operator": "=", "value": "user"},
            ],
        },
        "pagination": {"per_page": 1},
    }

    r = requests.post(
        "https://api.intercom.io/conversations/search",
        headers=HEADERS,
        json=search_body,
    )
    if r.status_code != 200:
        print(f"  ERROR: {r.status_code} - {r.text[:500]}")
        return

    convos = r.json().get("conversations", [])
    if not convos:
        print("  No conversations found in last 7 days.")
        return

    conv_id = convos[0]["id"]
    full = fetch_full_conversation(conv_id)
    if not full:
        print(f"  ERROR fetching full conversation {conv_id}")
        return

    # Print top-level keys
    print("  TOP-LEVEL KEYS:")
    for k, v in full.items():
        if k == "conversation_parts":
            print(f"    {k}: [... {len(v.get('conversation_parts', []))} parts]")
        elif isinstance(v, dict):
            print(f"    {k}: {json.dumps(v, indent=6)[:300]}")
        elif isinstance(v, str) and len(v) > 200:
            print(f"    {k}: {v[:200]}...")
        else:
            print(f"    {k}: {v}")

    # Specifically look for inbox/email routing
    print("\n  SOURCE OBJECT:")
    for k, v in full.get("source", {}).items():
        if k == "body":
            print(f"    source.{k}: [{len(v)} chars]")
        else:
            print(f"    source.{k}: {v}")

    print(f"\n  team_assignee_id: {full.get('team_assignee_id')}")
    print(f"  admin_assignee_id: {full.get('admin_assignee_id')}")
    print(f"  channel_initiated_from: {full.get('channel_initiated_from')}")

    # Check custom attributes
    if full.get("custom_attributes"):
        print(f"\n  CUSTOM ATTRIBUTES: {json.dumps(full['custom_attributes'], indent=4)}")

    # Check tags
    tags = full.get("tags", {}).get("tags", [])
    if tags:
        print(f"\n  TAGS: {[t.get('name') for t in tags]}")

    # Save full JSON for inspection
    dump_path = os.path.join(SCRIPT_DIR, "_intercom_sample_conversation.json")
    with open(dump_path, "w", encoding="utf-8") as f:
        json.dump(full, f, indent=2, default=str)
    print(f"\n  Full JSON saved to: {dump_path}")
    print("  ^ Check this file for any inbox/email routing fields we might have missed.")


def export(days_back):
    """Main export: fetch all conversations, enrich, save CSV."""
    print(f"\n  Searching for conversations from the past {days_back} days...")
    conversations = fetch_conversations(days_back)

    if not conversations:
        print("\n  No conversations found!")
        return None

    print(f"\n  Processing {len(conversations)} conversations (fetching full details)...")

    csv_data = []
    for i, conv_summary in enumerate(conversations, 1):
        if i % 10 == 0 or i == 1:
            print(f"  Processing {i}/{len(conversations)}...")

        conv_id = conv_summary.get("id")
        conv = fetch_full_conversation(conv_id)
        if not conv:
            conv = conv_summary  # Fall back to summary if full fetch fails

        time.sleep(0.1)  # Rate limiting

        inbox = extract_inbox_fields(conv)
        full_text = build_conversation_text(conv)

        created_at = datetime.fromtimestamp(conv.get("created_at", 0)).isoformat()
        updated_at = (
            datetime.fromtimestamp(conv["updated_at"]).isoformat()
            if conv.get("updated_at")
            else ""
        )

        # Resolve admin name
        admin_id = inbox["admin_assignee_id"]
        admin_name = fetch_admin_name(admin_id)

        csv_data.append({
            "id": conv.get("id"),
            "created_at": created_at,
            "updated_at": updated_at,
            "state": conv.get("state", "unknown"),
            "user_name": conv.get("source", {}).get("author", {}).get("name", ""),
            "user_email": conv.get("source", {}).get("author", {}).get("email", ""),
            "subject": conv.get("title") or conv.get("source", {}).get("subject", ""),
            "message_count": len(conv.get("conversation_parts", {}).get("conversation_parts", [])) + 1,
            "delivered_as": inbox["delivered_as"],
            "source_type": inbox["source_type"],
            "team_assignee_id": inbox["team_assignee_id"],
            "inbox_name": inbox["inbox_name"],
            "admin_assignee_id": admin_id,
            "admin_name": admin_name,
            "time_to_admin_reply": inbox["time_to_admin_reply"],
            "median_time_to_reply": inbox["median_time_to_reply"],
            "tags": inbox["tags"],
            "full_conversation": full_text,
        })

    # Write CSV
    filename = f"intercom_conversations_{days_back}days_{datetime.now().strftime('%Y%m%d')}.csv"
    filepath = os.path.join(SCRIPT_DIR, filename)

    fieldnames = [
        "id", "created_at", "updated_at", "state", "user_name", "user_email",
        "subject", "message_count", "delivered_as", "source_type",
        "team_assignee_id", "inbox_name", "admin_assignee_id", "admin_name",
        "time_to_admin_reply", "median_time_to_reply", "tags", "full_conversation",
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_data)

    print(f"\n  [OK] Exported {len(csv_data)} conversations")
    print(f"  [OK] Saved to: {filepath}")
    return filepath


def main():
    if not INTERCOM_API_KEY:
        print("ERROR: Set INTERCOM_API_KEY in your .env file")
        print("  Add this line to .env: INTERCOM_API_KEY=your_token_here")
        sys.exit(1)

    print("\n" + "=" * 55)
    print("  INTERCOM CONVERSATION EXPORTER")
    print("=" * 55)

    args = sys.argv[1:]

    # --discover mode: dump raw fields from 1 conversation
    if "--discover" in args:
        discover_fields()
        return

    # Parse days_back (first non-flag argument)
    days_back = 7
    for arg in args:
        if not arg.startswith("--"):
            try:
                days_back = int(arg)
            except ValueError:
                pass

    run_pipeline = "--run-pipeline" in args

    csv_path = export(days_back)

    if csv_path and run_pipeline:
        print("\n" + "-" * 55)
        print("  AUTO-RUNNING PIPELINE...")
        print("-" * 55)
        import subprocess
        pipeline_path = os.path.join(SCRIPT_DIR, "pipeline.py")
        result = subprocess.run(
            [sys.executable, pipeline_path, csv_path],
            cwd=SCRIPT_DIR,
        )
        if result.returncode == 0:
            # Auto-publish insights
            subprocess.run(
                [sys.executable, pipeline_path, "--publish-insights"],
                cwd=SCRIPT_DIR,
            )
            print("\n  ✓ Pipeline complete. Dashboard data updated.")
        else:
            print(f"\n  ✗ Pipeline exited with code {result.returncode}")

    elif csv_path:
        print(f"\n  To run the pipeline on this export:")
        print(f"    python pipeline.py {os.path.basename(csv_path)}")

    print()


if __name__ == "__main__":
    main()
