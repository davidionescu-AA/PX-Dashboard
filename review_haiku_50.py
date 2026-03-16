"""
Review Haiku results: show conversation snippets alongside categorizations.
"""
import csv
import json
import os
import re
import sys
import time

import anthropic
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"), override=True)

from pipeline import (
    CATEGORIZATION_SYSTEM,
    JUNK_EMAIL_PATTERNS,
    JUNK_SUBJECT_PATTERNS,
    is_real_conversation,
    clean_conversation,
)
from datetime import datetime

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
HAIKU_MODEL = "claude-haiku-4-5-20251001"
START_DATE = datetime(2026, 1, 5)
CSV_FILE = "intercom_conversations_68days_20260313.csv"


def load_clean_conversations():
    rows = []
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    clean = []
    for row in rows:
        created = row.get("created_at", "")
        try:
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y"]:
                try:
                    dt = datetime.strptime(created.strip(), fmt)
                    break
                except ValueError:
                    continue
            else:
                continue
            if dt < START_DATE:
                continue
        except Exception:
            continue

        if is_real_conversation(row):
            clean.append(row)

    return clean


def categorize_one(client, conversation_text):
    if len(conversation_text) > 3000:
        conversation_text = conversation_text[:1500] + "\n...[truncated]...\n" + conversation_text[-1500:]

    for attempt in range(3):
        try:
            response = client.messages.create(
                model=HAIKU_MODEL,
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

            text = response.content[0].text.strip()
            text = re.sub(r"^```json\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
            return json.loads(text)

        except Exception as e:
            if attempt < 2:
                time.sleep(2)
            else:
                return {"primary_macro": "UNCATEGORIZED", "primary_sub": "UNCATEGORIZED", "confidence": "failed"}

    return {"primary_macro": "UNCATEGORIZED", "primary_sub": "UNCATEGORIZED", "confidence": "failed"}


def safe_print(text):
    print(text.encode("ascii", "replace").decode())


def main():
    conversations = load_clean_conversations()
    test_set = conversations[:50]

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    print("=" * 100)
    print("HAIKU CATEGORIZATION REVIEW — 50 conversations with content snippets")
    print("=" * 100)

    for i, convo in enumerate(test_set):
        conv_text = clean_conversation(convo.get("full_conversation", ""))
        if not conv_text.strip():
            continue

        result = categorize_one(client, conv_text)

        macro = result.get("primary_macro", "?")
        sub = result.get("primary_sub", "?")
        conf = result.get("confidence", "?")
        sec = result.get("secondary_macro", "")
        reasoning = result.get("reasoning", "")
        subj = convo.get("subject", "")[:60]
        email = convo.get("user_email", "")[:35]

        # Get first 250 chars of cleaned conversation as snippet
        snippet = conv_text[:250].replace("\n", " | ")

        print(f"\n{'-' * 100}")
        safe_print(f"  #{i+1}  From: {email}  Subject: {subj}")
        safe_print(f"  CATEGORY: {macro} > {sub}" + (f" + {sec}" if sec else "") + f"  [{conf}]")
        if reasoning:
            safe_print(f"  REASONING: {reasoning[:120]}")
        safe_print(f"  SNIPPET: {snippet}...")

        if (i + 1) % 5 == 0:
            time.sleep(0.3)

    print(f"\n{'=' * 100}")
    print("END OF REVIEW")


if __name__ == "__main__":
    main()
