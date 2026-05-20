"""
Generate sample employee productivity data with embedded outliers and bulk-index it.
Normal employees: hours 35-45, tickets 5-15, commits 2-10, meetings 3-8
Outliers injected manually to give ML something obvious to find.
"""
import json
import random
import ssl
import urllib.request
import urllib.error
import os
import sys
from pathlib import Path
from datetime import date, timedelta


def _parse_kv_file(path):
    """Yield (key, value) pairs from a KEY=VALUE file, skipping comments and blanks."""
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                yield key.strip(), value.strip().strip("'\"")
    except FileNotFoundError:
        pass


def load_config():
    """
    Load credentials into os.environ from the repo root, walking up from this script.

    Resolution order (first wins for each key):
      1. Existing shell environment variables
      2. .env  — general config and Cloud API key
      3. .elastic-credentials — Elasticsearch endpoint and API key
    """
    here = Path(__file__).resolve().parent
    root = next(
        (d for d in [here, *here.parents] if (d / ".env").exists() or (d / ".elastic-credentials").exists()),
        here,
    )
    for key, value in _parse_kv_file(root / ".env"):
        os.environ.setdefault(key, value)
    # .elastic-credentials sections contain plain KEY=VALUE lines between headers
    for key, value in _parse_kv_file(root / ".elastic-credentials"):
        os.environ.setdefault(key, value)


load_config()

# Certificate verification disabled — works on all platforms without configuration
SSL_CONTEXT = ssl._create_unverified_context()

ES_URL = os.environ["ELASTICSEARCH_URL"]
API_KEY = os.environ["ELASTICSEARCH_API_KEY"]
INDEX = "employee-metrics"

random.seed(42)

departments = ["engineering", "sales", "support", "marketing"]

def normal_record(emp_id, dept, day):
    return {
        "employee_id": f"emp_{emp_id:03d}",
        "department": dept,
        "hours_worked": round(random.uniform(35, 45), 1),
        "tickets_closed": round(random.uniform(5, 15), 1),
        "commits": round(random.uniform(2, 10), 1),
        "meetings": round(random.uniform(3, 8), 1),
        "date": day.isoformat(),
    }

records = []
base = date(2025, 1, 1)

# 100 normal employees over 30 days
for emp_id in range(1, 101):
    dept = departments[emp_id % len(departments)]
    for d in range(30):
        records.append(normal_record(emp_id, dept, base + timedelta(days=d)))

# Outlier A: emp_901 — works extreme hours, almost no output
for d in range(30):
    records.append({
        "employee_id": "emp_901",
        "department": "engineering",
        "hours_worked": round(random.uniform(80, 95), 1),  # extreme
        "tickets_closed": round(random.uniform(0, 1), 1),  # near zero
        "commits": round(random.uniform(0, 1), 1),
        "meetings": round(random.uniform(0, 1), 1),
        "date": (base + timedelta(days=d)).isoformat(),
    })

# Outlier B: emp_902 — zero hours, very high tickets (data anomaly)
for d in range(30):
    records.append({
        "employee_id": "emp_902",
        "department": "support",
        "hours_worked": round(random.uniform(0, 2), 1),
        "tickets_closed": round(random.uniform(80, 120), 1),  # extreme
        "commits": round(random.uniform(0, 1), 1),
        "meetings": round(random.uniform(20, 30), 1),         # extreme
        "date": (base + timedelta(days=d)).isoformat(),
    })

# Outlier C: emp_903 — all metrics zero (inactive)
for d in range(30):
    records.append({
        "employee_id": "emp_903",
        "department": "marketing",
        "hours_worked": 0.0,
        "tickets_closed": 0.0,
        "commits": 0.0,
        "meetings": 0.0,
        "date": (base + timedelta(days=d)).isoformat(),
    })

print(f"Total records to index: {len(records)}")

# Bulk index
def bulk_index(batch):
    body = ""
    for doc in batch:
        body += json.dumps({"index": {"_index": INDEX}}) + "\n"
        body += json.dumps(doc) + "\n"

    req = urllib.request.Request(
        f"{ES_URL}/_bulk",
        data=body.encode(),
        headers={
            "Authorization": f"ApiKey {API_KEY}",
            "Content-Type": "application/x-ndjson",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, context=SSL_CONTEXT) as resp:
        result = json.loads(resp.read())
        if result.get("errors"):
            errs = [i for i in result["items"] if i.get("index", {}).get("error")]
            print(f"  Bulk errors: {len(errs)}", file=sys.stderr)

BATCH = 500
for i in range(0, len(records), BATCH):
    bulk_index(records[i:i+BATCH])
    print(f"  Indexed {min(i+BATCH, len(records))}/{len(records)}")

print("Done.")
