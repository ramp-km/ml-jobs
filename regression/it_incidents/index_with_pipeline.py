"""
Index new IT incident documents into 'it-incidents-live'.

Because 'it-incidents-live' has the ingest pipeline set as its
default_pipeline, every document is automatically scored by the model
at index time — no separate inference call needed.

After indexing, the script queries the documents back and prints
the predicted resolution time alongside the input features.

Usage
-----
  # index and display all built-in sample incidents
  python3 index_with_pipeline.py

  # index a single incident from the CLI
  python3 index_with_pipeline.py \
      --incident-id INC9010 --category security --severity 5 \
      --affected 200 --team 4 --hour 3 --dow 6 --comments 45
"""
import argparse
import json
import ssl
import urllib.request
import urllib.error
import os
import sys
from pathlib import Path


def _parse_kv_file(path):
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
    here = Path(__file__).resolve().parent
    root = next(
        (d for d in [here, *here.parents] if (d / ".env").exists() or (d / ".elastic-credentials").exists()),
        here,
    )
    for key, value in _parse_kv_file(root / ".env"):
        os.environ.setdefault(key, value)
    for key, value in _parse_kv_file(root / ".elastic-credentials"):
        os.environ.setdefault(key, value)


load_config()

SSL_CONTEXT = ssl._create_unverified_context()
ES_URL      = os.environ["ELASTICSEARCH_URL"]
API_KEY     = os.environ["ELASTICSEARCH_API_KEY"]
LIVE_INDEX  = "it-incidents-live"


def request(method, path, body=None):
    url  = f"{ES_URL}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {
        "Authorization": f"ApiKey {API_KEY}",
        "Content-Type":  "application/json",
    }
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"HTTP {e.code} {method} {path}: {err}", file=sys.stderr)
        sys.exit(1)


def resolution_tier(minutes):
    if minutes < 60:   return "Quick      (< 1 h)"
    if minutes < 180:  return "Moderate   (1–3 h)"
    if minutes < 480:  return "Significant(3–8 h)"
    return                    "Critical   (> 8 h)"


# ── CLI ───────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Index incident docs through the ingest pipeline.")
parser.add_argument("--incident-id", default=None)
parser.add_argument("--category",    default="software",
                    choices=["network", "hardware", "software", "security", "user_access"])
parser.add_argument("--severity",    type=int, default=None)
parser.add_argument("--affected",    type=int, default=None)
parser.add_argument("--team",        type=int, default=None)
parser.add_argument("--hour",        type=int, default=None)
parser.add_argument("--dow",         type=int, default=None)
parser.add_argument("--comments",    type=int, default=None)
args = parser.parse_args()

if all(v is not None for v in [args.severity, args.affected, args.team, args.hour, args.dow, args.comments]):
    is_biz = 1 if (9 <= args.hour <= 17 and args.dow <= 4) else 0
    incidents = [{
        "incident_id":        args.incident_id or "INC_CLI",
        "category":           args.category,
        "severity":           args.severity,
        "num_affected_users": args.affected,
        "team_size":          args.team,
        "hour_of_day":        args.hour,
        "day_of_week":        args.dow,
        "is_business_hours":  is_biz,
        "num_comments":       args.comments,
    }]
else:
    incidents = [
        {"incident_id": "INC9001", "category": "user_access", "severity": 1, "num_affected_users": 2,   "team_size": 1, "hour_of_day": 10, "day_of_week": 2, "is_business_hours": 1, "num_comments": 4},
        {"incident_id": "INC9002", "category": "software",    "severity": 3, "num_affected_users": 80,  "team_size": 3, "hour_of_day": 14, "day_of_week": 1, "is_business_hours": 1, "num_comments": 18},
        {"incident_id": "INC9003", "category": "network",     "severity": 4, "num_affected_users": 250, "team_size": 5, "hour_of_day": 11, "day_of_week": 0, "is_business_hours": 1, "num_comments": 35},
        {"incident_id": "INC9004", "category": "security",    "severity": 5, "num_affected_users": 300, "team_size": 6, "hour_of_day": 2,  "day_of_week": 6, "is_business_hours": 0, "num_comments": 55},
        {"incident_id": "INC9005", "category": "hardware",    "severity": 4, "num_affected_users": 120, "team_size": 2, "hour_of_day": 23, "day_of_week": 3, "is_business_hours": 0, "num_comments": 22},
    ]

# ── Index via bulk (pipeline is applied automatically) ────────────────────────
print(f"Indexing {len(incidents)} incident(s) into '{LIVE_INDEX}' (pipeline auto-applies)...\n")

body = ""
for doc in incidents:
    body += json.dumps({"index": {"_index": LIVE_INDEX, "_id": doc["incident_id"]}}) + "\n"
    body += json.dumps(doc) + "\n"

req = urllib.request.Request(
    f"{ES_URL}/_bulk?refresh=wait_for",
    data=body.encode(),
    headers={
        "Authorization": f"ApiKey {API_KEY}",
        "Content-Type":  "application/x-ndjson",
    },
    method="POST",
)
with urllib.request.urlopen(req, context=SSL_CONTEXT) as resp:
    bulk_result = json.loads(resp.read())

if bulk_result.get("errors"):
    for item in bulk_result["items"]:
        err = item.get("index", {}).get("error")
        if err:
            print(f"  Error indexing {item['index']['_id']}: {err}", file=sys.stderr)
    sys.exit(1)

# ── Fetch the indexed documents back ─────────────────────────────────────────
ids    = [inc["incident_id"] for inc in incidents]
search = request("POST", f"/{LIVE_INDEX}/_search", {
    "size": len(ids),
    "query": {"ids": {"values": ids}},
    "sort": [{"incident_id": "asc"}],
})
hits = search["hits"]["hits"]

# ── Print results ─────────────────────────────────────────────────────────────
col = 10
print(f"{'Inc ID':<{col}}  {'Cat':>10}  {'Sev':>4}  {'Aff':>5}  {'Team':>4}  {'BizH':>4}  {'Cmts':>4}   {'Predicted':>12}   Tier")
print("─" * 96)

for hit in hits:
    src  = hit["_source"]
    ml   = src.get("ml", {})
    pred = ml.get("resolution_time_minutes_prediction")

    if pred is None:
        print(f"{src['incident_id']:<{col}}  {'—':>60}  [inference failed — check pipeline tags]")
        continue

    tier_label = resolution_tier(pred)
    print(
        f"{src['incident_id']:<{col}}  "
        f"{src['category']:>10}  "
        f"{src['severity']:>4}  "
        f"{src['num_affected_users']:>5}  "
        f"{src['team_size']:>4}  "
        f"{src['is_business_hours']:>4}  "
        f"{src['num_comments']:>4}   "
        f"{pred:>8.1f} min   {tier_label}"
    )
    for fi in sorted(ml.get("feature_importance", []), key=lambda x: abs(x["importance"]), reverse=True):
        sign = "+" if fi["importance"] >= 0 else ""
        print(f"  {'':>{col}}  {'':>10}  {'':>4}  {'':>5}  {'':>4}  {'':>4}  {'':>4}   "
              f"  {fi['feature_name']}: {sign}{fi['importance']:.1f} min")

print(f"\n{len(hits)} document(s) retrieved from '{LIVE_INDEX}'.")
print("Prediction was added automatically by the ingest pipeline — no separate inference call.")
