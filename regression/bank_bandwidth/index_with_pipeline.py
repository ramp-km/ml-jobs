"""
Index new bank branch documents into 'bank-branches-live'.

Because 'bank-branches-live' has the ingest pipeline set as its
default_pipeline, every document is automatically scored by the model
at index time — no separate inference call needed.

After indexing, the script queries the documents back and prints
the predicted bandwidth alongside the input features.

Usage
-----
  # index and display all built-in sample branches
  python3 index_with_pipeline.py

  # index a single branch from the CLI
  python3 index_with_pipeline.py \
      --branch-id BR9010 --tier urban \
      --employees 40 --customers 750 --transactions 1500
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
LIVE_INDEX  = "bank-branches-live"


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


def bandwidth_tier(mbps):
    if mbps < 50:   return "Basic    (rural)"
    if mbps < 150:  return "Standard (suburban)"
    if mbps < 350:  return "High     (urban)"
    return              "Premium  (flagship)"


# ── CLI ───────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Index branch docs through the ingest pipeline.")
parser.add_argument("--branch-id",    default=None)
parser.add_argument("--tier",         default="urban")
parser.add_argument("--employees",    type=int, default=None)
parser.add_argument("--customers",    type=int, default=None)
parser.add_argument("--transactions", type=int, default=None)
args = parser.parse_args()

if args.employees and args.customers and args.transactions:
    branches = [{
        "branch_id":        args.branch_id or "BR_CLI",
        "branch_tier":      args.tier,
        "num_employees":    args.employees,
        "num_customers":    args.customers,
        "num_transactions": args.transactions,
    }]
else:
    branches = [
        {"branch_id": "BR9001", "branch_tier": "rural",    "num_employees": 5,   "num_customers": 80,   "num_transactions": 150},
        {"branch_id": "BR9002", "branch_tier": "suburban", "num_employees": 18,  "num_customers": 320,  "num_transactions": 650},
        {"branch_id": "BR9003", "branch_tier": "urban",    "num_employees": 45,  "num_customers": 900,  "num_transactions": 1_800},
        {"branch_id": "BR9004", "branch_tier": "flagship", "num_employees": 90,  "num_customers": 2_000,"num_transactions": 4_500},
        {"branch_id": "BR9005", "branch_tier": "suburban", "num_employees": 3,   "num_customers": 200,  "num_transactions": 1_200},
    ]

# ── Index via bulk (pipeline is applied automatically) ────────────────────────
print(f"Indexing {len(branches)} branch(es) into '{LIVE_INDEX}' (pipeline auto-applies)...\n")

body = ""
for doc in branches:
    body += json.dumps({"index": {"_index": LIVE_INDEX, "_id": doc["branch_id"]}}) + "\n"
    body += json.dumps(doc) + "\n"

req = urllib.request.Request(
    # refresh=wait_for ensures docs are searchable before this call returns
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
ids      = [b["branch_id"] for b in branches]
search   = request("POST", f"/{LIVE_INDEX}/_search", {
    "size": len(ids),
    "query": {"ids": {"values": ids}},
    "sort": [{"branch_id": "asc"}],
})
hits = search["hits"]["hits"]

# ── Print results ─────────────────────────────────────────────────────────────
col = 10
print(f"{'Branch ID':<{col}}  {'Tier':<12}  {'Empl':>5}  {'Cust':>6}  {'Txns':>6}   {'Predicted BW':>14}   Tier")
print("─" * 86)

for hit in hits:
    src  = hit["_source"]
    ml   = src.get("ml", {})
    pred = ml.get("bandwidth_mbps_prediction")

    if pred is None:
        print(f"{src['branch_id']:<{col}}  {'—':>50}  [inference failed — check pipeline tags]")
        continue

    tier_label = bandwidth_tier(pred)
    print(
        f"{src['branch_id']:<{col}}  "
        f"{src['branch_tier']:<12}  "
        f"{src['num_employees']:>5}  "
        f"{src['num_customers']:>6}  "
        f"{src['num_transactions']:>6}   "
        f"{pred:>10.1f} Mbps   {tier_label}"
    )
    # Feature importance
    for fi in sorted(ml.get("feature_importance", []), key=lambda x: abs(x["importance"]), reverse=True):
        sign = "+" if fi["importance"] >= 0 else ""
        print(f"  {'':>{col}}  {'':>12}  {'':>5}  {'':>6}  {'':>6}   "
              f"  {fi['feature_name']}: {sign}{fi['importance']:.1f} Mbps")

print(f"\n{len(hits)} document(s) retrieved from '{LIVE_INDEX}'.")
print("Prediction was added automatically by the ingest pipeline — no separate inference call.")
