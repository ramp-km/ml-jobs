"""
Real-time bandwidth prediction for bank branches.

Sends branch feature vectors to the deployed trained model and prints
the predicted bandwidth (Mbps) alongside a human-readable tier label.

Usage
-----
  # predict the built-in sample branches
  python3 realtime_inference.py

  # predict a single branch inline
  python3 realtime_inference.py --employees 35 --customers 800 --transactions 1500
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
ES_URL  = os.environ["ELASTICSEARCH_URL"]
API_KEY = os.environ["ELASTICSEARCH_API_KEY"]
JOB_ID  = "branch-bandwidth-regression"


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


def find_model_id():
    """Return the most recently trained model ID for the job."""
    resp   = request("GET", f"/_ml/trained_models?tags={JOB_ID}&size=10")
    models = resp.get("trained_model_configs", [])
    if not models:
        print(f"No trained model found for job '{JOB_ID}'.", file=sys.stderr)
        print("Run deploy_model.py first.", file=sys.stderr)
        sys.exit(1)
    return sorted(models, key=lambda m: m["model_id"])[-1]["model_id"]


def bandwidth_tier(mbps):
    """Map a predicted Mbps value to a human-readable provisioning tier."""
    if mbps < 50:
        return "Basic  (rural)"
    if mbps < 150:
        return "Standard (suburban)"
    if mbps < 350:
        return "High   (urban)"
    return "Premium (flagship)"


def infer(model_id, branches):
    """
    Send a list of branch dicts to the inference API.

    Each dict must contain: num_employees, num_customers, num_transactions.
    Returns a list of predicted bandwidth values (float, Mbps).
    """
    payload = {
        "docs": [
            {
                "num_employees":    b["num_employees"],
                "num_customers":    b["num_customers"],
                "num_transactions": b["num_transactions"],
            }
            for b in branches
        ]
    }
    resp = request("POST", f"/_ml/trained_models/{model_id}/_infer", payload)
    # tree_ensemble regression result key is "<dependent_variable>_prediction"
    return [
        (r["bandwidth_mbps_prediction"], r.get("feature_importance", []))
        for r in resp["inference_results"]
    ]


# ── CLI argument parsing ──────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Predict bank branch bandwidth in real time.")
parser.add_argument("--employees",    type=int, help="Number of employees at the branch")
parser.add_argument("--customers",    type=int, help="Number of customers per day")
parser.add_argument("--transactions", type=int, help="Number of transactions per day")
args = parser.parse_args()

# ── Resolve model ─────────────────────────────────────────────────────────────
model_id = find_model_id()
print(f"Using model : {model_id}\n")

# ── Branch list: CLI override or built-in samples ─────────────────────────────
if args.employees and args.customers and args.transactions:
    branches = [
        {
            "label":            "CLI input",
            "num_employees":    args.employees,
            "num_customers":    args.customers,
            "num_transactions": args.transactions,
        }
    ]
else:
    branches = [
        # Rural — small headcount, low footfall
        {"label": "Rural branch (Coorg)",          "num_employees": 5,   "num_customers": 80,   "num_transactions": 150},
        # Suburban — mid-size
        {"label": "Suburban branch (Whitefield)",  "num_employees": 18,  "num_customers": 320,  "num_transactions": 650},
        # Urban — busy city branch
        {"label": "Urban branch (MG Road)",        "num_employees": 45,  "num_customers": 900,  "num_transactions": 1_800},
        # Flagship — large hub
        {"label": "Flagship branch (Nariman Point)","num_employees": 90, "num_customers": 2_000,"num_transactions": 4_500},
        # Edge case — small staff, unusually high transactions (e.g. ATM-heavy branch)
        {"label": "ATM-heavy kiosk branch",        "num_employees": 3,   "num_customers": 200,  "num_transactions": 1_200},
    ]

# ── Run inference ─────────────────────────────────────────────────────────────
predictions = infer(model_id, branches)

# ── Print results ─────────────────────────────────────────────────────────────
col = 42
print(f"{'Branch':<{col}} {'Employees':>10} {'Customers':>10} {'Txns':>8}   {'Predicted BW':>14}   Tier")
print("-" * (col + 68))
for branch, (predicted_mbps, feature_importance) in zip(branches, predictions):
    tier = bandwidth_tier(predicted_mbps)
    print(
        f"{branch['label']:<{col}} "
        f"{branch['num_employees']:>10} "
        f"{branch['num_customers']:>10} "
        f"{branch['num_transactions']:>8}   "
        f"{predicted_mbps:>10.1f} Mbps   {tier}"
    )
    # Show feature importance breakdown when available
    if feature_importance:
        for fi in sorted(feature_importance, key=lambda x: abs(x["importance"]), reverse=True):
            sign = "+" if fi["importance"] >= 0 else ""
            print(f"    {'':>{col-4}} {'':>10} {'':>10} {'':>8}   "
                  f"  {fi['feature_name']}: {sign}{fi['importance']:.1f} Mbps")
