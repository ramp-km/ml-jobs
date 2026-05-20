"""
Real-time IT incident resolution-time prediction.

Sends incident feature vectors to the deployed trained model and prints
the predicted resolution time (minutes) alongside a priority tier label.

Usage
-----
  # predict the built-in sample incidents
  python3 realtime_inference.py

  # predict a single incident inline
  python3 realtime_inference.py \
      --category security --severity 5 \
      --affected 300 --team 6 \
      --hour 2 --dow 6 --comments 40
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
JOB_ID  = "incident-resolution-regression"


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


def resolution_tier(minutes):
    """Map a predicted resolution time to a human-readable priority tier."""
    if minutes < 60:
        return "Quick      (< 1 h)"
    if minutes < 180:
        return "Moderate   (1–3 h)"
    if minutes < 480:
        return "Significant(3–8 h)"
    return "Critical   (> 8 h)"


def infer(model_id, incidents):
    """
    Send a list of incident dicts to the inference API.

    Each dict must contain: category, severity, num_affected_users,
    team_size, hour_of_day, day_of_week, is_business_hours, num_comments.
    Returns a list of (predicted_minutes, feature_importance) tuples.
    """
    payload = {
        "docs": [
            {
                "category":           inc["category"],
                "severity":           inc["severity"],
                "num_affected_users": inc["num_affected_users"],
                "team_size":          inc["team_size"],
                "hour_of_day":        inc["hour_of_day"],
                "day_of_week":        inc["day_of_week"],
                "is_business_hours":  inc["is_business_hours"],
                "num_comments":       inc["num_comments"],
            }
            for inc in incidents
        ]
    }
    resp = request("POST", f"/_ml/trained_models/{model_id}/_infer", payload)
    return [
        (r["resolution_time_minutes_prediction"], r.get("feature_importance", []))
        for r in resp["inference_results"]
    ]


# ── CLI argument parsing ──────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Predict IT incident resolution time in real time.")
parser.add_argument("--category",  default=None, choices=["network", "hardware", "software", "security", "user_access"])
parser.add_argument("--severity",  type=int,  help="Severity level 1 (low) to 5 (critical)")
parser.add_argument("--affected",  type=int,  help="Number of affected users")
parser.add_argument("--team",      type=int,  help="Team size assigned to the incident")
parser.add_argument("--hour",      type=int,  help="Hour of day incident was opened (0-23)")
parser.add_argument("--dow",       type=int,  help="Day of week (0=Mon, 6=Sun)")
parser.add_argument("--comments",  type=int,  help="Number of comments on the incident")
args = parser.parse_args()

# ── Resolve model ─────────────────────────────────────────────────────────────
model_id = find_model_id()
print(f"Using model : {model_id}\n")

# ── Incident list: CLI override or built-in samples ───────────────────────────
if all(v is not None for v in [args.category, args.severity, args.affected, args.team, args.hour, args.dow, args.comments]):
    is_biz = 1 if (9 <= args.hour <= 17 and args.dow <= 4) else 0
    incidents = [
        {
            "label":              "CLI input",
            "category":           args.category,
            "severity":           args.severity,
            "num_affected_users": args.affected,
            "team_size":          args.team,
            "hour_of_day":        args.hour,
            "day_of_week":        args.dow,
            "is_business_hours":  is_biz,
            "num_comments":       args.comments,
        }
    ]
else:
    incidents = [
        # Low-severity user access issue during business hours
        {
            "label":              "Password reset (business hours)",
            "category":           "user_access",
            "severity":           1,
            "num_affected_users": 2,
            "team_size":          1,
            "hour_of_day":        10,
            "day_of_week":        2,
            "is_business_hours":  1,
            "num_comments":       4,
        },
        # Medium-severity software bug during business hours
        {
            "label":              "App crash — reporting service",
            "category":           "software",
            "severity":           3,
            "num_affected_users": 80,
            "team_size":          3,
            "hour_of_day":        14,
            "day_of_week":        1,
            "is_business_hours":  1,
            "num_comments":       18,
        },
        # High-severity network outage during business hours
        {
            "label":              "Network outage — HQ office",
            "category":           "network",
            "severity":           4,
            "num_affected_users": 250,
            "team_size":          5,
            "hour_of_day":        11,
            "day_of_week":        0,
            "is_business_hours":  1,
            "num_comments":       35,
        },
        # Critical security breach — weekend, off-hours
        {
            "label":              "Security breach — weekend night",
            "category":           "security",
            "severity":           5,
            "num_affected_users": 300,
            "team_size":          6,
            "hour_of_day":        2,
            "day_of_week":        6,
            "is_business_hours":  0,
            "num_comments":       55,
        },
        # Hardware failure — off-hours
        {
            "label":              "Server disk failure — midnight",
            "category":           "hardware",
            "severity":           4,
            "num_affected_users": 120,
            "team_size":          2,
            "hour_of_day":        23,
            "day_of_week":        3,
            "is_business_hours":  0,
            "num_comments":       22,
        },
    ]

# ── Run inference ─────────────────────────────────────────────────────────────
predictions = infer(model_id, incidents)

# ── Print results ─────────────────────────────────────────────────────────────
col = 40
print(f"{'Incident':<{col}} {'Cat':>10} {'Sev':>4} {'Aff':>5} {'Team':>5}   {'Predicted':>12}   Tier")
print("-" * (col + 66))
for inc, (predicted_min, feature_importance) in zip(incidents, predictions):
    tier = resolution_tier(predicted_min)
    print(
        f"{inc['label']:<{col}} "
        f"{inc['category']:>10} "
        f"{inc['severity']:>4} "
        f"{inc['num_affected_users']:>5} "
        f"{inc['team_size']:>5}   "
        f"{predicted_min:>8.1f} min   {tier}"
    )
    if feature_importance:
        for fi in sorted(feature_importance, key=lambda x: abs(x["importance"]), reverse=True):
            sign = "+" if fi["importance"] >= 0 else ""
            print(f"    {'':>{col-4}} {'':>10} {'':>4} {'':>5} {'':>5}   "
                  f"  {fi['feature_name']}: {sign}{fi['importance']:.1f} min")
