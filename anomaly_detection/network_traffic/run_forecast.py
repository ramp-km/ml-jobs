"""
Trigger an ML bandwidth forecast and poll until it completes.

The forecast covers the next 12 weeks (configurable via --duration).
Results are written to .ml-anomalies-* and read by check_threshold.py.

Usage:
  python3 run_forecast.py                   # 12-week forecast (default)
  python3 run_forecast.py --duration 8w     # shorter window
  python3 run_forecast.py --duration 16w    # longer window
"""
import argparse
import json
import ssl
import time
import urllib.request
import urllib.error
import os
import sys
from datetime import datetime, timezone
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
    for key, value in _parse_kv_file(root / ".elastic-credentials"):
        os.environ.setdefault(key, value)
    for key, value in _parse_kv_file(root / ".env"):
        os.environ.setdefault(key, value)


load_config()

SSL_CONTEXT = ssl._create_unverified_context()
ES_URL   = os.environ["ELASTICSEARCH_URL"]
API_KEY  = os.environ["ELASTICSEARCH_API_KEY"]
JOB_ID   = "network-traffic-anomaly"

parser = argparse.ArgumentParser(description="Trigger ML bandwidth forecast.")
parser.add_argument("--duration",   default="84d", help="Forecast window in days (default: 84d = 12 weeks)")
parser.add_argument("--expires-in", default="30d", help="How long to retain results (default: 30d)")
args = parser.parse_args()


def request(method, path, body=None):
    url     = f"{ES_URL}{path}"
    data    = json.dumps(body).encode() if body else None
    headers = {"Authorization": f"ApiKey {API_KEY}", "Content-Type": "application/json"}
    req     = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"HTTP {e.code} {method} {path}: {err}", file=sys.stderr)
        sys.exit(1)


# ── Verify the job has processed enough history ───────────────────────────────
stats     = request("GET", f"/_ml/anomaly_detectors/{JOB_ID}/_stats")
job_stats = stats.get("jobs", [{}])[0]
state     = job_stats.get("state", "unknown")
buckets   = job_stats.get("data_counts", {}).get("bucket_count", 0)
records   = job_stats.get("data_counts", {}).get("processed_record_count", 0)

print(f"Job '{JOB_ID}'")
print(f"  state           : {state}")
print(f"  buckets analyzed: {buckets:,}")
print(f"  records seen    : {records:,}")

if state not in ("opened", "closed"):
    print(f"\nJob must be in 'opened' or 'closed' state. Current: {state}", file=sys.stderr)
    print("Ensure setup_anomaly_job.py has been run first.", file=sys.stderr)
    sys.exit(1)

MIN_BUCKETS = 200   # need at least ~8 days of data per partition for a meaningful forecast
if buckets < MIN_BUCKETS:
    print(f"\nOnly {buckets:,} buckets analyzed (need {MIN_BUCKETS:,} minimum).")
    print("The datafeed is still processing historical data. Wait a few minutes and retry.")
    sys.exit(1)

# If closed (datafeed finished historical run), re-open for forecasting
if state == "closed":
    print(f"\nRe-opening closed job for forecasting...")
    result = request("POST", f"/_ml/anomaly_detectors/{JOB_ID}/_open")
    print(f"  opened: {result.get('opened')}")

# ── Trigger the forecast ──────────────────────────────────────────────────────
print(f"\nTriggering forecast: duration={args.duration}, expires_in={args.expires_in}...")
result = request(
    "POST",
    f"/_ml/anomaly_detectors/{JOB_ID}/_forecast",
    {"duration": args.duration, "expires_in": args.expires_in},
)
forecast_id = result.get("forecast_id")
print(f"  forecast_id  : {forecast_id}")
print(f"  acknowledged : {result.get('acknowledged')}")

# ── Poll for forecast completion ──────────────────────────────────────────────
# Primary signal: model_forecast_stats document (not available on all cluster versions).
# Fallback: count of model_forecast result docs, which stabilises when done.
print(f"\nPolling for forecast completion (this usually takes < 60 s)...")

BASE_FILTER = [
    {"term": {"job_id":      JOB_ID}},
    {"term": {"forecast_id": forecast_id}},
]
status       = "waiting"
prev_count   = -1
stable_ticks = 0

for attempt in range(180):
    time.sleep(5)

    # Try stats doc first
    stats_resp = request(
        "POST",
        "/.ml-anomalies-*/_search",
        {
            "size": 1,
            "_source": ["forecast_status", "forecast_progress"],
            "query": {"bool": {"filter": BASE_FILTER + [{"term": {"result_type": "model_forecast_stats"}}]}},
        },
    )
    hits = stats_resp.get("hits", {}).get("hits", [])
    if hits:
        src      = hits[0]["_source"]
        status   = src.get("forecast_status", "unknown")
        progress = src.get("forecast_progress", 0.0)
        print(f"  [{attempt * 5:>4}s]  status={status:<10}  progress={progress:.0%}")
        if status in ("finished", "failed"):
            break
        continue

    # Fallback: count model_forecast docs; stable count for 3 ticks = done
    count_resp = request(
        "POST",
        "/.ml-anomalies-*/_count",
        {"query": {"bool": {"filter": BASE_FILTER + [{"term": {"result_type": "model_forecast"}}]}}},
    )
    current_count = count_resp.get("count", 0)
    if current_count > 0 and current_count == prev_count:
        stable_ticks += 1
    else:
        stable_ticks = 0
    prev_count = current_count
    print(f"  [{attempt * 5:>4}s]  forecast docs={current_count:,}  (stable={stable_ticks}/3)")
    if stable_ticks >= 3:
        status = "finished"
        break

if status == "failed":
    print("\nForecast failed. Check Elasticsearch logs for details.", file=sys.stderr)
    sys.exit(1)

# Count forecast result documents
count_result = request(
    "POST",
    "/.ml-anomalies-*/_count",
    {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"job_id":      JOB_ID}},
                    {"term": {"forecast_id": forecast_id}},
                    {"term": {"result_type": "model_forecast"}},
                ]
            }
        }
    },
)
forecast_docs = count_result.get("count", 0)

print(f"""
Forecast complete.
  forecast_id   : {forecast_id}
  result docs   : {forecast_docs:,} (one per partition per bucket)
  duration      : {args.duration}
  expires_in    : {args.expires_in}

Find 90%% threshold crossing dates:
  python3 check_threshold.py --forecast-id {forecast_id}
""")
