"""
Create and start an Elasticsearch ML anomaly detection job for network traffic.

Job ID:     network-traffic-anomaly
Datafeed:   datafeed-network-traffic-anomaly
Detector:   high_mean(bandwidth_mbps) partitioned by segment_name
Bucket:     1h

The high_mean detector flags hours where bandwidth for a segment is
anomalously high relative to the learned seasonal baseline. Using
partition_field_name lets the model learn each segment's pattern independently
and produces per-segment forecasts.

After this script runs, the datafeed begins reading historical data from
network-traffic-metrics. When the datafeed has caught up (a few minutes),
run run_forecast.py to generate bandwidth predictions for the next 12 weeks.
"""
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
    for key, value in _parse_kv_file(root / ".elastic-credentials"):
        os.environ.setdefault(key, value)
    for key, value in _parse_kv_file(root / ".env"):
        os.environ.setdefault(key, value)


load_config()

SSL_CONTEXT = ssl._create_unverified_context()
ES_URL   = os.environ["ELASTICSEARCH_URL"]
API_KEY  = os.environ["ELASTICSEARCH_API_KEY"]
JOB_ID   = "network-traffic-anomaly"
FEED_ID  = f"datafeed-{JOB_ID}"
INDEX    = "network-traffic-metrics"


def request(method, path, body=None, ok_codes=(200, 201)):
    url     = f"{ES_URL}{path}"
    data    = json.dumps(body).encode() if body else None
    headers = {"Authorization": f"ApiKey {API_KEY}", "Content-Type": "application/json"}
    req     = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        if e.code == 409:
            return {"_already_exists": True}
        print(f"HTTP {e.code} {method} {path}: {err}", file=sys.stderr)
        sys.exit(1)


# Verify source data is ready
count = request("GET", f"/{INDEX}/_count")
doc_count = count["count"]
print(f"Source index '{INDEX}': {doc_count:,} documents")
if doc_count == 0:
    print("No data found. Run generate_sample_data.py first.", file=sys.stderr)
    sys.exit(1)

# ── 1. Create the anomaly detection job ──────────────────────────────────────
job_config = {
    "description": (
        "Detect bandwidth anomalies and forecast growth per network segment. "
        "Flags hours with unusually high throughput; supports _forecast API "
        "to predict when segments will cross the 90 % capacity threshold."
    ),
    "analysis_config": {
        "bucket_span": "1h",
        "detectors": [
            {
                "function":             "high_mean",
                "field_name":           "bandwidth_mbps",
                "partition_field_name": "segment_name.keyword",
                "detector_description": "Unusually high mean bandwidth per segment",
            }
        ],
        "influencers": ["segment_name.keyword"],
    },
    "analysis_limits": {
        "model_memory_limit": "128mb",
    },
    "data_description": {
        "time_field": "@timestamp",
    },
    "model_plot_config": {
        "enabled":                    True,
        "annotations_enabled":        True,
    },
}

print(f"\nCreating job '{JOB_ID}'...")
result = request("PUT", f"/_ml/anomaly_detectors/{JOB_ID}", job_config)
if result.get("_already_exists"):
    print("  Job already exists — skipping creation.")
else:
    print(f"  job_id      : {result.get('job_id')}")
    print(f"  bucket_span : {result.get('analysis_config', {}).get('bucket_span')}")

# ── 2. Open the job ───────────────────────────────────────────────────────────
print(f"\nOpening job '{JOB_ID}'...")
result = request("POST", f"/_ml/anomaly_detectors/{JOB_ID}/_open")
if result.get("_already_exists"):
    print("  Job is already open.")
else:
    print(f"  opened: {result.get('opened')}")

# ── 3. Create the datafeed ────────────────────────────────────────────────────
# No aggregations in the datafeed — raw events are sent to the job so that
# partition_field_name (segment_name) is correctly resolved per document.
# The job handles bucketing and per-partition modelling internally.
datafeed_config = {
    "job_id":  JOB_ID,
    "indices": [INDEX],
    "query":   {"match_all": {}},
    "chunking_config": {"mode": "auto"},
    "delayed_data_check_config": {"enabled": False},
}

print(f"\nCreating datafeed '{FEED_ID}'...")
result = request("PUT", f"/_ml/datafeeds/{FEED_ID}", datafeed_config)
if result.get("_already_exists"):
    print("  Datafeed already exists — skipping creation.")
else:
    print(f"  datafeed_id : {result.get('datafeed_id')}")
    print(f"  job_id      : {result.get('job_id')}")

# ── 4. Start the datafeed (processes all history, then runs in real-time) ─────
print(f"\nStarting datafeed '{FEED_ID}'...")
result = request("POST", f"/_ml/datafeeds/{FEED_ID}/_start")
if result.get("_already_exists"):
    print("  Datafeed is already running.")
else:
    print(f"  started : {result.get('started')}")

print(f"""
Setup complete.

The datafeed is reading ~6 months of hourly data from '{INDEX}'.
This typically takes 1–3 minutes. Monitor progress:

  GET /_ml/anomaly_detectors/{JOB_ID}/_stats
    → data_counts.processed_record_count   (should reach ~{doc_count:,})
    → data_counts.bucket_count             (should reach ~{doc_count // 4:,})

Once bucket_count is stable, run the forecast:
  python3 run_forecast.py

View anomalies in Kibana → Machine Learning → Anomaly Explorer
  Job: {JOB_ID}
""")
