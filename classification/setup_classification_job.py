"""
Create and start an Elasticsearch ML classification data frame analytics job
on the server-health-metrics index.

Job ID:               server-outage-classifier
Source index:         server-health-metrics
Dest index:           server-outage-predictions
Dependent variable:   is_outage (boolean)

Analyzed features:
  cpu_usage_pct, memory_usage_pct, disk_io_util_pct,
  error_log_count, warning_log_count, restart_count,
  network_drop_pct, active_connections,
  environment (categorical), server_role (categorical)

The job trains on 80 % of labeled rows. For each document in the dest index
the job writes:
  ml.predicted_is_outage        -- predicted class (true / false)
  ml.prediction_probability     -- confidence for the top predicted class
  ml.prediction_score           -- model score
  ml.top_classes[]              -- probability for each class
  ml.feature_importance[]       -- SHAP values for top 5 features
  ml.is_training                -- true if row was used for training
"""
import json
import ssl
import urllib.request
import urllib.error
import os
import sys
from pathlib import Path


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
    for key, value in _parse_kv_file(root / ".elastic-credentials"):
        os.environ.setdefault(key, value)


load_config()

SSL_CONTEXT = ssl._create_unverified_context()
ES_URL  = os.environ["ELASTICSEARCH_URL"]
API_KEY = os.environ["ELASTICSEARCH_API_KEY"]
JOB_ID  = "server-outage-classifier"
SRC_IDX = "server-health-metrics"
DST_IDX = "server-outage-predictions"


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
        if e.code == 409:
            return {"_already_exists": True}
        print(f"HTTP {e.code} {method} {path}: {err}", file=sys.stderr)
        sys.exit(1)


# Verify source data is ready
count = request("GET", f"/{SRC_IDX}/_count")
doc_count = count.get("count", 0)
print(f"Source index '{SRC_IDX}': {doc_count:,} documents")
if doc_count == 0:
    print("No data found. Run generate_sample_data.py first.", file=sys.stderr)
    sys.exit(1)

# ── Create the classification data frame analytics job ────────────────────────
job_config = {
    "description": (
        "Predict server outages from health metrics. "
        "Binary classification on is_outage (true/false) using CPU, memory, "
        "disk I/O, error counts, restarts, network drops, and connection counts."
    ),
    "source": {
        "index": SRC_IDX,
        "query": {"match_all": {}},
    },
    "dest": {
        "index": DST_IDX,
    },
    "analysis": {
        "classification": {
            "dependent_variable":               "is_outage",
            "num_top_classes":                   2,
            "training_percent":                  80,
            "prediction_field_name":             "ml.predicted_is_outage",
            "num_top_feature_importance_values": 5,
        }
    },
    # Exclude host_name — it is an identifier, not a predictive feature.
    "analyzed_fields": {
        "includes": [
            "cpu_usage_pct",
            "memory_usage_pct",
            "disk_io_util_pct",
            "error_log_count",
            "warning_log_count",
            "restart_count",
            "network_drop_pct",
            "active_connections",
            "environment",
            "server_role",
            "is_outage",
        ]
    },
    "model_memory_limit": "100mb",
}

print(f"\nCreating job '{JOB_ID}'...")
result = request("PUT", f"/_ml/data_frame/analytics/{JOB_ID}", job_config)
if result.get("_already_exists"):
    print("  Job already exists — skipping creation.")
else:
    print(f"  id:         {result.get('id')}")
    print(f"  dest index: {result.get('dest', {}).get('index')}")

# ── Start the job ─────────────────────────────────────────────────────────────
print(f"\nStarting job '{JOB_ID}'...")
result = request("POST", f"/_ml/data_frame/analytics/{JOB_ID}/_start")
if result.get("_already_exists"):
    print("  Job is already running.")
else:
    print(f"  acknowledged: {result.get('acknowledged')}")
    print(f"  node:         {result.get('node', 'N/A')}")

print(f"""
Job started. The job transitions through these states:
  stopped → started → reindexing → analyzing → stopped (100 %)

Poll status:
  GET /_ml/data_frame/analytics/{JOB_ID}/_stats

Query predictions when complete:
  POST /{DST_IDX}/_search
  {{
    "size": 5,
    "query": {{"term": {{"ml.predicted_is_outage": true}}}},
    "sort": [{{"ml.prediction_probability": {{"order": "desc"}}}}],
    "_source": [
      "host_name", "environment", "server_role",
      "cpu_usage_pct", "memory_usage_pct", "error_log_count",
      "is_outage", "ml.predicted_is_outage",
      "ml.prediction_probability", "ml.feature_importance"
    ]
  }}
""")
