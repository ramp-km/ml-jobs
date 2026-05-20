"""
Create and start an Elasticsearch ML outlier detection data frame analytics job
on the employee-metrics index.
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
    # .elastic-credentials sections contain plain KEY=VALUE lines between headers
    for key, value in _parse_kv_file(root / ".elastic-credentials"):
        os.environ.setdefault(key, value)


load_config()

ES_URL = os.environ["ELASTICSEARCH_URL"]
API_KEY = os.environ["ELASTICSEARCH_API_KEY"]
JOB_ID = "employee-outlier-detection"

# Certificate verification disabled — works on all platforms without configuration
SSL_CONTEXT = ssl._create_unverified_context()


def request(method, path, body=None):
    url = f"{ES_URL}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {
        "Authorization": f"ApiKey {API_KEY}",
        "Content-Type": "application/json",
    }
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"HTTP {e.code} {method} {path}: {err}", file=sys.stderr)
        sys.exit(1)


# Check index doc count
count = request("GET", "/employee-metrics/_count")
print(f"Documents in employee-metrics: {count['count']}")

# Create the data frame analytics job
job_config = {
    "source": {
        "index": "employee-metrics",
        "query": {"match_all": {}},
    },
    "dest": {
        "index": "employee-outlier-results",
    },
    "analysis": {
        "outlier_detection": {
            # Let ES auto-select the best algorithm (ensemble of LOF, LDOF, kNN-dist, kNN-density)
            "compute_feature_influence": True,
            "outlier_fraction": 0.05,
        }
    },
    "analyzed_fields": {
        "includes": ["hours_worked", "tickets_closed", "commits", "meetings"]
    },
    "model_memory_limit": "50mb",
    "description": "Detect productivity outliers in employee metrics",
}

print(f"\nCreating job '{JOB_ID}'...")
result = request("PUT", f"/_ml/data_frame/analytics/{JOB_ID}", job_config)
print(f"  id: {result.get('id')}")
print(f"  dest index: {result.get('dest', {}).get('index')}")

# Start the job
print(f"\nStarting job '{JOB_ID}'...")
result = request("POST", f"/_ml/data_frame/analytics/{JOB_ID}/_start")
print(f"  acknowledged: {result.get('acknowledged')}")
print(f"  node: {result.get('node', 'N/A')}")

print("\nJob started. Poll status with:")
print(f"  GET /_ml/data_frame/analytics/{JOB_ID}/_stats")
