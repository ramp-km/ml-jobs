"""
Create and start an Elasticsearch ML regression job that predicts
network bandwidth (Mbps) required for a bank branch, using
num_employees, num_customers, and num_transactions as features.
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
    for key, value in _parse_kv_file(root / ".env"):
        os.environ.setdefault(key, value)
    for key, value in _parse_kv_file(root / ".elastic-credentials"):
        os.environ.setdefault(key, value)


load_config()

SSL_CONTEXT = ssl._create_unverified_context()

ES_URL = os.environ["ELASTICSEARCH_URL"]
API_KEY = os.environ["ELASTICSEARCH_API_KEY"]
JOB_ID = "branch-bandwidth-regression"


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


count = request("GET", "/bank-branches/_count")
print(f"Documents in bank-branches: {count['count']}")

job_config = {
    "source": {
        "index": "bank-branches",
        "query": {"match_all": {}},
    },
    "dest": {
        "index": "bank-branches-regression-results",
    },
    "analysis": {
        "regression": {
            "dependent_variable": "bandwidth_mbps",
            "training_percent": 80,
            # Adds ml.feature_importance to each result doc so you can see
            # how much each feature drove the prediction for a specific branch.
            "num_top_feature_importance_values": 3,
        }
    },
    "analyzed_fields": {
        "includes": [
            "num_employees",
            "num_customers",
            "num_transactions",
            "bandwidth_mbps",
        ]
    },
    "model_memory_limit": "50mb",
    "description": "Predict required network bandwidth (Mbps) for bank branches",
}

print(f"\nCreating job '{JOB_ID}'...")
result = request("PUT", f"/_ml/data_frame/analytics/{JOB_ID}", job_config)
print(f"  id: {result.get('id')}")
print(f"  dest index: {result.get('dest', {}).get('index')}")

print(f"\nStarting job '{JOB_ID}'...")
result = request("POST", f"/_ml/data_frame/analytics/{JOB_ID}/_start")
print(f"  acknowledged: {result.get('acknowledged')}")
print(f"  node: {result.get('node', 'N/A')}")

print("\nJob started. Poll status with:")
print(f"  GET /_ml/data_frame/analytics/{JOB_ID}/_stats")
