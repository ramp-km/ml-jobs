"""
Generate synthetic server health metrics with labeled outage events and bulk-index them.

Normal servers  (is_outage=false): moderate CPU/memory/disk, few errors, rare restarts
Outage servers  (is_outage=true):  stressed metrics — two failure modes injected:
  - Resource exhaustion: high CPU + memory + disk + error flood
  - Connection storm:    CPU spike + massive connection count + high packet drop

Total: 2,400 records — 1,800 normal + 600 outage (25 % outage rate).
The classification job learns which metric combinations predict outages.
"""
import json
import random
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
INDEX   = "server-health-metrics"

random.seed(42)

ENVIRONMENTS  = ["production", "staging", "development"]
SERVER_ROLES  = ["web", "database", "cache", "message-queue"]


def normal_record(host_id):
    """Healthy server — moderate resource usage, few errors."""
    return {
        "host_name":          f"srv-{host_id:04d}",
        "environment":        ENVIRONMENTS[host_id % len(ENVIRONMENTS)],
        "server_role":        SERVER_ROLES[host_id % len(SERVER_ROLES)],
        "cpu_usage_pct":      round(random.uniform(10, 65), 1),
        "memory_usage_pct":   round(random.uniform(20, 70), 1),
        "disk_io_util_pct":   round(random.uniform(5, 55), 1),
        "error_log_count":    random.randint(0, 8),
        "warning_log_count":  random.randint(0, 25),
        "restart_count":      random.randint(0, 1),
        "network_drop_pct":   round(random.uniform(0.0, 1.5), 2),
        "active_connections": random.randint(20, 500),
        "is_outage":          False,
    }


def outage_record(host_id):
    """Server experiencing an outage — two distinct failure-mode patterns."""
    env  = ENVIRONMENTS[host_id % len(ENVIRONMENTS)]
    role = SERVER_ROLES[host_id % len(SERVER_ROLES)]

    if random.random() < 0.65:
        # Failure mode A: resource exhaustion — CPU + memory + disk saturated
        return {
            "host_name":          f"srv-{host_id:04d}",
            "environment":        env,
            "server_role":        role,
            "cpu_usage_pct":      round(random.uniform(85, 99), 1),
            "memory_usage_pct":   round(random.uniform(88, 99), 1),
            "disk_io_util_pct":   round(random.uniform(75, 99), 1),
            "error_log_count":    random.randint(30, 250),
            "warning_log_count":  random.randint(60, 400),
            "restart_count":      random.randint(2, 8),
            "network_drop_pct":   round(random.uniform(8.0, 35.0), 2),
            "active_connections": random.randint(5, 40),
            "is_outage":          True,
        }
    else:
        # Failure mode B: connection storm — connections explode, CPU spikes, drops surge
        return {
            "host_name":          f"srv-{host_id:04d}",
            "environment":        env,
            "server_role":        role,
            "cpu_usage_pct":      round(random.uniform(75, 95), 1),
            "memory_usage_pct":   round(random.uniform(65, 88), 1),
            "disk_io_util_pct":   round(random.uniform(20, 60), 1),
            "error_log_count":    random.randint(50, 300),
            "warning_log_count":  random.randint(100, 500),
            "restart_count":      random.randint(1, 5),
            "network_drop_pct":   round(random.uniform(15.0, 50.0), 2),
            "active_connections": random.randint(900, 5000),
            "is_outage":          True,
        }


# Build the labeled dataset
records  = [normal_record(i)              for i in range(1800)]
records += [outage_record(i % 600 + 9000) for i in range(600)]
random.shuffle(records)

n_normal = sum(1 for r in records if not r["is_outage"])
n_outage = sum(1 for r in records if     r["is_outage"])
print(f"Total records: {len(records)}")
print(f"  Normal  (is_outage=false): {n_normal}")
print(f"  Outage  (is_outage=true):  {n_outage}")


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
        if e.code == 400 and "resource_already_exists_exception" in err:
            return {"_already_exists": True}
        print(f"HTTP {e.code} {method} {path}: {err}", file=sys.stderr)
        sys.exit(1)


# Create index with explicit mapping so keyword/boolean fields are correctly typed
INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "host_name":          {"type": "keyword"},
            "environment":        {"type": "keyword"},
            "server_role":        {"type": "keyword"},
            "cpu_usage_pct":      {"type": "float"},
            "memory_usage_pct":   {"type": "float"},
            "disk_io_util_pct":   {"type": "float"},
            "error_log_count":    {"type": "integer"},
            "warning_log_count":  {"type": "integer"},
            "restart_count":      {"type": "integer"},
            "network_drop_pct":   {"type": "float"},
            "active_connections": {"type": "integer"},
            "is_outage":          {"type": "boolean"},
        }
    }
}

print(f"\nCreating index '{INDEX}'...")
result = request("PUT", f"/{INDEX}", INDEX_MAPPING)
if result.get("_already_exists"):
    print("  Index already exists — skipping creation.")
else:
    print(f"  acknowledged: {result.get('acknowledged')}")

# Check for existing data to avoid re-indexing
count_resp = request("GET", f"/{INDEX}/_count")
if count_resp.get("count", 0) > 0:
    print(f"\nIndex already contains {count_resp['count']:,} documents. Exiting without re-indexing.")
    print(f"To re-generate, delete the index first:\n  DELETE /{INDEX}")
    sys.exit(0)


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
            errs = [item for item in result["items"] if item.get("index", {}).get("error")]
            print(f"  Bulk errors: {len(errs)}", file=sys.stderr)


print(f"\nIndexing into '{INDEX}'...")
BATCH = 500
for i in range(0, len(records), BATCH):
    bulk_index(records[i : i + BATCH])
    print(f"  Indexed {min(i + BATCH, len(records)):>5}/{len(records)}")

print("\nDone. Next step:")
print("  python3 setup_classification_job.py")
