"""
Locate the trained model produced by the branch-bandwidth-regression job
and confirm it is ready to serve real-time inference.

Elasticsearch regression jobs produce a tree_ensemble model that is
immediately available for inference via the _infer API — no separate
deployment step is required (unlike PyTorch/NLP models).

This script:
  1. Looks up the model ID dynamically from the model registry using
     the job tag (the model ID contains a timestamp suffix, so it is
     not hard-coded here).
  2. Verifies the model is present and readable via /_stats.
  3. Runs a smoke-test inference call to confirm end-to-end readiness.
  4. Prints the model ID and a ready-to-paste curl example.
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


# ── 1. Find the trained model ─────────────────────────────────────────────────
print(f"Looking up trained model for job '{JOB_ID}'...")
resp   = request("GET", f"/_ml/trained_models?tags={JOB_ID}&size=10")
models = resp.get("trained_model_configs", [])
if not models:
    print(f"  No model found. Ensure the job has completed successfully.", file=sys.stderr)
    sys.exit(1)

model_id   = sorted(models, key=lambda m: m["model_id"])[-1]["model_id"]
model_type = models[0].get("model_type", "unknown")
print(f"  model_id   : {model_id}")
print(f"  model_type : {model_type}   (tree_ensemble models serve inference immediately)")

# ── 2. Verify model stats ─────────────────────────────────────────────────────
print(f"\nVerifying model stats...")
stats     = request("GET", f"/_ml/trained_models/{model_id}/_stats")
model_stats = stats["trained_model_stats"][0]
size_kb   = model_stats["model_size_stats"]["model_size_bytes"] // 1024
inf_count = model_stats["inference_stats"]["inference_count"]
print(f"  model size     : {size_kb} KB")
print(f"  inference calls: {inf_count} so far")

# ── 3. Smoke-test inference ───────────────────────────────────────────────────
print(f"\nRunning smoke-test inference (suburban branch)...")
result = request(
    "POST",
    f"/_ml/trained_models/{model_id}/_infer",
    {"docs": [{"num_employees": 15, "num_customers": 300, "num_transactions": 600}]},
)
predicted = result["inference_results"][0]["bandwidth_mbps_prediction"]
print(f"  Input  : employees=15, customers=300, transactions=600")
print(f"  Output : {predicted:.1f} Mbps  ✓")

# ── 4. Print ready summary ────────────────────────────────────────────────────
print(f"\nModel is ready for real-time inference.")
print(f"\nModel ID:\n  {model_id}")
print(f"\nSample curl call:")
print(f"""
  curl -s -k -X POST \\
    -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \\
    -H "Content-Type: application/json" \\
    "{ES_URL}/_ml/trained_models/{model_id}/_infer" \\
    -d '{{
      "docs": [{{
        "num_employees": 20,
        "num_customers": 500,
        "num_transactions": 1000
      }}]
    }}' | python3 -m json.tool
""")
print("Next step: run  python3 realtime_inference.py")
