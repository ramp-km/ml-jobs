"""
Create an Elasticsearch ingest pipeline that runs resolution-time prediction
automatically on every incident document as it is indexed.

What this script does
---------------------
1. Looks up the trained model ID from the job tag (timestamp suffix
   means the ID is never hard-coded).
2. Creates (or overwrites) an ingest pipeline called
   'incident-resolution-prediction' with a single inference processor.
3. Creates the live index 'it-incidents-live' and sets the pipeline
   as its default_pipeline, so every future document indexed there
   automatically receives:
       ml.resolution_time_minutes_prediction — predicted resolution time (min)
       ml.feature_importance                 — per-feature contribution
       ml.model_id                           — which model version scored it

   Note: 'it-incidents' (the training data index) is left untouched.
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

SSL_CONTEXT  = ssl._create_unverified_context()
ES_URL       = os.environ["ELASTICSEARCH_URL"]
API_KEY      = os.environ["ELASTICSEARCH_API_KEY"]
JOB_ID       = "incident-resolution-regression"
PIPELINE_ID  = "incident-resolution-prediction"
LIVE_INDEX   = "it-incidents-live"


def request(method, path, body=None, allow_404=False):
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
        if allow_404 and e.code == 404:
            return None
        err = e.read().decode()
        print(f"HTTP {e.code} {method} {path}: {err}", file=sys.stderr)
        sys.exit(1)


# ── 1. Resolve trained model ID ───────────────────────────────────────────────
print(f"Resolving trained model for job '{JOB_ID}'...")
resp   = request("GET", f"/_ml/trained_models?tags={JOB_ID}&size=10")
models = resp.get("trained_model_configs", [])
if not models:
    print("  No model found. Run setup_regression_job.py and wait for it to complete.", file=sys.stderr)
    sys.exit(1)
model_id = sorted(models, key=lambda m: m["model_id"])[-1]["model_id"]
print(f"  model_id : {model_id}")

# ── 2. Create / update the ingest pipeline ────────────────────────────────────
print(f"\nCreating ingest pipeline '{PIPELINE_ID}'...")
pipeline_def = {
    "description": "Predict IT incident resolution time (minutes) on ingest",
    "processors": [
        {
            "inference": {
                "model_id":      model_id,
                "target_field":  "ml",
                "field_mappings": {},
                "inference_config": {
                    "regression": {
                        "num_top_feature_importance_values": 7
                    }
                },
                "on_failure": [
                    {
                        "append": {
                            "field":  "tags",
                            "value":  "inference_failed",
                        }
                    }
                ],
            }
        }
    ],
}
result = request("PUT", f"/_ingest/pipeline/{PIPELINE_ID}", pipeline_def)
print(f"  acknowledged : {result.get('acknowledged')}")

# ── 3. Create live index with pipeline as default ─────────────────────────────
print(f"\nCreating index '{LIVE_INDEX}' with default_pipeline '{PIPELINE_ID}'...")

existing = request("GET", f"/{LIVE_INDEX}", allow_404=True)
if existing is not None:
    print(f"  Index already exists. Updating default_pipeline setting...")
    result = request("PUT", f"/{LIVE_INDEX}/_settings", {
        "index.default_pipeline": PIPELINE_ID,
    })
    print(f"  acknowledged : {result.get('acknowledged')}")
else:
    result = request("PUT", f"/{LIVE_INDEX}", {
        "settings": {
            "index.default_pipeline": PIPELINE_ID,
        },
        "mappings": {
            "properties": {
                "incident_id":            {"type": "keyword"},
                "category":               {"type": "keyword"},
                "severity":               {"type": "integer"},
                "num_affected_users":     {"type": "integer"},
                "team_size":              {"type": "integer"},
                "hour_of_day":            {"type": "integer"},
                "day_of_week":            {"type": "integer"},
                "is_business_hours":      {"type": "integer"},
                "num_comments":           {"type": "integer"},
                "ml": {
                    "properties": {
                        "resolution_time_minutes_prediction": {"type": "float"},
                        "model_id":                           {"type": "keyword"},
                        "feature_importance": {
                            "type": "nested",
                            "properties": {
                                "feature_name": {"type": "keyword"},
                                "importance":   {"type": "float"},
                            }
                        }
                    }
                }
            }
        }
    })
    print(f"  acknowledged : {result.get('acknowledged')}")


# ── 4. Summary ────────────────────────────────────────────────────────────────
print(f"""
Setup complete.

  Pipeline : {PIPELINE_ID}
  Index    : {LIVE_INDEX}  (default_pipeline → {PIPELINE_ID})

Every document indexed to '{LIVE_INDEX}' will automatically receive
'ml.resolution_time_minutes_prediction' without any extra API call.

Next step:  python3 index_with_pipeline.py
""")
