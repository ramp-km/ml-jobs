"""
Set up real-time inference using the trained server-outage-classifier model.

What this script does:
  1. Creates an ingest pipeline 'server-outage-inference' that calls the trained
     classification model on every indexed document.
  2. Validates the pipeline by simulating three test documents
     (healthy, borderline, outage) through _ingest/pipeline/_simulate —
     no documents are written to any index during this step.
  3. Creates 'server-health-live' with the pipeline as its default_pipeline,
     so every new document is scored automatically on write.

After setup, any process indexing to 'server-health-live' gets these fields
added automatically by the pipeline:
  ml.predicted_is_outage      -- predicted class (true / false)
  ml.prediction_probability   -- model confidence (0.0–1.0)
  ml.top_classes[]            -- probability for both classes
  ml.feature_importance[]     -- SHAP values for top 5 features

Model:      server-outage-classifier-1779949908206
Pipeline:   server-outage-inference
Live index: server-health-live
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

SSL_CONTEXT  = ssl._create_unverified_context()
ES_URL       = os.environ["ELASTICSEARCH_URL"]
API_KEY      = os.environ["ELASTICSEARCH_API_KEY"]

MODEL_ID     = "server-outage-classifier-1779949908206"
PIPELINE_ID  = "server-outage-inference"
LIVE_IDX     = "server-health-live"


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
        if e.code == 400 and "resource_already_exists_exception" in err:
            return {"_already_exists": True}
        if e.code == 409:
            return {"_already_exists": True}
        print(f"HTTP {e.code} {method} {path}: {err}", file=sys.stderr)
        sys.exit(1)


# ── 1. Verify the trained model exists ───────────────────────────────────────
print(f"Checking trained model '{MODEL_ID}'...")
result = request("GET", f"/_ml/trained_models/{MODEL_ID}")
configs = result.get("trained_model_configs", [])
if not configs:
    print(f"  Model not found. Has the classification job completed?", file=sys.stderr)
    print(f"  Check: GET /_ml/data_frame/analytics/server-outage-classifier/_stats", file=sys.stderr)
    sys.exit(1)

model_fields = configs[0].get("input", {}).get("field_names", [])
print(f"  Found. Input features ({len(model_fields)}): {', '.join(model_fields)}")


# ── 2. Create the ingest pipeline ────────────────────────────────────────────
# inference_config.classification:
#   results_field              → ml.predicted_is_outage   (matches training job output)
#   top_classes_results_field  → ml.top_classes
#   num_top_classes            → 2  (both true and false with probabilities)
#   num_top_feature_importance_values → 5 SHAP values per prediction
#
# on_failure: append "inference_failed" tag so bad docs are filterable,
# never silently dropped.

pipeline_config = {
    "description": "Score server health metrics with the trained outage classifier",
    "processors": [
        {
            "inference": {
                "model_id": MODEL_ID,
                "target_field": "ml",
                "field_mappings": {},
                "inference_config": {
                    "classification": {
                        "results_field":                     "predicted_is_outage",
                        "top_classes_results_field":         "top_classes",
                        "num_top_classes":                   2,
                        "num_top_feature_importance_values": 5,
                    }
                },
                "on_failure": [
                    {
                        "append": {
                            "field": "tags",
                            "value": "inference_failed",
                        }
                    }
                ],
            }
        }
    ],
}

print(f"\nCreating pipeline '{PIPELINE_ID}'...")
# PUT pipeline is always idempotent — overwrites silently, never 409
result = request("PUT", f"/_ingest/pipeline/{PIPELINE_ID}", pipeline_config)
print(f"  acknowledged: {result.get('acknowledged')}")


# ── 3. Simulate the pipeline on three test documents ─────────────────────────
# No documents are written — _simulate is a dry-run endpoint.
# Three scenarios: clearly healthy, borderline, clearly outage.

test_docs = [
    {
        "label": "Healthy server",
        "_source": {
            "host_name":          "srv-live-001",
            "environment":        "production",
            "server_role":        "web",
            "cpu_usage_pct":      28.4,
            "memory_usage_pct":   42.1,
            "disk_io_util_pct":   18.7,
            "error_log_count":    2,
            "warning_log_count":  6,
            "restart_count":      0,
            "network_drop_pct":   0.3,
            "active_connections": 215,
        },
    },
    {
        "label": "Borderline server",
        "_source": {
            "host_name":          "srv-live-002",
            "environment":        "staging",
            "server_role":        "database",
            "cpu_usage_pct":      74.2,
            "memory_usage_pct":   79.8,
            "disk_io_util_pct":   66.0,
            "error_log_count":    14,
            "warning_log_count":  42,
            "restart_count":      1,
            "network_drop_pct":   3.8,
            "active_connections": 85,
        },
    },
    {
        "label": "Outage server (resource exhaustion)",
        "_source": {
            "host_name":          "srv-live-003",
            "environment":        "production",
            "server_role":        "database",
            "cpu_usage_pct":      95.1,
            "memory_usage_pct":   97.4,
            "disk_io_util_pct":   92.3,
            "error_log_count":    183,
            "warning_log_count":  347,
            "restart_count":      5,
            "network_drop_pct":   24.7,
            "active_connections": 12,
        },
    },
]

simulate_payload = {"docs": [{"_source": d["_source"]} for d in test_docs]}

print(f"\nSimulating pipeline on 3 test documents (dry-run, no writes)...")
sim_result = request("POST", f"/_ingest/pipeline/{PIPELINE_ID}/_simulate", simulate_payload)

for i, (test_doc, sim_doc) in enumerate(zip(test_docs, sim_result.get("docs", []))):
    label  = test_doc["label"]
    source = sim_doc.get("doc", {}).get("_source", {})
    ml     = source.get("ml", {})
    tags   = source.get("tags", [])

    predicted   = ml.get("predicted_is_outage", "N/A")
    probability = ml.get("prediction_probability", None)
    top_classes = ml.get("top_classes", [])
    feature_imp = ml.get("feature_importance", [])

    print(f"\n  [{i+1}] {label}")
    if "inference_failed" in tags:
        print(f"       ⚠  inference failed — check model and field names")
        continue

    prob_str = f"{probability:.4f}" if probability is not None else "N/A"
    print(f"       predicted_is_outage  : {predicted}")
    print(f"       prediction_probability: {prob_str}")

    if top_classes:
        print(f"       top_classes:")
        for tc in top_classes:
            print(f"         {tc.get('class_name')!s:<8}  p={tc.get('class_probability', 0):.4f}")

    if feature_imp:
        # Classification feature importance has per-class SHAP values under
        # fi["classes"][{"class_name": ..., "importance": ...}].
        # Extract the importance for the predicted class to rank features.
        def class_importance(fi_entry, predicted):
            for c in fi_entry.get("classes", []):
                if c.get("class_name") == predicted:
                    return c.get("importance", 0.0)
            return fi_entry.get("importance", 0.0)   # regression fallback

        ranked = sorted(
            feature_imp,
            key=lambda x: abs(class_importance(x, predicted)),
            reverse=True,
        )
        print(f"       top feature importance (for predicted class):")
        for fi in ranked[:5]:
            val  = class_importance(fi, predicted)
            sign = "+" if val >= 0 else ""
            print(f"         {fi.get('feature_name'):<25}  {sign}{val:.4f}")


# ── 4. Create the live index with default_pipeline ───────────────────────────
# Incoming real-time documents do NOT need is_outage — the pipeline predicts it.
# ml.* fields are written by the inference processor automatically.

live_index_config = {
    "settings": {
        "default_pipeline": PIPELINE_ID,
    },
    "mappings": {
        "properties": {
            # Source fields (same schema as server-health-metrics)
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
            "tags":               {"type": "keyword"},
            # ML output fields written by the inference processor
            "ml": {
                "properties": {
                    "predicted_is_outage":    {"type": "boolean"},
                    "prediction_probability": {"type": "float"},
                    "prediction_score":       {"type": "float"},
                    "top_classes": {
                        "type": "nested",
                        "properties": {
                            "class_name":        {"type": "keyword"},
                            "class_probability": {"type": "float"},
                            "class_score":       {"type": "float"},
                        },
                    },
                    "feature_importance": {
                        "type": "nested",
                        "properties": {
                            "feature_name": {"type": "keyword"},
                            "importance":   {"type": "float"},
                        },
                    },
                }
            },
        }
    },
}

print(f"\nCreating live index '{LIVE_IDX}' (default_pipeline: {PIPELINE_ID})...")
result = request("PUT", f"/{LIVE_IDX}", live_index_config)
if result.get("_already_exists"):
    print("  Index already exists — skipping creation.")
else:
    print(f"  acknowledged: {result.get('acknowledged')}")

print(f"""
Setup complete.

Any document indexed to '{LIVE_IDX}' is automatically scored.
No labels needed — the pipeline predicts ml.predicted_is_outage on write.

Example — index a single document:
  POST /{LIVE_IDX}/_doc
  {{
    "host_name": "srv-prod-099",
    "environment": "production",
    "server_role": "web",
    "cpu_usage_pct": 91.2,
    "memory_usage_pct": 95.0,
    "disk_io_util_pct": 88.0,
    "error_log_count": 145,
    "warning_log_count": 280,
    "restart_count": 4,
    "network_drop_pct": 18.5,
    "active_connections": 22
  }}

Query predicted outages:
  POST /{LIVE_IDX}/_search
  {{
    "query": {{"term": {{"ml.predicted_is_outage": true}}}},
    "sort": [{{"ml.prediction_probability": {{"order": "desc"}}}}]
  }}
""")
