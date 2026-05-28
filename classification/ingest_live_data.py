"""
Ingest a batch of new unlabeled server health snapshots and read back predictions.

Because 'server-health-live' has 'server-outage-inference' set as its
default_pipeline, every document is automatically scored by the trained
classifier on write — no labels needed, no extra API calls.

What this script does:
  1. Generates N unlabeled server health documents (no is_outage field)
     representing a realistic mix of healthy, borderline, and at-risk servers.
  2. Bulk-indexes them to 'server-health-live' in a single request.
     The inference pipeline fires on every document as part of the write.
  3. Queries the stored documents back by their _id values.
  4. Prints a prediction summary:
       - Counts and confidence histogram
       - Predicted-outage servers sorted by confidence
       - Top feature drivers per prediction (SHAP values)

Usage:
  python3 ingest_live_data.py            # default: 50 documents
  python3 ingest_live_data.py 200        # custom batch size

Target index: server-health-live
Pipeline:     server-outage-inference (applied automatically on write)
"""
import json
import random
import ssl
import sys
import time
import urllib.request
import urllib.error
import os
import uuid
from datetime import datetime, timezone
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
ES_URL      = os.environ["ELASTICSEARCH_URL"]
API_KEY     = os.environ["ELASTICSEARCH_API_KEY"]
LIVE_IDX    = "server-health-live"

N_DOCS = int(sys.argv[1]) if len(sys.argv) > 1 else 50

random.seed()   # unseeded — different docs every run

ENVIRONMENTS = ["production", "staging", "development"]
SERVER_ROLES = ["web", "database", "cache", "message-queue"]
DATACENTERS  = ["us-east-1", "eu-west-1", "ap-south-1"]

BATCH_ID  = uuid.uuid4().hex[:8]   # unique tag for this run
TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Document generators (no is_outage label — that's what we're predicting) ──

def healthy_doc(host_id):
    return {
        "@timestamp":         TIMESTAMP,
        "batch_id":           BATCH_ID,
        "host_name":          f"srv-{host_id:05d}",
        "environment":        random.choice(ENVIRONMENTS),
        "server_role":        random.choice(SERVER_ROLES),
        "datacenter":         random.choice(DATACENTERS),
        "cpu_usage_pct":      round(random.uniform(8, 60), 1),
        "memory_usage_pct":   round(random.uniform(15, 68), 1),
        "disk_io_util_pct":   round(random.uniform(4, 52), 1),
        "error_log_count":    random.randint(0, 7),
        "warning_log_count":  random.randint(0, 22),
        "restart_count":      random.randint(0, 1),
        "network_drop_pct":   round(random.uniform(0.0, 1.2), 2),
        "active_connections": random.randint(25, 480),
    }


def borderline_doc(host_id):
    return {
        "@timestamp":         TIMESTAMP,
        "batch_id":           BATCH_ID,
        "host_name":          f"srv-{host_id:05d}",
        "environment":        random.choice(ENVIRONMENTS),
        "server_role":        random.choice(SERVER_ROLES),
        "datacenter":         random.choice(DATACENTERS),
        "cpu_usage_pct":      round(random.uniform(65, 82), 1),
        "memory_usage_pct":   round(random.uniform(70, 85), 1),
        "disk_io_util_pct":   round(random.uniform(55, 75), 1),
        "error_log_count":    random.randint(9, 25),
        "warning_log_count":  random.randint(28, 65),
        "restart_count":      random.randint(1, 2),
        "network_drop_pct":   round(random.uniform(2.0, 6.0), 2),
        "active_connections": random.randint(30, 200),
    }


def at_risk_doc(host_id):
    """Resource exhaustion or connection-storm pattern — likely outage."""
    if random.random() < 0.60:
        # Resource exhaustion
        return {
            "@timestamp":         TIMESTAMP,
            "batch_id":           BATCH_ID,
            "host_name":          f"srv-{host_id:05d}",
            "environment":        random.choice(["production", "staging"]),
            "server_role":        random.choice(SERVER_ROLES),
            "datacenter":         random.choice(DATACENTERS),
            "cpu_usage_pct":      round(random.uniform(86, 99), 1),
            "memory_usage_pct":   round(random.uniform(89, 99), 1),
            "disk_io_util_pct":   round(random.uniform(78, 99), 1),
            "error_log_count":    random.randint(35, 260),
            "warning_log_count":  random.randint(70, 420),
            "restart_count":      random.randint(2, 9),
            "network_drop_pct":   round(random.uniform(9.0, 38.0), 2),
            "active_connections": random.randint(4, 35),
        }
    else:
        # Connection storm
        return {
            "@timestamp":         TIMESTAMP,
            "batch_id":           BATCH_ID,
            "host_name":          f"srv-{host_id:05d}",
            "environment":        random.choice(["production", "staging"]),
            "server_role":        random.choice(SERVER_ROLES),
            "datacenter":         random.choice(DATACENTERS),
            "cpu_usage_pct":      round(random.uniform(76, 96), 1),
            "memory_usage_pct":   round(random.uniform(68, 90), 1),
            "disk_io_util_pct":   round(random.uniform(22, 62), 1),
            "error_log_count":    random.randint(55, 310),
            "warning_log_count":  random.randint(110, 520),
            "restart_count":      random.randint(1, 6),
            "network_drop_pct":   round(random.uniform(16.0, 52.0), 2),
            "active_connections": random.randint(950, 5200),
        }


# Build batch — rough mix: 60 % healthy, 20 % borderline, 20 % at-risk
n_healthy    = max(1, int(N_DOCS * 0.60))
n_borderline = max(1, int(N_DOCS * 0.20))
n_at_risk    = N_DOCS - n_healthy - n_borderline

docs = (
    [healthy_doc(i)             for i in range(1,              n_healthy + 1)]
    + [borderline_doc(i)        for i in range(10_000,         10_000 + n_borderline)]
    + [at_risk_doc(i)           for i in range(20_000,         20_000 + n_at_risk)]
)
random.shuffle(docs)

print(f"Batch {BATCH_ID}  |  {TIMESTAMP}")
print(f"Generating {len(docs)} unlabeled documents")
print(f"  healthy    : {n_healthy}")
print(f"  borderline : {n_borderline}")
print(f"  at-risk    : {n_at_risk}")


# ── HTTP helper ───────────────────────────────────────────────────────────────

def request(method, path, body=None):
    url     = f"{ES_URL}{path}"
    data    = json.dumps(body).encode() if body else None
    headers = {"Authorization": f"ApiKey {API_KEY}", "Content-Type": "application/json"}
    req     = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} {method} {path}: {e.read().decode()}", file=sys.stderr)
        sys.exit(1)


# ── Bulk index to server-health-live ────────────────────────────────────────
# The default_pipeline on the index fires the inference processor for every
# document — no pipeline name needed in the bulk action or request params.

print(f"\nIndexing to '{LIVE_IDX}' (inference pipeline fires on write)...")

body = ""
for doc in docs:
    body += json.dumps({"index": {"_index": LIVE_IDX}}) + "\n"
    body += json.dumps(doc) + "\n"

req = urllib.request.Request(
    # refresh=wait_for: ES waits for the next refresh cycle before returning,
    # so the documents are immediately searchable after the bulk call completes.
    f"{ES_URL}/_bulk?refresh=wait_for",
    data=body.encode(),
    headers={
        "Authorization": f"ApiKey {API_KEY}",
        "Content-Type": "application/x-ndjson",
    },
    method="POST",
)
with urllib.request.urlopen(req, context=SSL_CONTEXT) as resp:
    bulk_resp = json.loads(resp.read())

errors = [item for item in bulk_resp["items"] if item.get("index", {}).get("error")]
if errors:
    print(f"  {len(errors)} bulk error(s):", file=sys.stderr)
    for e in errors[:3]:
        print(f"    {e['index']['error']}", file=sys.stderr)

# Collect the _id values assigned by Elasticsearch
indexed_ids = [
    item["index"]["_id"]
    for item in bulk_resp["items"]
    if "index" in item and not item["index"].get("error")
]
print(f"  {len(indexed_ids)} documents written")


# ── Query back by _id to read the ml.* fields ─────────────────────────────
# A brief wait is not needed — the inference processor is synchronous:
# the document is fully enriched before the bulk response is returned.

print(f"\nRetrieving predictions...")
search_result = request(
    "POST",
    f"/{LIVE_IDX}/_search",
    {
        "size": len(indexed_ids),
        "query": {"ids": {"values": indexed_ids}},
        "sort": [{"ml.prediction_probability": {"order": "desc"}}],
        "_source": [
            "host_name", "environment", "server_role", "datacenter",
            "cpu_usage_pct", "memory_usage_pct", "disk_io_util_pct",
            "error_log_count", "warning_log_count", "restart_count",
            "network_drop_pct", "active_connections",
            "ml.predicted_is_outage", "ml.prediction_probability",
            "ml.feature_importance", "tags",
        ],
    },
)

hits = [h["_source"] for h in search_result.get("hits", {}).get("hits", [])]


# ── Summary ───────────────────────────────────────────────────────────────────

predicted_outage  = [h for h in hits if h.get("ml", {}).get("predicted_is_outage") is True]
predicted_healthy = [h for h in hits if h.get("ml", {}).get("predicted_is_outage") is False]
failed_inference  = [h for h in hits if "inference_failed" in h.get("tags", [])]

print(f"\n{'─'*60}")
print(f"  Prediction summary  (batch: {BATCH_ID})")
print(f"{'─'*60}")
print(f"  Documents scored      : {len(hits)}")
print(f"  Predicted OUTAGE      : {len(predicted_outage)}"
      + (f"  ← {len(predicted_outage)/len(hits)*100:.0f}%" if hits else ""))
print(f"  Predicted healthy     : {len(predicted_healthy)}")
if failed_inference:
    print(f"  Inference failures    : {len(failed_inference)}  (tagged inference_failed)")

# Confidence histogram for outage predictions
if predicted_outage:
    buckets = {"≥0.90": 0, "0.75–0.90": 0, "0.50–0.75": 0}
    for h in predicted_outage:
        p = h.get("ml", {}).get("prediction_probability", 0)
        if p >= 0.90:
            buckets["≥0.90"] += 1
        elif p >= 0.75:
            buckets["0.75–0.90"] += 1
        else:
            buckets["0.50–0.75"] += 1
    print(f"\n  Outage confidence breakdown:")
    for label, count in buckets.items():
        bar = "█" * count
        print(f"    {label}   {count:>3}  {bar}")


# ── Predicted-outage details ──────────────────────────────────────────────────

def top_driver(feature_importance, predicted_class):
    """Return the feature name with the highest SHAP magnitude for the predicted class."""
    best_name, best_val = None, 0.0
    for fi in feature_importance or []:
        for c in fi.get("classes", []):
            if c.get("class_name") == predicted_class:
                if abs(c.get("importance", 0)) > abs(best_val):
                    best_name = fi.get("feature_name")
                    best_val  = c["importance"]
    return best_name, best_val


if predicted_outage:
    print(f"\n{'─'*60}")
    print(f"  Predicted-outage servers  (sorted by confidence)")
    print(f"{'─'*60}")
    print(f"  {'Host':<14} {'Env':<12} {'Role':<14} {'Conf':>6}  Top driver")
    print(f"  {'─'*13} {'─'*11} {'─'*13} {'─'*6}  {'─'*24}")

    for h in predicted_outage:         # already sorted by probability desc
        ml   = h.get("ml", {})
        prob = ml.get("prediction_probability", 0)
        feat_imp = ml.get("feature_importance", [])
        driver, driver_val = top_driver(feat_imp, True)

        driver_str = f"{driver}  {driver_val:+.2f}" if driver else "—"
        print(
            f"  {h.get('host_name','?'):<14}"
            f" {h.get('environment','?'):<12}"
            f" {h.get('server_role','?'):<14}"
            f" {prob:>5.1%}"
            f"  {driver_str}"
        )

    # Full feature breakdown for the top-confidence outage server
    top = predicted_outage[0]
    top_ml = top.get("ml", {})
    print(f"\n  Feature importance detail — {top.get('host_name')} (p={top_ml.get('prediction_probability',0):.4f})")

    def class_importance(fi_entry, cls):
        for c in fi_entry.get("classes", []):
            if c.get("class_name") == cls:
                return c.get("importance", 0.0)
        return 0.0

    fi_entries = top_ml.get("feature_importance", [])
    ranked = sorted(fi_entries, key=lambda x: abs(class_importance(x, True)), reverse=True)

    for fi in ranked:
        val    = class_importance(fi, True)
        bar    = "▶" * min(int(abs(val) * 3), 20)
        sign   = "+" if val >= 0 else ""
        metric = fi.get("feature_name", "?")
        raw    = top.get(metric, "—")
        print(f"    {metric:<25}  {sign}{val:>6.3f}  {bar}  (value: {raw})")

print(f"\n{'─'*60}")
print(f"  All {len(indexed_ids)} documents stored in '{LIVE_IDX}'")
print(f"  Filter this batch:  batch_id = \"{BATCH_ID}\"")
print(f"{'─'*60}")
