# Elasticsearch ML Classification — Server Outage Prediction

A minimal example of Elasticsearch's [Data Frame Analytics](https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-df-analytics-overview.html) classification, using a synthetic server health dataset to predict outages.

## What it does

1. **Generates** 2,400 labeled records (1,800 normal + 600 outage) across 2,100 synthetic servers and bulk-indexes them into `server-health-metrics`.
2. **Creates and starts** a classification data frame analytics job that trains a boosted-tree model to predict `is_outage` (true/false) from ten health-metric features.
3. **Writes results** to `server-outage-predictions`, where every document gets:
   - `ml.predicted_is_outage` — predicted class
   - `ml.prediction_probability` — model confidence (0–1)
   - `ml.top_classes[]` — probability for each class
   - `ml.feature_importance[]` — SHAP values for the top 5 contributing features
   - `ml.is_training` — whether the row was used for training (80 %) or held out for evaluation (20 %)
4. **Deploys the trained model as an ingest pipeline** (`server-outage-inference`) so any new document indexed to `server-health-live` is scored automatically — no labels needed, no batch job to wait for.

## Prerequisites

- Python 3.8+
- An Elastic Cloud Serverless project (Observability or Elasticsearch tier) **or** a self-managed Elasticsearch 7.8+ cluster with a Platinum/Enterprise licence
- No third-party Python packages required — only the standard library
- **SSL**: Certificate verification is disabled by default. All platforms and proxy setups work without any SSL configuration.

## Setup

### 1. Create a `.env` file

Create `.env` in the **repo root** (`ml-jobs/`). The scripts walk up from their own location and load the first `.env` they find, so a single file at the root covers all jobs in the repo.

```bash
# ml-jobs/.env
ELASTICSEARCH_URL=https://<your-endpoint>.es.<region>.gcp.elastic.cloud
ELASTICSEARCH_API_KEY=<your-api-key>
```

Both `.env` and `.elastic-credentials` are gitignored at the repo root.

### 2. Required API key privileges

| Privilege | Scope |
|---|---|
| `manage_ml` | cluster |
| `create_index`, `index`, `read` | `server-health-metrics`, `server-outage-predictions` |

### 3. (Optional) Export variables directly

Shell exports take precedence over both `.env` and `.elastic-credentials`:

```bash
export ELASTICSEARCH_URL=https://<your-endpoint>.es.<region>.gcp.elastic.cloud
export ELASTICSEARCH_API_KEY=<your-api-key>
```

## Usage

### Step 1 — Index sample data

```bash
python3 generate_sample_data.py
```

Expected output:
```
Total records: 2400
  Normal  (is_outage=false): 1800
  Outage  (is_outage=true):  600

Creating index 'server-health-metrics'...
  acknowledged: True

Indexing into 'server-health-metrics'...
  Indexed   500/2400
  Indexed  1000/2400
  Indexed  1500/2400
  Indexed  2000/2400
  Indexed  2400/2400

Done. Next step:
  python3 setup_classification_job.py
```

### Step 2 — Create and start the ML job

```bash
python3 setup_classification_job.py
```

Expected output:
```
Source index 'server-health-metrics': 2,400 documents

Creating job 'server-outage-classifier'...
  id:         server-outage-classifier
  dest index: server-outage-predictions

Starting job 'server-outage-classifier'...
  acknowledged: True
```

### Step 3 — Poll job status

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/data_frame/analytics/server-outage-classifier/_stats" \
  | python3 -m json.tool | grep -E "state|progress_percent"
```

The job typically completes in under a minute. State transitions: `stopped` → `started` → `reindexing` → `analyzing` → `stopped` (at 100%).

### Step 4 — Deploy the trained model as an ingest pipeline

Once the job is `stopped` at 100 %, deploy the model for real-time inference:

```bash
python3 setup_inference_pipeline.py
```

This script:
1. Verifies the trained model exists and prints its input feature names
2. Creates the `server-outage-inference` ingest pipeline
3. **Simulates** the pipeline on three test documents (healthy / borderline / outage) — no data written
4. Creates the `server-health-live` index with the pipeline set as `default_pipeline`

Expected output (abridged):
```
Checking trained model 'server-outage-classifier-...'...
  Found. Input features (10): active_connections, cpu_usage_pct, ...

Creating pipeline 'server-outage-inference'...
  acknowledged: True

Simulating pipeline on 3 test documents (dry-run, no writes)...

  [1] Healthy server
       predicted_is_outage  : False
       prediction_probability: 0.9818
       top feature importance (for predicted class):
         warning_log_count          +1.0115
         network_drop_pct           +0.6258

  [2] Borderline server
       predicted_is_outage  : True
       prediction_probability: 0.5050
       ...

  [3] Outage server (resource exhaustion)
       predicted_is_outage  : True
       prediction_probability: 0.9304
       ...

Creating live index 'server-health-live' (default_pipeline: server-outage-inference)...
  acknowledged: True
```

### Step 5 — Inspect the trained model

After the job completes, find the trained model ID:

After the job completes, find the trained model ID:

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/trained_models?size=10" \
  | python3 -m json.tool | grep model_id
```

Get feature importance summary (which metrics matter most):

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/data_frame/analytics/server-outage-classifier/_stats" \
  | python3 -m json.tool | grep -A 30 "feature_importance_baseline"
```

### Step 5 — Query predictions

Top predicted outage servers (highest confidence):

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/server-outage-predictions/_search" \
  -d '{
    "size": 10,
    "query": {"term": {"ml.predicted_is_outage": true}},
    "sort": [{"ml.prediction_probability": {"order": "desc"}}],
    "_source": [
      "host_name", "environment", "server_role",
      "cpu_usage_pct", "memory_usage_pct", "disk_io_util_pct",
      "error_log_count", "restart_count", "network_drop_pct",
      "is_outage", "ml.predicted_is_outage",
      "ml.prediction_probability", "ml.feature_importance"
    ]
  }' | python3 -m json.tool
```

Evaluate model accuracy on the held-out 20 % test set:

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/server-outage-predictions/_search" \
  -d '{
    "size": 0,
    "query": {"term": {"ml.is_training": false}},
    "aggs": {
      "correct_predictions": {
        "filter": {
          "script": {
            "script": "doc['"'"'is_outage'"'"'].value == doc['"'"'ml.predicted_is_outage'"'"'].value"
          }
        }
      },
      "total_test": {"value_count": {"field": "host_name"}}
    }
  }' | python3 -m json.tool
```

False positives (predicted outage, actually healthy):

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/server-outage-predictions/_search" \
  -d '{
    "size": 5,
    "query": {
      "bool": {
        "filter": [
          {"term": {"ml.predicted_is_outage": true}},
          {"term": {"is_outage": false}},
          {"term": {"ml.is_training": false}}
        ]
      }
    },
    "_source": [
      "host_name", "cpu_usage_pct", "memory_usage_pct",
      "error_log_count", "is_outage", "ml.predicted_is_outage",
      "ml.prediction_probability"
    ]
  }' | python3 -m json.tool
```

## Sample dataset

| Field | Normal range | Outage range (resource exhaustion) | Outage range (connection storm) |
|---|---|---|---|
| `cpu_usage_pct` | 10–65 % | 85–99 % | 75–95 % |
| `memory_usage_pct` | 20–70 % | 88–99 % | 65–88 % |
| `disk_io_util_pct` | 5–55 % | 75–99 % | 20–60 % |
| `error_log_count` | 0–8 | 30–250 | 50–300 |
| `warning_log_count` | 0–25 | 60–400 | 100–500 |
| `restart_count` | 0–1 | 2–8 | 1–5 |
| `network_drop_pct` | 0–1.5 % | 8–35 % | 15–50 % |
| `active_connections` | 20–500 | 5–40 | 900–5,000 |

Categorical features:

| Field | Values |
|---|---|
| `environment` | `production`, `staging`, `development` |
| `server_role` | `web`, `database`, `cache`, `message-queue` |

Two outage failure modes are injected so the model learns that outages can look different:

| Failure mode | Share | Signature |
|---|---|---|
| Resource exhaustion | 65 % of outages | All three resource metrics > 85 %, error flood, low connections |
| Connection storm | 35 % of outages | CPU spike, extreme connection count (900–5,000), high packet drop |

## ML job configuration

| Parameter | Value |
|---|---|
| Source index | `server-health-metrics` |
| Dest index | `server-outage-predictions` |
| Dependent variable | `is_outage` (boolean) |
| Algorithm | Boosted tree (auto) |
| Training split | 80 % train / 20 % test |
| Prediction field | `ml.predicted_is_outage` |
| Feature importance | Top 5 features (SHAP values) |
| `model_memory_limit` | `100mb` |

`num_top_feature_importance_values: 5` writes SHAP values to each result document, showing which features contributed most to each individual prediction. This makes every prediction explainable.

## Files

| File | Purpose |
|---|---|
| `generate_sample_data.py` | Creates the `server-health-metrics` index (with explicit mapping) and bulk-loads 2,400 labeled records |
| `setup_classification_job.py` | Creates and starts the `server-outage-classifier` analytics job |
| `setup_inference_pipeline.py` | Discovers the trained model, creates the `server-outage-inference` ingest pipeline, simulates 3 test docs, and creates `server-health-live` |
| `ingest_live_data.py` | Generates unlabeled server health snapshots, bulk-indexes to `server-health-live` (pipeline enriches on write), then prints a prediction summary with feature drivers |

### Step 6 — Ingest live data and read predictions

```bash
python3 ingest_live_data.py          # default: 50 documents
python3 ingest_live_data.py 200      # custom batch size
```

The script generates unlabeled server health snapshots (no `is_outage` label), bulk-indexes them to `server-health-live`, and immediately prints a prediction summary. The inference pipeline fires on every document **synchronously as part of the write** — the `ml.*` fields are stored before the bulk response returns.

Expected output:
```
Batch a3f19c02  |  2026-05-28T06:47:25Z
Generating 50 unlabeled documents
  healthy    : 30
  borderline : 10
  at-risk    : 10

Indexing to 'server-health-live' (inference pipeline fires on write)...
  50 documents written

Retrieving predictions...

────────────────────────────────────────────────────────────
  Prediction summary  (batch: a3f19c02)
────────────────────────────────────────────────────────────
  Documents scored      : 50
  Predicted OUTAGE      : 20  ← 40%
  Predicted healthy     : 30

  Outage confidence breakdown:
    ≥0.90    10  ██████████
    0.75–0.90     0
    0.50–0.75    10  ██████████

────────────────────────────────────────────────────────────
  Predicted-outage servers  (sorted by confidence)
────────────────────────────────────────────────────────────
  Host           Env          Role           Conf  Top driver
  srv-20006      production   message-queue  93.0%  warning_log_count  +3.05
  srv-20000      staging      cache          93.0%  warning_log_count  +3.05
  ...

  Feature importance detail — srv-20006 (p=0.9304)
    warning_log_count          + 3.050  ▶▶▶▶▶▶▶▶▶  (value: 450)
    network_drop_pct           + 1.892  ▶▶▶▶▶  (value: 41.24)
```

Each batch gets a unique `batch_id` so you can filter results in Kibana or a follow-up query.

---

## Real-time ingestion after pipeline setup

Any document indexed to `server-health-live` is scored automatically — no `is_outage` label needed:

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/server-health-live/_doc" \
  -d '{
    "host_name": "srv-prod-042",
    "environment": "production",
    "server_role": "database",
    "cpu_usage_pct": 91.2,
    "memory_usage_pct": 95.0,
    "disk_io_util_pct": 88.0,
    "error_log_count": 145,
    "warning_log_count": 280,
    "restart_count": 4,
    "network_drop_pct": 18.5,
    "active_connections": 22
  }'
```

The stored document will contain all original fields **plus** `ml.*` fields written by the pipeline:

```json
{
  "host_name": "srv-prod-042",
  "cpu_usage_pct": 91.2,
  "...": "...",
  "ml": {
    "predicted_is_outage": true,
    "prediction_probability": 0.9304,
    "prediction_score": 0.9304,
    "top_classes": [
      {"class_name": true,  "class_probability": 0.9304},
      {"class_name": false, "class_probability": 0.0696}
    ],
    "feature_importance": [
      {"feature_name": "warning_log_count", "classes": [...]},
      {"feature_name": "network_drop_pct",  "classes": [...]}
    ]
  }
}
```

Query all predicted outages:

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/server-health-live/_search" \
  -d '{
    "query": {"term": {"ml.predicted_is_outage": true}},
    "sort": [{"ml.prediction_probability": {"order": "desc"}}]
  }' | python3 -m json.tool
```

## Cleanup

```bash
# Stop and delete the ML job
curl -s -X POST -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/data_frame/analytics/server-outage-classifier/_stop"

curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/data_frame/analytics/server-outage-classifier"

# Delete the ingest pipeline
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ingest/pipeline/server-outage-inference"

# Delete all indices
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/server-health-metrics,server-outage-predictions,server-health-live"
```

---

## Real-world IT classification use cases

### 1. Server / VM outage prediction

**Goal:** Predict whether a host will experience an outage in the next observation window.

**Entity:** `host_id`

**Features:** CPU utilization, memory pressure, disk I/O, error log rate, restart count, network drop rate, open file descriptors

**Why classification fits:** You have historical labeled incidents. Training on past outages lets the model score live servers before they go down.

### 2. Incident priority prediction

**Goal:** Automatically classify incoming incidents as P1 / P2 / P3 / P4.

**Entity:** `incident_id`

**Features:** Affected user count, customer tier, service criticality score, symptom keyword embedding, response-time SLA hours, previous escalation count

**Why classification fits:** Support engineers already assign priorities — those labels are the training data. A trained model can triage new tickets instantly.

### 3. Change-related failure prediction

**Goal:** Predict whether a scheduled change (deploy, config push) will cause a rollback or incident.

**Entity:** `change_id`

**Features:** Change size (lines diff), services affected count, deployer experience score, time-of-day, environment, recent incident rate for affected service, test coverage delta

**Why classification fits:** Every post-change incident or rollback is a labeled training example.

### 4. Alert actionability classification

**Goal:** Classify alerts as actionable (true positive requiring human response) vs noise (false positive / auto-resolved).

**Entity:** `alert_id`

**Features:** Alert rule hit count in last 7 days, alert duration, severity, service name, infrastructure tier, time-since-last-similar-alert, correlation with other open alerts

**Why classification fits:** Alert outcome (paged / auto-resolved / ignored) is available in historical data.

### 5. Security event classification

**Goal:** Classify network or host events as benign, suspicious, or malicious.

**Entity:** `event_id` or aggregated `user_id` / `host_id`

**Features:** Login failure count, distinct source IPs, off-hours access flag, data volume, privilege escalation events, process signature score

**Why classification fits:** Prior incident investigations and SIEM labels supply ground truth.

### 6. Hardware failure prediction

**Goal:** Classify storage drives or network interfaces as healthy vs likely to fail within 30 days.

**Entity:** `device_id`

**Features:** SMART attribute deltas (reallocated sectors, uncorrectable errors), temperature, power-on hours, I/O error rate, link-flap count

**Why classification fits:** SMART data + physical failure logs create a well-labeled training set.

### 7. SLA breach prediction

**Goal:** Classify in-flight support tickets as likely to breach SLA or not.

**Entity:** `ticket_id`

**Features:** Time open, reopen count, number of handoffs, customer tier, category, initial response time, comments per hour

**Why classification fits:** Closed tickets have a known SLA outcome — breach / met.

### 8. Patch compliance classification

**Goal:** Classify hosts as compliant, at-risk, or non-compliant before a compliance audit.

**Entity:** `host_id`

**Features:** Days since last patch, critical CVE count, OS version, patch cadence history, environment, owner team patch rate

**Why classification fits:** Compliance audit results from prior cycles provide labels.

## What makes a good feature set for classification

- Use **numeric** (float/integer) and **low-cardinality keyword** features. Avoid free-text or high-cardinality identifiers as features.
- The **dependent variable** must be boolean, keyword, or integer type — not text.
- Aim for **class balance** in training data. If outage events are rare (< 5 %), use `training_percent` tuning or pre-filter to balance classes before the job runs.
- Exclude **identifier fields** (e.g., `host_name`, `incident_id`) from `analyzed_fields` — they prevent the model from generalizing to unseen entities.
- Use `num_top_feature_importance_values` to get per-prediction SHAP values — essential for explaining why the model flagged a specific server or ticket.
