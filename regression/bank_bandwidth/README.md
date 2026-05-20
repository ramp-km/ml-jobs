# Elasticsearch ML Regression — Bank Branch Network Bandwidth

Predicts the network bandwidth (Mbps) required for a bank branch using Elasticsearch's
[Data Frame Analytics](https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-df-analytics-overview.html)
regression. Three operational features — employees, customers, and transactions — are used
to train the model. The same trained model is then surfaced in two ways:

- **On-demand** via the `_infer` API (`realtime_inference.py`)
- **Automatically at index time** via an ingest pipeline (`index_with_pipeline.py`)

## What it does

| Step | Script | Description |
|---|---|---|
| 1 | `generate_sample_data.py` | Generates 500 synthetic branch records and indexes them into `bank-branches` |
| 2 | `setup_regression_job.py` | Creates and starts the `branch-bandwidth-regression` Data Frame Analytics job |
| 3 | `deploy_model.py` | Verifies the trained model is ready for inference and prints a smoke-test result |
| 4 | `realtime_inference.py` | Calls `_infer` directly to predict bandwidth for any branch on demand |
| 5 | `setup_ingest_pipeline.py` | Creates the `branch-bandwidth-prediction` ingest pipeline and the `bank-branches-live` index |
| 6 | `index_with_pipeline.py` | Indexes new branch documents — the pipeline adds predictions automatically at write time |

## Prerequisites

- Python 3.8+
- An Elastic Cloud Serverless project **or** a self-managed Elasticsearch 7.8+ cluster with a Platinum/Enterprise licence
- No third-party Python packages required — only the standard library

## Setup

### 1. Credentials

Create `.env` in the **repo root** (`ml-jobs/`). All scripts walk up from their own location, so a single file at the root covers every job.

```bash
# ml-jobs/.env
ELASTICSEARCH_URL=https://<your-endpoint>.es.<region>.gcp.elastic.cloud
ELASTICSEARCH_API_KEY=<your-api-key>
```

### 2. Required API key privileges

| Privilege | Scope |
|---|---|
| `manage_ml` | cluster |
| `manage_ingest_pipelines` | cluster |
| `create_index`, `index`, `read` | `bank-branches`, `bank-branches-regression-results`, `bank-branches-live` |

---

## Usage

### Step 1 — Index sample data

```bash
python3 generate_sample_data.py
```

```
Total records to index: 500
  Indexed 250/500
  Indexed 500/500
Done.
```

### Step 2 — Create and start the ML job

```bash
python3 setup_regression_job.py
```

```
Documents in bank-branches: 500

Creating job 'branch-bandwidth-regression'...
  id: branch-bandwidth-regression
  dest index: bank-branches-regression-results

Starting job 'branch-bandwidth-regression'...
  acknowledged: True
```

### Step 3 — Poll job status

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/data_frame/analytics/branch-bandwidth-regression/_stats" \
  | python3 -m json.tool | grep -E "state|progress_percent"
```

State transitions: `stopped` → `started` → `reindexing` → `analyzing` → `writing_results` → `stopped` (100 %)

Wait until all phases report `100` before proceeding.

### Step 4 — Query batch results

Top 10 branches with the highest predicted bandwidth need (test set):

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/bank-branches-regression-results/_search" \
  -d '{
    "size": 10,
    "query": {"term": {"ml.is_training": false}},
    "sort": [{"ml.bandwidth_mbps_prediction": {"order": "desc"}}],
    "_source": ["branch_id", "branch_tier", "num_employees", "num_customers",
                "num_transactions", "bandwidth_mbps", "ml.bandwidth_mbps_prediction"]
  }' | python3 -m json.tool
```

Average predicted vs actual bandwidth by branch tier:

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/bank-branches-regression-results/_search" \
  -d '{
    "size": 0,
    "query": {"term": {"ml.is_training": false}},
    "aggs": {
      "by_tier": {
        "terms": {"field": "branch_tier", "size": 10},
        "aggs": {
          "avg_actual":    {"avg": {"field": "bandwidth_mbps"}},
          "avg_predicted": {"avg": {"field": "ml.bandwidth_mbps_prediction"}}
        }
      }
    }
  }' | python3 -m json.tool
```

Branches that may be under-provisioned (predicted > actual by more than 20 Mbps):

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/bank-branches-regression-results/_search" \
  -d '{
    "size": 20,
    "query": {
      "bool": {
        "must": [
          {"term": {"ml.is_training": false}},
          {"script": {
            "script": "doc['\''ml.bandwidth_mbps_prediction'\''].value - doc['\''bandwidth_mbps'\''].value > 20"
          }}
        ]
      }
    },
    "_source": ["branch_id", "branch_tier", "num_employees", "num_customers",
                "num_transactions", "bandwidth_mbps", "ml.bandwidth_mbps_prediction"]
  }' | python3 -m json.tool
```

---

## Real-time inference

The regression job produces a `tree_ensemble` model that is available for real-time inference
immediately after training — no separate deployment step is required.

### Step 5 — Verify the model

```bash
python3 deploy_model.py
```

```
Looking up trained model for job 'branch-bandwidth-regression'...
  model_id   : branch-bandwidth-regression-<timestamp>
  model_type : tree_ensemble   (tree_ensemble models serve inference immediately)

Verifying model stats...
  model size     : 68 KB
  inference calls: 0 so far

Running smoke-test inference (suburban branch)...
  Input  : employees=15, customers=300, transactions=600
  Output : 174.8 Mbps  ✓

Model is ready for real-time inference.
```

The script resolves the model ID dynamically from the job tag (the ID contains a
timestamp suffix and is never hard-coded).

### Step 6 — On-demand inference

```bash
# Predict for built-in sample branches across all tiers
python3 realtime_inference.py

# Predict for a single branch
python3 realtime_inference.py --employees 40 --customers 750 --transactions 1500
```

```
Branch                                      Employees  Customers     Txns     Predicted BW   Tier
--------------------------------------------------------------------------------------------------------------
Rural branch (Coorg)                                5         80      150         55.4 Mbps   Standard (suburban)
  num_customers: -152.0 Mbps
  num_transactions: -136.5 Mbps
  num_employees: -94.2 Mbps
Flagship branch (Nariman Point)                    90       2000     4500       1161.0 Mbps   Premium (flagship)
  num_customers: +318.7 Mbps
  num_transactions: +223.5 Mbps
  num_employees: +180.7 Mbps
...
```

Each prediction includes `feature_importance` — how much each feature pushed the
estimate up (+) or down (−) for that specific branch.

---

## Ingest pipeline

For operational use, the model is wired into an ingest pipeline so that predictions
are added **automatically at index time** — no separate inference call is needed.

```
Document → bank-branches-live → ingest pipeline → inference processor → ml.* fields stored
```

### Step 7 — Create the pipeline and live index

```bash
python3 setup_ingest_pipeline.py
```

```
Resolving trained model for job 'branch-bandwidth-regression'...
  model_id : branch-bandwidth-regression-<timestamp>

Creating ingest pipeline 'branch-bandwidth-prediction'...
  acknowledged : True

Creating index 'bank-branches-live' with default_pipeline 'branch-bandwidth-prediction'...
  acknowledged : True
```

This creates:
- **Pipeline** `branch-bandwidth-prediction` — contains a single `inference` processor
- **Index** `bank-branches-live` — has `index.default_pipeline` set to the pipeline above

The training index `bank-branches` is left untouched.

### Step 8 — Index documents and read predictions

```bash
# Index built-in sample branches and print results
python3 index_with_pipeline.py

# Index a single branch from the CLI
python3 index_with_pipeline.py \
  --branch-id BR9020 --tier urban \
  --employees 55 --customers 1100 --transactions 2200
```

```
Branch ID   Tier           Empl    Cust    Txns     Predicted BW   Tier
──────────────────────────────────────────────────────────────────────────────────────
BR9001      rural             5      80     150         55.4 Mbps   Standard (suburban)
              num_customers: -152.0 Mbps
              num_transactions: -136.5 Mbps
              num_employees: -94.2 Mbps
BR9003      urban            45     900    1800        507.4 Mbps   Premium  (flagship)
              num_customers: +41.7 Mbps
              num_employees: +15.3 Mbps
              num_transactions: +12.4 Mbps
...
Prediction was added automatically by the ingest pipeline — no separate inference call.
```

### Fields added by the pipeline

Every document stored in `bank-branches-live` automatically receives:

| Field | Description |
|---|---|
| `ml.bandwidth_mbps_prediction` | Predicted bandwidth in Mbps |
| `ml.feature_importance[].feature_name` | Feature name |
| `ml.feature_importance[].importance` | Signed contribution in Mbps (+ increases estimate, − decreases it) |
| `ml.model_id` | Model version that scored the document |

If inference fails for a document (e.g. a missing field), the pipeline's `on_failure`
handler tags it with `tags: inference_failed` rather than dropping it.

---

## Sample dataset

| Field | Range / Values | Type |
|---|---|---|
| `num_employees` | 2–120 | integer |
| `num_customers` | 20–2,500 | integer |
| `num_transactions` | 40–6,000 | integer |
| `branch_tier` | rural, suburban, urban, flagship | keyword |
| `bandwidth_mbps` *(target)* | ~5–600 Mbps | float |

Records are distributed evenly across four branch tiers so the model sees the full operating range:

| Tier | Employees | Customers/day | Transactions/day | Typical bandwidth |
|---|---|---|---|---|
| rural | 2–10 | 20–120 | 40–250 | ~15–60 Mbps |
| suburban | 8–25 | 100–450 | 200–900 | ~50–150 Mbps |
| urban | 20–60 | 350–1,100 | 700–2,500 | ~130–380 Mbps |
| flagship | 50–120 | 900–2,500 | 2,000–6,000 | ~280–600 Mbps |

### Bandwidth formula

```
bandwidth = 8.0                        # always-on infra (VPN, monitoring, CCTV)
          + 3.2  × num_employees       # workstations, VoIP, CBS per staff
          + 0.25 × num_customers       # internet banking, ATMs, kiosk sessions
          + 0.08 × num_transactions    # payment gateway + CBS round-trips
          + Gaussian noise (±6 %)
```

## ML job configuration

| Parameter | Value |
|---|---|
| Source index | `bank-branches` |
| Dest index | `bank-branches-regression-results` |
| `dependent_variable` | `bandwidth_mbps` |
| `training_percent` | `80` |
| `num_top_feature_importance_values` | `3` |
| `model_memory_limit` | `50mb` |

## Ingest pipeline configuration

| Parameter | Value |
|---|---|
| Pipeline ID | `branch-bandwidth-prediction` |
| Live index | `bank-branches-live` |
| `target_field` | `ml` |
| `num_top_feature_importance_values` | `3` |
| `on_failure` | Tags document with `inference_failed` |

---

## Cleanup

```bash
# Delete the ML job
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/data_frame/analytics/branch-bandwidth-regression"

# Delete the trained model
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/trained_models/branch-bandwidth-regression-<timestamp>"

# Delete the ingest pipeline
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ingest/pipeline/branch-bandwidth-prediction"

# Delete all indices
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/bank-branches,bank-branches-regression-results,bank-branches-live"
```

---

## Files

| File | Purpose |
|---|---|
| `generate_sample_data.py` | Creates `bank-branches` and bulk-loads 500 synthetic records |
| `setup_regression_job.py` | Creates and starts the `branch-bandwidth-regression` analytics job |
| `deploy_model.py` | Resolves the trained model ID, verifies stats, and runs a smoke-test inference |
| `realtime_inference.py` | On-demand `_infer` API calls with feature importance output; supports CLI flags |
| `setup_ingest_pipeline.py` | Creates the `branch-bandwidth-prediction` pipeline and `bank-branches-live` index |
| `index_with_pipeline.py` | Indexes branch documents — predictions added automatically by the pipeline |
