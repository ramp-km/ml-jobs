# Elasticsearch ML Regression — IT Incident Resolution Time

Predicts how long (in minutes) it will take to resolve an IT incident using Elasticsearch's
[Data Frame Analytics](https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-df-analytics-overview.html)
regression. Seven operational features are used to train the model. The trained model is then
surfaced in two ways:

- **On-demand** via the `_infer` API (`realtime_inference.py`)
- **Automatically at index time** via an ingest pipeline (`index_with_pipeline.py`)

## What it does

| Step | Script | Description |
|---|---|---|
| 1 | `generate_sample_data.py` | Generates 600 synthetic incident records and indexes them into `it-incidents` |
| 2 | `setup_regression_job.py` | Creates and starts the `incident-resolution-regression` Data Frame Analytics job |
| 3 | `deploy_model.py` | Verifies the trained model is ready for inference and prints a smoke-test result |
| 4 | `realtime_inference.py` | Calls `_infer` directly to predict resolution time for any incident on demand |
| 5 | `setup_ingest_pipeline.py` | Creates the `incident-resolution-prediction` ingest pipeline and the `it-incidents-live` index |
| 6 | `index_with_pipeline.py` | Indexes new incident documents — the pipeline adds predictions automatically at write time |

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
| `create_index`, `index`, `read` | `it-incidents`, `it-incidents-regression-results`, `it-incidents-live` |

---

## Usage

### Step 1 — Index sample data

```bash
python3 generate_sample_data.py
```

```
Total records to index: 600
  Indexed 300/600
  Indexed 600/600
Done.
```

### Step 2 — Create and start the ML job

```bash
python3 setup_regression_job.py
```

```
Documents in it-incidents: 600

Creating job 'incident-resolution-regression'...
  id: incident-resolution-regression
  dest index: it-incidents-regression-results

Starting job 'incident-resolution-regression'...
  acknowledged: True
```

### Step 3 — Poll job status

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/data_frame/analytics/incident-resolution-regression/_stats" \
  | python3 -m json.tool | grep -E "state|progress_percent"
```

State transitions: `stopped` → `started` → `reindexing` → `analyzing` → `writing_results` → `stopped` (100 %)

Wait until all phases report `100` before proceeding.

### Step 4 — Query batch results

Top 10 incidents with the longest predicted resolution time (test set):

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/it-incidents-regression-results/_search" \
  -d '{
    "size": 10,
    "query": {"term": {"ml.is_training": false}},
    "sort": [{"ml.resolution_time_minutes_prediction": {"order": "desc"}}],
    "_source": ["incident_id", "category", "severity", "num_affected_users",
                "team_size", "is_business_hours", "resolution_time_minutes",
                "ml.resolution_time_minutes_prediction"]
  }' | python3 -m json.tool
```

Average predicted vs actual resolution time by category:

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/it-incidents-regression-results/_search" \
  -d '{
    "size": 0,
    "query": {"term": {"ml.is_training": false}},
    "aggs": {
      "by_category": {
        "terms": {"field": "category", "size": 10},
        "aggs": {
          "avg_actual":    {"avg": {"field": "resolution_time_minutes"}},
          "avg_predicted": {"avg": {"field": "ml.resolution_time_minutes_prediction"}}
        }
      }
    }
  }' | python3 -m json.tool
```

Incidents that may need escalation (predicted > 480 min, i.e. more than 8 hours):

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/it-incidents-regression-results/_search" \
  -d '{
    "size": 20,
    "query": {
      "bool": {
        "must": [
          {"term": {"ml.is_training": false}},
          {"range": {"ml.resolution_time_minutes_prediction": {"gt": 480}}}
        ]
      }
    },
    "_source": ["incident_id", "category", "severity", "num_affected_users",
                "team_size", "is_business_hours", "ml.resolution_time_minutes_prediction"]
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
Looking up trained model for job 'incident-resolution-regression'...
  model_id   : incident-resolution-regression-<timestamp>
  model_type : tree_ensemble   (tree_ensemble models serve inference immediately)

Verifying model stats...
  model size     : 92 KB
  inference calls: 0 so far

Running smoke-test inference (medium-severity software incident)...
  Input  : category=software, severity=3, affected=50, team=3, hour=14, biz_hours=1, comments=15
  Output : 142.3 min  ✓

Model is ready for real-time inference.
```

The script resolves the model ID dynamically from the job tag (the ID contains a
timestamp suffix and is never hard-coded).

### Step 6 — On-demand inference

```bash
# Predict for built-in sample incidents across all categories
python3 realtime_inference.py

# Predict for a single incident
python3 realtime_inference.py \
    --category security --severity 5 \
    --affected 300 --team 6 \
    --hour 2 --dow 6 --comments 40
```

```
Incident                                   Cat  Sev   Aff  Team     Predicted   Tier
----------------------------------------------------------------------------------------------------------------------------------------
Password reset (business hours)       user_access    1     2     1       28.4 min   Quick      (< 1 h)
  category: -112.3 min
  severity: -45.2 min
  ...
Security breach — weekend night          security    5   300     6      872.1 min   Critical   (> 8 h)
  is_business_hours: +198.4 min
  severity: +156.7 min
  ...
```

Each prediction includes `feature_importance` — how much each feature pushed the
estimate up (+) or down (−) for that specific incident.

---

## Ingest pipeline

For operational use, the model is wired into an ingest pipeline so that predictions
are added **automatically at index time** — no separate inference call is needed.

```
Document → it-incidents-live → ingest pipeline → inference processor → ml.* fields stored
```

### Step 7 — Create the pipeline and live index

```bash
python3 setup_ingest_pipeline.py
```

```
Resolving trained model for job 'incident-resolution-regression'...
  model_id : incident-resolution-regression-<timestamp>

Creating ingest pipeline 'incident-resolution-prediction'...
  acknowledged : True

Creating index 'it-incidents-live' with default_pipeline 'incident-resolution-prediction'...
  acknowledged : True
```

This creates:
- **Pipeline** `incident-resolution-prediction` — contains a single `inference` processor
- **Index** `it-incidents-live` — has `index.default_pipeline` set to the pipeline above

The training index `it-incidents` is left untouched.

### Step 8 — Index documents and read predictions

```bash
# Index built-in sample incidents and print results
python3 index_with_pipeline.py

# Index a single incident from the CLI
python3 index_with_pipeline.py \
  --incident-id INC9020 --category network --severity 4 \
  --affected 180 --team 4 --hour 8 --dow 0 --comments 28
```

```
Inc ID      Cat  Sev    Aff  Team  BizH  Cmts      Predicted   Tier
────────────────────────────────────────────────────────────────────────────────────────────────────
INC9001   user_access     1      2     1     1     4       28.4 min   Quick      (< 1 h)
  category: -112.3 min
  ...
INC9004      security     5    300     6     0    55      872.1 min   Critical   (> 8 h)
  is_business_hours: +198.4 min
  ...

5 document(s) retrieved from 'it-incidents-live'.
Prediction was added automatically by the ingest pipeline — no separate inference call.
```

### Fields added by the pipeline

Every document stored in `it-incidents-live` automatically receives:

| Field | Description |
|---|---|
| `ml.resolution_time_minutes_prediction` | Predicted resolution time in minutes |
| `ml.feature_importance[].feature_name` | Feature name |
| `ml.feature_importance[].importance` | Signed contribution in minutes (+ increases estimate, − decreases it) |
| `ml.model_id` | Model version that scored the document |

If inference fails for a document (e.g. a missing field), the pipeline's `on_failure`
handler tags it with `tags: inference_failed` rather than dropping it.

---

## Sample dataset

| Field | Range / Values | Type |
|---|---|---|
| `severity` | 1 (low) to 5 (critical) | integer |
| `category` | network, hardware, software, security, user_access | keyword |
| `num_affected_users` | 1–300 | integer |
| `team_size` | 1–8 | integer |
| `hour_of_day` | 0–23 | integer |
| `day_of_week` | 0 (Mon) to 6 (Sun) | integer |
| `is_business_hours` | 0 or 1 | integer |
| `num_comments` | 2–60 | integer |
| `resolution_time_minutes` *(target)* | ~5–1,200 min | float |

### Resolution time formula

```
base_time = CATEGORY_BASE[category]    # user_access=25, software=55, network=90, hardware=130, security=160

time = base_time
     + severity × 20                   # higher severity → longer resolution
     + num_affected_users × 0.15       # more users impacted → more coordination
     + num_comments × 1.2              # more back-and-forth → longer time

time /= (1 + 0.35 × log(1 + team_size))   # larger team reduces time (diminishing returns)
if not is_business_hours:
    time × 1.55                            # off-hours incidents take longer (on-call lag)

+ Gaussian noise (±8 %)
```

### Category base times

| Category | Base time |
|---|---|
| user_access | 25 min |
| software | 55 min |
| network | 90 min |
| hardware | 130 min |
| security | 160 min |

## ML job configuration

| Parameter | Value |
|---|---|
| Source index | `it-incidents` |
| Dest index | `it-incidents-regression-results` |
| `dependent_variable` | `resolution_time_minutes` |
| `training_percent` | `80` |
| `num_top_feature_importance_values` | `7` |
| `model_memory_limit` | `100mb` |

## Ingest pipeline configuration

| Parameter | Value |
|---|---|
| Pipeline ID | `incident-resolution-prediction` |
| Live index | `it-incidents-live` |
| `target_field` | `ml` |
| `num_top_feature_importance_values` | `7` |
| `on_failure` | Tags document with `inference_failed` |

---

## Cleanup

```bash
# Delete the ML job
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/data_frame/analytics/incident-resolution-regression"

# Delete the trained model
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/trained_models/incident-resolution-regression-<timestamp>"

# Delete the ingest pipeline
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ingest/pipeline/incident-resolution-prediction"

# Delete all indices
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/it-incidents,it-incidents-regression-results,it-incidents-live"
```

---

## Files

| File | Purpose |
|---|---|
| `generate_sample_data.py` | Creates `it-incidents` and bulk-loads 600 synthetic records |
| `setup_regression_job.py` | Creates and starts the `incident-resolution-regression` analytics job |
| `deploy_model.py` | Resolves the trained model ID, verifies stats, and runs a smoke-test inference |
| `realtime_inference.py` | On-demand `_infer` API calls with feature importance output; supports CLI flags |
| `setup_ingest_pipeline.py` | Creates the `incident-resolution-prediction` pipeline and `it-incidents-live` index |
| `index_with_pipeline.py` | Indexes incident documents — predictions added automatically by the pipeline |

---

## Real World Use Cases for applying inference at ingest to predict numeric values using regression models

### 1. Predict ticket resolution time at ingest

A support case arrives with fields like:

- severity
- product area
- customer tier
- region
- issue category

A regression model predicts:

- expected resolution time in hours

Why do inference at ingest?

- route urgent cases faster
- alert if predicted resolution time exceeds SLA
- prioritize queues immediately

### 2. Predict shipment delay duration

Each shipment event is indexed with:

- origin
- destination
- carrier
- weather region
- day of week
- package type

A regression model predicts:

- expected delay in minutes/hours

Why at ingest?

- enrich each shipment record with predicted delay
- power dashboards and downstream automation
- trigger alerts for high-risk shipments

### 3. Predict cloud resource exhaustion lead time

Incoming infrastructure records contain:

- current utilization
- growth rate
- workload type
- region
- instance size
- historical trend features

A regression model predicts:

- hours/days until 90% or 100% capacity

Why at ingest?

- every new metric summary doc gets a "time_to_capacity" estimate
- alerting becomes simple
- capacity planning dashboards can use the stored prediction directly

### 4. Predict web request response time

Elastic's docs explicitly use examples like predicting:

- response time of a web request
- approximate amount of data exchanged with a client

For an incoming request summary document, a regression model could predict:

- expected latency in ms
- expected payload size

Why at ingest?

- compare actual vs predicted later
- flag requests likely to violate SLOs
- enrich observability data for analysis

### 5. Predict transaction amount or claim amount

For finance/insurance-style events, based on:

- customer profile
- product type
- geography
- prior behavior
- event attributes

A regression model predicts:

- expected claim amount
- expected transaction value
- expected loss amount

Why at ingest?

- immediate risk scoring
- downstream business rules
- anomaly comparison between predicted and actual values

### 6. Predict energy consumption or load

For smart building / industrial telemetry, incoming records may include:

- site
- equipment type
- temperature
- occupancy
- hour/day seasonality features

A regression model predicts:

- expected power consumption
- expected load

Why at ingest?

- compare actual vs expected in dashboards
- detect inefficiency
- trigger operational workflows

### Good fit vs less ideal fit

**Good fit** — use regression inference at ingest when:

- prediction is needed for every incoming document
- the prediction should be stored and reused
- latency at search time should stay low
- downstream alerts/filters depend on the predicted numeric field

**Less ideal** — it may be unsuitable when:

- features needed for prediction are incomplete at ingest time
- the model changes very frequently
- you only need predictions occasionally at query time
- inference cost at ingest would be too high for your throughput
