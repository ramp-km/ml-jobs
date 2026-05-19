# Elasticsearch ML Outlier Detection

A minimal example of Elasticsearch's [Data Frame Analytics](https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-df-analytics-overview.html) outlier detection, using a synthetic employee productivity dataset.

## What it does

1. **Generates** 3,090 documents across 103 employees (100 normal + 3 injected outliers) and bulk-indexes them into `employee-metrics`.
2. **Creates and starts** a data frame analytics job that runs an ensemble of outlier algorithms (LOF, LDOF, kNN-distance, kNN-density) over four numeric features.
3. **Writes results** to `employee-outlier-results`, where each document gets an `ml.outlier_score` (0–1) and per-feature influence scores.

## Prerequisites

- Python 3.8+
- An Elastic Cloud Serverless project (Observability or Elasticsearch tier) with an API key that has ML privileges
- No third-party Python packages required — only the standard library

## Setup

### 1. Create a credentials file

Create `.elastic-credentials` in this directory (it is gitignored):

```
# Project: <your-project-name> | id=<your-project-id>
ELASTICSEARCH_URL=https://<your-endpoint>.es.<region>.gcp.elastic.cloud
KIBANA_URL=https://<your-endpoint>.kb.<region>.gcp.elastic.cloud
ELASTICSEARCH_API_KEY=<your-api-key>
```

The API key needs the following cluster/index privileges:

| Privilege | Scope |
|---|---|
| `manage_ml` | cluster |
| `create_index`, `index`, `read` | `employee-metrics`, `employee-outlier-results` |

### 2. Export environment variables

```bash
export ELASTICSEARCH_URL=https://<your-endpoint>.es.<region>.gcp.elastic.cloud
export ELASTICSEARCH_API_KEY=<your-api-key>

# If you're behind a corporate proxy with a custom CA bundle:
export SSL_CERT_FILE=/etc/ssl/cert.pem   # macOS system bundle
# export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt  # Linux
```

## Usage

### Step 1 — Index sample data

```bash
python3 generate_sample_data.py
```

Expected output:
```
Total records to index: 3090
  Indexed 500/3090
  ...
  Indexed 3090/3090
Done.
```

### Step 2 — Create and start the ML job

```bash
python3 setup_outlier_job.py
```

Expected output:
```
Documents in employee-metrics: 3090

Creating job 'employee-outlier-detection'...
  id: employee-outlier-detection
  dest index: employee-outlier-results

Starting job 'employee-outlier-detection'...
  acknowledged: True
```

### Step 3 — Poll job status

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/data_frame/analytics/employee-outlier-detection/_stats" \
  | python3 -m json.tool | grep -E "state|progress_percent"
```

The job typically completes in under a minute. State transitions: `stopped` → `started` → `reindexing` → `analyzing` → `stopped` (at 100%).

### Step 4 — Query results

Top outliers by score:

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/employee-outlier-results/_search" \
  -d '{
    "size": 10,
    "sort": [{"ml.outlier_score": {"order": "desc"}}],
    "_source": ["employee_id", "department", "hours_worked", "tickets_closed", "commits", "meetings", "ml.outlier_score"]
  }' | python3 -m json.tool
```

Per-employee aggregate scores:

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/employee-outlier-results/_search" \
  -d '{
    "size": 0,
    "aggs": {
      "by_employee": {
        "terms": {"field": "employee_id", "size": 10, "order": {"max_score": "desc"}},
        "aggs": {
          "max_score": {"max": {"field": "ml.outlier_score"}},
          "avg_hours":   {"avg": {"field": "hours_worked"}},
          "avg_tickets": {"avg": {"field": "tickets_closed"}}
        }
      }
    }
  }' | python3 -m json.tool
```

## Sample dataset

| Field | Normal range | Type |
|---|---|---|
| `hours_worked` | 35–45 | float |
| `tickets_closed` | 5–15 | float |
| `commits` | 2–10 | float |
| `meetings` | 3–8 | float |

Three outliers are injected to give the model clear signals:

| Employee | Pattern | Expected score |
|---|---|---|
| `emp_901` | 80–95h worked, near-zero output | ~0.88 |
| `emp_902` | ~1h worked, 80–120 tickets, 20–30 meetings | ~0.997 |
| `emp_903` | All metrics zero (inactive) | Low — consistent zeros aren't anomalous |

## ML job configuration

| Parameter | Value |
|---|---|
| Source index | `employee-metrics` |
| Dest index | `employee-outlier-results` |
| Algorithm | Auto (ensemble) |
| Analyzed fields | `hours_worked`, `tickets_closed`, `commits`, `meetings` |
| `compute_feature_influence` | `true` |
| `outlier_fraction` | `0.05` |
| `model_memory_limit` | `50mb` |

`compute_feature_influence: true` adds `ml.feature_influence.*` fields to each result document, showing which feature contributed most to the anomaly score.

## Cleanup

To delete all resources created by this example:

```bash
# Delete the ML job
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/data_frame/analytics/employee-outlier-detection"

# Delete both indices
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/employee-metrics,employee-outlier-results"
```

## Files

| File | Purpose |
|---|---|
| `generate_sample_data.py` | Creates the `employee-metrics` index and bulk-loads synthetic data |
| `setup_outlier_job.py` | Creates and starts the `employee-outlier-detection` analytics job |
| `.elastic-credentials` | Local credentials file (gitignored) |
| `.env` | Local environment overrides (gitignored) |
