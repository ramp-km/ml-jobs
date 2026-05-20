# Elasticsearch ML Outlier Detection

A minimal example of Elasticsearch's [Data Frame Analytics](https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-df-analytics-overview.html) outlier detection, using a synthetic employee productivity dataset.

## What it does

1. **Generates** 3,090 documents across 103 employees (100 normal + 3 injected outliers) and bulk-indexes them into `employee-metrics`.
2. **Creates and starts** a data frame analytics job that runs an ensemble of outlier algorithms (LOF, LDOF, kNN-distance, kNN-density) over four numeric features.
3. **Writes results** to `employee-outlier-results`, where each document gets an `ml.outlier_score` (0–1) and per-feature influence scores.

## Prerequisites

- Python 3.8+
- An Elastic Cloud Serverless project (Observability or Elasticsearch tier) **or** a self-managed Elasticsearch 7.3+ cluster with a Platinum/Enterprise licence
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
| `create_index`, `index`, `read` | `employee-metrics`, `employee-outlier-results` |

### 3. (Optional) Export variables directly

Shell exports take precedence over both `.env` and `.elastic-credentials`:

```bash
export ELASTICSEARCH_URL=https://<your-endpoint>.es.<region>.gcp.elastic.cloud
export ELASTICSEARCH_API_KEY=<your-api-key>
```

### Config resolution order

The scripts walk up from their own location to find the repo root, then load credentials in this order (first value for each key wins):

| Source | Purpose |
|---|---|
| Shell environment | Highest precedence — overrides everything |
| `ml-jobs/.env` | General config, Cloud API key (`EC_API_KEY`) |
| `ml-jobs/.elastic-credentials` | Elasticsearch endpoint and API key |

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

## Real world outlier detection use cases

# 1. Unusual user behavior on a network

Goal: Find users whose behavior differs sharply from peer users.

Entity: user_id

Possible features:

    number of logins per day
    number of failed logins
    number of distinct source IPs
    number of distinct devices used
    average session duration
    bytes uploaded
    bytes downloaded
    number of privileged actions
    number of off-hours accesses
    number of countries or regions accessed from

Why it works:

Most users cluster into normal behavior patterns. A user with unusually high failed logins, many source IPs, and abnormal off-hours activity may stand out as an outlier.

# 2. Malware or compromised host detection

Goal: Identify hosts behaving differently from the rest of the fleet.

Entity: host.name or host.id

Possible features:

    process count
    number of unique processes launched
    number of unsigned binaries executed
    outbound connection count
    number of distinct destination IPs
    DNS request count
    bytes sent externally
    number of failed process executions
    number of security alerts
    CPU usage average
    memory usage average

Why it works:

A compromised machine may show unusual combinations like high outbound connections, many distinct destinations, and abnormal process activity.

# 3. Fraudulent payment card or account behavior

Goal: Detect accounts or cards whose transaction profile is unusual.

Entity: account_id or card_id

Possible features:

    transaction count per day
    average transaction amount
    max transaction amount
    number of merchants used
    number of countries used
    percentage of card-not-present transactions
    number of declined transactions
    number of transactions at unusual hours
    average time between transactions

Why it works:

Fraud often appears as a profile that differs from the broader population, such as many countries in a short period or unusually high transaction velocity.

# 4. E-commerce seller or buyer anomaly detection

Goal: Find sellers or buyers with abnormal marketplace behavior.

Entity: seller_id or buyer_id

Possible features:

    order count
    refund rate
    cancellation rate
    average basket value
    number of unique shipping addresses
    number of payment methods used
    number of customer complaints
    average fulfillment time
    return rate

Why it works:

A seller with unusually high cancellations and complaints, or a buyer with many shipping addresses and payment methods, may be worth investigating.

# 5. IoT device fleet monitoring

Goal: Detect devices whose telemetry profile is unusual.

Entity: device_id

Possible features:

    average temperature
    max temperature
    vibration average
    power consumption average
    reboot count
    error count
    packet loss rate
    message frequency
    battery level average
    firmware version encoded as categorical input if appropriate upstream

Why it works:

A failing or misconfigured device may differ from similar devices in temperature, reboot frequency, or communication behavior.

# 6. Server or VM fleet capacity outliers

Goal: Find infrastructure nodes that are behaving differently from peers.

Entity: host.name, instance_id, or vm_id

Possible features:

    CPU utilization average
    memory utilization average
    disk utilization average
    disk I/O rate
    network throughput
    process count
    load average
    restart count
    error log count
    open file descriptor count

Why it works:

One VM with much higher disk I/O and memory pressure than similar nodes may indicate a leak, noisy neighbor issue, or mis-sizing.

# 7. Customer support case outliers

Goal: Identify tickets or customers with unusual support patterns.

Entity: case_id or customer_id

Possible features:

    number of reopen events
    time to first response
    total resolution time
    number of handoffs
    severity
    number of comments
    number of attached logs/files
    escalation count
    product area encoded numerically/categorically upstream

Why it works:

Cases that require many handoffs and unusually long resolution times may indicate process issues or hidden product problems.

# 8. Manufacturing quality control

Goal: Detect parts, batches, or machines with unusual production characteristics.

Entity: batch_id, machine_id, or part_id

Possible features:

    defect count
    cycle time
    temperature average during production
    pressure average
    vibration average
    scrap rate
    rework count
    energy consumption per unit
    downtime minutes

Why it works:

A machine or batch that differs from the rest may indicate calibration drift, wear, or process instability.

# 9. Healthcare or operations workflow outliers

Goal: Find patients, visits, or facilities with unusual operational patterns.

Entity: visit_id, patient_id, or facility_id

Possible features:

    length of stay
    number of procedures
    number of medications
    readmission count
    wait time
    transfer count
    lab test count
    cost of visit

Why it works:

Outlier detection can highlight unusually complex or costly cases for review, though domain and privacy controls are critical.

# 10. Web service/API client outliers

Goal: Detect clients or services whose usage profile is unusual.

Entity: client_id, service.name, or api_key_id

Possible features:

    request count
    error rate
    average latency
    max latency
    bytes sent
    bytes received
    number of endpoints accessed
    authentication failure count
    request rate during off-hours

Why it works:

A client with unusual request volume, endpoint spread, and error rate may indicate abuse, integration bugs, or credential misuse.

## What makes a good feature set for outlier detection

In practice, the best features are usually:

    numeric or boolean
    entity-level summaries
    built over a meaningful window, such as:
    last 24 hours
    last 7 days
    last 30 days

Examples:

    counts
    averages
    maxima
    ratios
    distinct counts
    boolean flags

This is important because Elastic outlier detection works on a tabular data frame and requires analyzed fields to be numeric or boolean. Documents with missing/null/array values in included analyzed fields may be ignored for the analysis.

## Common pattern: build an entity-centric index first

A strong real-world workflow is:

    Raw events arrive continuously
    Use a transform to build one row per entity
    Compute summary features for that entity
    Run outlier detection on that derived index

For example:

    one row per user over the last 7 days
    one row per host over the last 24 hours
    one row per card over the last 30 days

That usually works much better than feeding raw event-level documents directly.

## When not to use outlier detection

Outlier detection is less suitable when:

    your problem is fundamentally time-series continuous monitoring
    you need real-time ongoing detection
    your data is mostly unstructured text
    you don’t have a meaningful entity-centric table

In those cases, anomaly detection may be a better fit.