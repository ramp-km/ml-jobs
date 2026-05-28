# Elasticsearch ML — Network Traffic Anomaly & Bandwidth Forecasting

Uses Elasticsearch [Anomaly Detection](https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-ad-overview.html)
to monitor hourly network bandwidth, detect traffic spikes and drops, and **forecast
when each segment will cross the 90 % capacity threshold** — telling you exactly
how many weeks you have before an upgrade is needed.

## What it does

| Step | Script | Description |
|---|---|---|
| 1 | `generate_sample_data.py` | 6 months of hourly bandwidth readings for 4 network segments → `network-traffic-metrics` |
| 2 | `setup_anomaly_job.py` | Creates job, datafeed, opens and starts ML analysis |
| 3 | `run_forecast.py` | Triggers 12-week forecast, polls until complete |
| 4 | `check_threshold.py` | Queries forecast results, prints threshold-crossing dates per segment |

## ML approach

| Concept | Value |
|---|---|
| Job type | Anomaly Detection (`_ml/anomaly_detectors`) |
| Detector | `high_mean(bandwidth_mbps)` |
| Partition | `segment_name` — each segment gets an independent model |
| Bucket span | `1h` |
| Influencers | `segment_name` |
| Forecast API | `POST /_ml/anomaly_detectors/{job_id}/_forecast` |

Using `high_mean` rather than plain `mean` means only **abnormally high** bandwidth
triggers an anomaly score — traffic drops are not flagged unless you add a second
`low_mean` detector.

The `partition_field_name` causes the ML engine to build a separate seasonality model
per segment (daily curve, weekly dip) and generates independent per-segment forecasts.

## Dataset

6 months × 4 segments × 24 readings/day ≈ **17,500 documents** in `network-traffic-metrics`.

| Segment | Capacity | Baseline | Monthly growth | ~Weeks to 90 % |
|---|---|---|---|---|
| `edge-dmz` | 2,000 Mbps | 64 % | 5.0 % | **~9 weeks** (central forecast) |
| `wan-primary` | 5,000 Mbps | 65 % | 2.5 % | ~9 weeks (upper bound only) |
| `core-uplink-1` | 10,000 Mbps | 55 % | 3.0 % | ~46 weeks |
| `datacenter-ic` | 20,000 Mbps | 45 % | 1.5 % | very long runway |

Traffic patterns baked into the synthetic data:
- **Daily seasonality** — business-hours peak (09:00–17:00), overnight trough (~28 % of peak)
- **Weekly seasonality** — weekends at ~45 % of weekday traffic
- **Growth trend** — compound monthly growth per segment
- **Injected anomalies** — ~1.5 % of hours have spikes (DDoS-like, backup bursts) or drops (link flaps)

## Prerequisites

- Python 3.8+ (standard library only — no third-party packages)
- Elasticsearch 8.x or Elastic Cloud Serverless with ML enabled
- An API key with the privileges below

### Required API key privileges

| Privilege | Scope |
|---|---|
| `manage_ml` | cluster |
| `create_index`, `index`, `read` | `network-traffic-metrics` |
| `read` | `.ml-anomalies-*` |

## Setup

### Credentials

Add to the repo root `.env` (or `.elastic-credentials`):

```bash
ELASTICSEARCH_URL=https://<your-endpoint>.es.<region>.gcp.elastic.cloud
ELASTICSEARCH_API_KEY=<your-api-key>
```

All scripts walk up the directory tree to find this file — a single file at
`ml-jobs/.env` covers every example in the repo.

---

## Usage

### Step 1 — Generate and index sample data

```bash
python3 generate_sample_data.py
```

```
Creating index 'network-traffic-metrics'...
  acknowledged: True

Generating 6-month history for 4 segments...

  Segment utilization at end of history window:
    edge-dmz              85.8% of 2,000 Mbps capacity
    core-uplink-1         65.7% of 10,000 Mbps capacity
    wan-primary           75.4% of 5,000 Mbps capacity
    datacenter-ic         49.2% of 20,000 Mbps capacity

  Total records: 17,280

Indexing into 'network-traffic-metrics'...
  Indexed    500/17280
  ...
  Indexed  17280/17280
```

The end-of-window utilization numbers show where each segment stands today —
`edge-dmz` at 80.4 % is the most urgent.

### Step 2 — Create and start the ML job

```bash
python3 setup_anomaly_job.py
```

```
Source index 'network-traffic-metrics': 17,520 documents

Creating job 'network-traffic-anomaly'...
  job_id      : network-traffic-anomaly
  bucket_span : 1h

Opening job 'network-traffic-anomaly'...
  opened: True

Creating datafeed 'datafeed-network-traffic-anomaly'...
  datafeed_id : datafeed-network-traffic-anomaly
  job_id      : network-traffic-anomaly

Starting datafeed 'datafeed-network-traffic-anomaly'...
  started : True
```

The datafeed now processes 6 months of hourly data. Monitor progress:

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/anomaly_detectors/network-traffic-anomaly/_stats" \
  | python3 -m json.tool | grep -E "bucket_count|processed_record_count|state"
```

Wait until `bucket_count` is ≥ 4,000 before running the forecast (~1–3 minutes).

### Step 3 — Run the forecast

```bash
python3 run_forecast.py
```

```
Job 'network-traffic-anomaly'
  state           : opened
  buckets analyzed: 4,380
  records seen    : 17,520

Triggering forecast: duration=84d, expires_in=30d...
  forecast_id  : Ml2IZJ4Bd0wfqUm57lCJ
  acknowledged : True

Polling for forecast completion (this usually takes < 60 s)...
  [   5s]  forecast docs=0     (stable=0/3)
  [  10s]  forecast docs=2,016 (stable=0/3)
  [  20s]  forecast docs=8,064 (stable=1/3)
  [  25s]  forecast docs=8,064 (stable=2/3)
  [  30s]  forecast docs=8,064 (stable=3/3)

Forecast complete.
  forecast_id   : Ml2IZJ4Bd0wfqUm57lCJ
  result docs   : 8,064 (one per partition per bucket)
  duration      : 84d
  expires_in    : 30d

Find 90%% threshold crossing dates:
  python3 check_threshold.py --forecast-id Ml2IZJ4Bd0wfqUm57lCJ
```

> **Note:** The `_forecast` API accepts `d` (days) and `h` (hours) — not `w` (weeks).

### Step 4 — Check threshold crossings

```bash
python3 check_threshold.py --forecast-id Ml2IZJ4Bd0wfqUm57lCJ
```

```
Network Bandwidth Forecast — 90% Capacity Threshold Analysis
========================================================================
  Job         : network-traffic-anomaly
  Forecast ID : Ml2IZJ4Bd0wfqUm57lCJ
  Window      : 2026-05-26 → 2026-08-18 (12 weeks)

SEGMENT             CAPACITY   THRESHOLD     CURRENT   NOW %   CROSSES  DETAILS
-----------------------------------------------------------------------------------------------
core-uplink-1       10,000 M     9,000 M     6,282 M   62.8%      SAFE  Peak forecast: 6,889 Mbps (68.9%)
datacenter-ic       20,000 M    18,000 M     9,108 M   45.5%      SAFE  Peak forecast: 10,146 Mbps (50.7%)
edge-dmz             2,000 M     1,800 M     1,538 M   76.9%       YES  2026-07-27 (~8.7 wks)
                                                                          Predicted: 1,801 Mbps  [1,519–2,083 Mbps 95% CI]
wan-primary          5,000 M     4,500 M     3,576 M   71.5%      RISK  upper bound crosses 2026-07-31 (~9.3 wks)
                                                                          Central peak: 3,965 Mbps (79.3%)  Upper bound: 4,502 Mbps

WATCH — central forecast crosses threshold within window:
  edge-dmz: crosses 90% on 2026-07-27 (8.7 wks) — forecast peak 1,801/2,000 Mbps

CAPACITY RISK — upper confidence bound crosses threshold (plan upgrade):
  wan-primary: upper bound reaches 90% by 2026-07-31 (9.3 wks) — central peak 3,965/5,000 Mbps

SAFE — threshold not crossed within forecast window: core-uplink-1, datacenter-ic
```

The output shows three signal levels:
- **YES / WATCH** — central forecast (50th percentile) crosses the threshold: plan a concrete upgrade
- **RISK** — upper confidence bound crosses but central stays below: monitor closely, begin capacity planning
- **SAFE** — well below threshold within the 12-week window

---

## Viewing anomalies in Kibana

After the datafeed processes history, anomalies are visible in:

**Machine Learning → Anomaly Explorer**
- Job: `network-traffic-anomaly`
- Swim lanes show anomaly scores per segment per hour
- Spikes from the injected anomalies (DDoS bursts, backup jobs) appear as red cells

**Machine Learning → Single Metric Viewer**
- Select any segment from the partition dropdown
- The shaded band is the model's expected range; outliers appear as dots above it
- Click "Forecast" to trigger a new forecast directly from the UI

---

## Querying results directly

### Latest anomaly records per segment

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/.ml-anomalies-*/_search" \
  -d '{
    "size": 10,
    "sort": [{"anomaly_score": {"order": "desc"}}],
    "query": {
      "bool": {
        "filter": [
          {"term": {"job_id": "network-traffic-anomaly"}},
          {"term": {"result_type": "record"}},
          {"range": {"anomaly_score": {"gte": 50}}}
        ]
      }
    },
    "_source": ["timestamp", "anomaly_score", "partition_field_value",
                "actual", "typical", "field_name"]
  }' | python3 -m json.tool
```

### Forecast values for a specific segment

```bash
curl -s -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  -H "Content-Type: application/json" \
  -X POST "$ELASTICSEARCH_URL/.ml-anomalies-*/_search" \
  -d '{
    "size": 5,
    "sort": [{"timestamp": "asc"}],
    "query": {
      "bool": {
        "filter": [
          {"term": {"job_id": "network-traffic-anomaly"}},
          {"term": {"forecast_id": "<FORECAST_ID>"}},
          {"term": {"result_type": "model_forecast"}},
          {"term": {"partition_field_value": "edge-dmz"}}
        ]
      }
    },
    "_source": ["timestamp", "forecast_prediction", "forecast_lower", "forecast_upper"]
  }' | python3 -m json.tool
```

---

## Re-running the forecast with a longer window

The default duration is 12 weeks. Extend to 6 months if you need a longer view:

```bash
python3 run_forecast.py --duration 168d --expires-in 60d   # 24 weeks
python3 check_threshold.py   # auto-picks the most recent forecast
```

---

## Cleanup

```bash
# Stop the datafeed and close the job
curl -s -X POST -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/datafeeds/datafeed-network-traffic-anomaly/_stop"

curl -s -X POST -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/anomaly_detectors/network-traffic-anomaly/_close"

# Delete job, datafeed, and results
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/datafeeds/datafeed-network-traffic-anomaly"

curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/_ml/anomaly_detectors/network-traffic-anomaly?force=true"

# Delete the source index
curl -s -X DELETE -H "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  "$ELASTICSEARCH_URL/network-traffic-metrics"
```

---

## Files

| File | Purpose |
|---|---|
| `generate_sample_data.py` | Creates `network-traffic-metrics` with 17,500 hourly bandwidth readings |
| `setup_anomaly_job.py` | Creates job + datafeed, opens and starts ML processing |
| `run_forecast.py` | Triggers `_forecast` API, polls for completion |
| `check_threshold.py` | Queries forecast results, reports threshold-crossing dates per segment |
