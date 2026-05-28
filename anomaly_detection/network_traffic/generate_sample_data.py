"""
Generate synthetic network traffic time-series data and bulk-index into Elasticsearch.

Produces one hourly bandwidth reading per network segment for the past 6 months.

Fields per document:
  @timestamp        -- ISO-8601 UTC hour boundary
  segment_name      -- network segment identifier (keyword)
  bandwidth_mbps    -- observed throughput (target for anomaly detection)
  capacity_mbps     -- provisioned link capacity (constant per segment)
  utilization_pct   -- bandwidth_mbps / capacity_mbps * 100

Patterns baked into the data:
  - Business-hours peak, overnight trough (daily seasonality)
  - Lower traffic on weekends (weekly seasonality)
  - Compound monthly growth that pushes utilization toward the 90 % threshold
  - ~1.5 % of hours have injected anomalies: spikes (DDoS-like, backup bursts)
    or drops (link flaps, routing events)

Growth rates are tuned so that edge-dmz crosses the 90 % threshold roughly
10 weeks into the forecast window -- making the forecast output immediately
actionable when you run check_threshold.py.
"""
import json
import math
import random
import ssl
import urllib.request
import urllib.error
import os
import sys
from datetime import datetime, timedelta, timezone
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
    for key, value in _parse_kv_file(root / ".elastic-credentials"):
        os.environ.setdefault(key, value)
    for key, value in _parse_kv_file(root / ".env"):
        os.environ.setdefault(key, value)


load_config()

SSL_CONTEXT = ssl._create_unverified_context()
ES_URL  = os.environ["ELASTICSEARCH_URL"]
API_KEY = os.environ["ELASTICSEARCH_API_KEY"]
INDEX   = "network-traffic-metrics"

random.seed(2024)

# Segment definitions — tweak growth_pct_per_month to change forecast horizon
SEGMENTS = {
    "edge-dmz": {
        "capacity_mbps":        2_000,
        "baseline_utilization": 0.64,   # utilization at the start of the 6-month window
        "growth_pct_per_month": 5.0,    # ~10 weeks until 90 % threshold from today
        "noise_pct":            0.08,
    },
    "core-uplink-1": {
        "capacity_mbps":        10_000,
        "baseline_utilization": 0.55,
        "growth_pct_per_month": 3.0,    # ~46 weeks until threshold
        "noise_pct":            0.06,
    },
    "wan-primary": {
        "capacity_mbps":        5_000,
        "baseline_utilization": 0.65,
        "growth_pct_per_month": 2.5,    # ~30 weeks until threshold
        "noise_pct":            0.07,
    },
    "datacenter-ic": {
        "capacity_mbps":        20_000,
        "baseline_utilization": 0.45,
        "growth_pct_per_month": 1.5,    # very long runway
        "noise_pct":            0.05,
    },
}

HISTORY_MONTHS = 6
NOW       = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
START_TS  = NOW - timedelta(days=HISTORY_MONTHS * 30)


def hourly_shape(hour: int, weekday: int) -> float:
    """
    Return a [0.25 .. 1.0] multiplier for the hour/weekday combination.

    Uses a double-Gaussian approximating a business-hours traffic curve
    with an overnight trough and a 45 % weekend scaling factor.
    """
    weekend_scale = 0.45 if weekday >= 5 else 1.0

    morning   = 0.85 * math.exp(-0.5 * ((hour - 10) / 3.0) ** 2)
    afternoon = 0.65 * math.exp(-0.5 * ((hour - 14) / 1.5) ** 2)
    evening   = 0.60 * math.exp(-0.5 * ((hour - 20) / 2.5) ** 2)
    base      = 0.28  # always-on: monitoring, VPN keep-alives, backups

    shape = min(base + max(morning, afternoon, evening), 1.0)
    return shape * weekend_scale


def generate_records() -> list:
    records     = []
    total_hours = int((NOW - START_TS).total_seconds() // 3600)

    for seg_name, cfg in SEGMENTS.items():
        capacity    = cfg["capacity_mbps"]
        baseline_bw = cfg["baseline_utilization"] * capacity
        rate        = cfg["growth_pct_per_month"] / 100.0
        noise_pct   = cfg["noise_pct"]

        for h in range(total_hours):
            ts             = START_TS + timedelta(hours=h)
            months_elapsed = h / (24 * 30)

            # Compound growth applied to the baseline
            trended_bw = baseline_bw * ((1.0 + rate) ** months_elapsed)

            # Modulate by time-of-day / day-of-week pattern
            bw = trended_bw * hourly_shape(ts.hour, ts.weekday())

            # Gaussian noise
            bw += random.gauss(0, bw * noise_pct)
            bw  = max(bw, 1.0)

            # Anomaly injection (~1.5 % of hours)
            anomaly_type = None
            r = random.random()
            if r < 0.015:
                if random.random() < 0.70:
                    bw *= random.uniform(1.4, 2.2)   # spike
                    anomaly_type = "spike"
                else:
                    bw *= random.uniform(0.05, 0.35)  # drop
                    anomaly_type = "drop"

            bw  = min(round(bw, 2), capacity)
            doc = {
                "@timestamp":     ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "segment_name":   seg_name,
                "bandwidth_mbps": bw,
                "capacity_mbps":  capacity,
                "utilization_pct": round(bw / capacity * 100, 2),
            }
            if anomaly_type:
                doc["anomaly_type"] = anomaly_type

            records.append(doc)

    return records


def request(method, path, body=None, ok_404=False):
    url     = f"{ES_URL}{path}"
    data    = json.dumps(body).encode() if body else None
    headers = {"Authorization": f"ApiKey {API_KEY}", "Content-Type": "application/json"}
    req     = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        if ok_404 and e.code == 404:
            return None
        print(f"HTTP {e.code} {method} {path}: {err}", file=sys.stderr)
        sys.exit(1)


def bulk_index(records: list):
    BATCH = 500
    total = len(records)
    for i in range(0, total, BATCH):
        batch = records[i : i + BATCH]
        body  = ""
        for doc in batch:
            body += json.dumps({"index": {"_index": INDEX}}) + "\n"
            body += json.dumps(doc) + "\n"

        req = urllib.request.Request(
            f"{ES_URL}/_bulk",
            data=body.encode(),
            headers={"Authorization": f"ApiKey {API_KEY}", "Content-Type": "application/x-ndjson"},
            method="POST",
        )
        with urllib.request.urlopen(req, context=SSL_CONTEXT) as resp:
            result = json.loads(resp.read())
            if result.get("errors"):
                errs = [item for item in result["items"] if item.get("index", {}).get("error")]
                print(f"  Bulk errors: {len(errs)}", file=sys.stderr)

        print(f"  Indexed {min(i + BATCH, total):>6}/{total}")


# Check if index already has data (avoid re-indexing); 404 = index not yet created
count_resp = request("GET", f"/{INDEX}/_count", ok_404=True)
if count_resp is not None and count_resp.get("count", 0) > 0:
    print(f"Index '{INDEX}' already contains {count_resp['count']:,} documents. Exiting without re-indexing.")
    print(f"To re-generate, delete the index first:")
    print(f"  DELETE /{INDEX}")
    sys.exit(0)

print(f"Generating {HISTORY_MONTHS}-month history for {len(SEGMENTS)} segments...")
records = generate_records()

# Print a preview of utilization at the end of the history window
print(f"\n  Segment utilization at end of history window:")
for seg, cfg in SEGMENTS.items():
    baseline = cfg["baseline_utilization"] * cfg["capacity_mbps"]
    rate     = cfg["growth_pct_per_month"] / 100.0
    final_bw = baseline * ((1.0 + rate) ** HISTORY_MONTHS)
    final_pct = final_bw / cfg["capacity_mbps"] * 100
    print(f"    {seg:<20} {final_pct:5.1f}% of {cfg['capacity_mbps']:,} Mbps capacity")

print(f"\n  Total records: {len(records):,}")

print(f"\nIndexing into '{INDEX}'...")
bulk_index(records)
print(f"\nDone. Next step:")
print(f"  python3 setup_anomaly_job.py")
