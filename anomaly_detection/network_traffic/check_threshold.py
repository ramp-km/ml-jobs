"""
Report when each network segment is forecast to cross the 90 % capacity threshold.

Reads model_forecast result documents from .ml-anomalies-* and finds, for each
segment, the earliest future timestamp at which the predicted bandwidth meets
or exceeds 0.9 × capacity_mbps.

Usage:
  python3 check_threshold.py --forecast-id <id>   # from run_forecast.py output
  python3 check_threshold.py                       # auto-resolves the most recent forecast

Options:
  --threshold    Capacity fraction to check (default: 0.90)
  --forecast-id  Specific forecast_id to query
"""
import argparse
import json
import ssl
import urllib.request
import urllib.error
import os
import sys
from datetime import datetime, timezone
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
ES_URL   = os.environ["ELASTICSEARCH_URL"]
API_KEY  = os.environ["ELASTICSEARCH_API_KEY"]
JOB_ID   = "network-traffic-anomaly"

# Must match capacity_mbps values in generate_sample_data.py
SEGMENT_CAPACITY = {
    "edge-dmz":       2_000,
    "core-uplink-1": 10_000,
    "wan-primary":    5_000,
    "datacenter-ic": 20_000,
}

parser = argparse.ArgumentParser(description="Find when bandwidth crosses the capacity threshold.")
parser.add_argument("--forecast-id", default=None, help="Forecast ID from run_forecast.py")
parser.add_argument("--threshold",   type=float, default=0.90, help="Capacity fraction (default: 0.90)")
args = parser.parse_args()


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
        print(f"HTTP {e.code} {method} {path}: {err}", file=sys.stderr)
        sys.exit(1)


def resolve_forecast_id() -> str:
    """Return the most recently created finished forecast_id for the job."""
    result = request(
        "POST",
        "/.ml-anomalies-*/_search",
        {
            "size": 1,
            "sort": [{"forecast_create_timestamp": {"order": "desc"}}],
            "_source": ["forecast_id", "forecast_status", "forecast_create_timestamp"],
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"job_id":      JOB_ID}},
                        {"term": {"result_type": "model_forecast_stats"}},
                        {"term": {"forecast_status": "finished"}},
                    ]
                }
            },
        },
    )
    hits = result.get("hits", {}).get("hits", [])
    if not hits:
        print(
            f"No finished forecasts found for job '{JOB_ID}'.\n"
            "Run run_forecast.py first.",
            file=sys.stderr,
        )
        sys.exit(1)
    return hits[0]["_source"]["forecast_id"]


def get_forecast_window(forecast_id: str) -> tuple:
    """Return (start_ts_ms, end_ts_ms) of the forecast window."""
    result = request(
        "POST",
        "/.ml-anomalies-*/_search",
        {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"job_id":      JOB_ID}},
                        {"term": {"forecast_id": forecast_id}},
                        {"term": {"result_type": "model_forecast"}},
                    ]
                }
            },
            "aggs": {
                "min_ts": {"min": {"field": "timestamp"}},
                "max_ts": {"max": {"field": "timestamp"}},
            },
        },
    )
    aggs = result.get("aggregations", {})
    return aggs.get("min_ts", {}).get("value"), aggs.get("max_ts", {}).get("value")


def find_threshold_crossing(forecast_id: str, segment: str, threshold_mbps: float,
                             use_upper_bound: bool = False):
    """
    Return the first forecast result doc where the predicted bandwidth >= threshold_mbps.

    use_upper_bound=False → check forecast_prediction (central estimate)
    use_upper_bound=True  → check forecast_upper (95th-pct capacity-risk signal)
    """
    field = "forecast_upper" if use_upper_bound else "forecast_prediction"
    result = request(
        "POST",
        "/.ml-anomalies-*/_search",
        {
            "size": 1,
            "sort": [{"timestamp": {"order": "asc"}}],
            "_source": [
                "timestamp", "forecast_prediction",
                "forecast_lower", "forecast_upper",
            ],
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"job_id":              JOB_ID}},
                        {"term": {"forecast_id":         forecast_id}},
                        {"term": {"result_type":         "model_forecast"}},
                        {"term": {"partition_field_value": segment}},
                    ],
                    "must": [
                        {"range": {field: {"gte": threshold_mbps}}}
                    ],
                }
            },
        },
    )
    hits = result.get("hits", {}).get("hits", [])
    return hits[0]["_source"] if hits else None


def get_peak_forecast(forecast_id: str, segment: str) -> float:
    """Return the maximum forecast_prediction value across the entire window."""
    result = request(
        "POST",
        "/.ml-anomalies-*/_search",
        {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"job_id":              JOB_ID}},
                        {"term": {"forecast_id":         forecast_id}},
                        {"term": {"result_type":         "model_forecast"}},
                        {"term": {"partition_field_value": segment}},
                    ]
                }
            },
            "aggs": {"max_pred": {"max": {"field": "forecast_prediction"}}},
        },
    )
    return result.get("aggregations", {}).get("max_pred", {}).get("value", 0.0)


def get_current_utilization(segment: str) -> float:
    """Return the most recent bandwidth_mbps reading for the segment."""
    result = request(
        "POST",
        "/network-traffic-metrics/_search",
        {
            "size": 1,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "_source": ["bandwidth_mbps"],
            "query": {"term": {"segment_name.keyword": segment}},
        },
    )
    hits = result.get("hits", {}).get("hits", [])
    return hits[0]["_source"]["bandwidth_mbps"] if hits else 0.0


# ── Resolve forecast ID ───────────────────────────────────────────────────────
forecast_id = args.forecast_id or resolve_forecast_id()
threshold_pct = args.threshold

print(f"Network Bandwidth Forecast — {threshold_pct:.0%} Capacity Threshold Analysis")
print("=" * 72)
print(f"  Job         : {JOB_ID}")
print(f"  Forecast ID : {forecast_id}")

win_start_ms, win_end_ms = get_forecast_window(forecast_id)
if win_start_ms and win_end_ms:
    win_start = datetime.fromtimestamp(win_start_ms / 1000, tz=timezone.utc)
    win_end   = datetime.fromtimestamp(win_end_ms   / 1000, tz=timezone.utc)
    win_weeks = (win_end - win_start).days / 7
    print(f"  Window      : {win_start.strftime('%Y-%m-%d')} → {win_end.strftime('%Y-%m-%d')} ({win_weeks:.0f} weeks)")

now = datetime.now(timezone.utc)
print()

# ── Per-segment threshold analysis ───────────────────────────────────────────
COL = 16
print(
    f"{'SEGMENT':<{COL}}  {'CAPACITY':>10}  {'THRESHOLD':>10}  "
    f"{'CURRENT':>10}  {'NOW %':>6}  {'CROSSES':>8}  DETAILS"
)
print("-" * 95)

alerts    = []
safe      = []
watchlist = []
risk      = []

for segment in sorted(SEGMENT_CAPACITY):
    capacity       = SEGMENT_CAPACITY[segment]
    threshold_mbps = capacity * threshold_pct
    current_bw     = get_current_utilization(segment)
    current_pct    = current_bw / capacity * 100
    crossing       = find_threshold_crossing(forecast_id, segment, threshold_mbps)
    upper_crossing = find_threshold_crossing(forecast_id, segment, threshold_mbps, use_upper_bound=True)
    peak_pred      = get_peak_forecast(forecast_id, segment)
    peak_pct       = peak_pred / capacity * 100

    if crossing:
        cross_ts    = datetime.fromtimestamp(crossing["timestamp"] / 1000, tz=timezone.utc)
        weeks_away  = (cross_ts - now).days / 7
        pred        = crossing["forecast_prediction"]
        lower       = crossing.get("forecast_lower", 0)
        upper       = crossing.get("forecast_upper", 0)
        status      = "YES"

        print(
            f"{segment:<{COL}}  {capacity:>8,} M  {threshold_mbps:>8,.0f} M  "
            f"{current_bw:>8,.0f} M  {current_pct:>5.1f}%  {status:>8}  "
            f"{cross_ts.strftime('%Y-%m-%d')} (~{weeks_away:.1f} wks)"
        )
        print(
            f"{'':>{COL+2}}  {'':>10}  {'':>10}  {'':>10}  {'':>6}  {'':>8}  "
            f"Predicted: {pred:,.0f} Mbps  [{lower:,.0f}–{upper:,.0f} Mbps 95% CI]"
        )

        if weeks_away <= 4:
            alerts.append((segment, cross_ts, weeks_away, capacity, pred, "central"))
        else:
            watchlist.append((segment, cross_ts, weeks_away, capacity, pred, "central"))
    elif upper_crossing:
        # Central estimate stays below threshold but upper confidence bound crosses —
        # a real capacity risk worth planning for.
        uc_ts      = datetime.fromtimestamp(upper_crossing["timestamp"] / 1000, tz=timezone.utc)
        uc_weeks   = (uc_ts - now).days / 7
        uc_upper   = upper_crossing.get("forecast_upper", 0)
        status     = "RISK"

        print(
            f"{segment:<{COL}}  {capacity:>8,} M  {threshold_mbps:>8,.0f} M  "
            f"{current_bw:>8,.0f} M  {current_pct:>5.1f}%  {status:>8}  "
            f"upper bound crosses {uc_ts.strftime('%Y-%m-%d')} (~{uc_weeks:.1f} wks)"
        )
        print(
            f"{'':>{COL+2}}  {'':>10}  {'':>10}  {'':>10}  {'':>6}  {'':>8}  "
            f"Central peak: {peak_pred:,.0f} Mbps ({peak_pct:.1f}%)  "
            f"Upper bound at crossing: {uc_upper:,.0f} Mbps"
        )
        risk.append((segment, uc_ts, uc_weeks, capacity, peak_pred))
    else:
        status = "SAFE"
        print(
            f"{segment:<{COL}}  {capacity:>8,} M  {threshold_mbps:>8,.0f} M  "
            f"{current_bw:>8,.0f} M  {current_pct:>5.1f}%  {status:>8}  "
            f"Peak forecast: {peak_pred:,.0f} Mbps ({peak_pct:.1f}%)"
        )
        safe.append(segment)

print()

# ── Action summary ────────────────────────────────────────────────────────────
if alerts:
    print("CRITICAL — upgrade required within 4 weeks:")
    for seg, ts, wks, cap, pred, _ in alerts:
        print(f"  {seg}: crosses {threshold_pct:.0%} on {ts.strftime('%Y-%m-%d')} "
              f"({wks:.1f} wks) — forecast peak {pred:,.0f}/{cap:,} Mbps")
    print()

if watchlist:
    print("WATCH — central forecast crosses threshold within window:")
    for seg, ts, wks, cap, pred, _ in watchlist:
        print(f"  {seg}: crosses {threshold_pct:.0%} on {ts.strftime('%Y-%m-%d')} "
              f"({wks:.1f} wks) — forecast peak {pred:,.0f}/{cap:,} Mbps")
    print()

if risk:
    print("CAPACITY RISK — upper confidence bound crosses threshold (plan upgrade):")
    for seg, ts, wks, cap, peak in risk:
        print(f"  {seg}: upper bound reaches {threshold_pct:.0%} by {ts.strftime('%Y-%m-%d')} "
              f"({wks:.1f} wks) — central peak {peak:,.0f}/{cap:,} Mbps")
    print()

if safe:
    print(f"SAFE — threshold not crossed within forecast window: {', '.join(safe)}")
    print()

# ── Raw ES|QL equivalent (for Kibana console) ─────────────────────────────────
print("Kibana Dev Tools query to replicate this report:")
print(f"""
POST .ml-anomalies-*/_search
{{
  "size": 1,
  "sort": [{{"timestamp": "asc"}}],
  "query": {{
    "bool": {{
      "filter": [
        {{"term": {{"job_id": "{JOB_ID}"}}}},
        {{"term": {{"forecast_id": "{forecast_id}"}}}},
        {{"term": {{"result_type": "model_forecast"}}}},
        {{"term": {{"partition_field_value": "edge-dmz"}}}}
      ],
      "must": [{{"range": {{"forecast_prediction": {{"gte": {SEGMENT_CAPACITY['edge-dmz'] * threshold_pct:.0f}}}}}}}]
    }}
  }}
}}
""")
