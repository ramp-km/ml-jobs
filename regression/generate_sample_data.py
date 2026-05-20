"""
Generate synthetic IT incident data and bulk-index it into Elasticsearch.

Features:
  severity        -- 1 (low) to 5 (critical)
  category        -- network | hardware | software | security | user_access
  num_affected_users
  team_size       -- engineers assigned to the incident
  hour_of_day     -- 0-23, when the incident was opened
  day_of_week     -- 0 (Mon) to 6 (Sun)
  num_comments    -- proxy for complexity / back-and-forth

Target:
  resolution_time_minutes  (what the model will learn to predict)

~600 incidents. Resolution time is driven by a deterministic formula + noise
so the model has a real signal to learn.
"""
import json
import math
import random
import ssl
import urllib.request
import urllib.error
import os
import sys
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
    for key, value in _parse_kv_file(root / ".env"):
        os.environ.setdefault(key, value)
    for key, value in _parse_kv_file(root / ".elastic-credentials"):
        os.environ.setdefault(key, value)


load_config()

SSL_CONTEXT = ssl._create_unverified_context()

ES_URL = os.environ["ELASTICSEARCH_URL"]
API_KEY = os.environ["ELASTICSEARCH_API_KEY"]
INDEX = "it-incidents"

random.seed(42)

# Base resolution time (minutes) per category before other factors are applied
CATEGORY_BASE = {
    "user_access": 25,
    "software":    55,
    "network":     90,
    "hardware":    130,
    "security":    160,
}
CATEGORIES = list(CATEGORY_BASE.keys())


def make_incident(incident_id):
    category        = random.choice(CATEGORIES)
    severity        = random.randint(1, 5)
    num_affected    = random.randint(1, 300)
    team_size       = random.randint(1, 8)
    hour_of_day     = random.randint(0, 23)
    day_of_week     = random.randint(0, 6)
    num_comments    = random.randint(2, 60)

    is_business_hours = 1 if (9 <= hour_of_day <= 17 and day_of_week <= 4) else 0

    # Resolution time formula:
    #   base + severity premium + user load + comment overhead
    #   reduced by team size (logarithmic — doubling the team doesn't halve the time)
    #   penalised when outside business hours (slower response, on-call lag)
    base = CATEGORY_BASE[category]
    time = (
        base
        + severity * 20
        + num_affected * 0.15
        + num_comments * 1.2
    )
    time /= (1 + 0.35 * math.log1p(team_size))   # team discount
    if not is_business_hours:
        time *= 1.55                               # off-hours penalty
    noise = random.gauss(0, time * 0.08)          # ±8 % noise
    resolution_time = round(max(time + noise, 5), 1)

    return {
        "incident_id":            f"INC{incident_id:05d}",
        "category":               category,
        "severity":               severity,
        "num_affected_users":     num_affected,
        "team_size":              team_size,
        "hour_of_day":            hour_of_day,
        "day_of_week":            day_of_week,
        "is_business_hours":      is_business_hours,
        "num_comments":           num_comments,
        "resolution_time_minutes": resolution_time,
    }


records = [make_incident(i) for i in range(1, 601)]
print(f"Total records to index: {len(records)}")


def bulk_index(batch):
    body = ""
    for doc in batch:
        body += json.dumps({"index": {"_index": INDEX}}) + "\n"
        body += json.dumps(doc) + "\n"

    req = urllib.request.Request(
        f"{ES_URL}/_bulk",
        data=body.encode(),
        headers={
            "Authorization": f"ApiKey {API_KEY}",
            "Content-Type": "application/x-ndjson",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, context=SSL_CONTEXT) as resp:
        result = json.loads(resp.read())
        if result.get("errors"):
            errs = [i for i in result["items"] if i.get("index", {}).get("error")]
            print(f"  Bulk errors: {len(errs)}", file=sys.stderr)


BATCH = 300
for i in range(0, len(records), BATCH):
    bulk_index(records[i:i + BATCH])
    print(f"  Indexed {min(i + BATCH, len(records))}/{len(records)}")

print("Done.")
