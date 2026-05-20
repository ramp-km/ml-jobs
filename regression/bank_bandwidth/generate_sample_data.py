"""
Generate synthetic bank branch data and bulk-index it into Elasticsearch.

Features:
  num_employees       -- staff at the branch (tellers, managers, back-office)
  num_customers       -- customers visiting / served per day
  num_transactions    -- total transactions processed per day
                         (cash, card, NEFT/RTGS, UPI, ATM)

Target:
  bandwidth_mbps  (what the regression model will learn to predict)

~500 branch-day records across four branch tiers (rural / suburban / urban / flagship).
Bandwidth is driven by a deterministic formula + noise so the model has a real signal.
"""
import json
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
INDEX = "bank-branches"

random.seed(42)

# Branch tiers: each defines realistic ranges for the three features
TIERS = {
    "rural": {
        "employees":    (2,  10),
        "customers":    (20,  120),
        "transactions": (40,  250),
    },
    "suburban": {
        "employees":    (8,  25),
        "customers":    (100, 450),
        "transactions": (200, 900),
    },
    "urban": {
        "employees":    (20, 60),
        "customers":    (350, 1_100),
        "transactions": (700, 2_500),
    },
    "flagship": {
        "employees":    (50, 120),
        "customers":    (900, 2_500),
        "transactions": (2_000, 6_000),
    },
}
TIER_NAMES = list(TIERS.keys())


def make_branch(record_id):
    tier   = TIER_NAMES[record_id % len(TIER_NAMES)]
    ranges = TIERS[tier]

    num_employees    = random.randint(*ranges["employees"])
    num_customers    = random.randint(*ranges["customers"])
    num_transactions = random.randint(*ranges["transactions"])

    # Bandwidth formula (Mbps):
    #
    #   Employees drive the largest chunk — each workstation handles
    #   core banking, email, VoIP, and screen-sharing.
    #   Customers add load via internet/mobile banking and in-branch kiosks.
    #   Transactions add short bursts for payment-gateway and CBS round-trips.
    #   A fixed base covers always-on infra: VPN tunnels, monitoring, CCTV.
    #
    #   bandwidth = base
    #             + 3.2  × employees          (workstation + VoIP + CBS per staff)
    #             + 0.25 × customers          (digital banking, ATM, kiosk sessions)
    #             + 0.08 × transactions       (payment gateway, CBS round-trips)
    #             + noise (±6 %)

    base      = 8.0
    bw        = (
        base
        + 3.2  * num_employees
        + 0.25 * num_customers
        + 0.08 * num_transactions
    )
    noise     = random.gauss(0, bw * 0.06)
    bandwidth = round(max(bw + noise, 5.0), 2)

    return {
        "branch_id":        f"BR{record_id:04d}",
        "branch_tier":      tier,
        "num_employees":    num_employees,
        "num_customers":    num_customers,
        "num_transactions": num_transactions,
        "bandwidth_mbps":   bandwidth,
    }


records = [make_branch(i) for i in range(1, 501)]
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


BATCH = 250
for i in range(0, len(records), BATCH):
    bulk_index(records[i:i + BATCH])
    print(f"  Indexed {min(i + BATCH, len(records))}/{len(records)}")

print("Done.")
