"""Class V: longitudinal divergence of denormalized copies after cutover.

    python lab/class5_drift.py --db lab_b --writes 20000

Every order embeds customer_address. Semantics adopted for the measurement: an OPEN order's
copy must equal the customer's current address; a SHIPPED order's copy is a historical
snapshot and is exempt. Two code paths change addresses: a 'service' path that fans the
change out to open orders (and can fail between the two writes), and an 'admin' path
(a support script) that updates the customer only. Divergence is measured every --step writes.
"""
import argparse
import csv
import os
import random
from datetime import datetime, timezone

from bson.decimal128 import Decimal128
from bson.int64 import Int64

from common import mongo_db, to_d128
from decimal import Decimal

DIVERGENCE = [
    {"$match": {"status": "open"}},
    {"$lookup": {"from": "customer", "localField": "customer_id", "foreignField": "customer_id", "as": "c"}},
    {"$unwind": "$c"},
    {"$match": {"$expr": {"$ne": ["$customer_address", "$c.address"]}}},
    {"$count": "n"},
]


def divergent(db):
    r = list(db.orders.aggregate(DIVERGENCE))
    return r[0]["n"] if r else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="lab_b")
    ap.add_argument("--writes", type=int, default=20_000)
    ap.add_argument("--step", type=int, default=1000)
    ap.add_argument("--admin-share", type=float, default=0.10, help="address changes made by the admin script")
    ap.add_argument("--fanout-failure", type=float, default=0.01, help="P(service path dies between its two writes)")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results"))
    a = ap.parse_args()
    rng = random.Random(a.seed)
    db = mongo_db(a.db)
    db.customer.create_index("customer_id")        # no-op on Path B; needed for $lookup on Path A
    cust_ids = [d["customer_id"] for d in db.customer.find({}, {"customer_id": 1})]
    template = db.orders.find_one()
    next_order = int(db.orders.find_one(sort=[("order_id", -1)])["order_id"]) + 1
    as_id = (lambda v: Int64(v)) if isinstance(template["order_id"], Int64) else int
    as_money = to_d128 if isinstance(template["total"], Decimal128) else float

    os.makedirs(a.out, exist_ok=True)
    path = os.path.join(a.out, f"{a.db}_class5.csv")
    rows = [("writes", "open_orders", "divergent_open_orders", "per_1000_open")]
    d0 = divergent(db)
    rows.append((0, db.orders.count_documents({"status": "open"}), d0, 0.0))
    counts = {"new_order": 0, "service": 0, "admin": 0, "ship": 0, "fanout_failed": 0}
    for w in range(1, a.writes + 1):
        op = rng.random()
        cid = rng.choice(cust_ids)
        if op < 0.25:                                           # new order copies current address
            c = db.customer.find_one({"customer_id": cid})
            doc = {k: v for k, v in template.items() if k != "_id"}
            doc.update(order_id=as_id(next_order), customer_id=cid, status="open",
                       total=as_money(Decimal(rng.randint(100, 99_999)) / 100),
                       created_at=datetime.now(timezone.utc), customer_address=c["address"])
            doc.pop("created_at_us", None)
            db.orders.insert_one(doc)
            next_order += 1
            counts["new_order"] += 1
        elif op < 0.85:                                         # address change
            addr = f"{rng.randint(1, 9999)} Murray Ave, Pittsburgh PA"
            db.customer.update_one({"customer_id": cid}, {"$set": {"address": addr}})
            if rng.random() < a.admin_share:
                counts["admin"] += 1                            # support script: customer only
            elif rng.random() < a.fanout_failure:
                counts["fanout_failed"] += 1                    # process died between the writes
            else:
                db.orders.update_many({"customer_id": cid, "status": "open"},
                                      {"$set": {"customer_address": addr}})
                counts["service"] += 1
        else:                                                   # ship: copy becomes a snapshot
            o = db.orders.find_one({"customer_id": cid, "status": "open"})
            if o:
                db.orders.update_one({"_id": o["_id"]}, {"$set": {"status": "shipped"}})
            counts["ship"] += 1
        if w % a.step == 0:
            n_open = db.orders.count_documents({"status": "open"})
            d = divergent(db)
            rows.append((w, n_open, d, round(1000 * d / n_open, 3) if n_open else 0))
            print(f"writes={w:>7} open={n_open:>7} divergent={d:>6}")
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)
    print(f"divergent at cutover (t=0): {d0}; ops: {counts}; written to {os.path.normpath(path)}")


if __name__ == "__main__":
    main()
