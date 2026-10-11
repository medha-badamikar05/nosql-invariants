"""Class IV: multi-entity updates under induced process failure.

    python lab/class4_torn.py --db lab_b --mode notxn
    python lab/class4_torn.py --db lab_b --mode txn

Each transfer debits one account, writes a debit leg, credits another account and writes
a credit leg: four writes that were one transaction in the source. Worker processes are
killed (os._exit) at random points between writes. Afterwards we check two invariants the
source made unfalsifiable: total balance is conserved, and every transfer has 0 or 2 legs.
"""
import argparse
import multiprocessing as mp
import os
import random
import time
from decimal import Decimal

from bson.decimal128 import Decimal128
from pymongo import MongoClient

from common import MONGO_URI, to_d128

CRASH = 3


def money(v):
    return v.to_decimal() if isinstance(v, Decimal128) else Decimal(v)


def worker(db_name, ids, quota, seed, mode, p, use_d128):
    rng = random.Random(seed)
    db = MongoClient(MONGO_URI)[db_name]

    def maybe_crash():
        if rng.random() < p:
            os._exit(CRASH)          # no cleanup, no rollback: exactly like a killed pod

    def transfer(tid, a, b, amt, s=None):
        neg = to_d128(-amt) if use_d128 else -float(amt)
        pos = to_d128(amt) if use_d128 else float(amt)
        db.account.update_one({"account_id": a}, {"$inc": {"balance": neg}}, session=s)
        maybe_crash()
        db.transfer_leg.insert_one({"tid": tid, "leg": "D", "account_id": a, "amount": neg}, session=s)
        maybe_crash()
        db.account.update_one({"account_id": b}, {"$inc": {"balance": pos}}, session=s)
        maybe_crash()
        db.transfer_leg.insert_one({"tid": tid, "leg": "C", "account_id": b, "amount": pos}, session=s)

    for i in range(quota):
        tid = f"{seed}-{i}"
        a, b = rng.sample(ids, 2)
        amt = Decimal(rng.randint(1, 100_000)) / 100
        if mode == "txn":
            with db.client.start_session() as s:
                s.with_transaction(lambda s: transfer(tid, a, b, amt, s))
        else:
            transfer(tid, a, b, amt)
    os._exit(0)


def total(db, ids):
    return sum(money(d["balance"]) for d in db.account.find({"account_id": {"$in": ids}}, {"balance": 1}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="lab_b")
    ap.add_argument("--mode", choices=["notxn", "txn"], default="notxn")
    ap.add_argument("--transfers", type=int, default=20_000)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--batch", type=int, default=250, help="transfers per worker process")
    ap.add_argument("--crash-prob", type=float, default=0.002, help="per step, per transfer")
    ap.add_argument("--accounts", type=int, default=1000)
    a = ap.parse_args()

    db = MongoClient(MONGO_URI)[a.db]
    db.transfer_leg.drop()
    db.transfer_leg.create_index("tid")
    ids = [int(d["account_id"]) for d in db.account.find({}, {"account_id": 1}).sort("account_id", 1).limit(a.accounts)]
    use_d128 = isinstance(db.account.find_one()["balance"], Decimal128)
    before = total(db, ids)

    ctx = mp.get_context("spawn")
    started, crashes, seed, running = 0, 0, 1, []
    t0 = time.time()
    while started < a.transfers or running:
        while started < a.transfers and len(running) < a.workers:
            q = min(a.batch, a.transfers - started)
            pr = ctx.Process(target=worker, args=(a.db, ids, q, seed, a.mode, a.crash_prob, use_d128))
            pr.start()
            running.append(pr)
            started += q
            seed += 1
        for pr in running[:]:
            pr.join(timeout=0.05)
            if pr.exitcode is not None:
                crashes += pr.exitcode == CRASH
                running.remove(pr)
    if a.mode == "txn":
        time.sleep(6)                      # let the server abort orphaned transactions

    after = total(db, ids)
    legs = list(db.transfer_leg.aggregate([{"$group": {"_id": "$tid", "n": {"$sum": 1}}},
                                           {"$group": {"_id": "$n", "count": {"$sum": 1}}}]))
    by_legs = {r["_id"]: r["count"] for r in legs}
    print(f"db={a.db} mode={a.mode} transfers_scheduled={a.transfers} process_crashes={crashes} "
          f"elapsed={time.time() - t0:.1f}s")
    print(f"balance total before={before} after={after} drift={after - before}")
    print(f"transfers with 2 legs={by_legs.get(2, 0)}  with 1 leg (torn)={by_legs.get(1, 0)}")
    print("note: a crash after the debit but before the first leg is torn yet leaves no leg; "
          "only the balance drift reveals it.")


if __name__ == "__main__":
    main()
