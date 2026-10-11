"""Synthetic OLTP workload against the SOURCE, used during the Path C live migration."""
import argparse
import random
import time
from datetime import datetime, timezone

from common import pg
from generate import amount, three_way, P53


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=120)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    c = pg()
    n_cust = c.execute("SELECT max(customer_id) AS m FROM customer").fetchone()["m"]
    n_acct = c.execute("SELECT max(account_id) AS m FROM account").fetchone()["m"]
    n_ord = c.execute("SELECT max(order_id) AS m FROM orders").fetchone()["m"]
    next_id, writes, end = 10**12, 0, time.time() + a.seconds
    while time.time() < end:
        op = rng.random()
        if op < 0.5:
            next_id += 1
            eid = P53 + 2 * next_id + 1 if rng.random() < 0.05 else next_id
            c.execute("INSERT INTO ledger_entry VALUES (%s,%s,%s,%s,%s)",
                      (eid, rng.randint(1, n_acct), amount(rng), three_way(rng, f"Ref-{eid}"),
                       datetime.now(timezone.utc)))
        elif op < 0.7:
            c.execute("UPDATE customer SET phone=%s, updated_at=now() WHERE customer_id=%s",
                      (three_way(rng, "+1-412-555-0000"), rng.randint(1, n_cust)))
        elif op < 0.9:
            c.execute("UPDATE customer SET address=%s, updated_at=now() WHERE customer_id=%s",
                      (f"{rng.randint(1, 9999)} Fifth Ave, Pittsburgh PA", rng.randint(1, n_cust)))
        else:
            c.execute("UPDATE orders SET status='shipped' WHERE order_id=%s", (rng.randint(1, n_ord),))
        writes += 1
    print(f"workload: {writes} writes in {a.seconds}s")


if __name__ == "__main__":
    main()
