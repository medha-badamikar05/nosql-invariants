"""Populate the adversarial source. Default scale ~= 1,000,000 rows in total.

    python lab/generate.py            # full size
    python lab/generate.py --scale 0.05   # quick smoke test
"""
import argparse
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from common import pg

P53 = 2 ** 53


def amount(rng):
    """Nonzero NUMERIC(19,4) values concentrated on binary64-hostile cases."""
    kind = rng.random()
    if kind < 0.60:                       # everyday money: 0.0001 .. 9,999.9999
        v = Decimal(rng.randint(1, 99_999_999)) / Decimal(10_000)
    elif kind < 0.90:                     # classic x.x5 / x.x9 prices
        v = Decimal(rng.randint(1, 9_999)) + Decimal(rng.choice(["0.1", "0.2", "0.3", "0.05", "0.99", "0.0001"]))
    else:                                 # large magnitudes: more than 15-16 significant digits
        v = Decimal(rng.randint(10**11, 10**14)) + Decimal(rng.randint(1, 9_999)) / Decimal(10_000)
    return v if rng.random() < 0.7 else -v


def three_way(rng, value):
    r = rng.random()
    return None if r < 1 / 3 else ("" if r < 2 / 3 else value)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--big-id-fraction", type=float, default=0.02)
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    n_cust, n_acct = int(20_000 * a.scale), int(30_000 * a.scale)
    n_ledger, n_orders = int(800_000 * a.scale), int(150_000 * a.scale)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with pg() as c:
        c.execute("TRUNCATE orders, ledger_entry, account, customer")
        with c.cursor().copy("COPY customer (customer_id,email,phone,address,updated_at) FROM STDIN") as cp:
            for i in range(1, n_cust + 1):
                email = f"user{i}@example.com" if i % 2 else f"User{i}@Example.COM"
                cp.write_row((i, email, three_way(rng, f"+1-412-555-{i % 10000:04d}"),
                              f"{rng.randint(1, 9999)} Forbes Ave, Pittsburgh PA", base + timedelta(microseconds=rng.randint(0, 10**12))))
        with c.cursor().copy("COPY account (account_id,customer_id,balance,status) FROM STDIN") as cp:
            for i in range(1, n_acct + 1):
                cp.write_row((i, rng.randint(1, n_cust), Decimal("1000000.0000"),
                              rng.choice(["open", "open", "open", "frozen", "closed"])))
        with c.cursor().copy("COPY ledger_entry (entry_id,account_id,amount,memo,posted_at) FROM STDIN") as cp:
            t = base
            for i in range(1, n_ledger + 1):
                if rng.random() < a.big_id_fraction:
                    # odd values above 2^53 are never exactly representable as binary64
                    eid = rng.choice([P53, 2 ** 62]) + 2 * i + 1
                else:
                    eid = i
                # 10% of rows share a millisecond with their predecessor but differ in microseconds
                t = t + (timedelta(microseconds=rng.randint(1, 900)) if rng.random() < 0.10
                         else timedelta(milliseconds=rng.randint(1, 50), microseconds=rng.randint(0, 999)))
                cp.write_row((eid, rng.randint(1, n_acct), amount(rng), three_way(rng, f"Ref-{eid}"), t))
        with c.cursor().copy("COPY orders (order_id,customer_id,total,status,created_at) FROM STDIN") as cp:
            for i in range(1, n_orders + 1):
                cp.write_row((i, rng.randint(1, n_cust), abs(amount(rng)), rng.choice(["open", "shipped"]),
                              base + timedelta(microseconds=rng.randint(0, 10**12))))
        c.execute("ANALYZE")
        for t in ["customer", "account", "ledger_entry", "orders"]:
            print(t, c.execute(f"SELECT count(*) AS n FROM {t}").fetchone()["n"])


if __name__ == "__main__":
    main()
