"""Path B (careful offline migration) and Path C (live migration with change data capture).

Path B:  python lab/path_b.py --db lab_b
         python lab/path_b.py --db lab_b_omit --null-policy omit     # Class II factor
Path C:  python lab/path_b.py --db lab_c --live --workload-seconds 120
         (add --fanout to make the CDC replay refresh embedded copies in orders)

Path B maps BIGINT->Int64, NUMERIC->Decimal128, keeps microseconds in a *_us companion
field, and rebuilds every constraint MongoDB can express: $jsonSchema validators, unique
indexes (with case-insensitive collation where the source used lower()), and a partial
unique index for uq_acct_ref. Foreign keys cannot be rebuilt and are deliberately not.
"""
import argparse
import subprocess
import sys
import time

import os

from pymongo import ASCENDING
from pymongo.collation import Collation

from common import pg, mongo_db, careful_doc, TABLES

CI = Collation(locale="en", strength=2)   # case-insensitive, like lower() in the source

VALIDATORS = {
    "customer": {"$jsonSchema": {"bsonType": "object",
        "required": ["customer_id", "email", "address", "updated_at"],
        "properties": {"customer_id": {"bsonType": "long"}, "email": {"bsonType": "string"},
                       "phone": {"bsonType": ["string", "null"]}, "address": {"bsonType": "string"},
                       "updated_at": {"bsonType": "date"}}}},
    "account": {"$jsonSchema": {"bsonType": "object",
        "required": ["account_id", "customer_id", "balance", "status"],
        "properties": {"account_id": {"bsonType": "long"}, "customer_id": {"bsonType": "long"},
                       "balance": {"bsonType": "decimal", "minimum": 0},
                       "status": {"enum": ["open", "frozen", "closed"]}}}},
    "ledger_entry": {"$and": [
        {"$jsonSchema": {"bsonType": "object", "required": ["entry_id", "account_id", "amount"],
            "properties": {"entry_id": {"bsonType": "long"}, "account_id": {"bsonType": "long"},
                           "amount": {"bsonType": "decimal"}, "memo": {"bsonType": ["string", "null"]},
                           "posted_at": {"bsonType": ["date", "null"]}}}},
        {"amount": {"$ne": 0}}]},                              # CHECK (amount <> 0)
    "orders": {"$jsonSchema": {"bsonType": "object",
        "required": ["order_id", "customer_id", "total", "status", "created_at"],
        "properties": {"order_id": {"bsonType": "long"}, "customer_id": {"bsonType": "long"},
                       "total": {"bsonType": "decimal", "exclusiveMinimum": True, "minimum": 0},
                       "status": {"enum": ["open", "shipped"]}, "created_at": {"bsonType": "date"}}}},
}


def create_target(db, strict):
    db.client.drop_database(db.name)
    for t, (pk, _) in TABLES.items():
        opts = {"validator": VALIDATORS[t], "validationLevel": "strict",
                "validationAction": "error"} if strict else {}
        db.create_collection(t, **opts)
        db[t].create_index([(pk, ASCENDING)], unique=True, name=f"{t}_pkey")
    db.customer.create_index([("email", ASCENDING)], unique=True, collation=CI, name="uq_customer_email")
    db.ledger_entry.create_index([("account_id", ASCENDING), ("memo", ASCENDING)], unique=True,
                                 collation=CI, partialFilterExpression={"memo": {"$gt": ""}},
                                 name="uq_acct_ref")
    db.orders.create_index([("customer_id", ASCENDING)], name="ix_orders_customer")


def fetch_sql(table):
    if table == "orders":   # denormalize the customer's address into each order (Class V)
        return ("SELECT o.*, c.address AS customer_address FROM orders o "
                "JOIN customer c USING (customer_id)")
    return f"SELECT * FROM {table}"


def copy_table(conn, db, table, policy, batch=5000):
    n = 0
    with conn.cursor(name=f"snap_{table}") as cur:
        cur.execute(fetch_sql(table))
        while rows := cur.fetchmany(batch):
            db[table].insert_many([careful_doc(r, policy) for r in rows], ordered=False)
            n += len(rows)
    return n


CDC_SQL = """
CREATE TABLE IF NOT EXISTS cdc_log (id BIGSERIAL PRIMARY KEY, tbl TEXT, pk BIGINT);
CREATE OR REPLACE FUNCTION cdc_capture() RETURNS trigger AS $$
DECLARE k BIGINT;
BEGIN
  k := (to_jsonb(COALESCE(NEW, OLD)) ->> TG_ARGV[0])::BIGINT;
  INSERT INTO cdc_log(tbl, pk) VALUES (TG_TABLE_NAME, k);
  RETURN NULL;
END $$ LANGUAGE plpgsql;
"""


def install_cdc(conn):
    conn.execute(CDC_SQL)
    for t, (pk, _) in TABLES.items():
        conn.execute(f"DROP TRIGGER IF EXISTS cdc_{t} ON {t}")
        conn.execute(f"CREATE TRIGGER cdc_{t} AFTER INSERT OR UPDATE OR DELETE ON {t} "
                     f"FOR EACH ROW EXECUTE FUNCTION cdc_capture('{pk}')")


def remove_cdc(conn):
    for t in TABLES:
        conn.execute(f"DROP TRIGGER IF EXISTS cdc_{t} ON {t}")


def replay(conn, db, policy, fanout, batch=5000):
    """Consume committed change records (DELETE ... RETURNING, so a transaction that took a
    low log id but committed late is never skipped) and re-read each changed row by key.
    Payloads are not shipped, because decoding jsonb in Python would itself turn NUMERIC
    into float. Returns the number of change records applied."""
    applied = 0
    while True:
        with conn.transaction():
            log = conn.execute("DELETE FROM cdc_log WHERE id IN (SELECT id FROM cdc_log ORDER BY id "
                               "LIMIT %s FOR UPDATE SKIP LOCKED) RETURNING id, tbl, pk", (batch,)).fetchall()
        if not log:
            return applied
        for e in sorted(log, key=lambda r: r["id"]):
            t, pk = e["tbl"], e["pk"]
            pkcol = TABLES[t][0]
            where = " WHERE o.order_id = %s" if t == "orders" else f" WHERE {pkcol} = %s"
            row = conn.execute(fetch_sql(t) + where, (pk,)).fetchone()
            if row is None:
                db[t].delete_one({pkcol: pk})
            else:
                db[t].replace_one({pkcol: pk}, careful_doc(row, policy), upsert=True)
                if t == "customer" and fanout:
                    # the step a table-to-collection CDC pipeline does not know it needs
                    db.orders.update_many({"customer_id": pk, "status": "open"},
                                          {"$set": {"customer_address": row["address"]}})
        applied += len(log)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="lab_b")
    ap.add_argument("--null-policy", choices=["keep", "omit", "zero"], default="keep")
    ap.add_argument("--no-validators", action="store_true")
    ap.add_argument("--live", action="store_true", help="Path C: snapshot + CDC while a workload runs")
    ap.add_argument("--workload-seconds", type=int, default=120)
    ap.add_argument("--fanout", action="store_true")
    a = ap.parse_args()
    db = mongo_db(a.db)
    create_target(db, strict=not a.no_validators)

    ctl = pg()
    wl = None
    if a.live:
        install_cdc(ctl)
        ctl.execute("TRUNCATE cdc_log")          # capture starts before the snapshot
        wl = subprocess.Popen([sys.executable, "pg_workload.py", "--seconds", str(a.workload_seconds)],
                              cwd=os.path.dirname(os.path.abspath(__file__)))
        time.sleep(3)

    snap = pg(autocommit=False)
    snap.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
    for t in TABLES:
        print(f"snapshot {t}: {copy_table(snap, db, t, a.null_policy)}")
    snap.rollback()

    if a.live:
        total = 0
        while wl.poll() is None:              # catch up continuously while writes continue
            total += replay(ctl, db, a.null_policy, a.fanout)
            time.sleep(1)
        total += replay(ctl, db, a.null_policy, a.fanout)      # final drain = cutover
        remove_cdc(ctl)
        print(f"cutover complete; {total} change records replayed")


if __name__ == "__main__":
    main()
