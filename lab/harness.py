"""Verification harness for Classes I-III. Run once per target database:

    python lab/harness.py --db lab_a
    python lab/harness.py --db lab_b

Writes results/<db>.json and prints a summary. Classes IV and V have their own scripts
(class4_torn.py, class5_drift.py) because they need a workload, not a read.
"""
import argparse
import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg
from bson.decimal128 import Decimal128
from bson.int64 import Int64
from pymongo.errors import OperationFailure

from common import pg, mongo_db, TABLES, EPOCH, UTC, to_d128
from path_b import fetch_sql

ABSENT = object()   # a field missing from a document; never conflated with None


# ---------------------------------------------------------------- Class I
def canon(v):
    """Type-tagged canonical form. No floating-point path: floats are expanded exactly,
    so the harness cannot hide the error it is looking for. NULL, absent and '' are
    three different outputs, so the harness cannot commit Class II collapse itself."""
    if v is ABSENT:
        return "\x00absent"
    if v is None:
        return "\x00null"
    if isinstance(v, bool):
        return f"b:{v}"
    if isinstance(v, Decimal128):
        v = v.to_decimal()
    if isinstance(v, float):
        v = Decimal(v)                      # exact binary expansion
    if isinstance(v, int):
        v = Decimal(v)
    if isinstance(v, Decimal):
        return "n:" + format(v.normalize(), "f")
    if isinstance(v, datetime):
        if v.tzinfo is None:                # defensive: treat naive values as UTC
            v = v.replace(tzinfo=UTC)
        return "t:" + v.astimezone(UTC).isoformat(timespec="microseconds")
    return "s:" + str(v)


def target_value(doc, col):
    us = doc.get(col + "_us")               # Path B keeps microseconds in a companion field
    if isinstance(us, int):
        return EPOCH + timedelta(microseconds=int(us))
    return doc.get(col, ABSENT)


def h(s):
    return hashlib.blake2b(s.encode("utf-8"), digest_size=8).digest()


def class_one(conn, db, table):
    pk = TABLES[table][0]
    src, cols = {}, None
    with conn.transaction(), conn.cursor(name=f"h_{table}") as cur:
        cur.execute(fetch_sql(table))
        for row in cur:
            if cols is None:
                cols = list(row.keys())
            src[canon(row[pk])] = tuple(h(canon(row[c])) for c in cols)
    n_src = len(src)
    n_tgt, matched, lenient, seen = 0, 0, 0, Counter()
    col_mismatch, null_vs_absent = Counter(), Counter()
    tgt_ts = set()
    ts_col = TABLES[table][1][0] if TABLES[table][1] else None
    null_h, absent_h = h("\x00null"), h("\x00absent")
    for doc in db[table].find({}, {"_id": 0}):
        n_tgt += 1
        key = canon(target_value(doc, pk))
        seen[key] += 1
        tv = [h(canon(target_value(doc, c))) for c in cols]
        if ts_col:
            tgt_ts.add(canon(target_value(doc, ts_col)))
        sv = src.get(key)
        if sv is None:
            continue
        if tuple(tv) == sv:
            matched += 1
        ok_lenient = True
        for c, a, b in zip(cols, sv, tv):
            if a != b:
                if a == null_h and b == absent_h:
                    null_vs_absent[c] += 1          # counts as agreement under a lenient harness
                else:
                    col_mismatch[c] += 1
                    ok_lenient = False
        lenient += ok_lenient
    out = {
        "record_parity": n_tgt / n_src if n_src else None,
        "source_rows": n_src, "target_docs": n_tgt,
        "digest_parity_strict": matched / n_src,
        "digest_parity_lenient_null": lenient / n_src,
        "keys_not_found_in_target": sum(1 for k in src if k not in seen),
        "target_key_collisions": sum(n - 1 for n in seen.values() if n > 1),
        "mismatches_by_column": dict(col_mismatch),
        "null_became_absent_by_column": dict(null_vs_absent),
    }
    if ts_col:
        n_distinct = conn.execute(f"SELECT count(DISTINCT {ts_col}) AS n FROM {table}").fetchone()["n"]
        out["timestamp_distinct_source"] = n_distinct
        out["timestamp_distinct_target"] = len(tgt_ts)
        out["timestamp_ties_created"] = n_distinct - len(tgt_ts)
    return out


def aggregate_drift(conn, db, table, col):
    s = conn.execute(f"SELECT sum({col}) AS s FROM {table}").fetchone()["s"]
    r = list(db[table].aggregate([{"$group": {"_id": None, "s": {"$sum": f"${col}"}}}]))
    t = r[0]["s"] if r else 0
    t = t.to_decimal() if isinstance(t, Decimal128) else Decimal(t)
    return {"source_sum": str(s), "target_sum": format(t, "f"),
            "absolute_drift": format(abs(t - s), "f"),
            "relative_drift": float(abs(t - s) / abs(s)) if s else None,
            "target_sum_type": type(r[0]["s"]).__name__ if r else None}


# ---------------------------------------------------------------- Class II
PREDICATES = [
    # (label, table, SQL predicate, natural MongoDB translation)
    ("phone IS NULL",            "customer", "phone IS NULL",               {"phone": None}),
    ("phone IS NOT NULL",        "customer", "phone IS NOT NULL",           {"phone": {"$ne": None}}),
    ("phone = ''",               "customer", "phone = ''",                  {"phone": ""}),
    ("phone <> ''",              "customer", "phone <> ''",                 {"phone": {"$ne": ""}}),
    ("phone IS NULL OR = ''",    "customer", "phone IS NULL OR phone = ''", {"phone": {"$in": [None, ""]}}),
    ("NOT (phone = '')",         "customer", "NOT (phone = '')",            {"phone": {"$not": {"$eq": ""}}}),
    ("memo IS NULL",             "ledger_entry", "memo IS NULL",            {"memo": None}),
    ("memo <> ''",               "ledger_entry", "memo <> ''",              {"memo": {"$ne": ""}}),
    ("memo IS DISTINCT FROM ''", "ledger_entry", "memo IS DISTINCT FROM ''", {"memo": {"$ne": ""}}),
]


def class_two(conn, db):
    rows = []
    for label, t, sql, mq in PREDICATES:
        s = conn.execute(f"SELECT count(*) AS n FROM {t} WHERE {sql}").fetchone()["n"]
        m = db[t].count_documents(mq)
        rows.append({"predicate": label, "sql": s, "mongo": m, "diverges": s != m})
    return {"predicates": rows, "predicate_divergence": sum(r["diverges"] for r in rows)}


# ---------------------------------------------------------------- Class III
INVENTORY_SQL = """
SELECT c.conrelid::regclass::text AS tbl, c.conname AS name, c.contype AS kind,
       pg_get_constraintdef(c.oid) AS def, NULL::text AS col
  FROM pg_constraint c WHERE c.conrelid::regclass::text = ANY(%(t)s)
UNION ALL
SELECT i.indrelid::regclass::text, i.indexrelid::regclass::text, 'i', pg_get_indexdef(i.indexrelid), NULL
  FROM pg_index i
 WHERE i.indisunique AND i.indrelid::regclass::text = ANY(%(t)s)
   AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid)
UNION ALL
SELECT a.attrelid::regclass::text, a.attname || '_not_null', 'n', 'NOT NULL', a.attname
  FROM pg_attribute a
 WHERE a.attnotnull AND a.attnum > 0 AND NOT a.attisdropped AND a.attrelid::regclass::text = ANY(%(t)s)
ORDER BY 1, 3, 2
"""

# CHECK constraints and unique indexes need a hand-written minimal violation.
# Every other kind is generated from the catalog. Anything left over is reported UNPROBED.
HAND = {
    ("account", "balance_nonneg"):         ({}, lambda d: {"balance": -1}),
    ("account", "status_enum"):            ({}, lambda d: {"status": "bogus"}),
    ("ledger_entry", "nonzero"):           ({}, lambda d: {"amount": 0}),
    ("orders", "total_pos"):               ({}, lambda d: {"total": 0}),
    ("orders", "order_status"):            ({}, lambda d: {"status": "lost"}),
    ("customer", "uq_customer_email"):     ({}, lambda d: {"email": d["email"].swapcase()}),
    ("ledger_entry", "uq_acct_ref"):       ("memo_nonempty", lambda d: {"memo": d["memo"].swapcase()}),
}
DELETE = object()
EXPECTED_SQLSTATE = {"p": "23505", "i": "23505", "f": "23503", "n": "23502", "c": "23514"}
# Columns under a unique rule other than the primary key. Every probe gives them fresh
# values first, so that a probe aimed at one constraint cannot be rejected by another.
NEUTRALIZE = {"customer": lambda n: {"email": f"probe{n}@probe.test"},
              "ledger_entry": lambda n: {"memo": f"probe-{n}"}}


def like(existing, v):
    """Give a probe value the same BSON type as the field it replaces, so a validator
    rejects the probe for the violated rule and not for an incidental type change."""
    if isinstance(v, bool) or not isinstance(v, (int, Decimal)):
        return v
    if isinstance(existing, Decimal128):
        return to_d128(Decimal(v))
    if isinstance(existing, float):
        return float(v)
    if isinstance(existing, Int64):
        return Int64(v)
    if isinstance(existing, Decimal):
        return Decimal(v)
    return v


def build_probes(inv):
    probes = []
    for it in inv:
        t, name, kind = it["tbl"], it["name"], it["kind"]
        pk = TABLES[t][0]
        if kind == "p":
            probes.append((t, name, kind, "duplicate key", None, lambda d: {}, False))
        elif kind == "f":
            col = it["def"].split("(")[1].split(")")[0]
            probes.append((t, name, kind, f"{col} -> nonexistent", None,
                           lambda d, c=col: {c: 999_999_999_999}, True))
        elif kind == "n":
            col = it["col"]
            probes.append((t, name, kind, f"{col} = null", None, lambda d, c=col: {c: None}, col != pk))
            probes.append((t, name, kind, f"{col} absent", None, lambda d, c=col: {c: DELETE}, col != pk))
        elif (t, name) in HAND:
            base, fn = HAND[(t, name)]
            probes.append((t, name, kind, "hand-written violation", base or None, fn, True))
        else:
            probes.append((t, name, kind, "UNPROBED", None, None, False))
    return probes


def apply(base, mut, fresh_pk, pkcol, mongo, table, seq):
    d = dict(base)
    d.pop("_id", None)
    d.update(NEUTRALIZE.get(table, lambda n: {})(seq))
    if not mongo:
        d.pop("customer_address", None)
    if fresh_pk is not None:
        d[pkcol] = like(base.get(pkcol), fresh_pk)
    for k, v in mut(base).items():
        if v is DELETE:
            d.pop(k, None)
            d.pop(k + "_us", None)
        else:
            d[k] = like(base.get(k), v) if mongo else v
            if mongo and v is None:
                d.pop(k + "_us", None)
    return d


def pg_rejects(conn, table, row):
    cols = list(row)
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))})"
    try:
        with conn.transaction(force_rollback=True):
            conn.execute(sql, [row[c] for c in cols])
        return False, None
    except psycopg.Error as e:
        return True, e.sqlstate


def mongo_rejects(db, table, doc):
    try:
        r = db[table].insert_one(doc)
        db[table].delete_one({"_id": r.inserted_id})
        return False, None
    except OperationFailure as e:        # WriteError, DuplicateKeyError, ... all derive from this
        return True, e.code


def class_three(conn, db):
    inv = conn.execute(INVENTORY_SQL, {"t": list(TABLES)}).fetchall()
    results, counter = [], 9_000_000_000_000
    for t, name, kind, what, base_kind, mut, fresh in build_probes(inv):
        pk = TABLES[t][0]
        rec = {"table": t, "constraint": name, "probe": what}
        if mut is None:
            rec["status"] = "UNPROBED"
            results.append(rec)
            continue
        where_sql, where_mq = ("memo <> ''", {"memo": {"$gt": ""}}) if base_kind == "memo_nonempty" else ("TRUE", {})
        src = conn.execute(f"SELECT * FROM {t} WHERE {where_sql} ORDER BY {pk} LIMIT 1").fetchone()
        tgt = db[t].find_one(where_mq, sort=[(pk, 1)])
        if src is None or tgt is None:
            rec["status"] = "NO_BASE_ROW"
            results.append(rec)
            continue
        counter += 1
        fresh_pk = counter if fresh else None
        rej_src, code_src = pg_rejects(conn, t, apply(src, mut, fresh_pk, pk, False, t, counter))
        # control: the source must reject the probe, and for the intended reason
        valid = rej_src and code_src == EXPECTED_SQLSTATE[kind]
        rec.update(source_rejects=rej_src, source_sqlstate=code_src)
        if valid:
            rej_tgt, code_tgt = mongo_rejects(db, t, apply(tgt, mut, fresh_pk, pk, True, t, counter))
            rec.update(target_rejects=rej_tgt, target_code=code_tgt,
                       status="PRESERVED" if rej_tgt else "EVAPORATED")
        else:
            rec["status"] = "INVALID_PROBE"
        results.append(rec)
    valid = [r for r in results if r["status"] in ("PRESERVED", "EVAPORATED")]
    return {"inventory_size": len(inv), "probes": results,
            "constraint_survival": (sum(r["status"] == "PRESERVED" for r in valid) / len(valid)) if valid else None,
            "status_counts": dict(Counter(r["status"] for r in results))}


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results"))
    a = ap.parse_args()
    conn, db = pg(), mongo_db(a.db)
    res = {"db": a.db, "class_I": {t: class_one(conn, db, t) for t in TABLES}}
    res["class_I"]["aggregate_drift"] = {"ledger_entry.amount": aggregate_drift(conn, db, "ledger_entry", "amount"),
                                         "orders.total": aggregate_drift(conn, db, "orders", "total")}
    res["class_II"] = class_two(conn, db)
    res["class_III"] = class_three(conn, db)
    os.makedirs(a.out, exist_ok=True)
    path = os.path.join(a.out, f"{a.db}.json")
    with open(path, "w") as f:
        json.dump(res, f, indent=2, default=str)

    print(f"\n=== {a.db} ===")
    for t in TABLES:
        r = res["class_I"][t]
        print(f"{t:13s} record parity {r['record_parity']:.4f} | digest parity strict {r['digest_parity_strict']:.4f}"
              f" lenient {r['digest_parity_lenient_null']:.4f} | keys lost {r['keys_not_found_in_target']}"
              f" | collisions {r['target_key_collisions']} | mismatches {r['mismatches_by_column']}")
        if "timestamp_ties_created" in r:
            print(f"{'':13s} timestamp ties created: {r['timestamp_ties_created']}")
    for k, v in res["class_I"]["aggregate_drift"].items():
        print(f"drift {k}: {v['absolute_drift']} (relative {v['relative_drift']:.3e}, target type {v['target_sum_type']})")
    print(f"Class II predicate divergence: {res['class_II']['predicate_divergence']} of {len(PREDICATES)}")
    for p in res["class_II"]["predicates"]:
        print(f"   {p['predicate']:26s} sql={p['sql']:>8} mongo={p['mongo']:>8} {'DIVERGES' if p['diverges'] else ''}")
    c3 = res["class_III"]
    print(f"Class III: {c3['inventory_size']} declared constraints; survival {c3['constraint_survival']}; {c3['status_counts']}")
    for p in c3["probes"]:
        print(f"   {p['status']:13s} {p['table']}.{p['constraint']} ({p['probe']})")
    print(f"\nwritten to {os.path.normpath(path)}")


if __name__ == "__main__":
    main()
