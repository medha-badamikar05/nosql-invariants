"""Shared settings, table metadata and the careful (Path B) type mapping."""
import os
from datetime import datetime, timezone
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row
from pymongo import MongoClient
from bson.decimal128 import Decimal128, create_decimal128_context
from bson.int64 import Int64

PG_DSN = os.environ.get("PG_DSN", "postgresql://lab:lab@localhost:5432/lab")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/?directConnection=true")

# table -> (primary key column, timestamp columns)
TABLES = {
    "customer":     ("customer_id", ["updated_at"]),
    "account":      ("account_id",  []),
    "ledger_entry": ("entry_id",    ["posted_at"]),
    "orders":       ("order_id",    ["created_at"]),
}
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
UTC = timezone.utc
_D128 = create_decimal128_context()


def pg(autocommit=True):
    return psycopg.connect(PG_DSN, autocommit=autocommit, row_factory=dict_row)


def mongo_db(name):
    # tz_aware=True: datetimes come back as aware UTC values, not naive ones.
    return MongoClient(MONGO_URI, tz_aware=True)[name]


def to_d128(d: Decimal) -> Decimal128:
    with __import__("decimal").localcontext(_D128) as ctx:
        return Decimal128(ctx.create_decimal(d))


def careful_value(v, keep_micros=True):
    """Path B mapping for one value. Returns a list of (suffix, value) pairs so that a
    timestamp can emit a companion *_us field carrying the microseconds BSON drops."""
    if isinstance(v, bool):
        return [("", v)]
    if isinstance(v, int):
        return [("", Int64(v))]
    if isinstance(v, Decimal):
        return [("", to_d128(v))]
    if isinstance(v, datetime):
        out = [("", v)]                       # BSON date: millisecond resolution
        if keep_micros:
            delta = v - EPOCH
            us = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
            out.append(("_us", Int64(us)))
        return out
    return [("", v)]


def careful_doc(row, null_policy="keep", keep_micros=True):
    """null_policy: keep -> field: null ; omit -> field absent ; zero -> '' or 0."""
    doc = {}
    for k, v in row.items():
        if v is None:
            if null_policy == "keep":
                doc[k] = None
            elif null_policy == "zero":
                doc[k] = ""                    # every nullable column in this schema is text
            continue                           # omit
        for suffix, val in careful_value(v, keep_micros):
            doc[k + suffix] = val
    return doc
