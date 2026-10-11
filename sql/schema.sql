-- Adversarial source schema: each relation stresses one or more loss classes.
-- Four relations; see README for how each column maps to a class.

CREATE TABLE customer (
  customer_id  BIGINT PRIMARY KEY,
  email        TEXT NOT NULL,
  phone        TEXT,                               -- Class II: NULL / '' / value, 1/3 each
  address      TEXT NOT NULL,                      -- Class V: embedded into orders in the target
  updated_at   TIMESTAMP(6) WITH TIME ZONE NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_customer_email ON customer (lower(email));  

CREATE TABLE account (
  account_id   BIGINT PRIMARY KEY,
  customer_id  BIGINT NOT NULL REFERENCES customer(customer_id),
  balance      NUMERIC(19,4) NOT NULL DEFAULT 0,
  status       TEXT NOT NULL,
  CONSTRAINT balance_nonneg CHECK (balance >= 0),
  CONSTRAINT status_enum    CHECK (status IN ('open','frozen','closed'))
);

CREATE TABLE ledger_entry (
  entry_id     BIGINT PRIMARY KEY,                 -- Class I: a fraction exceed 2^53
  account_id   BIGINT NOT NULL REFERENCES account(account_id),
  amount       NUMERIC(19,4) NOT NULL,             -- Class I: not exactly representable in binary64
  memo         TEXT,                               -- Class II: NULL / '' / value, 1/3 each
  posted_at    TIMESTAMP(6) WITH TIME ZONE,        -- Class I: sub-millisecond ordering
  CONSTRAINT nonzero CHECK (amount <> 0)
);
-- Composite, partial, functional unique index (Class III).
-- memos on one account would collide and the three-way NULL/''/value split is impossible.
CREATE UNIQUE INDEX uq_acct_ref ON ledger_entry (account_id, lower(memo)) WHERE memo <> '';

CREATE TABLE orders (
  order_id     BIGINT PRIMARY KEY,
  customer_id  BIGINT NOT NULL REFERENCES customer(customer_id),
  total        NUMERIC(19,4) NOT NULL,
  status       TEXT NOT NULL,
  created_at   TIMESTAMP(6) WITH TIME ZONE NOT NULL,
  CONSTRAINT total_pos    CHECK (total > 0),
  CONSTRAINT order_status CHECK (status IN ('open','shipped'))
);
CREATE INDEX ix_orders_customer ON orders(customer_id);
