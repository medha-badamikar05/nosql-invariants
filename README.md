# Migration-loss lab (PostgreSQL 16 → MongoDB 8.0)

Runs the experiment in *Losing Invariants, Not Rows*: an adversarial PostgreSQL source,
three migration paths into MongoDB, and one check per loss class.

| Script | What it does | Loss class |
|---|---|---|
| `sql/schema.sql` | 4-relation adversarial source schema | all |
| `lab/generate.py` | ~1,000,000 rows, boundary cases concentrated | all |
| `lab/path_a.sh` + `transform.js` | Path A: psql → Node.js → mongoimport, defaults only | — |
| `lab/path_b.py` | Path B: careful mapping + validators + unique indexes; `--live` = Path C (CDC) | — |
| `lab/harness.py` | digests, aggregate drift, predicate corpus, constraint probes | I, II, III |
| `lab/class4_torn.py` | transfers under induced process crashes | IV |
| `lab/class5_drift.py` | post-cutover divergence of embedded copies | V |

## A. Local machine (recommended first)

Needs: Docker Desktop (or Docker Engine + compose plugin), Python ≥ 3.10, Node.js ≥ 18,
8 GB free RAM, 10 GB free disk.

```bash
# 1. start both databases (MongoDB runs as a 1-node replica set so transactions work)
cd migration-lab
docker compose up -d
docker compose ps                      # wait until both show "healthy" (~30 s)

# 2. Python environment
python3 -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. smoke test at 5% scale (about 1 minute end to end)
python lab/generate.py --scale 0.05
bash lab/path_a.sh lab_a
python lab/path_b.py --db lab_b
python lab/harness.py --db lab_a
python lab/harness.py --db lab_b

# 4. full run, in the order that keeps results valid (20–40 min)
bash run_all.sh                        # everything is logged to results/run_log.txt
```

Step by step instead of `run_all.sh`:

```bash
python lab/generate.py                                   # full scale
bash lab/path_a.sh lab_a                                 # Path A
python lab/path_b.py --db lab_b                          # Path B
python lab/path_b.py --db lab_b_omit --null-policy omit  # Class II factor: null policy
python lab/path_b.py --db lab_b_novalid --no-validators  # Class III factor: validators off
for db in lab_a lab_b lab_b_omit lab_b_novalid; do python lab/harness.py --db $db; done

python lab/class4_torn.py --db lab_a --mode notxn        # Class IV (mutates the target)
python lab/class4_torn.py --db lab_b --mode notxn
python lab/class4_torn.py --db lab_b --mode txn

python lab/path_b.py --db lab_b5 && python lab/class5_drift.py --db lab_b5   # Class V

# Path C: live migration while a workload writes to the source.
# It CHANGES the source, so do it last (or regenerate first).
python lab/generate.py
python lab/path_b.py --db lab_c --live --workload-seconds 120
python lab/harness.py --db lab_c
python lab/class5_drift.py --db lab_c --writes 0          # divergence AT cutover
python lab/generate.py
python lab/path_b.py --db lab_c_fanout --live --workload-seconds 120 --fanout
python lab/harness.py --db lab_c_fanout
python lab/class5_drift.py --db lab_c_fanout --writes 0
```

Results land in `results/<db>.json` and `results/<db>_class5.csv`.
Record the exact versions for the Threats-to-Validity section:

```bash
docker compose exec mongo mongod --version
docker compose exec postgres psql -U lab -d lab -c "select version()"
```

Tear down: `docker compose down -v`.

## B. Cloud VM (same steps, more headroom)

1. Create an Ubuntu 24.04 VM with 4 vCPU / 16 GB RAM / 30 GB disk
   (AWS EC2 `t3.xlarge`, GCP `e2-standard-4`, Azure `D4s_v5`). Only SSH needs to be open.
2. Install tools:
   ```bash
   sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 python3-venv nodejs unzip
   sudo usermod -aG docker $USER && newgrp docker
   ```
3. Copy the kit up (`scp migration-lab.zip ubuntu@<vm>:~`), `unzip`, then follow section A.
4. Copy `results/` back (`scp -r ubuntu@<vm>:~/migration-lab/results .`) and stop the VM.

## C. Managed services (optional: RDS/Cloud SQL + MongoDB Atlas)

Use this only if the paper should report a managed-service configuration.

1. PostgreSQL 16 instance; run `sql/schema.sql` against it.
2. Atlas cluster M10 or larger (M0/Flex tiers restrict transactions and server parameters).
   Allow-list your IP and create a database user.
3. Install `psql`, `mongosh` and the MongoDB Database Tools (`mongoimport`) locally.
4. ```bash
   export PG_DSN="postgresql://USER:PASS@HOST:5432/DB"
   export MONGO_URI="mongodb+srv://USER:PASS@CLUSTER.mongodb.net/"
   USE_DOCKER=0 bash lab/path_a.sh lab_a
   python lab/path_b.py --db lab_b      # and so on, as in section A
   ```
5. Atlas keeps the default 60 s transaction lifetime; in `class4_torn.py` raise the
   post-run `time.sleep(6)` to `time.sleep(65)`.

## Optional Path A′: a real migration product

To test a vendor tool's defaults (e.g. MongoDB Relational Migrator), point it at the
PostgreSQL container (`localhost:5432`, user/password `lab`) and the MongoDB container,
accept its default mappings, write to database `lab_rm`, then run
`python lab/harness.py --db lab_rm`.

## What each check reports

* **Record parity**: target docs ÷ source rows (expected to be 1.0 everywhere).
* **Digest parity (strict / lenient)**: rows whose every field matches exactly. The
  lenient score treats NULL→absent as a match, which is what a naive harness does.
* **Keys lost / key collisions**: BIGINT keys altered by binary64 rounding.
* **Timestamp ties created**: distinct µs instants that became equal at ms resolution.
* **Aggregate drift**: SUM in PostgreSQL vs `$sum` in MongoDB.
* **Predicate divergence**: SQL predicate vs its natural MongoDB translation, by count.
* **Constraint survival**: minimally invalid writes, one per declared constraint. Each
  probe is first run against PostgreSQL as a control; if the source does not reject it
  with the expected SQLSTATE, the probe is marked `INVALID_PROBE` and excluded.
* **Torn transfers / balance drift**: Class IV.
* **Divergent open orders per 1,000 writes**: Class V.
