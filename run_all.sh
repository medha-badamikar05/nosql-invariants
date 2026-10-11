#!/usr/bin/env bash
# Runs the whole experiment in the order that keeps results valid. ~20-40 min at full scale.
# Harness runs happen BEFORE the workloads that mutate targets (Class IV/V) and BEFORE
# Path C, whose live workload changes the source.
set -euo pipefail
cd "$(dirname "$0")"
PY="${PY:-python3}"
mkdir -p results
exec > >(tee results/run_log.txt) 2>&1

echo "== versions";  docker compose exec -T mongo mongod --version | head -1
docker compose exec -T postgres psql -U lab -d lab -Atc "select version()"

echo "== generate";  $PY lab/generate.py
echo "== path A";    bash lab/path_a.sh lab_a
echo "== path B";    $PY lab/path_b.py --db lab_b
$PY lab/path_b.py --db lab_b_omit --null-policy omit
$PY lab/path_b.py --db lab_b_novalid --no-validators

echo "== harness (classes I-III)"
for db in lab_a lab_b lab_b_omit lab_b_novalid; do $PY lab/harness.py --db "$db"; done

echo "== class IV (torn state)"
for db in lab_a lab_b; do
  $PY lab/class4_torn.py --db "$db" --mode notxn
done
$PY lab/class4_torn.py --db lab_b --mode txn

echo "== class V (deferred divergence)"
$PY lab/path_b.py --db lab_b5 && $PY lab/class5_drift.py --db lab_b5

echo "== path C (live, changes the source; regenerate afterwards if re-running A/B)"
$PY lab/generate.py
$PY lab/path_b.py --db lab_c --live --workload-seconds 120
$PY lab/harness.py --db lab_c
$PY lab/class5_drift.py --db lab_c --writes 0
$PY lab/generate.py
$PY lab/path_b.py --db lab_c_fanout --live --workload-seconds 120 --fanout
$PY lab/harness.py --db lab_c_fanout
$PY lab/class5_drift.py --db lab_c_fanout --writes 0
echo "== done; see results/"
