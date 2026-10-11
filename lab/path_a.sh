#!/usr/bin/env bash
# Path A: default-configuration pipeline  psql row_to_json -> Node transform -> mongoimport.
# No validators, no unique indexes, MongoDB-generated _id.   Usage: bash lab/path_a.sh [db]
#
# Local Docker (default): uses the psql/mongoimport inside the compose containers.
# Managed cloud (RDS + Atlas): set USE_DOCKER=0 plus PG_DSN and MONGO_URI; needs psql,
# mongosh and mongoimport (MongoDB Database Tools) installed on the machine running this.
set -euo pipefail
cd "$(dirname "$0")"
DB="${1:-lab_a}"
DC="docker compose -f ../docker-compose.yml"

if [[ "${USE_DOCKER:-1}" == "1" ]]; then
  PSQL="$DC exec -T postgres psql -U lab -d lab"
  MSH="$DC exec -T mongo mongosh --quiet $DB"
  MIMP="$DC exec -T mongo mongoimport --quiet --db $DB"
else
  PSQL="psql $PG_DSN"
  MSH="mongosh --quiet $MONGO_URI"
  MIMP="mongoimport --quiet --uri $MONGO_URI --db $DB"
fi

if [[ "${USE_DOCKER:-1}" == "1" ]]; then
  $MSH --eval "db.dropDatabase()" >/dev/null
else
  $MSH --eval "db.getSiblingDB('$DB').dropDatabase()" >/dev/null
fi
for tbl in customer account ledger_entry orders; do
  case "$tbl" in
    customer) query="SELECT row_to_json(t) FROM customer t" ;;
    account) query="SELECT row_to_json(t) FROM account t" ;;
    ledger_entry) query="SELECT row_to_json(t) FROM ledger_entry t" ;;
    orders) query="SELECT row_to_json(t) FROM (SELECT o.*, c.address AS customer_address FROM orders o JOIN customer c USING (customer_id)) t" ;;
  esac
  echo "Path A: $tbl"
  $PSQL -X -At -v FETCH_COUNT=20000 -c "$query" \
    | node transform.js \
    | $MIMP --collection "$tbl" --numInsertionWorkers 4
done
echo "Path A complete -> database $DB"
