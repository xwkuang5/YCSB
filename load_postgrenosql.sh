#!/bin/bash
NUM_CLIENTS=64
NUM_RECORDS=100000
TABLE_NAME="test_load_2kb_${NUM_CLIENTS}_${NUM_RECORDS}"

psql -d test -U postgres -c "CREATE TABLE $TABLE_NAME (YCSB_KEY VARCHAR(255) PRIMARY KEY not NULL, YCSB_VALUE JSONB not NULL);"

for ((i=1; i<=NUM_CLIENTS; i++)); do
  record_count=$((NUM_RECORDS / NUM_CLIENTS));
  insert_start=$(((i-1) * record_count));
  ./bin/ycsb.sh load postgrenosql -P workloads/workloada -P postgrenosql/conf/postgrenosql.properties -p requestdistribution=uniform -p recordcount=$record_count -p fieldlength=100 -p fieldcount=20 -p fieldnameprefix=field -p insertstart=$insert_start -p table=$TABLE_NAME &> "/tmp/test_client_$i.txt" &
done

wait
