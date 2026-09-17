#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Records pgoutput messages (protocol version 1, text tuples) from the lab containers into
# nifi-cdc-postgresql-processors/src/test/resources/pgoutput/<server>/<scenario>.hex, one hex-encoded
# message per line. Requires `docker compose up -d postgres14 postgres18`.
set -euo pipefail

cd "$(dirname "$0")/.."
OUT=nifi-cdc-postgresql-processors/src/test/resources/pgoutput
SLOT=fixture_capture

for target in cdc-postgres14:pg14 cdc-postgres18:pg18; do
    container=${target%%:*}
    dir=$OUT/${target##*:}
    mkdir -p "$dir"

    sql() {
        docker exec -i "$container" psql -U postgres -d cdc_lab -v ON_ERROR_STOP=1 -Atq "$@"
    }

    # name, statements (executed as one implicit transaction unless BEGIN/COMMIT are used)
    capture() {
        local name=$1
        local statements=$2
        sql -c "SELECT pg_create_logical_replication_slot('$SLOT', 'pgoutput')" > /dev/null
        sql -c "$statements" > /dev/null
        sql -c "SELECT encode(data, 'hex') FROM pg_logical_slot_get_binary_changes('$SLOT', NULL, NULL, 'proto_version', '1', 'publication_names', 'nifi_cdc_pub')" > "$dir/$name.hex"
        sql -c "SELECT pg_drop_replication_slot('$SLOT')" > /dev/null
        printf '%-8s %-32s %3d messages\n' "${target##*:}" "$name" "$(wc -l < "$dir/$name.hex")"
    }

    sql -c "SELECT pg_drop_replication_slot('$SLOT') FROM pg_replication_slots WHERE slot_name = '$SLOT'" > /dev/null
    sql -f - < docker/fixtures/setup.sql
    sql -c "DELETE FROM lab.orders; DELETE FROM lab.customers WHERE id > 2" > /dev/null

    capture insert-customers \
        "INSERT INTO lab.customers (id, name, email, balance) VALUES (100, 'Fixture One', 'one@example.com', 10.50)"

    capture update-customers-default-identity \
        "UPDATE lab.customers SET balance = 11.50, email = NULL WHERE id = 100"

    capture update-customers-key-change \
        "UPDATE lab.customers SET id = 101 WHERE id = 100"

    capture delete-customers \
        "DELETE FROM lab.customers WHERE id = 101"

    capture orders-full-identity "BEGIN;
        INSERT INTO lab.orders (id, customer_id, amount, status, details, ordered_on)
            VALUES ('0f4b3c2e-9a1d-4c5e-8b7f-1a2b3c4d5e6f', 1, 99.99, 'new', '{\"items\": [1, 2]}', '2026-09-17');
        UPDATE lab.orders SET status = 'paid' WHERE id = '0f4b3c2e-9a1d-4c5e-8b7f-1a2b3c4d5e6f';
        DELETE FROM lab.orders WHERE id = '0f4b3c2e-9a1d-4c5e-8b7f-1a2b3c4d5e6f';
        COMMIT"

    capture types-insert "BEGIN;
        INSERT INTO lab.type_samples VALUES (
            1, -32768, 9223372036854775807, 12345.678, 0.1234567890123456789, 1.5, 2.25, TRUE,
            'text with ünïcödé and \"quotes\"', 'varchar', 'ab', DATE '2026-09-17', TIME '13:45:30.123456',
            TIMESTAMP '2026-09-17 13:45:30.5', TIMESTAMPTZ '2026-09-17 13:45:30+02', '123e4567-e89b-12d3-a456-426614174000',
            '{\"a\": 1}', '{\"b\": [true, null]}', '\\x00ff10', ARRAY[1, 2, 3], 'happy', INTERVAL '1 day 02:03:04', TIME WITH TIME ZONE '10:00:00+05:30');
        INSERT INTO lab.type_samples (id) VALUES (2);
        INSERT INTO lab.type_samples (id, c_numeric_free, c_float8, c_date, c_timestamp) VALUES (3, 'NaN', 'Infinity', 'infinity', '-infinity');
        COMMIT"

    capture toast-update "BEGIN;
        INSERT INTO lab.toast_samples VALUES (1, repeat('payload-', 2000), 'original');
        COMMIT;
        UPDATE lab.toast_samples SET note = 'changed' WHERE id = 1"

    capture multi-table-transaction "BEGIN;
        INSERT INTO lab.customers (id, name) VALUES (200, 'Multi');
        INSERT INTO lab.orders (id, customer_id, amount) VALUES ('7d1e5f0a-2b3c-4d5e-9f8a-0b1c2d3e4f5a', 200, 5);
        UPDATE lab.customers SET balance = 5 WHERE id = 200;
        COMMIT"

    capture truncate \
        "TRUNCATE lab.type_samples, lab.toast_samples RESTART IDENTITY CASCADE"

    sql -c "DELETE FROM lab.orders; DELETE FROM lab.customers WHERE id > 2" > /dev/null
done
