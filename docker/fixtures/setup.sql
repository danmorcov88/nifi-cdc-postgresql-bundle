-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Tables used by docker/capture-fixtures.sh to record pgoutput test fixtures. Idempotent.

DROP TABLE IF EXISTS lab.type_samples;
DROP TABLE IF EXISTS lab.toast_samples;
DROP TYPE IF EXISTS lab.mood;

-- A user-defined type makes the server emit a Type ('Y') message before the Relation message.
CREATE TYPE lab.mood AS ENUM ('happy', 'sad');

CREATE TABLE lab.type_samples (
    id              INT4 PRIMARY KEY,
    c_int2          INT2,
    c_int8          INT8,
    c_numeric       NUMERIC(10, 3),
    c_numeric_free  NUMERIC,
    c_float4        FLOAT4,
    c_float8        FLOAT8,
    c_bool          BOOLEAN,
    c_text          TEXT,
    c_varchar       VARCHAR(20),
    c_bpchar        CHAR(5),
    c_date          DATE,
    c_time          TIME,
    c_timestamp     TIMESTAMP,
    c_timestamptz   TIMESTAMPTZ,
    c_uuid          UUID,
    c_json          JSON,
    c_jsonb         JSONB,
    c_bytea         BYTEA,
    c_int_array     INT4[],
    c_mood          lab.mood,
    c_interval      INTERVAL,
    c_timetz        TIMETZ
);

-- payload is stored out of line without compression, so an unchanged payload is sent as 'u' (unchanged TOAST).
CREATE TABLE lab.toast_samples (
    id       INT4 PRIMARY KEY,
    payload  TEXT,
    note     TEXT
);
ALTER TABLE lab.toast_samples ALTER COLUMN payload SET STORAGE EXTERNAL;

GRANT SELECT ON lab.type_samples, lab.toast_samples TO nifi_cdc;

ALTER PUBLICATION nifi_cdc_pub SET TABLE lab.customers, lab.orders, lab.type_samples, lab.toast_samples;
