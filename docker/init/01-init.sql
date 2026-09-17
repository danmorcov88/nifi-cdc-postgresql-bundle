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

-- Lab setup executed once on first start of the container (database cdc_lab, as superuser).
-- Mirrors what a DBA has to prepare for the processor: a replication user, the tables and a publication.

CREATE ROLE nifi_cdc WITH LOGIN REPLICATION PASSWORD 'nifi_cdc';

CREATE SCHEMA lab;
GRANT USAGE ON SCHEMA lab TO nifi_cdc;

-- Default replica identity: only primary key columns are available in the "before" image of UPDATE/DELETE.
CREATE TABLE lab.customers (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    email       VARCHAR(255),
    balance     NUMERIC(12, 2) NOT NULL DEFAULT 0,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Full replica identity: the complete old row is available in the "before" image.
CREATE TABLE lab.orders (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id  BIGINT NOT NULL REFERENCES lab.customers (id),
    amount       NUMERIC(12, 2) NOT NULL,
    status       TEXT NOT NULL DEFAULT 'new',
    details      JSONB,
    ordered_on   DATE NOT NULL DEFAULT current_date
);
ALTER TABLE lab.orders REPLICA IDENTITY FULL;

GRANT SELECT ON ALL TABLES IN SCHEMA lab TO nifi_cdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA lab GRANT SELECT ON TABLES TO nifi_cdc;

CREATE PUBLICATION nifi_cdc_pub FOR TABLE lab.customers, lab.orders;

INSERT INTO lab.customers (name, email, balance) VALUES
    ('Ada Lovelace', 'ada@example.com', 100.00),
    ('Grace Hopper', 'grace@example.com', 250.50);
