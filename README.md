<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# NiFi PostgreSQL CDC Bundle

`CaptureChangePostgreSQL`: an Apache NiFi 2.x processor that captures INSERT, UPDATE, DELETE and TRUNCATE events
from PostgreSQL through logical replication with the built-in `pgoutput` plugin, and writes them as records.

Intended for contribution to Apache NiFi under [NIFI-4239](https://issues.apache.org/jira/browse/NIFI-4239).
The module layout, build and coding conventions follow `nifi-extension-bundles/nifi-cdc` on the NiFi `main` branch.

**Status:** work in progress. The processor captures and delivers changes (MVP); reconnection, integration tests
and documentation are still being worked on.

## Requirements

- Java 21
- Apache NiFi 2.12.0
- PostgreSQL 14 or newer, with `wal_level = logical`

## Releases

Tagged versions are published on the [Releases](https://github.com/danmorcov88/nifi-cdc-postgresql-bundle/releases) page with
the NAR attached. The NAR is named after the NiFi version it is built against (`nifi-cdc-postgresql-nar-2.12.0.nar`).

## Build

```
./mvnw verify                      # compile, unit tests, NAR
./mvnw verify -P contrib-check     # same, plus the upstream RAT, checkstyle and PMD checks
```

The NAR is produced at `nifi-cdc-postgresql-nar/target/nifi-cdc-postgresql-nar-2.12.0.nar`.
Copy it into the `lib/` directory (or the NAR auto-load directory, `nar_extensions/` in the Docker image) of a NiFi 2.12.0 installation.

## Development lab

`docker-compose.yml` starts PostgreSQL 14 and 18 configured for logical replication (ports 5414 and 5418), with a
replication user, two tables and a publication created by `docker/init/01-init.sql`. A NiFi 2.12.0 container that
loads the built NAR is available under the `nifi` profile:

```
docker compose up -d postgres14 postgres18
./mvnw package
docker compose --profile nifi up -d          # https://localhost:8443/nifi  (admin / adminadminadmin)
```

`docker/capture-fixtures.sh` records pgoutput messages from both servers into
`nifi-cdc-postgresql-processors/src/test/resources/pgoutput/`; the decoder unit tests run against these recordings.

## License

Apache License 2.0. The NAR bundles the PostgreSQL JDBC driver (BSD-2-Clause); see `META-INF/LICENSE` in the NAR.
