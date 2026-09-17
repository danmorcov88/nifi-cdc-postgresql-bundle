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

# CaptureChangePostgreSQL

## Summary

This processor reads row changes from a PostgreSQL database through logical replication. It connects with a
replication connection, consumes a replication slot with the built-in `pgoutput` plugin and turns the INSERT, UPDATE,
DELETE and TRUNCATE messages of a publication into records. No extension has to be installed on the server.

Changes are delivered at least once. The position reported to the server is only advanced after the FlowFiles that
contain the changes have been committed, so a change is never lost, but it can be delivered again after a failure.

## Preparing the server

PostgreSQL 14 or later is required, with logical decoding enabled in `postgresql.conf`:

```
wal_level = logical
max_replication_slots = 4   # at least one per processor
max_wal_senders = 4
```

Changing `wal_level` requires a restart of the server.

Create a user with the `REPLICATION` privilege, a publication for the tables to capture, and grant the user access to
the tables:

```sql
CREATE ROLE nifi_cdc WITH LOGIN REPLICATION PASSWORD '...';
CREATE PUBLICATION nifi_cdc_pub FOR TABLE inventory.products, inventory.orders;
GRANT SELECT ON inventory.products, inventory.orders TO nifi_cdc;
```

The user must be allowed to connect in `pg_hba.conf`. Replication connections created with `replication=database`
are matched against the database name, like regular connections.

The replication slot is created by the processor when it does not exist and `Create Replication Slot` is true.
To create it manually:

```sql
SELECT pg_create_logical_replication_slot('nifi_cdc_slot', 'pgoutput');
```

## Initial snapshot

By default only changes made after the replication slot was created are captured. With `Initial Snapshot` set to
`On Slot Creation`, the processor reads the rows of the published tables when it creates the slot and writes them
first, as events with `operation` `snapshot`, `before` null and the row in `after`. The rows are read in a
transaction that uses the snapshot the server exports together with the new slot, so they show the database exactly
at the position from which streaming continues; every later change arrives as a regular event. Snapshot events carry
the position of the slot as `lsn`, `0` as `xid` and the time the snapshot started as `commit_timestamp`. Each table
is written in FlowFiles of `Events Per FlowFile` rows (one FlowFile per table with `One Transaction Per FlowFile`).

No snapshot is taken for a slot that already exists. A snapshot that is interrupted, by stopping the processor or by
a failure, cannot be resumed: at the next start the processor drops the slot it created, the only case in which it
drops a slot, creates a new one and takes the snapshot again. Rows written before the interruption are delivered
again.

Column lists and row filters of the publication (PostgreSQL 15 and later) apply to the snapshot as they apply to
the streamed changes.

## Retained write-ahead log

A replication slot makes the server keep write-ahead log until the consumer confirms it. While the processor is
stopped, or when NiFi cannot keep up, this log accumulates and can fill the disk of the server. The processor does
not drop the slot, except to start an interrupted initial snapshot over. When the processor is removed from a flow,
drop the slot:

```sql
SELECT pg_drop_replication_slot('nifi_cdc_slot');
```

Set `WAL Retention Warning Threshold` to get a warning bulletin when the retained log grows beyond a limit, and
consider `max_slot_wal_keep_size` on the server as a hard limit.

## Large transactions

By default the server decodes a transaction completely before sending it, spilling it to disk when it exceeds
`logical_decoding_work_mem`. With `Large Transaction Streaming` enabled, the server streams such transactions
while they are still in progress (protocol version 2). The processor keeps the streamed changes in temporary files
under the temporary directory of the JVM and writes them when the transaction commits, so a FlowFile still holds
whole transactions and nothing is written for a transaction that is rolled back. `Max Streamed Transaction Size`
bounds the temporary files of one transaction; a larger transaction is reported as an error until the limit is
raised. The files are removed when the processor stops or reconnects, as the server sends the transactions in
progress again.

## Output

Each FlowFile holds the events of one table from one batch, written with the configured Record Writer. A batch always
ends at a transaction boundary, so the events of a transaction are never split across batches. Events of a transaction
that touches several tables end up in several FlowFiles, transferred together; their order can be recovered from the
`lsn` field.

| Field              | Type      | Content                                                                  |
|--------------------|-----------|--------------------------------------------------------------------------|
| `operation`        | string    | `insert`, `update`, `delete`, `truncate` or `snapshot`                   |
| `schema`           | string    | Schema of the table                                                      |
| `table`            | string    | Name of the table                                                        |
| `lsn`              | string    | Position of the change in the write-ahead log, for example `0/1783488`   |
| `xid`              | long      | Transaction id                                                           |
| `commit_timestamp` | timestamp | Commit time of the transaction                                           |
| `before`           | record    | Row before the change, see below; null for INSERT, TRUNCATE and snapshot |
| `after`            | record    | Row after the change; null for DELETE and TRUNCATE                       |

The content of `before` depends on the replica identity of the table:

- `DEFAULT`: only the primary key columns are set, and only for DELETE and for UPDATE when a key column changed.
  Other UPDATE events have no `before` image.
- `FULL` (`ALTER TABLE ... REPLICA IDENTITY FULL`): the complete old row is sent for UPDATE and DELETE.

With the default replica identity, an UPDATE does not include large values stored out of line (TOAST) when they did
not change. `Unchanged TOAST Value Strategy` selects how such columns appear in the `after` image.

## Data types

| PostgreSQL                          | Record type                       |
|-------------------------------------|-----------------------------------|
| `boolean`                           | boolean                           |
| `smallint`, `integer`               | int                               |
| `bigint`                            | long                              |
| `real`                              | float                             |
| `double precision`                  | double                            |
| `numeric`                           | decimal (38,10 when unconstrained)|
| `text`, `varchar`, `char`, `name`   | string                            |
| `date`                              | date                              |
| `time`                              | time (millisecond precision)      |
| `timestamp`, `timestamptz`          | timestamp                         |
| `uuid`                              | uuid                              |
| `json`, `jsonb`                     | string                            |
| `bytea`                             | array of byte                     |
| arrays, `interval`, `timetz`, enums, ranges, other types | string, as PostgreSQL prints the value |

`NaN` and `Infinity` in numeric and floating point columns, and `infinity` or BC values in date and timestamp
columns, cannot be written by the record writers. They are written as null and a warning is logged once per column.

With `Transfer Format` set to `Binary`, the server sends values in the binary format of their type instead of text,
which saves the text conversion on both sides. The types of the table above are decoded to the same values as in
text format. The server sends every other type in binary as well; those values are written as hexadecimal strings
(`\x...`) with a warning once per column, so keep the text format for tables with columns of such types.

## Notes

- Keep the run schedule of the processor at `0 sec`. Status updates to the server are sent while the processor runs;
  a long pause between runs lets `wal_sender_timeout` close the connection.
- After a crash of NiFi the server keeps the slot marked active until `wal_sender_timeout` (60 seconds by default)
  expires. The processor reports that the slot is in use and starts once it is released.
- When the definition of a table changes, the following events of that table are written to a new FlowFile with the
  new schema.
- In a cluster the processor runs on the primary node only and stores the confirmed position in cluster state.
