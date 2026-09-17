/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.cdc.postgresql.processors;

import org.apache.nifi.cdc.postgresql.client.SnapshotConnection;
import org.apache.nifi.cdc.postgresql.client.SnapshotCursor;
import org.apache.nifi.cdc.postgresql.event.ChangeEventRecordFactory;
import org.apache.nifi.cdc.postgresql.event.ChangeOperation;
import org.apache.nifi.cdc.postgresql.event.TransactionInfo;
import org.apache.nifi.cdc.postgresql.pgoutput.RelationMessage;
import org.apache.nifi.cdc.postgresql.pgoutput.TupleData;
import org.apache.nifi.serialization.record.Record;
import org.postgresql.replication.LogSequenceNumber;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;

/**
 * Reads the rows that existed when the replication slot was created, one batch per call, and writes them as snapshot
 * events positioned at the consistent point of the slot. A batch holds rows of a single table, so that every table
 * gets its own FlowFiles; the snapshot is complete once all tables have been read.
 */
class InitialSnapshot implements AutoCloseable {

    /** Snapshot events belong to no transaction. */
    static final long SNAPSHOT_XID = 0;

    private final SnapshotConnection connection;
    private final LogSequenceNumber consistentPoint;
    private final Iterator<RelationMessage> tables;
    private final int tableCount;
    private final ChangeEventRecordFactory recordFactory;
    private final int fetchSize;
    private final int rowsPerBatch;
    private final TransactionInfo transaction;

    private RelationMessage currentTable;
    private SnapshotCursor cursor;
    private long rowCount;

    /**
     * @param connection transaction that sees the database at the consistent point
     * @param consistentPoint position of the slot, written as the LSN of every snapshot event
     * @param tables tables to read
     * @param recordFactory builds the event records
     * @param fetchSize rows fetched from the server at a time
     * @param rowsPerBatch maximum number of rows written per call of {@link #readBatch(EventBatch)}
     */
    InitialSnapshot(final SnapshotConnection connection, final LogSequenceNumber consistentPoint, final List<RelationMessage> tables,
                    final ChangeEventRecordFactory recordFactory, final int fetchSize, final int rowsPerBatch) {
        this.connection = connection;
        this.consistentPoint = consistentPoint;
        this.tables = tables.iterator();
        this.tableCount = tables.size();
        this.recordFactory = recordFactory;
        this.fetchSize = fetchSize;
        this.rowsPerBatch = rowsPerBatch;
        this.transaction = new TransactionInfo(SNAPSHOT_XID, Instant.now());
    }

    LogSequenceNumber getConsistentPoint() {
        return consistentPoint;
    }

    int getTableCount() {
        return tableCount;
    }

    long getRowCount() {
        return rowCount;
    }

    /**
     * Write the next rows of the current table to the batch. The batch ends when it holds the configured number of
     * rows or when the table has no more rows; tables without rows are skipped.
     *
     * @return whether all tables have been read
     */
    boolean readBatch(final EventBatch batch) throws SQLException, IOException {
        while (true) {
            if (cursor == null) {
                if (!tables.hasNext()) {
                    return true;
                }
                currentTable = tables.next();
                cursor = connection.openCursor(currentTable, fetchSize);
            }

            int rows = 0;
            TupleData row;
            while (rows < rowsPerBatch && (row = cursor.next()) != null) {
                final Record record = recordFactory.createRecord(ChangeOperation.SNAPSHOT, currentTable, transaction, consistentPoint.asString(), null, row);
                batch.write(currentTable, record, consistentPoint, transaction);
                rows++;
                rowCount++;
            }
            if (rows == rowsPerBatch) {
                // the table may have more rows; they go to the next batch
                return false;
            }

            closeCursor();
            if (rows > 0) {
                return !tables.hasNext();
            }
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            closeCursor();
        } finally {
            connection.close();
        }
    }

    private void closeCursor() throws SQLException {
        final SnapshotCursor current = cursor;
        cursor = null;
        currentTable = null;
        if (current != null) {
            current.close();
        }
    }
}
