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

import org.apache.nifi.cdc.postgresql.client.PgjdbcReplicationClient;
import org.apache.nifi.cdc.postgresql.client.ReplicationClient;
import org.apache.nifi.cdc.postgresql.client.ReplicationSlot;
import org.apache.nifi.cdc.postgresql.client.SlotCreation;
import org.apache.nifi.cdc.postgresql.client.SnapshotConnection;
import org.apache.nifi.cdc.postgresql.client.SnapshotCursor;
import org.apache.nifi.cdc.postgresql.pgoutput.RelationMessage;
import org.apache.nifi.cdc.postgresql.pgoutput.TupleData;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * In-memory stand-in for a PostgreSQL server: answers the validation queries and serves recorded pgoutput messages.
 */
class FakeReplicationClient implements ReplicationClient {

    static final String DATABASE = "cdc_lab";
    static final String PUBLICATION = "nifi_cdc_pub";
    static final String SLOT = "nifi_cdc_slot";

    int serverVersion = 140012;
    String walLevel = "logical";
    boolean replicationPrivilege = true;
    final Set<String> publications = new HashSet<>(Set.of(PUBLICATION));
    final Map<String, ReplicationSlot> slots = new HashMap<>();
    final List<String> createdSlots = new ArrayList<>();
    final List<String> droppedSlots = new ArrayList<>();
    /** Tables and rows served by the snapshot of a newly created slot, in publication order. */
    final Map<RelationMessage, List<TupleData>> snapshotTables = new LinkedHashMap<>();
    /** Thrown by the snapshot cursor after this many rows have been read, when set. */
    SQLException snapshotFailure;
    int snapshotFailureAfterRows;
    final List<String> openedSnapshots = new ArrayList<>();
    FakeSnapshotConnection snapshotConnection;
    final List<LogSequenceNumber> streamStartPositions = new ArrayList<>();
    final Deque<ByteBuffer> pendingMessages = new ArrayDeque<>();
    /** Thrown by the stream once the pending messages are exhausted, when set. */
    SQLException readFailure;

    long retainedWalBytes;
    /** Thrown by {@link #connect()} when set. */
    SQLException connectFailure;

    int connections;
    boolean closed;
    FakeReplicationStream stream;

    FakeReplicationClient() {
        slots.put(SLOT, new ReplicationSlot(SLOT, PgjdbcReplicationClient.OUTPUT_PLUGIN, DATABASE, false, LogSequenceNumber.valueOf("0/1000")));
    }

    void addMessages(final List<ByteBuffer> messages) {
        pendingMessages.addAll(messages);
    }

    @Override
    public void connect() throws SQLException {
        if (connectFailure != null) {
            throw connectFailure;
        }
        connections++;
        closed = false;
    }

    @Override
    public long getRetainedWalBytes(final String slotName) {
        return retainedWalBytes;
    }

    @Override
    public int getServerVersionNumber() {
        return serverVersion;
    }

    @Override
    public String getWalLevel() {
        return walLevel;
    }

    @Override
    public boolean hasReplicationPrivilege() {
        return replicationPrivilege;
    }

    @Override
    public boolean publicationExists(final String publicationName) {
        return publications.contains(publicationName);
    }

    @Override
    public Optional<ReplicationSlot> findReplicationSlot(final String slotName) {
        return Optional.ofNullable(slots.get(slotName));
    }

    @Override
    public SlotCreation createReplicationSlot(final String slotName) {
        createdSlots.add(slotName);
        final LogSequenceNumber consistentPoint = LogSequenceNumber.valueOf("0/2000");
        slots.put(slotName, new ReplicationSlot(slotName, PgjdbcReplicationClient.OUTPUT_PLUGIN, DATABASE, false, consistentPoint));
        return new SlotCreation(consistentPoint, "00000003-0000001B-" + createdSlots.size());
    }

    @Override
    public void dropReplicationSlot(final String slotName) throws SQLException {
        if (slots.remove(slotName) == null) {
            throw new SQLException(String.format("replication slot \"%s\" does not exist", slotName));
        }
        droppedSlots.add(slotName);
    }

    @Override
    public SnapshotConnection openSnapshot(final String snapshotName) {
        openedSnapshots.add(snapshotName);
        snapshotConnection = new FakeSnapshotConnection();
        return snapshotConnection;
    }

    @Override
    public PGReplicationStream startReplicationStream(final String slotName, final String publicationName, final LogSequenceNumber startPosition,
                                                      final Duration statusInterval) {
        streamStartPositions.add(startPosition);
        stream = new FakeReplicationStream();
        return stream;
    }

    @Override
    public void close() {
        closed = true;
    }

    class FakeSnapshotConnection implements SnapshotConnection {

        boolean closed;
        int openCursors;
        int rowsRead;

        @Override
        public List<RelationMessage> listTables(final String publicationName) {
            return List.copyOf(snapshotTables.keySet());
        }

        @Override
        public SnapshotCursor openCursor(final RelationMessage table, final int fetchSize) {
            openCursors++;
            final Iterator<TupleData> rows = snapshotTables.get(table).iterator();
            return new SnapshotCursor() {
                @Override
                public TupleData next() throws SQLException {
                    if (snapshotFailure != null && rowsRead >= snapshotFailureAfterRows) {
                        final SQLException failure = snapshotFailure;
                        snapshotFailure = null;
                        throw failure;
                    }
                    if (!rows.hasNext()) {
                        return null;
                    }
                    rowsRead++;
                    return rows.next();
                }

                @Override
                public void close() {
                    openCursors--;
                }
            };
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /**
     * Serves the pending messages of the client; the LSN of each message is a counter, as the recorded fixtures do not
     * include the WAL positions of the individual messages.
     */
    class FakeReplicationStream implements PGReplicationStream {

        private LogSequenceNumber lastReceived = LogSequenceNumber.INVALID_LSN;
        private LogSequenceNumber flushed = LogSequenceNumber.INVALID_LSN;
        private LogSequenceNumber applied = LogSequenceNumber.INVALID_LSN;
        private boolean streamClosed;
        int statusUpdates;

        LogSequenceNumber getFlushed() {
            return flushed;
        }

        LogSequenceNumber getApplied() {
            return applied;
        }

        @Override
        public ByteBuffer read() throws SQLException {
            return readPending();
        }

        @Override
        public ByteBuffer readPending() throws SQLException {
            final ByteBuffer next = pendingMessages.poll();
            if (next == null) {
                if (readFailure != null) {
                    final SQLException failure = readFailure;
                    readFailure = null;
                    throw failure;
                }
                return null;
            }
            lastReceived = LogSequenceNumber.valueOf(lastReceived.asLong() + 16);
            return next.duplicate();
        }

        @Override
        public LogSequenceNumber getLastReceiveLSN() {
            return lastReceived;
        }

        @Override
        public LogSequenceNumber getLastFlushedLSN() {
            return flushed;
        }

        @Override
        public LogSequenceNumber getLastAppliedLSN() {
            return applied;
        }

        @Override
        public void setFlushedLSN(final LogSequenceNumber flushed) {
            this.flushed = flushed;
        }

        @Override
        public void setAppliedLSN(final LogSequenceNumber applied) {
            this.applied = applied;
        }

        @Override
        public void forceUpdateStatus() {
            statusUpdates++;
        }

        @Override
        public boolean isClosed() {
            return streamClosed;
        }

        @Override
        public void close() {
            streamClosed = true;
        }
    }
}
