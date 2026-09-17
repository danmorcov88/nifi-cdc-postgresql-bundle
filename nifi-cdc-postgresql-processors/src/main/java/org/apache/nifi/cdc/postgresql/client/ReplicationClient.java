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
package org.apache.nifi.cdc.postgresql.client;

import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

/**
 * Operations the processor needs from a PostgreSQL logical replication connection.
 */
public interface ReplicationClient extends AutoCloseable {

    /**
     * Establish the connection.
     */
    void connect() throws SQLException;

    /**
     * @return server version as reported by {@code server_version_num}, for example 140012
     */
    int getServerVersionNumber() throws SQLException;

    /**
     * @return value of the {@code wal_level} setting
     */
    String getWalLevel() throws SQLException;

    /**
     * @return whether the connected role has the REPLICATION privilege or is a superuser
     */
    boolean hasReplicationPrivilege() throws SQLException;

    boolean publicationExists(String publicationName) throws SQLException;

    Optional<ReplicationSlot> findReplicationSlot(String slotName) throws SQLException;

    /**
     * Create a logical replication slot using the pgoutput plugin.
     *
     * @return the consistent point of the new slot, from which streaming can start
     */
    LogSequenceNumber createReplicationSlot(String slotName) throws SQLException;

    /**
     * Measure the write-ahead log the slot forces the server to retain. Uses a separate short-lived connection, because the
     * replication connection cannot run queries while it streams.
     *
     * @return bytes between the current write position of the server and the restart position of the slot
     */
    long getRetainedWalBytes(String slotName) throws SQLException;

    /**
     * Start streaming changes.
     *
     * @param slotName replication slot to consume
     * @param publicationName publication whose tables are streamed
     * @param startPosition position to resume from, or null to resume from the position confirmed on the slot
     * @param statusInterval how often the driver reports the confirmed position to the server
     */
    PGReplicationStream startReplicationStream(String slotName, String publicationName, LogSequenceNumber startPosition, Duration statusInterval)
            throws SQLException;

    @Override
    void close() throws SQLException;
}
