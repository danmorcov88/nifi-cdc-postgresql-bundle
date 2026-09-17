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

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * {@link ReplicationClient} backed by the replication API of the PostgreSQL JDBC driver. A single
 * {@code replication=database} connection serves both the catalog queries and the replication stream.
 */
public class PgjdbcReplicationClient implements ReplicationClient {

    public static final String OUTPUT_PLUGIN = "pgoutput";
    private static final int PROTOCOL_VERSION = 1;

    private final ConnectionSettings settings;
    private Connection connection;

    public PgjdbcReplicationClient(final ConnectionSettings settings) {
        this.settings = settings;
    }

    @Override
    public void connect() throws SQLException {
        if (connection != null) {
            throw new IllegalStateException("Already connected");
        }
        connection = DriverManager.getConnection(settings.getJdbcUrl(), settings.getReplicationProperties());
    }

    @Override
    public int getServerVersionNumber() throws SQLException {
        return Integer.parseInt(queryString("SELECT current_setting('server_version_num')"));
    }

    @Override
    public String getWalLevel() throws SQLException {
        return queryString("SELECT current_setting('wal_level')");
    }

    @Override
    public boolean hasReplicationPrivilege() throws SQLException {
        try (Statement statement = getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT rolreplication OR rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user")) {
            return resultSet.next() && resultSet.getBoolean(1);
        }
    }

    @Override
    public boolean publicationExists(final String publicationName) throws SQLException {
        try (PreparedStatement statement = getConnection().prepareStatement("SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = ?")) {
            statement.setString(1, publicationName);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    @Override
    public Optional<ReplicationSlot> findReplicationSlot(final String slotName) throws SQLException {
        try (PreparedStatement statement = getConnection().prepareStatement(
                "SELECT plugin, database, active, confirmed_flush_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = ?")) {
            statement.setString(1, slotName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                final String confirmedFlushLsn = resultSet.getString(4);
                return Optional.of(new ReplicationSlot(slotName, resultSet.getString(1), resultSet.getString(2), resultSet.getBoolean(3),
                        confirmedFlushLsn == null ? null : LogSequenceNumber.valueOf(confirmedFlushLsn)));
            }
        }
    }

    @Override
    public long getRetainedWalBytes(final String slotName) throws SQLException {
        try (Connection plainConnection = DriverManager.getConnection(settings.getJdbcUrl(), settings.getProperties());
             PreparedStatement statement = plainConnection.prepareStatement(
                     "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) FROM pg_catalog.pg_replication_slots WHERE slot_name = ?")) {
            statement.setString(1, slotName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new SQLException(String.format("Replication slot [%s] not found", slotName));
                }
                return resultSet.getLong(1);
            }
        }
    }

    @Override
    public LogSequenceNumber createReplicationSlot(final String slotName) throws SQLException {
        return getPgConnection().getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName(slotName)
                .withOutputPlugin(OUTPUT_PLUGIN)
                .make()
                .getConsistentPoint();
    }

    @Override
    public PGReplicationStream startReplicationStream(final String slotName, final String publicationName, final LogSequenceNumber startPosition,
                                                      final Duration statusInterval) throws SQLException {
        final ChainedLogicalStreamBuilder builder = getPgConnection().getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withSlotOption("proto_version", PROTOCOL_VERSION)
                .withSlotOption("publication_names", publicationName)
                .withStatusInterval((int) statusInterval.toMillis(), TimeUnit.MILLISECONDS)
                // the processor decides which positions are confirmed; the driver must not advance them on its own
                .withAutomaticFlush(false);
        if (startPosition != null) {
            builder.withStartPosition(startPosition);
        }
        return builder.start();
    }

    @Override
    public void close() throws SQLException {
        if (connection != null) {
            try {
                connection.close();
            } finally {
                connection = null;
            }
        }
    }

    private String queryString(final String sql) throws SQLException {
        try (Statement statement = getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                throw new SQLException(String.format("Query returned no rows: %s", sql));
            }
            return resultSet.getString(1);
        }
    }

    private Connection getConnection() {
        if (connection == null) {
            throw new IllegalStateException("Not connected");
        }
        return connection;
    }

    private PGConnection getPgConnection() throws SQLException {
        return getConnection().unwrap(PGConnection.class);
    }
}
