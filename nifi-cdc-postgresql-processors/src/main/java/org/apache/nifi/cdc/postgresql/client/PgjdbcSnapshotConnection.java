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

import org.apache.nifi.cdc.postgresql.pgoutput.ColumnValue;
import org.apache.nifi.cdc.postgresql.pgoutput.RelationColumn;
import org.apache.nifi.cdc.postgresql.pgoutput.RelationMessage;
import org.apache.nifi.cdc.postgresql.pgoutput.ReplicaIdentity;
import org.apache.nifi.cdc.postgresql.pgoutput.TupleData;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * {@link SnapshotConnection} on a regular JDBC connection whose transaction imported the snapshot exported by
 * {@code CREATE_REPLICATION_SLOT}. Tables are described from the catalog the same way pgoutput describes them in
 * Relation messages, so that snapshot rows and streamed changes share the record schemas.
 */
public class PgjdbcSnapshotConnection implements SnapshotConnection {

    /** First server version whose publications can have column lists and row filters. */
    private static final int COLUMN_LIST_SERVER_VERSION = 15;

    private static final String TABLES_QUERY = "SELECT c.oid, pt.schemaname, pt.tablename, c.relreplident%s "
            + "FROM pg_catalog.pg_publication_tables pt "
            + "JOIN pg_catalog.pg_namespace n ON n.nspname = pt.schemaname "
            + "JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename "
            + "WHERE pt.pubname = ? ORDER BY pt.schemaname, pt.tablename";

    /** The key flag follows the replica identity, as in the Relation message: the primary key for DEFAULT, the identity index for INDEX. */
    private static final String COLUMNS_QUERY = "SELECT a.attname, a.atttypid, a.atttypmod, a.attgenerated <> '', "
            + "a.attnum = ANY (SELECT unnest(i.indkey::int2[]) FROM pg_catalog.pg_index i "
            + "WHERE i.indrelid = a.attrelid AND (i.indisreplident OR (i.indisprimary AND c.relreplident = 'd'))) "
            + "FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
            + "WHERE a.attrelid = ? AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum";

    private final Connection connection;
    private final boolean columnListsSupported;
    /** Row filter expressions of the listed tables, keyed by table OID. */
    private final Map<Integer, String> rowFilters = new HashMap<>();

    /**
     * Open a connection and import the snapshot into a REPEATABLE READ transaction. Must be called before the
     * replication connection that exported the snapshot runs another command.
     */
    public static PgjdbcSnapshotConnection open(final ConnectionSettings settings, final String snapshotName) throws SQLException {
        final Connection connection = DriverManager.getConnection(settings.getJdbcUrl(), settings.getSnapshotProperties());
        try {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            try (Statement statement = connection.createStatement()) {
                // the snapshot must be imported by the first statement of the transaction; SET does not take parameters
                statement.execute(String.format("SET TRANSACTION SNAPSHOT '%s'", snapshotName.replace("'", "''")));
            }
            return new PgjdbcSnapshotConnection(connection, connection.getMetaData().getDatabaseMajorVersion() >= COLUMN_LIST_SERVER_VERSION);
        } catch (final SQLException | RuntimeException e) {
            try {
                connection.close();
            } catch (final SQLException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    PgjdbcSnapshotConnection(final Connection connection, final boolean columnListsSupported) {
        this.connection = connection;
        this.columnListsSupported = columnListsSupported;
    }

    @Override
    public List<RelationMessage> listTables(final String publicationName) throws SQLException {
        final List<RelationMessage> tables = new ArrayList<>();
        final String sql = String.format(TABLES_QUERY, columnListsSupported ? ", pt.attnames, pt.rowfilter" : "");
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, publicationName);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    final int relationId = resultSet.getInt(1);
                    final ReplicaIdentity replicaIdentity = ReplicaIdentity.fromCode((byte) resultSet.getString(4).charAt(0));
                    final Set<String> publishedColumns = columnListsSupported ? toSet(resultSet.getArray(5)) : null;
                    final List<RelationColumn> columns = listColumns(relationId, replicaIdentity, publishedColumns);
                    tables.add(new RelationMessage(relationId, resultSet.getString(2), resultSet.getString(3), replicaIdentity, columns));
                    if (columnListsSupported && resultSet.getString(6) != null) {
                        rowFilters.put(relationId, resultSet.getString(6));
                    }
                }
            }
        }
        return tables;
    }

    @Override
    public SnapshotCursor openCursor(final RelationMessage table, final int fetchSize) throws SQLException {
        final StringJoiner columns = new StringJoiner(", ");
        for (final RelationColumn column : table.columns()) {
            columns.add(quote(column.name()));
        }
        final String rowFilter = rowFilters.get(table.relationId());
        final String sql = String.format("SELECT %s FROM %s.%s%s", columns, quote(table.namespace()), quote(table.name()),
                rowFilter == null ? "" : " WHERE " + rowFilter);

        final PreparedStatement statement = connection.prepareStatement(sql);
        try {
            statement.setFetchSize(fetchSize);
            return new ResultSetCursor(statement, statement.executeQuery(), table.columns().size());
        } catch (final SQLException | RuntimeException e) {
            try {
                statement.close();
            } catch (final SQLException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            connection.rollback();
        } finally {
            connection.close();
        }
    }

    private List<RelationColumn> listColumns(final int relationId, final ReplicaIdentity replicaIdentity, final Set<String> publishedColumns)
            throws SQLException {
        final List<RelationColumn> columns = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(COLUMNS_QUERY)) {
            statement.setInt(1, relationId);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    final String name = resultSet.getString(1);
                    final boolean generated = resultSet.getBoolean(4);
                    // without column lists the server publishes every column except generated ones
                    final boolean published = publishedColumns == null ? !generated : publishedColumns.contains(name);
                    if (!published) {
                        continue;
                    }
                    final boolean key = switch (replicaIdentity) {
                        case FULL -> true;
                        case NOTHING -> false;
                        case DEFAULT, INDEX -> resultSet.getBoolean(5);
                    };
                    columns.add(new RelationColumn(name, key, resultSet.getInt(2), resultSet.getInt(3)));
                }
            }
        }
        return columns;
    }

    private static Set<String> toSet(final Array array) throws SQLException {
        return array == null ? null : new HashSet<>(Arrays.asList((String[]) array.getArray()));
    }

    private static String quote(final String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static class ResultSetCursor implements SnapshotCursor {

        private final Statement statement;
        private final ResultSet resultSet;
        private final int columnCount;

        private ResultSetCursor(final Statement statement, final ResultSet resultSet, final int columnCount) {
            this.statement = statement;
            this.resultSet = resultSet;
            this.columnCount = columnCount;
        }

        @Override
        public TupleData next() throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            final List<ColumnValue> values = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                final String text = resultSet.getString(i);
                values.add(text == null ? ColumnValue.NULL : ColumnValue.text(text));
            }
            return new TupleData(values);
        }

        @Override
        public void close() throws SQLException {
            try (statement; resultSet) {
                // closes the server side cursor with the statement
            }
        }
    }
}
