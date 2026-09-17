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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.apache.nifi.cdc.postgresql.event.UnchangedToastStrategy;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.replication.LogSequenceNumber;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration tests against a real PostgreSQL server started with Testcontainers. Subclasses choose the server version.
 * Run with {@code ./mvnw verify -P integration-tests}; Docker must be available.
 */
abstract class AbstractCaptureChangePostgreSQLIT {

    private static final String DATABASE = "cdc_lab";
    private static final String PUBLICATION = "nifi_cdc_pub";
    private static final String WRITER_ID = "writer";
    private static final int POSTGRESQL_PORT = 5432;
    private static final Duration COLLECT_TIMEOUT = Duration.ofSeconds(60);
    private static final AtomicInteger SLOT_COUNTER = new AtomicInteger();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    protected static PostgreSQLContainer container;

    private TestRunner runner;
    private String slotName;

    /**
     * @param image Docker image of the server under test
     */
    static void startContainer(final String image) {
        // a fixed host port survives a restart of the container, which a random mapping does not
        final int hostPort = findFreePort();
        container = new PostgreSQLContainer(DockerImageName.parse(image))
                .withDatabaseName(DATABASE)
                .withUsername("postgres")
                .withPassword("postgres")
                .withCreateContainerCmdModifier(command -> command.getHostConfig()
                        .withPortBindings(new PortBinding(Ports.Binding.bindPort(hostPort), new ExposedPort(POSTGRESQL_PORT))))
                .withCopyFileToContainer(MountableFile.forHostPath(labFile("init/01-init.sql")), "/docker-entrypoint-initdb.d/01-init.sql")
                .withCopyFileToContainer(MountableFile.forHostPath(labFile("fixtures/setup.sql")), "/docker-entrypoint-initdb.d/02-setup.sql")
                .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=8", "-c", "max_wal_senders=8",
                        "-c", "wal_sender_timeout=5s", "-c", "fsync=off");
        container.start();
    }

    @AfterAll
    static void stopContainer() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        slotName = "it_slot_" + SLOT_COUNTER.incrementAndGet();
        runner = createRunner(slotName, UnchangedToastStrategy.NULL);
        execute("DELETE FROM lab.orders", "DELETE FROM lab.customers WHERE id > 2", "DELETE FROM lab.type_samples", "DELETE FROM lab.toast_samples");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (runner != null) {
            runner.stop();
        }
        execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '" + slotName + "'");
    }

    @Test
    void testInsertUpdateDeleteWithReplicaIdentityDefaultAndFull() throws Exception {
        start(runner);
        execute("INSERT INTO lab.customers (id, name, email, balance) VALUES (100, 'Ada', 'ada@example.com', 10.50)",
                "UPDATE lab.customers SET balance = 11.50, email = NULL WHERE id = 100",
                "INSERT INTO lab.orders (id, customer_id, amount, details) VALUES ('0f4b3c2e-9a1d-4c5e-8b7f-1a2b3c4d5e6f', 100, 99.99, '{\"items\": [1, 2]}')",
                "UPDATE lab.orders SET status = 'paid' WHERE customer_id = 100",
                "DELETE FROM lab.orders WHERE customer_id = 100",
                "DELETE FROM lab.customers WHERE id = 100");

        final List<JsonNode> events = collect(runner, 6);

        final JsonNode insert = events.get(0);
        assertEquals("insert", insert.get("operation").asText());
        assertEquals("customers", insert.get("table").asText());
        assertTrue(insert.get("before").isNull());
        assertEquals("Ada", insert.get("after").get("name").asText());
        assertEquals(0, new BigDecimal("10.50").compareTo(insert.get("after").get("balance").decimalValue()));

        final JsonNode customerUpdate = events.get(1);
        assertEquals("update", customerUpdate.get("operation").asText());
        assertTrue(customerUpdate.get("before").isNull(), "REPLICA IDENTITY DEFAULT sends no old row when the key is unchanged");
        assertTrue(customerUpdate.get("after").get("email").isNull());

        final JsonNode orderUpdate = events.get(3);
        assertEquals("orders", orderUpdate.get("table").asText());
        assertEquals("new", orderUpdate.get("before").get("status").asText());
        assertEquals("paid", orderUpdate.get("after").get("status").asText());
        assertEquals("{\"items\": [1, 2]}", orderUpdate.get("before").get("details").asText());

        final JsonNode orderDelete = events.get(4);
        assertEquals("delete", orderDelete.get("operation").asText());
        assertEquals("paid", orderDelete.get("before").get("status").asText(), "REPLICA IDENTITY FULL sends the whole old row");

        final JsonNode customerDelete = events.get(5);
        assertEquals(100, customerDelete.get("before").get("id").asInt());
        assertTrue(customerDelete.get("before").get("name").isNull(), "REPLICA IDENTITY DEFAULT sends only the key");
        assertTrue(customerDelete.get("after").isNull());

        for (final JsonNode event : events) {
            assertTrue(event.get("xid").asLong() > 0);
            assertTrue(event.get("lsn").asText().matches("[0-9A-F]+/[0-9A-F]+"));
            assertTrue(event.get("commit_timestamp").asLong() > Instant.parse("2026-01-01T00:00:00Z").toEpochMilli());
        }
    }

    @Test
    void testDataTypes() throws Exception {
        start(runner);
        execute("INSERT INTO lab.type_samples VALUES (1, -32768, 9223372036854775807, 12345.678, 0.1234567890123456789, 1.5, 2.25, TRUE, "
                        + "'text with ünïcödé', 'varchar', 'ab', DATE '2026-09-17', TIME '13:45:30.123456', TIMESTAMP '2026-09-17 13:45:30.5', "
                        + "TIMESTAMPTZ '2026-09-17 13:45:30+02', '123e4567-e89b-12d3-a456-426614174000', '{\"a\": 1}', '{\"b\": [true, null]}', "
                        + "'\\x00ff10', ARRAY[1, 2, 3], 'happy', INTERVAL '1 day 02:03:04', TIME WITH TIME ZONE '10:00:00+05:30')",
                "INSERT INTO lab.type_samples (id) VALUES (2)",
                "INSERT INTO lab.type_samples (id, c_numeric_free, c_float8, c_date) VALUES (3, 'NaN', 'Infinity', 'infinity')");

        assertTypeSamples(collect(runner, 3), "insert");
    }

    private void assertTypeSamples(final List<JsonNode> events, final String operation) {
        assertEquals(operation, events.get(0).get("operation").asText());
        final JsonNode row = events.get(0).get("after");
        assertEquals(-32768, row.get("c_int2").asInt());
        assertEquals(Long.MAX_VALUE, row.get("c_int8").asLong());
        assertEquals(0, new BigDecimal("12345.678").compareTo(row.get("c_numeric").decimalValue()));
        assertEquals(0, new BigDecimal("0.1234567890123456789").compareTo(row.get("c_numeric_free").decimalValue()));
        assertEquals(1.5, row.get("c_float4").asDouble());
        assertEquals(2.25, row.get("c_float8").asDouble());
        assertTrue(row.get("c_bool").asBoolean());
        assertEquals("text with ünïcödé", row.get("c_text").asText());
        assertEquals("ab   ", row.get("c_bpchar").asText());
        assertEquals(java.sql.Date.valueOf(LocalDate.of(2026, 9, 17)).getTime(), row.get("c_date").asLong());
        assertEquals(java.sql.Timestamp.valueOf(LocalDateTime.of(2026, 9, 17, 13, 45, 30, 500_000_000)).getTime(), row.get("c_timestamp").asLong());
        assertEquals(Instant.parse("2026-09-17T11:45:30Z").toEpochMilli(), row.get("c_timestamptz").asLong());
        assertEquals("123e4567-e89b-12d3-a456-426614174000", row.get("c_uuid").asText());
        assertEquals("{\"a\": 1}", row.get("c_json").asText());
        assertEquals("{\"b\": [true, null]}", row.get("c_jsonb").asText());
        assertEquals(List.of(0, -1, 16), OBJECT_MAPPER.convertValue(row.get("c_bytea"), List.class));
        assertEquals("{1,2,3}", row.get("c_int_array").asText());
        assertEquals("happy", row.get("c_mood").asText());
        assertEquals("1 day 02:03:04", row.get("c_interval").asText());
        assertEquals("10:00:00+05:30", row.get("c_timetz").asText());

        final JsonNode nulls = events.get(1).get("after");
        assertEquals(2, nulls.get("id").asInt());
        assertTrue(nulls.get("c_text").isNull());
        assertTrue(nulls.get("c_timestamptz").isNull());

        final JsonNode specials = events.get(2).get("after");
        assertTrue(specials.get("c_numeric_free").isNull(), "NaN has no decimal representation");
        assertTrue(specials.get("c_date").isNull(), "infinity has no date representation");
        assertTrue(specials.get("c_float8").isNull(), "Infinity cannot be serialized by the record writers");
        assertEquals(3, runner.getLogger().getWarnMessages().size(), "one warning per column with unsupported values");
    }

    @Test
    void testLargeTransaction() throws Exception {
        start(runner);
        final int rows = 20_000;
        execute("INSERT INTO lab.customers (id, name, balance) SELECT g, 'bulk ' || g, g FROM generate_series(1000, " + (1000 + rows - 1) + ") AS g");

        final List<MockFlowFile> flowFiles = collectFlowFiles(runner, rows);

        assertEquals(1, flowFiles.size(), "a transaction is never split across FlowFiles");
        flowFiles.get(0).assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_EVENT_COUNT, Integer.toString(rows));
        final Set<Integer> ids = new TreeSet<>();
        for (final JsonNode event : parse(flowFiles.get(0))) {
            ids.add(event.get("after").get("id").asInt());
        }
        assertEquals(rows, ids.size());
        assertEquals(1000, ids.iterator().next());
    }

    @Test
    void testProcessorRestartDeliversEveryTransactionOnce() throws Exception {
        runner.setProperty(CaptureChangePostgreSQL.EVENTS_PER_FLOWFILE, "7");
        start(runner);
        final List<String> statements = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            statements.add("INSERT INTO lab.customers (id, name) VALUES (" + (2000 + i) + ", 'restart " + i + "')");
        }
        execute(statements.toArray(new String[0]));

        final List<JsonNode> firstPart = collect(runner, 14);
        runner.stop();
        final Map<String, String> state = runner.getStateManager().getState(Scope.CLUSTER).toMap();
        assertFalse(state.isEmpty());

        execute("INSERT INTO lab.customers (id, name) VALUES (2051, 'restart 51')");
        final TestRunner secondRunner = createRunner(slotName, UnchangedToastStrategy.NULL);
        secondRunner.setProperty(CaptureChangePostgreSQL.EVENTS_PER_FLOWFILE, "7");
        secondRunner.getStateManager().setState(state, Scope.CLUSTER);
        start(secondRunner);
        final List<JsonNode> secondPart = collect(secondRunner, 51 - firstPart.size());
        runner = secondRunner;

        final List<Integer> ids = new ArrayList<>();
        firstPart.forEach(event -> ids.add(event.get("after").get("id").asInt()));
        secondPart.forEach(event -> ids.add(event.get("after").get("id").asInt()));
        final List<Integer> expected = new ArrayList<>();
        for (int i = 1; i <= 51; i++) {
            expected.add(2000 + i);
        }
        assertEquals(expected, ids, "every transaction delivered exactly once, in order");
    }

    @Test
    void testServerRestart() throws Exception {
        start(runner);
        execute("INSERT INTO lab.customers (id, name) VALUES (300, 'before restart')");
        assertEquals("before restart", collect(runner, 1).get(0).get("after").get("name").asText());

        container.getDockerClient().restartContainerCmd(container.getContainerId()).exec();
        waitForServer();
        execute("INSERT INTO lab.customers (id, name) VALUES (301, 'after restart')");

        assertEquals("after restart", collect(runner, 1).get(0).get("after").get("name").asText());
        assertFalse(runner.getLogger().getErrorMessages().isEmpty(), "the lost connection is reported");
        assertEquals(2, runner.getLogger().getInfoMessages().stream().filter(message -> message.getMsg().contains("Started replication stream")).count());
    }

    @Test
    void testIdleStreamSurvivesWalSenderTimeout() throws Exception {
        start(runner);
        final long until = System.currentTimeMillis() + Duration.ofSeconds(12).toMillis();
        while (System.currentTimeMillis() < until) {
            runner.run(1, false, false);
            Thread.sleep(200);
        }
        execute("INSERT INTO lab.customers (id, name) VALUES (400, 'after idle')");

        assertEquals("after idle", collect(runner, 1).get(0).get("after").get("name").asText());
        assertTrue(runner.getLogger().getErrorMessages().isEmpty(), "no reconnection was needed");
    }

    @Test
    void testUnchangedToastValue() throws Exception {
        runner.stop();
        runner = createRunner(slotName, UnchangedToastStrategy.PLACEHOLDER);
        start(runner);
        execute("INSERT INTO lab.toast_samples VALUES (1, repeat('payload-', 4000), 'original')",
                "UPDATE lab.toast_samples SET note = 'changed' WHERE id = 1",
                "UPDATE lab.toast_samples SET payload = 'short' WHERE id = 1");

        final List<JsonNode> events = collect(runner, 3);

        assertEquals(32_000, events.get(0).get("after").get("payload").asText().length());
        assertEquals("__unchanged_toast_value__", events.get(1).get("after").get("payload").asText());
        assertEquals("changed", events.get(1).get("after").get("note").asText());
        assertEquals("short", events.get(2).get("after").get("payload").asText());
    }

    @Test
    void testTruncate() throws Exception {
        start(runner);
        execute("INSERT INTO lab.toast_samples VALUES (1, 'x', 'y')", "TRUNCATE lab.toast_samples, lab.type_samples");

        final List<JsonNode> events = collect(runner, 3);

        assertEquals("insert", events.get(0).get("operation").asText());
        final Set<String> truncated = new TreeSet<>();
        for (final JsonNode event : events.subList(1, 3)) {
            assertEquals("truncate", event.get("operation").asText());
            assertTrue(event.get("before").isNull());
            assertTrue(event.get("after").isNull());
            truncated.add(event.get("table").asText());
        }
        assertEquals(Set.of("toast_samples", "type_samples"), truncated);
    }

    @Test
    void testAddedColumnAppearsInFollowingEvents() throws Exception {
        start(runner);
        try {
            execute("INSERT INTO lab.customers (id, name) VALUES (500, 'before')",
                    "ALTER TABLE lab.customers ADD COLUMN nickname TEXT",
                    "INSERT INTO lab.customers (id, name, nickname) VALUES (501, 'after', 'nick')");

            final List<MockFlowFile> flowFiles = collectFlowFiles(runner, 2);

            assertEquals(2, flowFiles.size(), "the table gets a new FlowFile when its definition changes");
            final JsonNode before = parse(flowFiles.get(0)).get(0);
            assertNull(before.get("after").get("nickname"));
            final JsonNode after = parse(flowFiles.get(1)).get(0);
            assertEquals("nick", after.get("after").get("nickname").asText());
        } finally {
            execute("ALTER TABLE lab.customers DROP COLUMN IF EXISTS nickname");
        }
    }

    @Test
    void testSchemaAndTableFilters() throws Exception {
        runner.setProperty(CaptureChangePostgreSQL.TABLE_NAME_PATTERN, "ord.*");
        start(runner);
        execute("INSERT INTO lab.customers (id, name) VALUES (600, 'filtered')",
                "INSERT INTO lab.orders (id, customer_id, amount) VALUES ('7d1e5f0a-2b3c-4d5e-9f8a-0b1c2d3e4f5a', 600, 1)");

        final List<JsonNode> events = collect(runner, 1);

        assertEquals("orders", events.get(0).get("table").asText());
        runner.run(1, false, false);
        runner.assertTransferCount(CaptureChangePostgreSQL.REL_SUCCESS, 0);
    }

    @Test
    void testInitialSnapshotIsConsistentWithFollowingChanges() throws Exception {
        execute("INSERT INTO lab.customers (id, name) VALUES (100, 'before slot'), (101, 'before slot too')");
        runner.setProperty(CaptureChangePostgreSQL.INITIAL_SNAPSHOT, InitialSnapshotMode.ON_SLOT_CREATION);
        runner.setProperty(CaptureChangePostgreSQL.EVENTS_PER_FLOWFILE, "1");
        // the first trigger creates the slot, opens the snapshot and writes one row; the cursor stays open across triggers
        start(runner);
        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 1);
        execute("INSERT INTO lab.customers (id, name) VALUES (700, 'after slot')");

        final List<JsonNode> events = collect(runner, 5);

        final List<Integer> snapshotIds = new ArrayList<>();
        for (final JsonNode event : events.subList(0, 4)) {
            assertEquals("snapshot", event.get("operation").asText());
            assertEquals("customers", event.get("table").asText());
            assertEquals(0, event.get("xid").asLong());
            assertTrue(event.get("before").isNull());
            snapshotIds.add(event.get("after").get("id").asInt());
        }
        assertEquals(List.of(1, 2, 100, 101), snapshotIds, "the snapshot sees the rows that existed when the slot was created");
        final JsonNode insert = events.get(4);
        assertEquals("insert", insert.get("operation").asText());
        assertEquals(700, insert.get("after").get("id").asInt());
        // the first change after the slot was created can start exactly at the consistent point
        assertTrue(LogSequenceNumber.valueOf(insert.get("lsn").asText()).asLong() >= LogSequenceNumber.valueOf(events.get(0).get("lsn").asText()).asLong());

        final Map<String, String> state = runner.getStateManager().getState(Scope.CLUSTER).toMap();
        assertNull(state.get(CaptureChangePostgreSQL.STATE_SNAPSHOT_PENDING));
        assertTrue(LogSequenceNumber.valueOf(state.get(CaptureChangePostgreSQL.STATE_LSN)).asLong() > LogSequenceNumber.valueOf(insert.get("lsn").asText()).asLong(),
                "the state follows the stream after the snapshot");
        runner.run(1, false, false);
        runner.assertTransferCount(CaptureChangePostgreSQL.REL_SUCCESS, 0);
    }

    @Test
    void testInitialSnapshotDataTypes() throws Exception {
        execute("INSERT INTO lab.type_samples VALUES (1, -32768, 9223372036854775807, 12345.678, 0.1234567890123456789, 1.5, 2.25, TRUE, "
                        + "'text with ünïcödé', 'varchar', 'ab', DATE '2026-09-17', TIME '13:45:30.123456', TIMESTAMP '2026-09-17 13:45:30.5', "
                        + "TIMESTAMPTZ '2026-09-17 13:45:30+02', '123e4567-e89b-12d3-a456-426614174000', '{\"a\": 1}', '{\"b\": [true, null]}', "
                        + "'\\x00ff10', ARRAY[1, 2, 3], 'happy', INTERVAL '1 day 02:03:04', TIME WITH TIME ZONE '10:00:00+05:30')",
                "INSERT INTO lab.type_samples (id) VALUES (2)",
                "INSERT INTO lab.type_samples (id, c_numeric_free, c_float8, c_date) VALUES (3, 'NaN', 'Infinity', 'infinity')");
        runner.setProperty(CaptureChangePostgreSQL.INITIAL_SNAPSHOT, InitialSnapshotMode.ON_SLOT_CREATION);
        runner.setProperty(CaptureChangePostgreSQL.TABLE_NAME_PATTERN, "type_samples");
        start(runner);

        final List<JsonNode> events = collect(runner, 3);

        events.sort(Comparator.comparingInt(event -> event.get("after").get("id").asInt()));
        assertTypeSamples(events, "snapshot");
    }

    @Test
    void testStopDuringInitialSnapshotStartsOverWithNewSlot() throws Exception {
        execute("INSERT INTO lab.customers (id, name) VALUES (100, 'a'), (101, 'b'), (102, 'c')");
        runner.setProperty(CaptureChangePostgreSQL.INITIAL_SNAPSHOT, InitialSnapshotMode.ON_SLOT_CREATION);
        runner.setProperty(CaptureChangePostgreSQL.EVENTS_PER_FLOWFILE, "2");
        start(runner);
        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 1);
        runner.stop();
        final Map<String, String> state = runner.getStateManager().getState(Scope.CLUSTER).toMap();
        assertEquals("true", state.get(CaptureChangePostgreSQL.STATE_SNAPSHOT_PENDING));
        final String firstRestartLsn = querySlotRestartLsn();

        final TestRunner secondRunner = createRunner(slotName, UnchangedToastStrategy.NULL);
        secondRunner.setProperty(CaptureChangePostgreSQL.INITIAL_SNAPSHOT, InitialSnapshotMode.ON_SLOT_CREATION);
        secondRunner.setProperty(CaptureChangePostgreSQL.EVENTS_PER_FLOWFILE, "2");
        secondRunner.getStateManager().setState(state, Scope.CLUSTER);
        start(secondRunner);
        runner = secondRunner;
        assertTrue(secondRunner.getLogger().getWarnMessages().stream().anyMatch(message -> message.getMsg().contains("starts over")));
        assertFalse(firstRestartLsn.equals(querySlotRestartLsn()), "the slot was recreated");

        final List<JsonNode> snapshot = collect(secondRunner, 5);
        final List<Integer> ids = new ArrayList<>();
        for (final JsonNode event : snapshot) {
            assertEquals("snapshot", event.get("operation").asText());
            ids.add(event.get("after").get("id").asInt());
        }
        assertEquals(List.of(1, 2, 100, 101, 102), ids, "the whole snapshot is written again");

        execute("INSERT INTO lab.customers (id, name) VALUES (103, 'streamed')");
        assertEquals("insert", collect(secondRunner, 1).get(0).get("operation").asText());
        assertNull(secondRunner.getStateManager().getState(Scope.CLUSTER).get(CaptureChangePostgreSQL.STATE_SNAPSHOT_PENDING));
    }

    @Test
    void testInitialSnapshotHonoursColumnListAndRowFilter() throws Exception {
        Assumptions.assumeTrue(serverMajorVersion() >= 15, "column lists and row filters require PostgreSQL 15");
        execute("INSERT INTO lab.customers (id, name, email) VALUES (100, 'filtered out', 'x@example.com'), (101, 'kept', 'y@example.com')",
                "DROP PUBLICATION IF EXISTS it_filtered_pub",
                "CREATE PUBLICATION it_filtered_pub FOR TABLE lab.customers (id, name) WHERE (id > 100)");
        try {
            runner.setProperty(CaptureChangePostgreSQL.PUBLICATION_NAME, "it_filtered_pub");
            runner.setProperty(CaptureChangePostgreSQL.INITIAL_SNAPSHOT, InitialSnapshotMode.ON_SLOT_CREATION);
            start(runner);
            execute("INSERT INTO lab.customers (id, name, email) VALUES (102, 'streamed', 'z@example.com')");

            final List<JsonNode> events = collect(runner, 2);

            assertEquals("snapshot", events.get(0).get("operation").asText());
            assertEquals(101, events.get(0).get("after").get("id").asInt());
            assertNull(events.get(0).get("after").get("email"), "columns outside the column list are not published");
            assertEquals("insert", events.get(1).get("operation").asText());
            assertEquals(102, events.get(1).get("after").get("id").asInt());
            assertNull(events.get(1).get("after").get("email"));
        } finally {
            runner.stop();
            execute("DROP PUBLICATION IF EXISTS it_filtered_pub");
        }
    }

    private String querySlotRestartLsn() throws SQLException {
        try (Connection connection = DriverManager.getConnection(container.getJdbcUrl(), "postgres", "postgres");
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = '" + slotName + "'")) {
            assertTrue(resultSet.next(), "slot exists");
            return resultSet.getString(1);
        }
    }

    private static int serverMajorVersion() throws SQLException {
        try (Connection connection = DriverManager.getConnection(container.getJdbcUrl(), "postgres", "postgres")) {
            return connection.getMetaData().getDatabaseMajorVersion();
        }
    }

    private TestRunner createRunner(final String slot, final UnchangedToastStrategy toastStrategy) throws InitializationException {
        final TestRunner newRunner = TestRunners.newTestRunner(CaptureChangePostgreSQL.class);
        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        newRunner.addControllerService(WRITER_ID, writer);
        newRunner.enableControllerService(writer);
        newRunner.setProperty(CaptureChangePostgreSQL.HOSTNAME, container.getHost());
        newRunner.setProperty(CaptureChangePostgreSQL.PORT, Integer.toString(container.getMappedPort(POSTGRESQL_PORT)));
        newRunner.setProperty(CaptureChangePostgreSQL.DATABASE_NAME, DATABASE);
        newRunner.setProperty(CaptureChangePostgreSQL.USERNAME, "nifi_cdc");
        newRunner.setProperty(CaptureChangePostgreSQL.PASSWORD, "nifi_cdc");
        newRunner.setProperty(CaptureChangePostgreSQL.PUBLICATION_NAME, PUBLICATION);
        newRunner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT_NAME, slot);
        newRunner.setProperty(CaptureChangePostgreSQL.RECORD_WRITER, WRITER_ID);
        newRunner.setProperty(CaptureChangePostgreSQL.EVENTS_PER_FLOWFILE, "100000");
        newRunner.setProperty(CaptureChangePostgreSQL.UNCHANGED_TOAST_STRATEGY, toastStrategy);
        return newRunner;
    }

    private static void start(final TestRunner testRunner) {
        testRunner.run(1, false, true);
    }

    /**
     * Triggers the processor until the expected number of events has been transferred, then clears the transferred FlowFiles.
     *
     * @return the events in commit order, independent of how they were split into FlowFiles
     */
    private static List<JsonNode> collect(final TestRunner testRunner, final int expectedEvents) throws IOException {
        final List<JsonNode> events = new ArrayList<>();
        for (final MockFlowFile flowFile : collectFlowFiles(testRunner, expectedEvents)) {
            events.addAll(parse(flowFile));
        }
        events.sort(Comparator.comparingLong(event -> LogSequenceNumber.valueOf(event.get("lsn").asText()).asLong()));
        return events;
    }

    private static List<MockFlowFile> collectFlowFiles(final TestRunner testRunner, final int expectedEvents) {
        final long deadline = System.currentTimeMillis() + COLLECT_TIMEOUT.toMillis();
        while (true) {
            testRunner.run(1, false, false);
            int count = 0;
            for (final MockFlowFile flowFile : testRunner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)) {
                count += Integer.parseInt(flowFile.getAttribute(CaptureChangePostgreSQL.ATTRIBUTE_EVENT_COUNT));
            }
            if (count >= expectedEvents) {
                final List<MockFlowFile> flowFiles = new ArrayList<>(testRunner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS));
                testRunner.clearTransferState();
                assertEquals(expectedEvents, count, "more events than expected");
                return flowFiles;
            }
            if (System.currentTimeMillis() > deadline) {
                final StringBuilder errors = new StringBuilder();
                testRunner.getLogger().getErrorMessages().forEach(message -> errors.append(message.getMsg()).append(" / ").append(message.getThrowable()).append(System.lineSeparator()));
                fail(String.format("Expected %d events but received %d within %s; errors: %s", expectedEvents, count, COLLECT_TIMEOUT, errors));
            }
            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted");
            }
        }
    }

    private static List<JsonNode> parse(final MockFlowFile flowFile) throws IOException {
        if (Boolean.getBoolean("it.debug")) {
            System.out.println(flowFile.getAttributes() + System.lineSeparator() + flowFile.getContent());
        }
        final List<JsonNode> events = new ArrayList<>();
        OBJECT_MAPPER.readTree(flowFile.toByteArray()).forEach(events::add);
        return events;
    }

    private static void execute(final String... statements) throws SQLException {
        try (Connection connection = DriverManager.getConnection(container.getJdbcUrl(), "postgres", "postgres");
             Statement statement = connection.createStatement()) {
            for (final String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private static void waitForServer() throws InterruptedException {
        final long deadline = System.currentTimeMillis() + COLLECT_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                execute("SELECT 1");
                return;
            } catch (final SQLException e) {
                Thread.sleep(500);
            }
        }
        fail("PostgreSQL did not come back after the restart");
    }

    private static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Path labFile(final String relativePath) {
        for (final Path base : List.of(Path.of("..", "docker"), Path.of("docker"))) {
            final Path candidate = base.resolve(relativePath);
            if (Files.exists(candidate)) {
                return candidate.toAbsolutePath().normalize();
            }
        }
        throw new IllegalStateException("Lab file not found: " + relativePath);
    }
}
