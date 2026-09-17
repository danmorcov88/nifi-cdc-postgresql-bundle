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

import org.apache.nifi.cdc.postgresql.client.ConnectionSettings;
import org.apache.nifi.cdc.postgresql.client.ReplicationClient;
import org.apache.nifi.cdc.postgresql.client.ReplicationSlot;
import org.apache.nifi.cdc.postgresql.client.SSLMode;
import org.apache.nifi.cdc.postgresql.event.UnchangedToastStrategy;
import org.apache.nifi.cdc.postgresql.pgoutput.CommitMessage;
import org.apache.nifi.cdc.postgresql.pgoutput.PgOutputFixtures;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.replication.LogSequenceNumber;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CaptureChangePostgreSQLTest {

    private static final String SERVER = "pg18";
    private static final String WRITER_ID = "writer";

    private TestRunner runner;
    private FakeReplicationClient client;

    @BeforeEach
    void setUp() throws InitializationException {
        client = new FakeReplicationClient();
        runner = TestRunners.newTestRunner(new CaptureChangePostgreSQL() {
            @Override
            protected ReplicationClient createReplicationClient(final ConnectionSettings settings) {
                return client;
            }
        });

        final MockRecordWriter writer = new MockRecordWriter("header", false);
        runner.addControllerService(WRITER_ID, writer);
        runner.enableControllerService(writer);

        runner.setProperty(CaptureChangePostgreSQL.HOSTNAME, "localhost");
        runner.setProperty(CaptureChangePostgreSQL.DATABASE_NAME, FakeReplicationClient.DATABASE);
        runner.setProperty(CaptureChangePostgreSQL.USERNAME, "nifi_cdc");
        runner.setProperty(CaptureChangePostgreSQL.PASSWORD, "secret");
        runner.setProperty(CaptureChangePostgreSQL.PUBLICATION_NAME, FakeReplicationClient.PUBLICATION);
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT_NAME, FakeReplicationClient.SLOT);
        runner.setProperty(CaptureChangePostgreSQL.RECORD_WRITER, WRITER_ID);
    }

    @Test
    void testValidation() {
        runner.assertValid();

        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT_NAME, "Not-Valid");
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.REPLICATION_SLOT_NAME, FakeReplicationClient.SLOT);

        runner.setProperty(CaptureChangePostgreSQL.SSL_MODE, SSLMode.VERIFY_FULL);
        runner.assertNotValid();
        runner.setProperty(CaptureChangePostgreSQL.SSL_MODE, SSLMode.REQUIRED);
        runner.assertValid();
    }

    @Test
    void testInsertWritesOneFlowFileForTheTable() throws IOException {
        client.addMessages(PgOutputFixtures.load(SERVER, "insert-customers"));

        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_SCHEMA, "lab");
        flowFile.assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_TABLE, "customers");
        flowFile.assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_EVENT_COUNT, "1");
        flowFile.assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_RECORD_COUNT, "1");
        flowFile.assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_LSN, LogSequenceNumber.valueOf(48).asString());
        flowFile.assertAttributeExists(CaptureChangePostgreSQL.ATTRIBUTE_XID);
        flowFile.assertAttributeEquals("mime.type", "text/plain");
        final String content = flowFile.getContent();
        assertTrue(content.contains("insert,lab,customers,"), content);
        assertTrue(content.contains("name=Fixture One"), content);
        assertTrue(content.contains("balance=10.50"), content);

        final String endLsn = lastCommitEndLsn("insert-customers");
        assertEquals(endLsn, runner.getStateManager().getState(Scope.CLUSTER).get(CaptureChangePostgreSQL.STATE_LSN));
        assertEquals(1, runner.getProvenanceEvents().size());
    }

    @Test
    void testConfirmedPositionIsReportedAfterCommit() {
        client.addMessages(PgOutputFixtures.load(SERVER, "insert-customers"));
        final LogSequenceNumber endLsn = LogSequenceNumber.valueOf(lastCommitEndLsn("insert-customers"));

        runner.run(1, false);
        assertEquals(LogSequenceNumber.INVALID_LSN, client.stream.getFlushed());

        runner.run(1, false, false);
        assertEquals(endLsn, client.stream.getFlushed());
        assertEquals(endLsn, client.stream.getApplied());

        runner.stop();
        assertTrue(client.stream.isClosed());
        assertEquals(1, client.stream.statusUpdates);
        assertTrue(client.closed);
    }

    @Test
    void testTableFilterSkipsEventsButAdvancesPosition() throws IOException {
        runner.setProperty(CaptureChangePostgreSQL.TABLE_NAME_PATTERN, "orders");
        client.addMessages(PgOutputFixtures.load(SERVER, "insert-customers"));

        runner.run();

        runner.assertTransferCount(CaptureChangePostgreSQL.REL_SUCCESS, 0);
        assertEquals(lastCommitEndLsn("insert-customers"), runner.getStateManager().getState(Scope.CLUSTER).get(CaptureChangePostgreSQL.STATE_LSN));
        assertEquals(LogSequenceNumber.valueOf(lastCommitEndLsn("insert-customers")), client.stream.getFlushed());
    }

    @Test
    void testSchemaFilterSkipsEvents() {
        runner.setProperty(CaptureChangePostgreSQL.SCHEMA_NAME_PATTERN, "public");
        client.addMessages(PgOutputFixtures.load(SERVER, "insert-customers"));

        runner.run();

        runner.assertTransferCount(CaptureChangePostgreSQL.REL_SUCCESS, 0);
    }

    @Test
    void testTransactionWithSeveralTablesWritesOneFlowFilePerTable() {
        client.addMessages(PgOutputFixtures.load(SERVER, "multi-table-transaction"));

        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_TABLE, "customers");
        flowFiles.get(0).assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_EVENT_COUNT, "2");
        assertTrue(flowFiles.get(0).getContent().contains("update,lab,customers,"));
        flowFiles.get(1).assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_TABLE, "orders");
        flowFiles.get(1).assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_EVENT_COUNT, "1");
        assertEquals(flowFiles.get(0).getAttribute(CaptureChangePostgreSQL.ATTRIBUTE_XID), flowFiles.get(1).getAttribute(CaptureChangePostgreSQL.ATTRIBUTE_XID));
    }

    @Test
    void testOneTransactionPerFlowFile() throws IOException {
        runner.setProperty(CaptureChangePostgreSQL.BATCH_STRATEGY, BatchStrategy.ONE_TRANSACTION);
        runner.setProperty(CaptureChangePostgreSQL.UNCHANGED_TOAST_STRATEGY, UnchangedToastStrategy.PLACEHOLDER);
        runner.setProperty(CaptureChangePostgreSQL.UNCHANGED_TOAST_PLACEHOLDER, "<toast>");
        client.addMessages(PgOutputFixtures.load(SERVER, "toast-update"));

        runner.run(2);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS);
        assertTrue(flowFiles.get(0).getContent().contains("insert,lab,toast_samples,"));
        assertTrue(flowFiles.get(1).getContent().contains("update,lab,toast_samples,"));
        assertTrue(flowFiles.get(1).getContent().contains("payload=<toast>"));
        assertTrue(flowFiles.get(1).getContent().contains("note=changed"));
        assertEquals(lastCommitEndLsn("toast-update"), runner.getStateManager().getState(Scope.CLUSTER).get(CaptureChangePostgreSQL.STATE_LSN));
    }

    @Test
    void testUnchangedToastOmitted() {
        runner.setProperty(CaptureChangePostgreSQL.UNCHANGED_TOAST_STRATEGY, UnchangedToastStrategy.OMIT);
        client.addMessages(PgOutputFixtures.load(SERVER, "toast-update"));

        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_EVENT_COUNT, "2");
        final String update = flowFile.getContent().lines().filter(line -> line.startsWith("update")).findFirst().orElseThrow();
        assertFalse(update.contains("payload="), update);
        assertTrue(update.contains("note=changed"), update);
    }

    @Test
    void testBatchCombinesTransactionsUntilEventLimit() throws IOException {
        runner.setProperty(CaptureChangePostgreSQL.EVENTS_PER_FLOWFILE, "2");
        client.addMessages(PgOutputFixtures.load(SERVER, "insert-customers"));
        client.addMessages(PgOutputFixtures.load(SERVER, "update-customers-default-identity"));
        client.addMessages(PgOutputFixtures.load(SERVER, "delete-customers"));

        runner.run(2);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS);
        flowFiles.get(0).assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_EVENT_COUNT, "2");
        flowFiles.get(1).assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_EVENT_COUNT, "1");
        assertTrue(flowFiles.get(1).getContent().contains("delete,lab,customers,"));
        assertEquals(lastCommitEndLsn("delete-customers"), runner.getStateManager().getState(Scope.CLUSTER).get(CaptureChangePostgreSQL.STATE_LSN));
    }

    @Test
    void testTruncate() {
        client.addMessages(PgOutputFixtures.load(SERVER, "truncate"));

        runner.run();

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 2);
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(CaptureChangePostgreSQL.REL_SUCCESS)) {
            flowFile.assertAttributeEquals(CaptureChangePostgreSQL.ATTRIBUTE_EVENT_COUNT, "1");
            assertTrue(flowFile.getContent().contains("truncate,lab,"));
        }
    }

    @Test
    void testReadFailureRollsBackAndReopensStream() throws IOException {
        client.addMessages(PgOutputFixtures.load(SERVER, "insert-customers").subList(0, 3));
        client.readFailure = new SQLException("connection lost");

        runner.run(1, false);

        runner.assertTransferCount(CaptureChangePostgreSQL.REL_SUCCESS, 0);
        assertNull(runner.getStateManager().getState(Scope.CLUSTER).get(CaptureChangePostgreSQL.STATE_LSN));
        assertTrue(client.stream.isClosed());
        assertEquals(1, runner.getLogger().getErrorMessages().size());

        client.addMessages(PgOutputFixtures.load(SERVER, "insert-customers"));
        runner.run(1, false, false);

        runner.assertAllFlowFilesTransferred(CaptureChangePostgreSQL.REL_SUCCESS, 1);
        assertEquals(2, client.streamStartPositions.size());
    }

    @Test
    void testWriteFailureRollsBackWithoutAdvancingPosition() throws Exception {
        final MockRecordWriter failingWriter = new MockRecordWriter("header", false, 0);
        runner.addControllerService("failing", failingWriter);
        runner.enableControllerService(failingWriter);
        runner.setProperty(CaptureChangePostgreSQL.RECORD_WRITER, "failing");
        client.addMessages(PgOutputFixtures.load(SERVER, "insert-customers"));

        runner.run(1, false);
        assertEquals(1, runner.getLogger().getErrorMessages().size());
        assertTrue(client.stream.isClosed());

        // an idle trigger after the failure must not confirm the position of the discarded transaction
        client.pendingMessages.clear();
        runner.run(1, false, false);

        runner.assertTransferCount(CaptureChangePostgreSQL.REL_SUCCESS, 0);
        assertNull(runner.getStateManager().getState(Scope.CLUSTER).get(CaptureChangePostgreSQL.STATE_LSN));
        assertEquals(LogSequenceNumber.INVALID_LSN, client.stream.getFlushed());
        assertEquals(2, client.streamStartPositions.size());
    }

    @Test
    void testCommitOutsideTransactionIsRejected() throws Exception {
        client.addMessages(PgOutputFixtures.load(SERVER, "insert-customers").subList(3, 4));

        runner.run(1, false);

        runner.assertTransferCount(CaptureChangePostgreSQL.REL_SUCCESS, 0);
        assertNull(runner.getStateManager().getState(Scope.CLUSTER).get(CaptureChangePostgreSQL.STATE_LSN));
        assertEquals(LogSequenceNumber.INVALID_LSN, client.stream.getFlushed());
        assertTrue(client.stream.isClosed());
        assertEquals(1, runner.getLogger().getErrorMessages().size());
    }

    @Test
    void testResumesFromStoredPosition() throws Exception {
        runner.getStateManager().setState(Map.of(CaptureChangePostgreSQL.STATE_LSN, "0/ABC"), Scope.CLUSTER);

        runner.run();

        assertEquals(List.of(LogSequenceNumber.valueOf("0/ABC")), client.streamStartPositions);
        assertEquals(LogSequenceNumber.valueOf("0/ABC"), client.stream.getFlushed());
    }

    @Test
    void testStartsFromSlotPositionWithoutState() {
        runner.run();

        assertEquals(Collections.singletonList(null), client.streamStartPositions);
    }

    @Test
    void testCreatesMissingSlotAndIgnoresStoredPosition() throws Exception {
        client.slots.clear();
        runner.getStateManager().setState(Map.of(CaptureChangePostgreSQL.STATE_LSN, "0/ABC"), Scope.CLUSTER);

        runner.run();

        assertEquals(List.of(FakeReplicationClient.SLOT), client.createdSlots);
        assertNull(client.streamStartPositions.get(0));
        assertNull(runner.getStateManager().getState(Scope.CLUSTER).get(CaptureChangePostgreSQL.STATE_LSN));
    }

    @Test
    void testFailsWhenSlotMissingAndCreationDisabled() {
        client.slots.clear();
        runner.setProperty(CaptureChangePostgreSQL.CREATE_REPLICATION_SLOT, "false");

        assertSetupFails("does not exist");
        assertTrue(client.createdSlots.isEmpty());
    }

    @Test
    void testFailsWhenSlotUsesAnotherPlugin() {
        client.slots.put(FakeReplicationClient.SLOT, new ReplicationSlot(FakeReplicationClient.SLOT, "wal2json", FakeReplicationClient.DATABASE, false, null));

        assertSetupFails("wal2json");
    }

    @Test
    void testFailsWhenSlotBelongsToAnotherDatabase() {
        client.slots.put(FakeReplicationClient.SLOT, new ReplicationSlot(FakeReplicationClient.SLOT, "pgoutput", "other", false, null));

        assertSetupFails("other");
    }

    @Test
    void testFailsWhenSlotIsActive() {
        client.slots.put(FakeReplicationClient.SLOT, new ReplicationSlot(FakeReplicationClient.SLOT, "pgoutput", FakeReplicationClient.DATABASE, true, null));

        assertSetupFails("in use");
    }

    @Test
    void testFailsWhenPublicationMissing() {
        client.publications.clear();

        assertSetupFails("Publication [nifi_cdc_pub] does not exist");
    }

    @Test
    void testFailsWhenServerTooOld() {
        client.serverVersion = 130010;

        assertSetupFails("130010");
    }

    @Test
    void testFailsWhenWalLevelNotLogical() {
        client.walLevel = "replica";

        assertSetupFails("wal_level");
    }

    @Test
    void testFailsWithoutReplicationPrivilege() {
        client.replicationPrivilege = false;

        assertSetupFails("REPLICATION");
    }

    private void assertSetupFails(final String expectedMessage) {
        final AssertionError error = assertThrows(AssertionError.class, () -> runner.run());
        Throwable cause = error;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        assertInstanceOf(ProcessException.class, cause);
        assertTrue(cause.getMessage().contains(expectedMessage), cause.getMessage());
        assertTrue(client.closed);
    }

    private static String lastCommitEndLsn(final String scenario) {
        return PgOutputFixtures.decode(SERVER, scenario).stream()
                .filter(CommitMessage.class::isInstance)
                .map(CommitMessage.class::cast)
                .reduce((first, second) -> second)
                .map(commit -> LogSequenceNumber.valueOf(commit.endLsn()).asString())
                .orElseThrow();
    }
}
