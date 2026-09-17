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
package org.apache.nifi.cdc.postgresql.pgoutput;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Decodes transactions recorded with protocol version 2 and streaming from servers with a 64kB logical_decoding_work_mem
 * (see docker/capture-fixtures.sh).
 */
class PgOutputStreamFixtureTest {

    static Stream<String> servers() {
        return PgOutputFixtures.SERVERS.stream().map(server -> server + "-stream");
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testLargeTransactionArrivesInSegments(final String server) {
        final List<PgOutputMessage> messages = PgOutputFixtures.decodeStream(server, "stream-large-transaction");

        final StreamStartMessage first = assertInstanceOf(StreamStartMessage.class, messages.get(0));
        assertTrue(first.firstSegment());
        assertInstanceOf(RelationMessage.class, messages.get(1), "the relation is sent inside the first segment");
        final StreamCommitMessage commit = assertInstanceOf(StreamCommitMessage.class, messages.get(messages.size() - 1));
        assertEquals(first.xid(), commit.xid());
        assertTrue(commit.endLsn() > commit.commitLsn());

        int segments = 0;
        int inserts = 0;
        boolean inStream = false;
        for (final PgOutputMessage message : messages) {
            if (message instanceof StreamStartMessage start) {
                assertFalse(inStream, "segments do not nest");
                assertEquals(first.xid(), start.xid());
                assertEquals(segments == 0, start.firstSegment());
                inStream = true;
                segments++;
            } else if (message instanceof StreamStopMessage) {
                inStream = false;
            } else if (message instanceof InsertMessage) {
                assertTrue(inStream, "changes of a streamed transaction arrive inside segments");
                inserts++;
            }
        }
        assertTrue(segments > 1, "more than one segment");
        assertEquals(1200, inserts);

        final ByteBuffer rawInsert = PgOutputFixtures.load(server, "stream-large-transaction").get(2);
        assertEquals(first.xid(), PgOutputDecoder.streamedTransactionId(rawInsert), "changes made directly by the transaction carry its id");
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testSubTransactionRollback(final String server) {
        final List<PgOutputMessage> messages = PgOutputFixtures.decodeStream(server, "stream-subtransaction-rollback");
        final List<ByteBuffer> raw = PgOutputFixtures.load(server, "stream-subtransaction-rollback");

        final StreamStartMessage first = assertInstanceOf(StreamStartMessage.class, messages.get(0));
        final StreamAbortMessage abort = messages.stream().filter(StreamAbortMessage.class::isInstance).map(StreamAbortMessage.class::cast).findFirst().orElseThrow();
        assertEquals(first.xid(), abort.xid());
        assertFalse(abort.isWholeTransaction());
        assertInstanceOf(StreamCommitMessage.class, messages.get(messages.size() - 1));

        final Set<Integer> transactionIds = new HashSet<>();
        int keptBeforeAbort = 0;
        boolean inStream = false;
        boolean aborted = false;
        for (int i = 0; i < messages.size(); i++) {
            final PgOutputMessage message = messages.get(i);
            if (message instanceof StreamStartMessage) {
                inStream = true;
            } else if (message instanceof StreamStopMessage) {
                inStream = false;
            } else if (message instanceof StreamAbortMessage) {
                assertFalse(inStream, "the abort arrives between segments");
                aborted = true;
            } else if (message instanceof InsertMessage) {
                final int xid = PgOutputDecoder.streamedTransactionId(raw.get(i));
                transactionIds.add(xid);
                if (!aborted && xid == first.xid()) {
                    keptBeforeAbort++;
                }
            }
        }
        assertEquals(600, keptBeforeAbort, "rows inserted by the transaction itself before the savepoint");
        assertTrue(transactionIds.contains(abort.subTransactionXid()), "rows of the rolled back subtransaction were streamed before the rollback");
        assertEquals(3, transactionIds.size(), "transaction, rolled back subtransaction and the subtransaction after ROLLBACK TO");
    }
}
