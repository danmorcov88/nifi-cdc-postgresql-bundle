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

import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamedTransactionsTest {

    private static final int XID = 1000;
    private static final int SUB_XID = 1001;
    private static final int OTHER_SUB_XID = 1002;

    @TempDir
    Path directory;

    private StreamedTransactions transactions;

    @BeforeEach
    void setUp() {
        transactions = new StreamedTransactions(1024 * 1024, new MockComponentLog("test", this), directory);
    }

    @AfterEach
    void tearDown() throws IOException {
        transactions.close();
    }

    @Test
    void testSpoolRoundTripAcrossBufferBoundaries() throws IOException {
        try (StreamedTransactionSpool spool = new StreamedTransactionSpool(XID, directory, 10 * 1024 * 1024)) {
            final List<byte[]> expected = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                // 500 byte messages fill the staging buffer several times; the last one is larger than the buffer
                final byte[] message = insert(XID, i == 299 ? 100_000 : 500, (byte) i);
                expected.add(message);
                spool.append(1000 + i, XID, ByteBuffer.wrap(message));
            }
            assertEquals(300, spool.getMessageCount());

            final List<Long> lsns = new ArrayList<>();
            final List<byte[]> replayed = new ArrayList<>();
            spool.replay((lsn, message) -> {
                lsns.add(lsn);
                final byte[] bytes = new byte[message.remaining()];
                message.get(bytes);
                replayed.add(bytes);
            });

            assertEquals(300, replayed.size());
            assertEquals(1000L, lsns.get(0));
            assertEquals(1299L, lsns.get(299));
            for (int i = 0; i < 300; i++) {
                assertArrayEquals(expected.get(i), replayed.get(i), "message " + i);
            }
        }
        try (Stream<Path> files = Files.list(directory)) {
            assertEquals(0, files.count(), "closing the spool deletes its file");
        }
    }

    @Test
    void testSubTransactionAbortTruncatesToItsFirstMessage() throws IOException {
        try (StreamedTransactionSpool spool = new StreamedTransactionSpool(XID, directory, 10 * 1024 * 1024)) {
            spool.append(1, XID, ByteBuffer.wrap(insert(XID, 10, (byte) 1)));
            spool.append(2, SUB_XID, ByteBuffer.wrap(insert(SUB_XID, 10, (byte) 2)));
            spool.append(3, OTHER_SUB_XID, ByteBuffer.wrap(insert(OTHER_SUB_XID, 10, (byte) 3)));
            spool.append(4, SUB_XID, ByteBuffer.wrap(insert(SUB_XID, 10, (byte) 4)));
            final long sizeBeforeSubTransaction = 12 + 5 + 10;

            spool.abortSubTransaction(OTHER_SUB_XID + 1);
            assertEquals(4, spool.getMessageCount(), "a subtransaction without changes has nothing to discard");

            spool.abortSubTransaction(SUB_XID);
            assertEquals(1, spool.getMessageCount());
            assertEquals(sizeBeforeSubTransaction, spool.getSize());

            spool.append(5, XID, ByteBuffer.wrap(insert(XID, 10, (byte) 5)));
            spool.append(6, OTHER_SUB_XID, ByteBuffer.wrap(insert(OTHER_SUB_XID, 10, (byte) 6)));
            spool.abortSubTransaction(OTHER_SUB_XID);

            final List<Byte> markers = new ArrayList<>();
            spool.replay((lsn, message) -> markers.add(message.get(message.limit() - 1)));
            assertEquals(List.of((byte) 1, (byte) 5), markers);
        }
    }

    @Test
    void testSizeLimit() throws IOException {
        try (StreamedTransactionSpool spool = new StreamedTransactionSpool(XID, directory, 100)) {
            spool.append(1, XID, ByteBuffer.wrap(insert(XID, 40, (byte) 1)));
            final IOException exception = assertThrows(IOException.class, () -> spool.append(2, XID, ByteBuffer.wrap(insert(XID, 40, (byte) 2))));
            assertTrue(exception.getMessage().contains("exceeds"), exception.getMessage());
        }
    }

    @Test
    void testSegmentsAndCommit() throws IOException {
        assertFalse(transactions.isSegmentOpen());
        transactions.beginSegment(XID, true);
        assertTrue(transactions.isSegmentOpen());
        transactions.append(1, ByteBuffer.wrap(insert(XID, 10, (byte) 1)));
        transactions.endSegment();
        transactions.beginSegment(XID, false);
        transactions.append(2, ByteBuffer.wrap(insert(SUB_XID, 10, (byte) 2)));
        transactions.endSegment();
        assertEquals(1, transactions.getTransactionCount());

        try (StreamedTransactionSpool spool = transactions.remove(XID)) {
            assertEquals(2, spool.getMessageCount());
        }
        assertEquals(0, transactions.getTransactionCount());
        assertThrows(IllegalStateException.class, () -> transactions.remove(XID));
    }

    @Test
    void testAbort() throws IOException {
        transactions.beginSegment(XID, true);
        transactions.append(1, ByteBuffer.wrap(insert(SUB_XID, 10, (byte) 1)));
        transactions.endSegment();

        transactions.abort(XID, SUB_XID);
        assertEquals(1, transactions.getTransactionCount());
        transactions.abort(XID, XID);
        assertEquals(0, transactions.getTransactionCount());
        transactions.abort(XID + 5, XID + 5);
        try (Stream<Path> files = Files.list(spoolDirectory())) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void testFirstSegmentReplacesAnEarlierSpoolOfTheSameTransaction() throws IOException {
        transactions.beginSegment(XID, true);
        transactions.append(1, ByteBuffer.wrap(insert(XID, 10, (byte) 1)));
        transactions.endSegment();

        transactions.beginSegment(XID, true);
        transactions.endSegment();

        try (StreamedTransactionSpool spool = transactions.remove(XID)) {
            assertEquals(0, spool.getMessageCount());
        }
    }

    @Test
    void testProtocolStateErrors() throws IOException {
        assertThrows(IllegalStateException.class, () -> transactions.endSegment());
        assertThrows(IllegalStateException.class, () -> transactions.append(1, ByteBuffer.wrap(insert(XID, 10, (byte) 1))));
        assertThrows(IllegalStateException.class, () -> transactions.beginSegment(XID, false), "no first segment");

        transactions.beginSegment(XID, true);
        assertThrows(IllegalStateException.class, () -> transactions.beginSegment(XID + 1, true), "segments do not nest");
        transactions.endSegment();
    }

    @Test
    void testClearDiscardsEverythingAndCloseRemovesTheDirectory() throws IOException {
        transactions.beginSegment(XID, true);
        transactions.append(1, ByteBuffer.wrap(insert(XID, 10, (byte) 1)));
        final Path spoolDirectory = spoolDirectory();
        assertTrue(Files.isDirectory(spoolDirectory));

        transactions.clear();
        assertFalse(transactions.isSegmentOpen());
        assertEquals(0, transactions.getTransactionCount());

        transactions.close();
        assertFalse(Files.exists(spoolDirectory));
    }

    private Path spoolDirectory() throws IOException {
        // the directory is created under the base directory on first use
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(path -> path.getFileName().toString().startsWith("nifi-cdc-postgresql-")).findFirst().orElseThrow();
        }
    }

    /**
     * An Insert message with a transaction id prefix, as received inside a streamed segment; the payload is filler ending with a marker.
     */
    private static byte[] insert(final int xid, final int payloadLength, final byte marker) {
        final ByteBuffer buffer = ByteBuffer.allocate(1 + Integer.BYTES + payloadLength);
        buffer.put((byte) 'I').putInt(xid);
        for (int i = 0; i < payloadLength - 1; i++) {
            buffer.put((byte) 'x');
        }
        buffer.put(marker);
        return buffer.array();
    }
}
