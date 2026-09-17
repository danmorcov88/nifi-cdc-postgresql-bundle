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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PgOutputDecoderTest {

    private static final long LSN = 0x0000000101748DC8L;
    private static final long END_LSN = LSN + 0x40;
    private static final int XID = 745;
    private static final int RELATION_ID = 16401;

    /** 2026-09-17T06:32:52.782Z expressed in microseconds since 2000-01-01. */
    private static final long COMMIT_MICROS = 842_941_972_782_000L;
    private static final Instant COMMIT_TIME = Instant.parse("2026-09-17T06:32:52.782Z");

    private final PgOutputDecoder decoder = new PgOutputDecoder();

    @Test
    void testDecodeBegin() {
        final ByteBuffer buffer = new MessageBuilder('B').int64(LSN).int64(COMMIT_MICROS).int32(XID).build();

        final BeginMessage begin = assertInstanceOf(BeginMessage.class, decoder.decode(buffer));

        assertEquals(LSN, begin.finalLsn());
        assertEquals(COMMIT_TIME, begin.commitTime());
        assertEquals(XID, begin.xid());
    }

    @Test
    void testDecodeCommit() {
        final ByteBuffer buffer = new MessageBuilder('C').int8(0).int64(LSN).int64(END_LSN).int64(COMMIT_MICROS).build();

        final CommitMessage commit = assertInstanceOf(CommitMessage.class, decoder.decode(buffer));

        assertEquals(LSN, commit.commitLsn());
        assertEquals(END_LSN, commit.endLsn());
        assertEquals(COMMIT_TIME, commit.commitTime());
    }

    @Test
    void testDecodeTimestampBeforePostgreSQLEpoch() {
        final ByteBuffer buffer = new MessageBuilder('B').int64(LSN).int64(-1_500_000L).int32(XID).build();

        final BeginMessage begin = assertInstanceOf(BeginMessage.class, decoder.decode(buffer));

        assertEquals(Instant.parse("1999-12-31T23:59:58.500Z"), begin.commitTime());
    }

    @Test
    void testDecodeOrigin() {
        final ByteBuffer buffer = new MessageBuilder('O').int64(LSN).string("node_a").build();

        final OriginMessage origin = assertInstanceOf(OriginMessage.class, decoder.decode(buffer));

        assertEquals(LSN, origin.originLsn());
        assertEquals("node_a", origin.name());
    }

    @Test
    void testDecodeRelation() {
        final ByteBuffer buffer = new MessageBuilder('R')
                .int32(RELATION_ID).string("lab").string("customers").int8('d')
                .int16(2)
                .int8(1).string("id").int32(PostgreSQLTypeMapper.OID_INT8).int32(-1)
                .int8(0).string("balance").int32(PostgreSQLTypeMapper.OID_NUMERIC).int32(786438)
                .build();

        final RelationMessage relation = assertInstanceOf(RelationMessage.class, decoder.decode(buffer));

        assertEquals(RELATION_ID, relation.relationId());
        assertEquals("lab", relation.namespace());
        assertEquals("customers", relation.name());
        assertEquals(ReplicaIdentity.DEFAULT, relation.replicaIdentity());
        assertEquals(List.of(
                new RelationColumn("id", true, PostgreSQLTypeMapper.OID_INT8, -1),
                new RelationColumn("balance", false, PostgreSQLTypeMapper.OID_NUMERIC, 786438)
        ), relation.columns());
    }

    @Test
    void testDecodeRelationWithUnknownReplicaIdentity() {
        final ByteBuffer buffer = new MessageBuilder('R').int32(RELATION_ID).string("lab").string("t").int8('x').int16(0).build();

        final PgOutputException exception = assertThrows(PgOutputException.class, () -> decoder.decode(buffer));

        assertTrue(exception.getMessage().contains("[x]"));
    }

    @Test
    void testDecodeType() {
        final ByteBuffer buffer = new MessageBuilder('Y').int32(24576).string("lab").string("mood").build();

        final TypeMessage type = assertInstanceOf(TypeMessage.class, decoder.decode(buffer));

        assertEquals(24576, type.typeId());
        assertEquals("lab", type.namespace());
        assertEquals("mood", type.name());
    }

    @Test
    void testDecodeInsert() {
        final ByteBuffer buffer = new MessageBuilder('I').int32(RELATION_ID).int8('N')
                .int16(3).int8('t').text("100").int8('n').int8('u')
                .build();

        final InsertMessage insert = assertInstanceOf(InsertMessage.class, decoder.decode(buffer));

        assertEquals(RELATION_ID, insert.relationId());
        assertEquals(List.of(ColumnValue.text("100"), ColumnValue.NULL, ColumnValue.UNCHANGED_TOAST), insert.newTuple().columns());
    }

    @Test
    void testDecodeInsertWithUnicodeText() {
        final ByteBuffer buffer = new MessageBuilder('I').int32(RELATION_ID).int8('N').int16(1).int8('t').text("ünïcödé €").build();

        final InsertMessage insert = assertInstanceOf(InsertMessage.class, decoder.decode(buffer));

        assertEquals("ünïcödé €", insert.newTuple().columns().get(0).text());
    }

    @Test
    void testDecodeUpdateWithoutOldTuple() {
        final ByteBuffer buffer = new MessageBuilder('U').int32(RELATION_ID).int8('N').int16(1).int8('t').text("new").build();

        final UpdateMessage update = assertInstanceOf(UpdateMessage.class, decoder.decode(buffer));

        assertFalse(update.hasOldTuple());
        assertNull(update.oldTupleKind());
        assertNull(update.oldTuple());
        assertEquals(List.of(ColumnValue.text("new")), update.newTuple().columns());
    }

    @Test
    void testDecodeUpdateWithKeyTuple() {
        final ByteBuffer buffer = new MessageBuilder('U').int32(RELATION_ID)
                .int8('K').int16(2).int8('t').text("1").int8('n')
                .int8('N').int16(2).int8('t').text("2").int8('t').text("x")
                .build();

        final UpdateMessage update = assertInstanceOf(UpdateMessage.class, decoder.decode(buffer));

        assertEquals(OldTupleKind.KEY, update.oldTupleKind());
        assertEquals(List.of(ColumnValue.text("1"), ColumnValue.NULL), update.oldTuple().columns());
        assertEquals(List.of(ColumnValue.text("2"), ColumnValue.text("x")), update.newTuple().columns());
    }

    @Test
    void testDecodeUpdateWithOldTuple() {
        final ByteBuffer buffer = new MessageBuilder('U').int32(RELATION_ID)
                .int8('O').int16(1).int8('t').text("before")
                .int8('N').int16(1).int8('t').text("after")
                .build();

        final UpdateMessage update = assertInstanceOf(UpdateMessage.class, decoder.decode(buffer));

        assertEquals(OldTupleKind.OLD, update.oldTupleKind());
        assertEquals(List.of(ColumnValue.text("before")), update.oldTuple().columns());
        assertEquals(List.of(ColumnValue.text("after")), update.newTuple().columns());
    }

    @Test
    void testDecodeUpdateWithoutNewTuple() {
        final ByteBuffer buffer = new MessageBuilder('U').int32(RELATION_ID).int8('K').int16(1).int8('t').text("1").build();

        assertThrows(PgOutputException.class, () -> decoder.decode(buffer));
    }

    @Test
    void testDecodeDelete() {
        final ByteBuffer buffer = new MessageBuilder('D').int32(RELATION_ID).int8('O').int16(1).int8('t').text("gone").build();

        final DeleteMessage delete = assertInstanceOf(DeleteMessage.class, decoder.decode(buffer));

        assertEquals(RELATION_ID, delete.relationId());
        assertEquals(OldTupleKind.OLD, delete.oldTupleKind());
        assertEquals(List.of(ColumnValue.text("gone")), delete.oldTuple().columns());
    }

    @Test
    void testDecodeDeleteWithInvalidTupleKind() {
        final ByteBuffer buffer = new MessageBuilder('D').int32(RELATION_ID).int8('N').int16(0).build();

        assertThrows(PgOutputException.class, () -> decoder.decode(buffer));
    }

    @Test
    void testDecodeTruncate() {
        final ByteBuffer buffer = new MessageBuilder('T').int32(2).int8(3).int32(RELATION_ID).int32(RELATION_ID + 1).build();

        final TruncateMessage truncate = assertInstanceOf(TruncateMessage.class, decoder.decode(buffer));

        assertTrue(truncate.cascade());
        assertTrue(truncate.restartIdentity());
        assertEquals(List.of(RELATION_ID, RELATION_ID + 1), truncate.relationIds());
    }

    @Test
    void testDecodeTruncateWithoutOptions() {
        final ByteBuffer buffer = new MessageBuilder('T').int32(1).int8(0).int32(RELATION_ID).build();

        final TruncateMessage truncate = assertInstanceOf(TruncateMessage.class, decoder.decode(buffer));

        assertFalse(truncate.cascade());
        assertFalse(truncate.restartIdentity());
    }

    @Test
    void testDecodeEmptyMessage() {
        assertThrows(PgOutputException.class, () -> decoder.decode(ByteBuffer.allocate(0)));
    }

    @Test
    void testDecodeUnsupportedMessageType() {
        final ByteBuffer buffer = new MessageBuilder('M').int8(0).int64(LSN).string("prefix").int32(0).build();

        final PgOutputException exception = assertThrows(PgOutputException.class, () -> decoder.decode(buffer));

        assertTrue(exception.getMessage().contains("[M]"));
    }

    @Test
    void testDecodeTruncatedMessage() {
        final ByteBuffer buffer = new MessageBuilder('B').int64(LSN).int32(XID).build();

        final PgOutputException exception = assertThrows(PgOutputException.class, () -> decoder.decode(buffer));

        assertTrue(exception.getMessage().contains("Truncated"));
    }

    @Test
    void testDecodeMessageWithTrailingBytes() {
        final ByteBuffer buffer = new MessageBuilder('B').int64(LSN).int64(COMMIT_MICROS).int32(XID).int8(0).build();

        final PgOutputException exception = assertThrows(PgOutputException.class, () -> decoder.decode(buffer));

        assertTrue(exception.getMessage().contains("trailing"));
    }

    @Test
    void testDecodeBinaryColumnValue() {
        final ByteBuffer buffer = new MessageBuilder('I').int32(RELATION_ID).int8('N').int16(2).int8('b').int32(4).int32(-5).int8('n').build();

        final InsertMessage insert = (InsertMessage) decoder.decode(buffer);

        assertEquals(List.of(ColumnValue.binary(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFB}), ColumnValue.NULL), insert.newTuple().columns());
    }

    @Test
    void testDecodeUnknownColumnValueKind() {
        final ByteBuffer buffer = new MessageBuilder('I').int32(RELATION_ID).int8('N').int16(1).int8('x').int32(1).int8(0).build();

        final PgOutputException exception = assertThrows(PgOutputException.class, () -> decoder.decode(buffer));

        assertTrue(exception.getMessage().contains("[x]"));
    }

    @Test
    void testDecodeColumnValueLongerThanMessage() {
        final ByteBuffer buffer = new MessageBuilder('I').int32(RELATION_ID).int8('N').int16(1).int8('t').int32(100).int8('x').build();

        final PgOutputException exception = assertThrows(PgOutputException.class, () -> decoder.decode(buffer));

        assertTrue(exception.getMessage().contains("length"));
    }

    private static class MessageBuilder {

        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        MessageBuilder(final char type) {
            int8(type);
        }

        MessageBuilder int8(final int value) {
            bytes.write(value);
            return this;
        }

        MessageBuilder int16(final int value) {
            bytes.write(value >>> 8);
            bytes.write(value);
            return this;
        }

        MessageBuilder int32(final int value) {
            int16(value >>> 16);
            int16(value);
            return this;
        }

        MessageBuilder int64(final long value) {
            int32((int) (value >>> 32));
            int32((int) value);
            return this;
        }

        MessageBuilder string(final String value) {
            bytes.writeBytes(value.getBytes(StandardCharsets.UTF_8));
            bytes.write(0);
            return this;
        }

        MessageBuilder text(final String value) {
            final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
            int32(encoded.length);
            bytes.writeBytes(encoded);
            return this;
        }

        ByteBuffer build() {
            return ByteBuffer.wrap(bytes.toByteArray());
        }
    }
}
