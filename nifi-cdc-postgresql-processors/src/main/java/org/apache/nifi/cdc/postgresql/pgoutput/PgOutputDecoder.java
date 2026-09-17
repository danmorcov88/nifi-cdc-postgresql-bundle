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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Decoder for the binary messages of the pgoutput logical decoding plugin, protocol version 1 with tuples in text
 * format, as documented in the "Logical Replication Message Formats" chapter of the PostgreSQL manual.
 * The decoder is stateless: one call decodes exactly one message and fails when bytes remain after it.
 */
public class PgOutputDecoder {

    private static final byte MESSAGE_BEGIN = 'B';
    private static final byte MESSAGE_COMMIT = 'C';
    private static final byte MESSAGE_ORIGIN = 'O';
    private static final byte MESSAGE_RELATION = 'R';
    private static final byte MESSAGE_TYPE = 'Y';
    private static final byte MESSAGE_INSERT = 'I';
    private static final byte MESSAGE_UPDATE = 'U';
    private static final byte MESSAGE_DELETE = 'D';
    private static final byte MESSAGE_TRUNCATE = 'T';

    private static final byte TUPLE_NEW = 'N';
    private static final byte TUPLE_KEY = 'K';
    private static final byte TUPLE_OLD = 'O';

    private static final byte COLUMN_NULL = 'n';
    private static final byte COLUMN_UNCHANGED_TOAST = 'u';
    private static final byte COLUMN_TEXT = 't';

    private static final byte COLUMN_KEY_FLAG = 1;
    private static final byte TRUNCATE_CASCADE_FLAG = 1;
    private static final byte TRUNCATE_RESTART_IDENTITY_FLAG = 2;

    /** Seconds between the Unix epoch and the PostgreSQL epoch (2000-01-01 00:00:00 UTC). */
    private static final long POSTGRESQL_EPOCH_SECONDS = 946_684_800L;
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long NANOS_PER_MICRO = 1_000L;

    /**
     * Decode one message.
     *
     * @param buffer buffer positioned at the message type byte; the whole remaining content must belong to the message
     * @return the decoded message
     * @throws PgOutputException when the message type is not supported or the content is malformed
     */
    public PgOutputMessage decode(final ByteBuffer buffer) {
        final byte type;
        try {
            type = buffer.get();
        } catch (final BufferUnderflowException e) {
            throw new PgOutputException("Empty pgoutput message", e);
        }

        final PgOutputMessage message;
        try {
            message = switch (type) {
                case MESSAGE_BEGIN -> decodeBegin(buffer);
                case MESSAGE_COMMIT -> decodeCommit(buffer);
                case MESSAGE_ORIGIN -> decodeOrigin(buffer);
                case MESSAGE_RELATION -> decodeRelation(buffer);
                case MESSAGE_TYPE -> decodeType(buffer);
                case MESSAGE_INSERT -> decodeInsert(buffer);
                case MESSAGE_UPDATE -> decodeUpdate(buffer);
                case MESSAGE_DELETE -> decodeDelete(buffer);
                case MESSAGE_TRUNCATE -> decodeTruncate(buffer);
                default -> throw new PgOutputException(String.format("Unsupported pgoutput message type [%c]", (char) type));
            };
        } catch (final BufferUnderflowException e) {
            throw new PgOutputException(String.format("Truncated pgoutput message of type [%c]", (char) type), e);
        }

        if (buffer.hasRemaining()) {
            throw new PgOutputException(String.format("Unexpected %d trailing bytes after pgoutput message of type [%c]", buffer.remaining(), (char) type));
        }
        return message;
    }

    private BeginMessage decodeBegin(final ByteBuffer buffer) {
        final long finalLsn = buffer.getLong();
        final Instant commitTime = readTimestamp(buffer);
        final int xid = buffer.getInt();
        return new BeginMessage(finalLsn, commitTime, xid);
    }

    private CommitMessage decodeCommit(final ByteBuffer buffer) {
        buffer.get(); // flags, currently unused and always zero
        final long commitLsn = buffer.getLong();
        final long endLsn = buffer.getLong();
        final Instant commitTime = readTimestamp(buffer);
        return new CommitMessage(commitLsn, endLsn, commitTime);
    }

    private OriginMessage decodeOrigin(final ByteBuffer buffer) {
        final long originLsn = buffer.getLong();
        final String name = readString(buffer);
        return new OriginMessage(originLsn, name);
    }

    private RelationMessage decodeRelation(final ByteBuffer buffer) {
        final int relationId = buffer.getInt();
        final String namespace = readString(buffer);
        final String name = readString(buffer);
        final ReplicaIdentity replicaIdentity = ReplicaIdentity.fromCode(buffer.get());
        final int columnCount = Short.toUnsignedInt(buffer.getShort());
        final List<RelationColumn> columns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            final boolean key = (buffer.get() & COLUMN_KEY_FLAG) != 0;
            final String columnName = readString(buffer);
            final int typeId = buffer.getInt();
            final int typeModifier = buffer.getInt();
            columns.add(new RelationColumn(columnName, key, typeId, typeModifier));
        }
        return new RelationMessage(relationId, namespace, name, replicaIdentity, columns);
    }

    private TypeMessage decodeType(final ByteBuffer buffer) {
        final int typeId = buffer.getInt();
        final String namespace = readString(buffer);
        final String name = readString(buffer);
        return new TypeMessage(typeId, namespace, name);
    }

    private InsertMessage decodeInsert(final ByteBuffer buffer) {
        final int relationId = buffer.getInt();
        expectTupleKind(buffer.get(), TUPLE_NEW);
        return new InsertMessage(relationId, readTuple(buffer));
    }

    private UpdateMessage decodeUpdate(final ByteBuffer buffer) {
        final int relationId = buffer.getInt();
        byte tupleKind = buffer.get();

        OldTupleKind oldTupleKind = null;
        TupleData oldTuple = null;
        if (tupleKind == TUPLE_KEY || tupleKind == TUPLE_OLD) {
            oldTupleKind = tupleKind == TUPLE_KEY ? OldTupleKind.KEY : OldTupleKind.OLD;
            oldTuple = readTuple(buffer);
            tupleKind = buffer.get();
        }

        expectTupleKind(tupleKind, TUPLE_NEW);
        return new UpdateMessage(relationId, oldTupleKind, oldTuple, readTuple(buffer));
    }

    private DeleteMessage decodeDelete(final ByteBuffer buffer) {
        final int relationId = buffer.getInt();
        final byte tupleKind = buffer.get();
        final OldTupleKind oldTupleKind = switch (tupleKind) {
            case TUPLE_KEY -> OldTupleKind.KEY;
            case TUPLE_OLD -> OldTupleKind.OLD;
            default -> throw new PgOutputException(String.format("Unexpected tuple kind [%c] in Delete message", (char) tupleKind));
        };
        return new DeleteMessage(relationId, oldTupleKind, readTuple(buffer));
    }

    private TruncateMessage decodeTruncate(final ByteBuffer buffer) {
        final int relationCount = buffer.getInt();
        final byte options = buffer.get();
        final List<Integer> relationIds = new ArrayList<>(relationCount);
        for (int i = 0; i < relationCount; i++) {
            relationIds.add(buffer.getInt());
        }
        final boolean cascade = (options & TRUNCATE_CASCADE_FLAG) != 0;
        final boolean restartIdentity = (options & TRUNCATE_RESTART_IDENTITY_FLAG) != 0;
        return new TruncateMessage(cascade, restartIdentity, relationIds);
    }

    private TupleData readTuple(final ByteBuffer buffer) {
        final int columnCount = Short.toUnsignedInt(buffer.getShort());
        final List<ColumnValue> columns = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            final byte kind = buffer.get();
            switch (kind) {
                case COLUMN_NULL -> columns.add(ColumnValue.NULL);
                case COLUMN_UNCHANGED_TOAST -> columns.add(ColumnValue.UNCHANGED_TOAST);
                case COLUMN_TEXT -> {
                    final int length = buffer.getInt();
                    if (length < 0 || length > buffer.remaining()) {
                        throw new PgOutputException(String.format("Invalid column value length [%d] with %d bytes remaining", length, buffer.remaining()));
                    }
                    final byte[] bytes = new byte[length];
                    buffer.get(bytes);
                    columns.add(ColumnValue.text(new String(bytes, StandardCharsets.UTF_8)));
                }
                default -> throw new PgOutputException(String.format("Unsupported column value kind [%c]; only text format tuples are supported", (char) kind));
            }
        }
        return new TupleData(columns);
    }

    private void expectTupleKind(final byte actual, final byte expected) {
        if (actual != expected) {
            throw new PgOutputException(String.format("Expected tuple kind [%c] but found [%c]", (char) expected, (char) actual));
        }
    }

    /**
     * Read a null-terminated UTF-8 string.
     */
    private String readString(final ByteBuffer buffer) {
        final int start = buffer.position();
        while (buffer.get() != 0) {
            // advance to the terminator
        }
        final int length = buffer.position() - start - 1;
        final byte[] bytes = new byte[length];
        buffer.get(start, bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Read a timestamp expressed in microseconds since the PostgreSQL epoch.
     */
    private Instant readTimestamp(final ByteBuffer buffer) {
        final long micros = buffer.getLong();
        final long seconds = Math.floorDiv(micros, MICROS_PER_SECOND);
        final long nanos = Math.floorMod(micros, MICROS_PER_SECOND) * NANOS_PER_MICRO;
        return Instant.ofEpochSecond(POSTGRESQL_EPOCH_SECONDS + seconds, nanos);
    }
}
