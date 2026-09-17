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

import org.apache.nifi.serialization.record.RecordFieldType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Decodes messages recorded from real PostgreSQL servers (see docker/capture-fixtures.sh).
 */
class PgOutputFixtureTest {

    private static final List<String> CUSTOMER_COLUMNS = List.of("id", "name", "email", "balance", "active", "created_at");
    private static final List<String> ORDER_COLUMNS = List.of("id", "customer_id", "amount", "status", "details", "ordered_on");

    static Stream<String> servers() {
        return PgOutputFixtures.SERVERS.stream();
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testInsert(final String server) {
        final List<PgOutputMessage> messages = PgOutputFixtures.decode(server, "insert-customers");
        assertTypes(messages, BeginMessage.class, RelationMessage.class, InsertMessage.class, CommitMessage.class);

        final BeginMessage begin = (BeginMessage) messages.get(0);
        final RelationMessage relation = (RelationMessage) messages.get(1);
        final InsertMessage insert = (InsertMessage) messages.get(2);
        final CommitMessage commit = (CommitMessage) messages.get(3);

        assertTrue(begin.xid() > 0);
        assertEquals(begin.finalLsn(), commit.commitLsn());
        assertTrue(commit.endLsn() > commit.commitLsn());
        assertEquals(begin.commitTime(), commit.commitTime());
        assertTrue(commit.commitTime().isAfter(Instant.parse("2026-01-01T00:00:00Z")));

        assertEquals("lab", relation.namespace());
        assertEquals("customers", relation.name());
        assertEquals(ReplicaIdentity.DEFAULT, relation.replicaIdentity());
        assertEquals(CUSTOMER_COLUMNS, relation.columns().stream().map(RelationColumn::name).toList());
        assertEquals(List.of(true, false, false, false, false, false), relation.columns().stream().map(RelationColumn::key).toList());
        assertEquals(PostgreSQLTypeMapper.OID_INT8, relation.columns().get(0).typeId());
        assertEquals(PostgreSQLTypeMapper.OID_TEXT, relation.columns().get(1).typeId());
        assertEquals(PostgreSQLTypeMapper.OID_VARCHAR, relation.columns().get(2).typeId());
        assertEquals(PostgreSQLTypeMapper.OID_NUMERIC, relation.columns().get(3).typeId());
        assertEquals(PostgreSQLTypeMapper.OID_BOOL, relation.columns().get(4).typeId());
        assertEquals(PostgreSQLTypeMapper.OID_TIMESTAMPTZ, relation.columns().get(5).typeId());

        assertEquals(relation.relationId(), insert.relationId());
        final List<ColumnValue> row = insert.newTuple().columns();
        assertEquals(ColumnValue.text("100"), row.get(0));
        assertEquals(ColumnValue.text("Fixture One"), row.get(1));
        assertEquals(ColumnValue.text("one@example.com"), row.get(2));
        assertEquals(ColumnValue.text("10.50"), row.get(3));
        assertEquals(ColumnValue.text("t"), row.get(4));
        assertEquals(ColumnValue.Kind.TEXT, row.get(5).kind());
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testUpdateWithDefaultIdentityHasNoOldTuple(final String server) {
        final List<PgOutputMessage> messages = PgOutputFixtures.decode(server, "update-customers-default-identity");
        assertTypes(messages, BeginMessage.class, RelationMessage.class, UpdateMessage.class, CommitMessage.class);

        final UpdateMessage update = (UpdateMessage) messages.get(2);
        assertFalse(update.hasOldTuple());
        assertNull(update.oldTupleKind());
        final List<ColumnValue> row = update.newTuple().columns();
        assertEquals(ColumnValue.text("100"), row.get(0));
        assertEquals(ColumnValue.NULL, row.get(2));
        assertEquals(ColumnValue.text("11.50"), row.get(3));
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testUpdateOfKeyColumnSendsKeyTuple(final String server) {
        final List<PgOutputMessage> messages = PgOutputFixtures.decode(server, "update-customers-key-change");
        assertTypes(messages, BeginMessage.class, RelationMessage.class, UpdateMessage.class, CommitMessage.class);

        final UpdateMessage update = (UpdateMessage) messages.get(2);
        assertEquals(OldTupleKind.KEY, update.oldTupleKind());
        assertEquals(List.of(ColumnValue.text("100"), ColumnValue.NULL, ColumnValue.NULL, ColumnValue.NULL, ColumnValue.NULL, ColumnValue.NULL),
                update.oldTuple().columns());
        assertEquals(ColumnValue.text("101"), update.newTuple().columns().get(0));
        assertEquals(ColumnValue.text("Fixture One"), update.newTuple().columns().get(1));
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testDeleteWithDefaultIdentitySendsKeyTuple(final String server) {
        final List<PgOutputMessage> messages = PgOutputFixtures.decode(server, "delete-customers");
        assertTypes(messages, BeginMessage.class, RelationMessage.class, DeleteMessage.class, CommitMessage.class);

        final DeleteMessage delete = (DeleteMessage) messages.get(2);
        assertEquals(OldTupleKind.KEY, delete.oldTupleKind());
        assertEquals(ColumnValue.text("101"), delete.oldTuple().columns().get(0));
        assertEquals(ColumnValue.NULL, delete.oldTuple().columns().get(1));
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testFullIdentitySendsWholeOldRow(final String server) {
        final List<PgOutputMessage> messages = PgOutputFixtures.decode(server, "orders-full-identity");
        assertTypes(messages, BeginMessage.class, RelationMessage.class, InsertMessage.class, UpdateMessage.class, DeleteMessage.class, CommitMessage.class);

        final RelationMessage relation = (RelationMessage) messages.get(1);
        assertEquals("orders", relation.name());
        assertEquals(ReplicaIdentity.FULL, relation.replicaIdentity());
        assertEquals(ORDER_COLUMNS, relation.columns().stream().map(RelationColumn::name).toList());
        assertEquals(PostgreSQLTypeMapper.OID_UUID, relation.columns().get(0).typeId());
        assertEquals(PostgreSQLTypeMapper.OID_JSONB, relation.columns().get(4).typeId());
        assertEquals(PostgreSQLTypeMapper.OID_DATE, relation.columns().get(5).typeId());

        final InsertMessage insert = (InsertMessage) messages.get(2);
        assertEquals(ColumnValue.text("0f4b3c2e-9a1d-4c5e-8b7f-1a2b3c4d5e6f"), insert.newTuple().columns().get(0));
        assertEquals(ColumnValue.text("{\"items\": [1, 2]}"), insert.newTuple().columns().get(4));
        assertEquals(ColumnValue.text("2026-09-17"), insert.newTuple().columns().get(5));

        final UpdateMessage update = (UpdateMessage) messages.get(3);
        assertEquals(OldTupleKind.OLD, update.oldTupleKind());
        assertEquals(ColumnValue.text("new"), update.oldTuple().columns().get(3));
        assertEquals(ColumnValue.text("paid"), update.newTuple().columns().get(3));
        assertEquals(insert.newTuple().columns().get(4), update.oldTuple().columns().get(4));

        final DeleteMessage delete = (DeleteMessage) messages.get(4);
        assertEquals(OldTupleKind.OLD, delete.oldTupleKind());
        assertEquals(update.newTuple(), delete.oldTuple());
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testUserDefinedTypeAndValueConversion(final String server) {
        final List<PgOutputMessage> messages = PgOutputFixtures.decode(server, "types-insert");
        assertTypes(messages, BeginMessage.class, TypeMessage.class, RelationMessage.class, InsertMessage.class, InsertMessage.class, InsertMessage.class,
                CommitMessage.class);

        final TypeMessage type = (TypeMessage) messages.get(1);
        assertEquals("lab", type.namespace());
        assertEquals("mood", type.name());

        final RelationMessage relation = (RelationMessage) messages.get(2);
        assertEquals(23, relation.columns().size());
        assertEquals(type.typeId(), column(relation, "c_mood").typeId());

        final PostgreSQLTypeMapper mapper = new PostgreSQLTypeMapper();
        final InsertMessage values = (InsertMessage) messages.get(3);
        assertEquals(1, convert(mapper, relation, values, "id"));
        assertEquals(-32768, convert(mapper, relation, values, "c_int2"));
        assertEquals(Long.MAX_VALUE, convert(mapper, relation, values, "c_int8"));
        assertEquals(new BigDecimal("12345.678"), convert(mapper, relation, values, "c_numeric"));
        assertEquals(RecordFieldType.DECIMAL.getDecimalDataType(10, 3), mapper.getDataType(column(relation, "c_numeric")));
        assertEquals(new BigDecimal("0.1234567890123456789"), convert(mapper, relation, values, "c_numeric_free"));
        assertEquals(RecordFieldType.DECIMAL.getDecimalDataType(PostgreSQLTypeMapper.DEFAULT_NUMERIC_PRECISION, PostgreSQLTypeMapper.DEFAULT_NUMERIC_SCALE),
                mapper.getDataType(column(relation, "c_numeric_free")));
        assertEquals(1.5f, convert(mapper, relation, values, "c_float4"));
        assertEquals(2.25d, convert(mapper, relation, values, "c_float8"));
        assertEquals(Boolean.TRUE, convert(mapper, relation, values, "c_bool"));
        assertEquals("text with ünïcödé and \"quotes\"", convert(mapper, relation, values, "c_text"));
        assertEquals("varchar", convert(mapper, relation, values, "c_varchar"));
        assertEquals("ab   ", convert(mapper, relation, values, "c_bpchar"));
        assertEquals(Date.valueOf(LocalDate.of(2026, 9, 17)), convert(mapper, relation, values, "c_date"));
        assertEquals(new Time(Time.valueOf(LocalTime.of(13, 45, 30)).getTime() + 123), convert(mapper, relation, values, "c_time"));
        assertEquals(Timestamp.valueOf(LocalDateTime.of(2026, 9, 17, 13, 45, 30, 500_000_000)), convert(mapper, relation, values, "c_timestamp"));
        assertEquals(Timestamp.from(Instant.parse("2026-09-17T11:45:30Z")), convert(mapper, relation, values, "c_timestamptz"));
        assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), convert(mapper, relation, values, "c_uuid"));
        assertEquals("{\"a\": 1}", convert(mapper, relation, values, "c_json"));
        assertEquals("{\"b\": [true, null]}", convert(mapper, relation, values, "c_jsonb"));
        assertArrayEquals(new Byte[]{0, (byte) 0xFF, 0x10}, (Byte[]) convert(mapper, relation, values, "c_bytea"));
        assertEquals("{1,2,3}", convert(mapper, relation, values, "c_int_array"));
        assertEquals("happy", convert(mapper, relation, values, "c_mood"));
        assertEquals("1 day 02:03:04", convert(mapper, relation, values, "c_interval"));
        assertEquals("10:00:00+05:30", convert(mapper, relation, values, "c_timetz"));

        final InsertMessage nulls = (InsertMessage) messages.get(4);
        assertEquals(ColumnValue.text("2"), nulls.newTuple().columns().get(0));
        assertTrue(nulls.newTuple().columns().subList(1, 23).stream().allMatch(ColumnValue::isNull));

        final InsertMessage specials = (InsertMessage) messages.get(5);
        assertEquals("NaN", convert(mapper, relation, specials, "c_numeric_free"));
        assertEquals(Double.POSITIVE_INFINITY, convert(mapper, relation, specials, "c_float8"));
        assertEquals("infinity", convert(mapper, relation, specials, "c_date"));
        assertEquals("-infinity", convert(mapper, relation, specials, "c_timestamp"));
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testUnchangedToastValue(final String server) {
        final List<PgOutputMessage> messages = PgOutputFixtures.decode(server, "toast-update");
        assertTypes(messages, BeginMessage.class, RelationMessage.class, InsertMessage.class, CommitMessage.class,
                BeginMessage.class, UpdateMessage.class, CommitMessage.class);

        final InsertMessage insert = (InsertMessage) messages.get(2);
        assertEquals(16_000, insert.newTuple().columns().get(1).text().length());

        final UpdateMessage update = (UpdateMessage) messages.get(5);
        assertFalse(update.hasOldTuple());
        assertEquals(List.of(ColumnValue.text("1"), ColumnValue.UNCHANGED_TOAST, ColumnValue.text("changed")), update.newTuple().columns());
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testMultipleTablesInOneTransaction(final String server) {
        final List<PgOutputMessage> messages = PgOutputFixtures.decode(server, "multi-table-transaction");
        assertTypes(messages, BeginMessage.class, RelationMessage.class, InsertMessage.class, RelationMessage.class, InsertMessage.class,
                UpdateMessage.class, CommitMessage.class);

        final RelationCache cache = new RelationCache();
        messages.stream().filter(RelationMessage.class::isInstance).map(RelationMessage.class::cast).forEach(cache::put);
        assertEquals(2, cache.size());

        assertEquals("customers", cache.get(((InsertMessage) messages.get(2)).relationId()).name());
        assertEquals("orders", cache.get(((InsertMessage) messages.get(4)).relationId()).name());
        assertEquals("customers", cache.get(((UpdateMessage) messages.get(5)).relationId()).name());
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testTruncate(final String server) {
        final List<PgOutputMessage> messages = PgOutputFixtures.decode(server, "truncate");
        assertTypes(messages, BeginMessage.class, TypeMessage.class, RelationMessage.class, RelationMessage.class, TruncateMessage.class, CommitMessage.class);

        final TruncateMessage truncate = (TruncateMessage) messages.get(4);
        assertTrue(truncate.cascade());
        assertTrue(truncate.restartIdentity());
        assertEquals(List.of(((RelationMessage) messages.get(2)).relationId(), ((RelationMessage) messages.get(3)).relationId()), truncate.relationIds());
    }

    private static void assertTypes(final List<PgOutputMessage> messages, final Class<?>... expected) {
        assertEquals(List.of(expected), messages.stream().map(PgOutputMessage::getClass).toList());
    }

    private static RelationColumn column(final RelationMessage relation, final String name) {
        return relation.columns().stream().filter(c -> c.name().equals(name)).findFirst().orElseThrow();
    }

    private static Object convert(final PostgreSQLTypeMapper mapper, final RelationMessage relation, final InsertMessage insert, final String name) {
        final int index = relation.columns().indexOf(column(relation, name));
        final ColumnValue value = insert.newTuple().columns().get(index);
        assertInstanceOf(String.class, value.text(), name);
        return mapper.convert(relation.columns().get(index), value.text());
    }
}
