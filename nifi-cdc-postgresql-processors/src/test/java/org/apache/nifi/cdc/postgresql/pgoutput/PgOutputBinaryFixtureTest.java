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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Decodes the scenarios recorded with binary tuples and checks that every value converts to the same Java value as
 * its text counterpart, except for the types without binary mapping, which become hexadecimal strings.
 */
class PgOutputBinaryFixtureTest {

    private static final List<String> SCENARIOS = List.of("insert-customers", "update-customers-default-identity", "update-customers-key-change",
            "delete-customers", "orders-full-identity", "types-insert", "toast-update", "multi-table-transaction", "truncate");

    /** Columns of lab.type_samples whose types have no binary mapping. */
    private static final Set<String> UNMAPPED_COLUMNS = Set.of("c_int_array", "c_mood", "c_interval", "c_timetz");
    /** Columns filled by a default at insert time, so their values differ between the two recordings. */
    private static final Set<String> GENERATED_COLUMNS = Set.of("created_at");
    /** OIDs below this value belong to built-in types; tables and user-defined types get new OIDs in every recording. */
    private static final int FIRST_NORMAL_OID = 16384;

    private final PostgreSQLTypeMapper mapper = new PostgreSQLTypeMapper();

    static Stream<Arguments> recordings() {
        return PgOutputFixtures.SERVERS.stream().flatMap(server -> SCENARIOS.stream().map(scenario -> Arguments.of(server, scenario)));
    }

    static Stream<String> servers() {
        return PgOutputFixtures.SERVERS.stream();
    }

    @ParameterizedTest
    @MethodSource("recordings")
    void testBinaryValuesConvertLikeTextValues(final String server, final String scenario) {
        final List<PgOutputMessage> textMessages = PgOutputFixtures.decode(server, scenario);
        final List<PgOutputMessage> binaryMessages = PgOutputFixtures.decode(server + "-binary", scenario);
        assertEquals(textMessages.stream().map(PgOutputMessage::getClass).toList(), binaryMessages.stream().map(PgOutputMessage::getClass).toList());

        final RelationCache relations = new RelationCache();
        int comparedValues = 0;
        for (int i = 0; i < textMessages.size(); i++) {
            final PgOutputMessage textMessage = textMessages.get(i);
            final PgOutputMessage binaryMessage = binaryMessages.get(i);
            switch (textMessage) {
                case RelationMessage relation -> {
                    assertSameTable(relation, (RelationMessage) binaryMessage);
                    relations.put(relation);
                }
                case InsertMessage insert -> comparedValues += compare(relations.get(insert.relationId()), insert.newTuple(), ((InsertMessage) binaryMessage).newTuple());
                case UpdateMessage update -> {
                    final UpdateMessage binaryUpdate = (UpdateMessage) binaryMessage;
                    assertEquals(update.oldTupleKind(), binaryUpdate.oldTupleKind());
                    if (update.hasOldTuple()) {
                        comparedValues += compare(relations.get(update.relationId()), update.oldTuple(), binaryUpdate.oldTuple());
                    }
                    comparedValues += compare(relations.get(update.relationId()), update.newTuple(), binaryUpdate.newTuple());
                }
                case DeleteMessage delete -> comparedValues += compare(relations.get(delete.relationId()), delete.oldTuple(), ((DeleteMessage) binaryMessage).oldTuple());
                case TruncateMessage truncate -> assertEquals(truncate.relationIds().size(), ((TruncateMessage) binaryMessage).relationIds().size());
                default -> {
                    // Begin, Commit and Type messages carry no tuples; their positions and times differ between recordings
                }
            }
        }
        assertTrue(comparedValues > 0 || scenario.equals("truncate"), "no values compared");
    }

    @ParameterizedTest
    @MethodSource("servers")
    void testBinaryTypeSamples(final String server) {
        final List<PgOutputMessage> messages = PgOutputFixtures.decode(server + "-binary", "types-insert");
        final RelationMessage relation = (RelationMessage) messages.get(2);
        final InsertMessage values = (InsertMessage) messages.get(3);

        assertEquals(1, convert(relation, values, "id"));
        assertEquals(-32768, convert(relation, values, "c_int2"));
        assertEquals(Long.MAX_VALUE, convert(relation, values, "c_int8"));
        assertEquals(new BigDecimal("12345.678"), convert(relation, values, "c_numeric"));
        assertEquals(new BigDecimal("0.1234567890123456789"), convert(relation, values, "c_numeric_free"));
        assertEquals(1.5f, convert(relation, values, "c_float4"));
        assertEquals(2.25d, convert(relation, values, "c_float8"));
        assertEquals(Boolean.TRUE, convert(relation, values, "c_bool"));
        assertEquals("text with ünïcödé and \"quotes\"", convert(relation, values, "c_text"));
        assertEquals("ab   ", convert(relation, values, "c_bpchar"));
        assertEquals(Timestamp.valueOf(LocalDateTime.of(2026, 9, 17, 13, 45, 30, 500_000_000)), convert(relation, values, "c_timestamp"));
        assertEquals(Timestamp.from(Instant.parse("2026-09-17T11:45:30Z")), convert(relation, values, "c_timestamptz"));
        assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), convert(relation, values, "c_uuid"));
        assertEquals("{\"a\": 1}", convert(relation, values, "c_json"));
        assertEquals("{\"b\": [true, null]}", convert(relation, values, "c_jsonb"));
        assertArrayEquals(new Byte[]{0, (byte) 0xFF, 0x10}, (Byte[]) convert(relation, values, "c_bytea"));
        // array_send: 1 dimension, no nulls, element type int4, 3 elements from lower bound 1, each with its length
        assertEquals("\\x0000000100000000000000170000000300000001000000040000000100000004000000020000000400000003", convert(relation, values, "c_int_array"));
        assertEquals("\\x6861707079", convert(relation, values, "c_mood"), "enum labels are sent as text");
        assertFalse(mapper.hasBinaryMapping(column(relation, "c_interval")));

        final InsertMessage specials = (InsertMessage) messages.get(5);
        assertEquals("NaN", assertThrows(UnsupportedValueException.class, () -> convert(relation, specials, "c_numeric_free")).getText());
        assertEquals("Infinity", assertThrows(UnsupportedValueException.class, () -> convert(relation, specials, "c_float8")).getText());
        assertEquals("infinity", assertThrows(UnsupportedValueException.class, () -> convert(relation, specials, "c_date")).getText());
        assertEquals("-infinity", assertThrows(UnsupportedValueException.class, () -> convert(relation, specials, "c_timestamp")).getText());
    }

    /**
     * @return number of values compared
     */
    private int compare(final RelationMessage relation, final TupleData textTuple, final TupleData binaryTuple) {
        assertEquals(textTuple.columns().size(), binaryTuple.columns().size());
        int compared = 0;
        for (int i = 0; i < textTuple.columns().size(); i++) {
            final RelationColumn column = relation.columns().get(i);
            final ColumnValue text = textTuple.columns().get(i);
            final ColumnValue binary = binaryTuple.columns().get(i);
            if (GENERATED_COLUMNS.contains(column.name())) {
                continue;
            }
            if (text.kind() != ColumnValue.Kind.TEXT) {
                assertEquals(text, binary, column.name());
                continue;
            }
            assertEquals(ColumnValue.Kind.BINARY, binary.kind(), column.name());
            compared++;
            if (UNMAPPED_COLUMNS.contains(column.name())) {
                assertFalse(mapper.hasBinaryMapping(column), column.name());
                assertTrue(((String) mapper.convert(column, binary.binary())).startsWith("\\x"), column.name());
                continue;
            }
            assertTrue(mapper.hasBinaryMapping(column), column.name());
            final Object expected = convertOrNull(column, text);
            final Object actual = convertOrNull(column, binary);
            if (expected instanceof Byte[] expectedBytes) {
                assertArrayEquals(expectedBytes, (Byte[]) actual, column.name());
            } else {
                assertEquals(expected, actual, column.name());
            }
        }
        return compared;
    }

    private static void assertSameTable(final RelationMessage expected, final RelationMessage actual) {
        assertEquals(expected.namespace(), actual.namespace());
        assertEquals(expected.name(), actual.name());
        assertEquals(expected.replicaIdentity(), actual.replicaIdentity());
        assertEquals(expected.columns().size(), actual.columns().size());
        for (int i = 0; i < expected.columns().size(); i++) {
            final RelationColumn expectedColumn = expected.columns().get(i);
            final RelationColumn actualColumn = actual.columns().get(i);
            assertEquals(expectedColumn.name(), actualColumn.name());
            assertEquals(expectedColumn.key(), actualColumn.key());
            assertEquals(expectedColumn.typeModifier(), actualColumn.typeModifier());
            if (expectedColumn.typeId() < FIRST_NORMAL_OID) {
                assertEquals(expectedColumn.typeId(), actualColumn.typeId(), expectedColumn.name());
            }
        }
    }

    private Object convertOrNull(final RelationColumn column, final ColumnValue value) {
        try {
            return value.kind() == ColumnValue.Kind.TEXT ? mapper.convert(column, value.text()) : mapper.convert(column, value.binary());
        } catch (final UnsupportedValueException e) {
            return null;
        }
    }

    private Object convert(final RelationMessage relation, final InsertMessage insert, final String name) {
        final RelationColumn column = column(relation, name);
        final ColumnValue value = insert.newTuple().columns().get(relation.columns().indexOf(column));
        if (value.kind() != ColumnValue.Kind.BINARY) {
            fail(String.format("Column %s was not sent in binary format: %s", name, value));
        }
        return mapper.convert(column, value.binary());
    }

    private static RelationColumn column(final RelationMessage relation, final String name) {
        final List<RelationColumn> matching = new ArrayList<>();
        relation.columns().stream().filter(c -> c.name().equals(name)).forEach(matching::add);
        assertEquals(1, matching.size(), name);
        return matching.get(0);
    }
}
