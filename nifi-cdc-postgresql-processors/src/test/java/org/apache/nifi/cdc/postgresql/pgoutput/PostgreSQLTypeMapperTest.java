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

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgreSQLTypeMapperTest {

    private static final int NO_TYPE_MODIFIER = -1;
    private static final int OID_INT4_ARRAY = 1007;
    private static final int OID_INTERVAL = 1186;

    private final PostgreSQLTypeMapper mapper = new PostgreSQLTypeMapper();

    static Stream<Arguments> dataTypes() {
        return Stream.of(
                Arguments.of(PostgreSQLTypeMapper.OID_BOOL, RecordFieldType.BOOLEAN.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_INT2, RecordFieldType.INT.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_INT4, RecordFieldType.INT.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_INT8, RecordFieldType.LONG.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_FLOAT4, RecordFieldType.FLOAT.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_FLOAT8, RecordFieldType.DOUBLE.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_TEXT, RecordFieldType.STRING.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_VARCHAR, RecordFieldType.STRING.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_BPCHAR, RecordFieldType.STRING.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_NAME, RecordFieldType.STRING.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_CHAR, RecordFieldType.STRING.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_DATE, RecordFieldType.DATE.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_TIME, RecordFieldType.TIME.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_TIMESTAMP, RecordFieldType.TIMESTAMP.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_TIMESTAMPTZ, RecordFieldType.TIMESTAMP.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_UUID, RecordFieldType.UUID.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_JSON, RecordFieldType.STRING.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_JSONB, RecordFieldType.STRING.getDataType()),
                Arguments.of(PostgreSQLTypeMapper.OID_BYTEA, RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())),
                Arguments.of(OID_INT4_ARRAY, RecordFieldType.STRING.getDataType()),
                Arguments.of(OID_INTERVAL, RecordFieldType.STRING.getDataType())
        );
    }

    @ParameterizedTest
    @MethodSource("dataTypes")
    void testDataType(final int typeId, final DataType expected) {
        assertEquals(expected, mapper.getDataType(column(typeId)));
    }

    @Test
    void testNumericDataType() {
        assertEquals(RecordFieldType.DECIMAL.getDecimalDataType(10, 3), mapper.getDataType(column(PostgreSQLTypeMapper.OID_NUMERIC, numericTypeModifier(10, 3))));
        assertEquals(RecordFieldType.DECIMAL.getDecimalDataType(5, 0), mapper.getDataType(column(PostgreSQLTypeMapper.OID_NUMERIC, numericTypeModifier(5, 0))));
        assertEquals(RecordFieldType.DECIMAL.getDecimalDataType(5, 0), mapper.getDataType(column(PostgreSQLTypeMapper.OID_NUMERIC, numericTypeModifier(5, -2))));
        assertEquals(RecordFieldType.DECIMAL.getDecimalDataType(PostgreSQLTypeMapper.DEFAULT_NUMERIC_PRECISION, PostgreSQLTypeMapper.DEFAULT_NUMERIC_SCALE),
                mapper.getDataType(column(PostgreSQLTypeMapper.OID_NUMERIC)));
    }

    @Test
    void testConvertNumbers() {
        assertEquals(-7, mapper.convert(column(PostgreSQLTypeMapper.OID_INT2), "-7"));
        assertEquals(Integer.MAX_VALUE, mapper.convert(column(PostgreSQLTypeMapper.OID_INT4), "2147483647"));
        assertEquals(Long.MIN_VALUE, mapper.convert(column(PostgreSQLTypeMapper.OID_INT8), "-9223372036854775808"));
        assertEquals(1.5f, mapper.convert(column(PostgreSQLTypeMapper.OID_FLOAT4), "1.5"));
        assertThrows(UnsupportedValueException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_FLOAT4), "-Infinity"));
        assertEquals(1.0e-300, mapper.convert(column(PostgreSQLTypeMapper.OID_FLOAT8), "1e-300"));
        assertThrows(UnsupportedValueException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_FLOAT8), "NaN"));
        assertThrows(UnsupportedValueException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_FLOAT8), "Infinity"));
        assertEquals(new BigDecimal("-12345.678"), mapper.convert(column(PostgreSQLTypeMapper.OID_NUMERIC), "-12345.678"));
        assertThrows(UnsupportedValueException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_NUMERIC), "NaN"));
        assertThrows(UnsupportedValueException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_NUMERIC), "Infinity"));
    }

    @Test
    void testConvertBoolean() {
        assertEquals(Boolean.TRUE, mapper.convert(column(PostgreSQLTypeMapper.OID_BOOL), "t"));
        assertEquals(Boolean.FALSE, mapper.convert(column(PostgreSQLTypeMapper.OID_BOOL), "f"));
    }

    @Test
    void testConvertStrings() {
        assertEquals("plain", mapper.convert(column(PostgreSQLTypeMapper.OID_TEXT), "plain"));
        assertEquals("{1,2}", mapper.convert(column(OID_INT4_ARRAY), "{1,2}"));
        assertEquals("{\"k\": null}", mapper.convert(column(PostgreSQLTypeMapper.OID_JSONB), "{\"k\": null}"));
    }

    @Test
    void testConvertTemporal() {
        assertEquals(Date.valueOf(LocalDate.of(1970, 1, 1)), mapper.convert(column(PostgreSQLTypeMapper.OID_DATE), "1970-01-01"));
        assertThrows(UnsupportedValueException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_DATE), "infinity"));
        assertThrows(UnsupportedValueException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_DATE), "0044-03-15 BC"));

        assertEquals(Time.valueOf(LocalTime.of(0, 0)), mapper.convert(column(PostgreSQLTypeMapper.OID_TIME), "00:00:00"));
        assertEquals(new Time(Time.valueOf(LocalTime.of(23, 59, 59)).getTime() + 999), mapper.convert(column(PostgreSQLTypeMapper.OID_TIME), "23:59:59.999999"));

        assertEquals(Timestamp.valueOf(LocalDateTime.of(2026, 9, 17, 13, 45, 30)), mapper.convert(column(PostgreSQLTypeMapper.OID_TIMESTAMP), "2026-09-17 13:45:30"));
        assertEquals(Timestamp.valueOf(LocalDateTime.of(2026, 9, 17, 13, 45, 30, 120_000)), mapper.convert(column(PostgreSQLTypeMapper.OID_TIMESTAMP), "2026-09-17 13:45:30.00012"));
        assertThrows(UnsupportedValueException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_TIMESTAMP), "-infinity"));

        assertEquals(timestamp("2026-09-17T11:45:30Z"), mapper.convert(column(PostgreSQLTypeMapper.OID_TIMESTAMPTZ), "2026-09-17 13:45:30+02"));
        assertEquals(timestamp("2026-09-17T08:15:30.5Z"), mapper.convert(column(PostgreSQLTypeMapper.OID_TIMESTAMPTZ), "2026-09-17 13:45:30.5+05:30"));
        assertEquals(timestamp("2026-09-17T13:45:30Z"), mapper.convert(column(PostgreSQLTypeMapper.OID_TIMESTAMPTZ), "2026-09-17 13:45:30+00"));
        assertEquals(timestamp("2026-09-17T16:45:30Z"), mapper.convert(column(PostgreSQLTypeMapper.OID_TIMESTAMPTZ), "2026-09-17 13:45:30-03"));
        assertEquals(timestamp("2026-09-17T13:45:15Z"), mapper.convert(column(PostgreSQLTypeMapper.OID_TIMESTAMPTZ), "2026-09-17 13:45:30+00:00:15"));
        final UnsupportedValueException exception = assertThrows(UnsupportedValueException.class,
                () -> mapper.convert(column(PostgreSQLTypeMapper.OID_TIMESTAMPTZ), "infinity"));
        assertEquals("c", exception.getColumnName());
        assertEquals("infinity", exception.getText());
    }

    @Test
    void testConvertUuid() {
        assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                mapper.convert(column(PostgreSQLTypeMapper.OID_UUID), "123e4567-e89b-12d3-a456-426614174000"));
    }

    @Test
    void testConvertBytea() {
        assertArrayEquals(new Byte[]{}, (Byte[]) mapper.convert(column(PostgreSQLTypeMapper.OID_BYTEA), "\\x"));
        assertArrayEquals(new Byte[]{0, (byte) 0xAB, 0x10}, (Byte[]) mapper.convert(column(PostgreSQLTypeMapper.OID_BYTEA), "\\x00ab10"));
        assertArrayEquals(new Byte[]{(byte) 'a', 0, (byte) '\\', (byte) 0xFF, (byte) 'z'},
                (Byte[]) mapper.convert(column(PostgreSQLTypeMapper.OID_BYTEA), "a\\000\\\\\\377z"));
        assertThrows(PgOutputException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_BYTEA), "abc\\1"));
    }

    @Test
    void testConvertedValuesAreCompatibleWithDataTypes() {
        assertCompatible(PostgreSQLTypeMapper.OID_BOOL, "t");
        assertCompatible(PostgreSQLTypeMapper.OID_INT2, "1");
        assertCompatible(PostgreSQLTypeMapper.OID_INT8, "1");
        assertCompatible(PostgreSQLTypeMapper.OID_FLOAT4, "1.5");
        assertCompatible(PostgreSQLTypeMapper.OID_FLOAT8, "1.5");
        assertCompatible(PostgreSQLTypeMapper.OID_NUMERIC, "1.5");
        assertCompatible(PostgreSQLTypeMapper.OID_TEXT, "x");
        assertCompatible(PostgreSQLTypeMapper.OID_DATE, "2026-09-17");
        assertCompatible(PostgreSQLTypeMapper.OID_TIME, "13:45:30");
        assertCompatible(PostgreSQLTypeMapper.OID_TIMESTAMP, "2026-09-17 13:45:30");
        assertCompatible(PostgreSQLTypeMapper.OID_TIMESTAMPTZ, "2026-09-17 13:45:30+00");
        assertCompatible(PostgreSQLTypeMapper.OID_UUID, "123e4567-e89b-12d3-a456-426614174000");
        assertCompatible(PostgreSQLTypeMapper.OID_BYTEA, "\\x00");
        assertCompatible(OID_INTERVAL, "1 day");
    }

    private void assertCompatible(final int typeId, final String text) {
        final RelationColumn column = column(typeId);
        final Object value = mapper.convert(column, text);
        assertTrue(DataTypeUtils.isCompatibleDataType(value, mapper.getDataType(column)), () -> String.format("OID %d value %s", typeId, value));
    }

    private static Timestamp timestamp(final String instant) {
        return Timestamp.from(Instant.parse(instant));
    }

    private static RelationColumn column(final int typeId) {
        return column(typeId, NO_TYPE_MODIFIER);
    }

    private static RelationColumn column(final int typeId, final int typeModifier) {
        return new RelationColumn("c", false, typeId, typeModifier);
    }

    /**
     * Packs precision and scale the way PostgreSQL does for numeric type modifiers.
     */
    private static int numericTypeModifier(final int precision, final int scale) {
        return ((precision << 16) | (scale & 0x7FF)) + 4;
    }
}
