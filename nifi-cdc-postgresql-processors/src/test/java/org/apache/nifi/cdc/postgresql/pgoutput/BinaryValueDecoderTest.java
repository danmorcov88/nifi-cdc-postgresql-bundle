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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HexFormat;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BinaryValueDecoderTest {

    private static final int NO_TYPE_MODIFIER = -1;
    private static final int OID_INTERVAL = 1186;
    private static final int NUMERIC_POSITIVE = 0x0000;
    private static final int NUMERIC_NEGATIVE = 0x4000;

    private final PostgreSQLTypeMapper mapper = new PostgreSQLTypeMapper();

    @Test
    void testIntegers() {
        assertEquals(Short.MIN_VALUE + 0, mapper.convert(column(PostgreSQLTypeMapper.OID_INT2), bytes(2, Short.MIN_VALUE)));
        assertEquals(-1, mapper.convert(column(PostgreSQLTypeMapper.OID_INT4), bytes(4, -1)));
        assertEquals(Long.MIN_VALUE, mapper.convert(column(PostgreSQLTypeMapper.OID_INT8), bytes(8, Long.MIN_VALUE)));
        assertThrows(PgOutputException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_INT4), bytes(8, 1L)));
    }

    @Test
    void testFloatingPoint() {
        assertEquals(-1.5f, mapper.convert(column(PostgreSQLTypeMapper.OID_FLOAT4), ByteBuffer.allocate(4).putFloat(-1.5f).array()));
        assertEquals(Double.MAX_VALUE, mapper.convert(column(PostgreSQLTypeMapper.OID_FLOAT8), ByteBuffer.allocate(8).putDouble(Double.MAX_VALUE).array()));
        assertEquals("NaN", assertThrows(UnsupportedValueException.class,
                () -> mapper.convert(column(PostgreSQLTypeMapper.OID_FLOAT4), ByteBuffer.allocate(4).putFloat(Float.NaN).array())).getText());
        assertEquals("-Infinity", assertThrows(UnsupportedValueException.class,
                () -> mapper.convert(column(PostgreSQLTypeMapper.OID_FLOAT8), ByteBuffer.allocate(8).putDouble(Double.NEGATIVE_INFINITY).array())).getText());
    }

    @Test
    void testBooleanAndStrings() {
        assertEquals(Boolean.TRUE, mapper.convert(column(PostgreSQLTypeMapper.OID_BOOL), new byte[]{1}));
        assertEquals(Boolean.FALSE, mapper.convert(column(PostgreSQLTypeMapper.OID_BOOL), new byte[]{0}));
        assertEquals("ünïcödé", mapper.convert(column(PostgreSQLTypeMapper.OID_TEXT), "ünïcödé".getBytes(StandardCharsets.UTF_8)));
        assertEquals("ab   ", mapper.convert(column(PostgreSQLTypeMapper.OID_BPCHAR), "ab   ".getBytes(StandardCharsets.UTF_8)));
        assertEquals("", mapper.convert(column(PostgreSQLTypeMapper.OID_VARCHAR), new byte[0]));
        assertEquals("{\"a\": 1}", mapper.convert(column(PostgreSQLTypeMapper.OID_JSON), "{\"a\": 1}".getBytes(StandardCharsets.UTF_8)));
        assertEquals("[1]", mapper.convert(column(PostgreSQLTypeMapper.OID_JSONB), new byte[]{1, '[', '1', ']'}));
        assertThrows(PgOutputException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_JSONB), new byte[]{2, '[', ']'}));
        assertThrows(PgOutputException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_JSONB), new byte[0]));
    }

    @Test
    void testByteaAndUuid() {
        assertArrayEquals(new Byte[]{0, (byte) 0xFF, 0x10}, (Byte[]) mapper.convert(column(PostgreSQLTypeMapper.OID_BYTEA), new byte[]{0, (byte) 0xFF, 0x10}));
        assertArrayEquals(new Byte[0], (Byte[]) mapper.convert(column(PostgreSQLTypeMapper.OID_BYTEA), new byte[0]));

        final UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        final byte[] bytes = ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array();
        assertEquals(uuid, mapper.convert(column(PostgreSQLTypeMapper.OID_UUID), bytes));
        assertThrows(PgOutputException.class, () -> mapper.convert(column(PostgreSQLTypeMapper.OID_UUID), new byte[15]));
    }

    @Test
    void testDate() {
        final RelationColumn column = column(PostgreSQLTypeMapper.OID_DATE);
        assertEquals(Date.valueOf(LocalDate.of(2000, 1, 1)), mapper.convert(column, bytes(4, 0)));
        assertEquals(Date.valueOf(LocalDate.of(1999, 12, 31)), mapper.convert(column, bytes(4, -1)));
        assertEquals(Date.valueOf(LocalDate.of(2026, 9, 17)), mapper.convert(column, bytes(4, (int) LocalDate.of(2026, 9, 17).toEpochDay() - (int) LocalDate.of(2000, 1, 1).toEpochDay())));
        assertEquals("infinity", assertThrows(UnsupportedValueException.class, () -> mapper.convert(column, bytes(4, Integer.MAX_VALUE))).getText());
        assertEquals("-infinity", assertThrows(UnsupportedValueException.class, () -> mapper.convert(column, bytes(4, Integer.MIN_VALUE))).getText());
        final int daysToYearZero = (int) (LocalDate.of(0, 12, 31).toEpochDay() - LocalDate.of(2000, 1, 1).toEpochDay());
        assertEquals("0001-12-31 BC", assertThrows(UnsupportedValueException.class, () -> mapper.convert(column, bytes(4, daysToYearZero))).getText());
    }

    @Test
    void testTime() {
        final RelationColumn column = column(PostgreSQLTypeMapper.OID_TIME);
        assertEquals(Time.valueOf(LocalTime.MIDNIGHT), mapper.convert(column, bytes(8, 0L)));
        final long micros = LocalTime.of(13, 45, 30, 123_456_000).toNanoOfDay() / 1_000;
        assertEquals(new Time(Time.valueOf(LocalTime.of(13, 45, 30)).getTime() + 123), mapper.convert(column, bytes(8, micros)));
        assertThrows(UnsupportedValueException.class, () -> mapper.convert(column, bytes(8, 24L * 3600 * 1_000_000)), "24:00:00 has no java.sql.Time");
    }

    @Test
    void testTimestamps() {
        final RelationColumn timestamp = column(PostgreSQLTypeMapper.OID_TIMESTAMP);
        assertEquals(Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 0, 0)), mapper.convert(timestamp, bytes(8, 0L)));
        assertEquals(Timestamp.valueOf(LocalDateTime.of(1999, 12, 31, 23, 59, 59, 999_999_000)), mapper.convert(timestamp, bytes(8, -1L)));
        assertEquals("infinity", assertThrows(UnsupportedValueException.class, () -> mapper.convert(timestamp, bytes(8, Long.MAX_VALUE))).getText());
        assertEquals("-infinity", assertThrows(UnsupportedValueException.class, () -> mapper.convert(timestamp, bytes(8, Long.MIN_VALUE))).getText());
        final long microsToYearZero = java.time.Duration.between(LocalDateTime.of(2000, 1, 1, 0, 0), LocalDateTime.of(0, 1, 1, 0, 0)).toMillis() * 1_000;
        assertTrue(assertThrows(UnsupportedValueException.class, () -> mapper.convert(timestamp, bytes(8, microsToYearZero))).getText().endsWith(" BC"));

        final RelationColumn timestamptz = column(PostgreSQLTypeMapper.OID_TIMESTAMPTZ);
        final Instant instant = Instant.parse("2026-09-17T11:45:30.5Z");
        final long micros = java.time.Duration.between(Instant.parse("2000-01-01T00:00:00Z"), instant).toNanos() / 1_000;
        assertEquals(Timestamp.from(instant), mapper.convert(timestamptz, bytes(8, micros)));
        assertEquals("infinity", assertThrows(UnsupportedValueException.class, () -> mapper.convert(timestamptz, bytes(8, Long.MAX_VALUE))).getText());
    }

    @Test
    void testNumeric() {
        final RelationColumn column = column(PostgreSQLTypeMapper.OID_NUMERIC);
        assertEquals(new BigDecimal("12345.678"), mapper.convert(column, numeric(1, NUMERIC_POSITIVE, 3, 1, 2345, 6780)));
        assertEquals(new BigDecimal("0.00"), mapper.convert(column, numeric(0, NUMERIC_POSITIVE, 2)));
        assertEquals(BigDecimal.ZERO, mapper.convert(column, numeric(0, NUMERIC_POSITIVE, 0)));
        assertEquals(new BigDecimal("-1.0001"), mapper.convert(column, numeric(0, NUMERIC_NEGATIVE, 4, 1, 1)));
        assertEquals(new BigDecimal("0.0001"), mapper.convert(column, numeric(-1, NUMERIC_POSITIVE, 4, 1)));
        assertEquals(new BigDecimal("100000000"), mapper.convert(column, numeric(2, NUMERIC_POSITIVE, 0, 1)));
        assertEquals(new BigDecimal("1.50"), mapper.convert(column, numeric(0, NUMERIC_POSITIVE, 2, 1, 5000)));
        assertEquals(new BigDecimal("0.1234567890123456789"), mapper.convert(column, numeric(-1, NUMERIC_POSITIVE, 19, 1234, 5678, 9012, 3456, 7890)));
        assertEquals(new BigDecimal("-99999999999999999999.99"), mapper.convert(column, numeric(4, NUMERIC_NEGATIVE, 2, 9999, 9999, 9999, 9999, 9999, 9900)));

        assertEquals("NaN", assertThrows(UnsupportedValueException.class, () -> mapper.convert(column, numeric(0, 0xC000, 0))).getText());
        assertEquals("Infinity", assertThrows(UnsupportedValueException.class, () -> mapper.convert(column, numeric(0, 0xD000, 0))).getText());
        assertEquals("-Infinity", assertThrows(UnsupportedValueException.class, () -> mapper.convert(column, numeric(0, 0xF000, 0))).getText());
        assertThrows(PgOutputException.class, () -> mapper.convert(column, numeric(0, 0x1000, 0)), "unknown sign");
        assertThrows(PgOutputException.class, () -> mapper.convert(column, numeric(0, NUMERIC_POSITIVE, 0, 10_000)), "digit out of range");
        assertThrows(PgOutputException.class, () -> mapper.convert(column, new byte[]{0, 1, 0, 0, 0, 0, 0, 0}), "declared digits missing");
        assertThrows(PgOutputException.class, () -> mapper.convert(column, new byte[3]), "truncated header");
    }

    @Test
    void testTypesWithoutBinaryMappingBecomeHexStrings() {
        final RelationColumn column = column(OID_INTERVAL);
        assertFalse(mapper.hasBinaryMapping(column));
        assertTrue(mapper.hasBinaryMapping(column(PostgreSQLTypeMapper.OID_NUMERIC)));
        // interval_send: 1 day 02:03:04 as microseconds, days and months
        assertEquals("\\x00000001b81ee6000000000100000000", mapper.convert(column, HexFormat.of().parseHex("00000001b81ee6000000000100000000")));
        assertEquals("\\x0102ff", mapper.convert(column, new byte[]{1, 2, (byte) 0xFF}));
        assertEquals("\\x", mapper.convert(column, new byte[0]));
    }

    private static RelationColumn column(final int typeId) {
        return new RelationColumn("c", false, typeId, NO_TYPE_MODIFIER);
    }

    private static byte[] bytes(final int length, final long value) {
        final ByteBuffer buffer = ByteBuffer.allocate(length);
        switch (length) {
            case 2 -> buffer.putShort((short) value);
            case 4 -> buffer.putInt((int) value);
            default -> buffer.putLong(value);
        }
        return buffer.array();
    }

    /**
     * Builds a value in the numeric_send layout.
     */
    private static byte[] numeric(final int weight, final int sign, final int scale, final int... digits) {
        final ByteBuffer buffer = ByteBuffer.allocate(8 + digits.length * 2);
        buffer.putShort((short) digits.length).putShort((short) weight).putShort((short) sign).putShort((short) scale);
        for (final int digit : digits) {
            buffer.putShort((short) digit);
        }
        return buffer.array();
    }
}
