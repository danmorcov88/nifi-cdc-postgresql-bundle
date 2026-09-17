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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.UUID;

import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_BOOL;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_BPCHAR;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_BYTEA;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_DATE;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_FLOAT4;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_FLOAT8;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_INT2;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_INT4;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_INT8;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_JSON;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_JSONB;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_NAME;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_NUMERIC;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_TEXT;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_TIME;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_TIMESTAMP;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_TIMESTAMPTZ;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_UUID;
import static org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper.OID_VARCHAR;

/**
 * Converts values in the binary send format of the built-in PostgreSQL types to the same Java values that
 * {@link PostgreSQLTypeMapper} produces from the text format. The formats are those of the send functions in the
 * PostgreSQL sources: network byte order integers and floats, microseconds since 2000-01-01 for timestamps, days since
 * 2000-01-01 for dates, and base 10000 digits for numeric. Values of types without a binary mapping are returned as
 * hexadecimal strings.
 */
final class BinaryValueDecoder {

    private static final LocalDate POSTGRESQL_EPOCH_DATE = LocalDate.of(2000, 1, 1);
    private static final LocalDateTime POSTGRESQL_EPOCH_DATE_TIME = POSTGRESQL_EPOCH_DATE.atStartOfDay();
    private static final Instant POSTGRESQL_EPOCH_INSTANT = POSTGRESQL_EPOCH_DATE_TIME.toInstant(ZoneOffset.UTC);

    private static final int DATE_NEGATIVE_INFINITY = Integer.MIN_VALUE;
    private static final int DATE_POSITIVE_INFINITY = Integer.MAX_VALUE;
    private static final long TIMESTAMP_NEGATIVE_INFINITY = Long.MIN_VALUE;
    private static final long TIMESTAMP_POSITIVE_INFINITY = Long.MAX_VALUE;
    private static final String NEGATIVE_INFINITY = "-infinity";
    private static final String POSITIVE_INFINITY = "infinity";

    private static final int NUMERIC_POSITIVE = 0x0000;
    private static final int NUMERIC_NEGATIVE = 0x4000;
    private static final int NUMERIC_NAN = 0xC000;
    private static final int NUMERIC_POSITIVE_INFINITY = 0xD000;
    private static final int NUMERIC_NEGATIVE_INFINITY = 0xF000;
    /** Decimal digits per base 10000 digit. */
    private static final int NUMERIC_DIGIT_WIDTH = 4;

    private static final byte JSONB_VERSION = 1;
    private static final long NANOS_PER_MICRO = 1_000L;
    private static final String HEX_PREFIX = "\\x";

    /**
     * @return whether values of the column type are decoded, rather than returned as hexadecimal strings
     */
    boolean hasMapping(final int typeId) {
        return switch (typeId) {
            case OID_BOOL, OID_INT2, OID_INT4, OID_INT8, OID_FLOAT4, OID_FLOAT8, OID_NUMERIC, OID_TEXT, OID_VARCHAR, OID_BPCHAR, OID_NAME,
                 OID_JSON, OID_JSONB, OID_BYTEA, OID_UUID, OID_DATE, OID_TIME, OID_TIMESTAMP, OID_TIMESTAMPTZ -> true;
            default -> false;
        };
    }

    /**
     * @param column column description from the Relation message
     * @param bytes binary representation as sent by pgoutput
     * @return the value in the Java type matching {@link PostgreSQLTypeMapper#getDataType(RelationColumn)}
     * @throws UnsupportedValueException when the value cannot be represented in the mapped record type
     * @throws PgOutputException when the bytes do not form a valid value of the type
     */
    Object decode(final RelationColumn column, final byte[] bytes) {
        return switch (column.typeId()) {
            case OID_BOOL -> fixed(column, bytes, 1).get() != 0;
            case OID_INT2 -> (int) fixed(column, bytes, 2).getShort();
            case OID_INT4 -> fixed(column, bytes, 4).getInt();
            case OID_INT8 -> fixed(column, bytes, 8).getLong();
            case OID_FLOAT4 -> decodeFloat(column, fixed(column, bytes, 4).getFloat());
            case OID_FLOAT8 -> decodeDouble(column, fixed(column, bytes, 8).getDouble());
            case OID_NUMERIC -> decodeNumeric(column, bytes);
            case OID_TEXT, OID_VARCHAR, OID_BPCHAR, OID_NAME, OID_JSON -> new String(bytes, StandardCharsets.UTF_8);
            case OID_JSONB -> decodeJsonb(column, bytes);
            case OID_BYTEA -> PostgreSQLTypeMapper.box(bytes);
            case OID_UUID -> decodeUuid(column, bytes);
            case OID_DATE -> decodeDate(column, fixed(column, bytes, 4).getInt());
            case OID_TIME -> decodeTime(column, fixed(column, bytes, 8).getLong());
            case OID_TIMESTAMP -> decodeTimestamp(column, fixed(column, bytes, 8).getLong());
            case OID_TIMESTAMPTZ -> decodeTimestampWithTimeZone(column, fixed(column, bytes, 8).getLong());
            default -> HEX_PREFIX + HexFormat.of().formatHex(bytes);
        };
    }

    private static ByteBuffer fixed(final RelationColumn column, final byte[] bytes, final int expectedLength) {
        if (bytes.length != expectedLength) {
            throw new PgOutputException(String.format("Binary value of column [%s] (type OID %d) has %d bytes but %d are expected",
                    column.name(), column.typeId(), bytes.length, expectedLength));
        }
        return ByteBuffer.wrap(bytes);
    }

    private static Object decodeFloat(final RelationColumn column, final float value) {
        if (Float.isNaN(value) || Float.isInfinite(value)) {
            throw new UnsupportedValueException(column, Float.toString(value));
        }
        return value;
    }

    private static Object decodeDouble(final RelationColumn column, final double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new UnsupportedValueException(column, Double.toString(value));
        }
        return value;
    }

    /**
     * Layout of numeric_send: number of digits, weight of the first digit in base 10000, sign, display scale, digits.
     */
    private static Object decodeNumeric(final RelationColumn column, final byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (bytes.length < 8) {
            throw new PgOutputException(String.format("Binary numeric value of column [%s] is truncated", column.name()));
        }
        final int digitCount = Short.toUnsignedInt(buffer.getShort());
        final int weight = buffer.getShort();
        final int sign = Short.toUnsignedInt(buffer.getShort());
        final int scale = Short.toUnsignedInt(buffer.getShort());
        if (buffer.remaining() != digitCount * 2) {
            throw new PgOutputException(String.format("Binary numeric value of column [%s] declares %d digits but holds %d bytes", column.name(),
                    digitCount, buffer.remaining()));
        }
        switch (sign) {
            case NUMERIC_NAN -> throw new UnsupportedValueException(column, "NaN");
            case NUMERIC_POSITIVE_INFINITY -> throw new UnsupportedValueException(column, "Infinity");
            case NUMERIC_NEGATIVE_INFINITY -> throw new UnsupportedValueException(column, "-Infinity");
            case NUMERIC_POSITIVE, NUMERIC_NEGATIVE -> {
                // regular value
            }
            default -> throw new PgOutputException(String.format("Binary numeric value of column [%s] has an unknown sign [0x%04X]", column.name(), sign));
        }

        final StringBuilder digits = new StringBuilder(digitCount * NUMERIC_DIGIT_WIDTH + 1);
        for (int i = 0; i < digitCount; i++) {
            final int digit = buffer.getShort();
            if (digit < 0 || digit >= 10_000) {
                throw new PgOutputException(String.format("Binary numeric value of column [%s] has an invalid digit [%d]", column.name(), digit));
            }
            digits.append((char) ('0' + digit / 1000)).append((char) ('0' + digit / 100 % 10)).append((char) ('0' + digit / 10 % 10)).append((char) ('0' + digit % 10));
        }
        // the digits are the coefficient of 10000^(weight - digitCount + 1); the display scale gives the fraction digits
        final BigDecimal magnitude = digitCount == 0 ? BigDecimal.ZERO
                : new BigDecimal(new BigInteger(digits.toString()), -(weight - digitCount + 1) * NUMERIC_DIGIT_WIDTH);
        final BigDecimal value = magnitude.setScale(scale, RoundingMode.HALF_UP);
        return sign == NUMERIC_NEGATIVE ? value.negate() : value;
    }

    private static Object decodeJsonb(final RelationColumn column, final byte[] bytes) {
        if (bytes.length == 0 || bytes[0] != JSONB_VERSION) {
            throw new PgOutputException(String.format("Binary jsonb value of column [%s] has an unsupported version", column.name()));
        }
        return new String(bytes, 1, bytes.length - 1, StandardCharsets.UTF_8);
    }

    private static Object decodeUuid(final RelationColumn column, final byte[] bytes) {
        final ByteBuffer buffer = fixed(column, bytes, 16);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static Object decodeDate(final RelationColumn column, final int days) {
        if (days == DATE_NEGATIVE_INFINITY) {
            throw new UnsupportedValueException(column, NEGATIVE_INFINITY);
        }
        if (days == DATE_POSITIVE_INFINITY) {
            throw new UnsupportedValueException(column, POSITIVE_INFINITY);
        }
        final LocalDate date = POSTGRESQL_EPOCH_DATE.plusDays(days);
        if (date.getYear() < 1) {
            throw new UnsupportedValueException(column, date.withYear(1 - date.getYear()) + " BC");
        }
        return Date.valueOf(date);
    }

    private static Object decodeTime(final RelationColumn column, final long micros) {
        try {
            return PostgreSQLTypeMapper.toSqlTime(LocalTime.ofNanoOfDay(micros * NANOS_PER_MICRO));
        } catch (final DateTimeException e) {
            throw new UnsupportedValueException(column, Long.toString(micros));
        }
    }

    private static Object decodeTimestamp(final RelationColumn column, final long micros) {
        checkTimestampBounds(column, micros);
        final LocalDateTime dateTime = POSTGRESQL_EPOCH_DATE_TIME.plus(micros, ChronoUnit.MICROS);
        if (dateTime.getYear() < 1) {
            throw new UnsupportedValueException(column, dateTime.withYear(1 - dateTime.getYear()) + " BC");
        }
        return Timestamp.valueOf(dateTime);
    }

    private static Object decodeTimestampWithTimeZone(final RelationColumn column, final long micros) {
        checkTimestampBounds(column, micros);
        final Instant instant = POSTGRESQL_EPOCH_INSTANT.plus(micros, ChronoUnit.MICROS);
        if (instant.atOffset(ZoneOffset.UTC).getYear() < 1) {
            throw new UnsupportedValueException(column, instant + " BC");
        }
        return Timestamp.from(instant);
    }

    private static void checkTimestampBounds(final RelationColumn column, final long micros) {
        if (micros == TIMESTAMP_NEGATIVE_INFINITY) {
            throw new UnsupportedValueException(column, NEGATIVE_INFINITY);
        }
        if (micros == TIMESTAMP_POSITIVE_INFINITY) {
            throw new UnsupportedValueException(column, POSITIVE_INFINITY);
        }
    }
}
