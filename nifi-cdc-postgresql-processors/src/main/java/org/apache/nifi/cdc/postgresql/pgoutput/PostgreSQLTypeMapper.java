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

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Maps PostgreSQL data types, identified by their OID in Relation messages, to NiFi record data types, and converts
 * the text or binary representation sent by pgoutput into the matching Java values. Temporal values use the
 * {@code java.sql} classes, as the JDBC based record readers of NiFi do.
 * <p>
 * Types without a dedicated mapping (arrays, intervals, ranges, geometric types, user-defined types, ...) are
 * mapped to strings and keep their PostgreSQL text representation; in binary format their values become
 * hexadecimal strings. Special values that have no Java equivalent in the mapped type, or that the record writers
 * of NiFi cannot serialize ({@code NaN} and {@code Infinity} for numeric and floating point columns, {@code infinity}
 * and BC dates for temporal types), are reported through {@link UnsupportedValueException}.
 */
public class PostgreSQLTypeMapper {

    public static final int OID_BOOL = 16;
    public static final int OID_BYTEA = 17;
    public static final int OID_CHAR = 18;
    public static final int OID_NAME = 19;
    public static final int OID_INT8 = 20;
    public static final int OID_INT2 = 21;
    public static final int OID_INT4 = 23;
    public static final int OID_TEXT = 25;
    public static final int OID_JSON = 114;
    public static final int OID_FLOAT4 = 700;
    public static final int OID_FLOAT8 = 701;
    public static final int OID_BPCHAR = 1042;
    public static final int OID_VARCHAR = 1043;
    public static final int OID_DATE = 1082;
    public static final int OID_TIME = 1083;
    public static final int OID_TIMESTAMP = 1114;
    public static final int OID_TIMESTAMPTZ = 1184;
    public static final int OID_NUMERIC = 1700;
    public static final int OID_UUID = 2950;
    public static final int OID_JSONB = 3802;

    /** Precision and scale used for numeric columns declared without them. */
    public static final int DEFAULT_NUMERIC_PRECISION = 38;
    public static final int DEFAULT_NUMERIC_SCALE = 10;

    /** Size of the variable length header included in type modifiers. */
    private static final int VARHDRSZ = 4;
    private static final int NO_TYPE_MODIFIER = -1;

    private static final String BYTEA_HEX_PREFIX = "\\x";
    private static final int NANOS_PER_MILLI = 1_000_000;

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter();

    private static final DateTimeFormatter TIMESTAMPTZ_FORMATTER = new DateTimeFormatterBuilder()
            .append(TIMESTAMP_FORMATTER)
            .appendOffset("+HH:mm:ss", "Z")
            .toFormatter();

    private final BinaryValueDecoder binaryDecoder = new BinaryValueDecoder();

    /**
     * @param column column description from the Relation message
     * @return the NiFi data type for the column
     */
    public DataType getDataType(final RelationColumn column) {
        return switch (column.typeId()) {
            case OID_BOOL -> RecordFieldType.BOOLEAN.getDataType();
            case OID_INT2, OID_INT4 -> RecordFieldType.INT.getDataType();
            case OID_INT8 -> RecordFieldType.LONG.getDataType();
            case OID_FLOAT4 -> RecordFieldType.FLOAT.getDataType();
            case OID_FLOAT8 -> RecordFieldType.DOUBLE.getDataType();
            case OID_NUMERIC -> getNumericDataType(column.typeModifier());
            case OID_DATE -> RecordFieldType.DATE.getDataType();
            case OID_TIME -> RecordFieldType.TIME.getDataType();
            case OID_TIMESTAMP, OID_TIMESTAMPTZ -> RecordFieldType.TIMESTAMP.getDataType();
            case OID_UUID -> RecordFieldType.UUID.getDataType();
            case OID_BYTEA -> RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            default -> RecordFieldType.STRING.getDataType();
        };
    }

    /**
     * Convert the text representation of a value to the Java type matching {@link #getDataType(RelationColumn)}.
     *
     * @param column column description from the Relation message
     * @param text text representation as sent by pgoutput, not null
     * @return the converted value
     * @throws UnsupportedValueException when the value cannot be represented in the mapped record type
     */
    public Object convert(final RelationColumn column, final String text) {
        return switch (column.typeId()) {
            case OID_BOOL -> "t".equals(text);
            case OID_INT2, OID_INT4 -> Integer.valueOf(text);
            case OID_INT8 -> Long.valueOf(text);
            case OID_FLOAT4 -> convertFloat(column, text);
            case OID_FLOAT8 -> convertDouble(column, text);
            case OID_NUMERIC -> convertNumeric(column, text);
            case OID_DATE -> convertDate(column, text);
            case OID_TIME -> convertTime(column, text);
            case OID_TIMESTAMP -> convertTimestamp(column, text);
            case OID_TIMESTAMPTZ -> convertTimestampWithTimeZone(column, text);
            case OID_UUID -> UUID.fromString(text);
            case OID_BYTEA -> convertBytea(text);
            default -> text;
        };
    }

    /**
     * Convert the binary representation of a value to the Java type matching {@link #getDataType(RelationColumn)}.
     *
     * @param column column description from the Relation message
     * @param bytes binary representation as sent by pgoutput with the binary option, not null
     * @return the converted value, or a hexadecimal string for a type without binary mapping
     * @throws UnsupportedValueException when the value cannot be represented in the mapped record type
     */
    public Object convert(final RelationColumn column, final byte[] bytes) {
        return binaryDecoder.decode(column, bytes);
    }

    /**
     * @return whether binary values of the column type are decoded, rather than written as hexadecimal strings
     */
    public boolean hasBinaryMapping(final RelationColumn column) {
        return binaryDecoder.hasMapping(column.typeId());
    }

    private DataType getNumericDataType(final int typeModifier) {
        if (typeModifier == NO_TYPE_MODIFIER) {
            return RecordFieldType.DECIMAL.getDecimalDataType(DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE);
        }
        // Same unpacking as numeric_typmod_precision() and numeric_typmod_scale() in the PostgreSQL sources;
        // the scale is an 11-bit signed value since PostgreSQL 15, and a negative scale has no record equivalent.
        final int packed = typeModifier - VARHDRSZ;
        final int precision = (packed >> 16) & 0xFFFF;
        final int scale = ((packed & 0x7FF) ^ 1024) - 1024;
        return RecordFieldType.DECIMAL.getDecimalDataType(precision, Math.max(scale, 0));
    }

    /**
     * NaN and the infinities are valid float values in Java, but the record writers of NiFi cannot serialize them.
     */
    private Object convertFloat(final RelationColumn column, final String text) {
        final float value = Float.parseFloat(text);
        if (Float.isNaN(value) || Float.isInfinite(value)) {
            throw new UnsupportedValueException(column, text);
        }
        return value;
    }

    private Object convertDouble(final RelationColumn column, final String text) {
        final double value = Double.parseDouble(text);
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new UnsupportedValueException(column, text);
        }
        return value;
    }

    private Object convertNumeric(final RelationColumn column, final String text) {
        try {
            return new BigDecimal(text);
        } catch (final NumberFormatException e) {
            throw new UnsupportedValueException(column, text);
        }
    }

    private Object convertDate(final RelationColumn column, final String text) {
        try {
            return Date.valueOf(LocalDate.parse(text));
        } catch (final DateTimeParseException e) {
            throw new UnsupportedValueException(column, text);
        }
    }

    /**
     * Fractional seconds are kept up to millisecond precision, the precision of {@link Time}.
     */
    private Object convertTime(final RelationColumn column, final String text) {
        try {
            return toSqlTime(LocalTime.parse(text));
        } catch (final DateTimeParseException e) {
            throw new UnsupportedValueException(column, text);
        }
    }

    static Time toSqlTime(final LocalTime time) {
        return new Time(Time.valueOf(time).getTime() + time.getNano() / NANOS_PER_MILLI);
    }

    private Object convertTimestamp(final RelationColumn column, final String text) {
        try {
            return Timestamp.valueOf(LocalDateTime.parse(text, TIMESTAMP_FORMATTER));
        } catch (final DateTimeParseException e) {
            throw new UnsupportedValueException(column, text);
        }
    }

    private Object convertTimestampWithTimeZone(final RelationColumn column, final String text) {
        try {
            return Timestamp.from(OffsetDateTime.parse(text, TIMESTAMPTZ_FORMATTER).toInstant());
        } catch (final DateTimeParseException e) {
            throw new UnsupportedValueException(column, text);
        }
    }

    /**
     * Decode the {@code hex} output format ({@code \x0a0b}) and the legacy {@code escape} output format of bytea.
     * The result is boxed because NiFi represents binary values as arrays of {@link Byte}.
     */
    private Byte[] convertBytea(final String text) {
        return box(text.startsWith(BYTEA_HEX_PREFIX)
                ? HexFormat.of().parseHex(text, BYTEA_HEX_PREFIX.length(), text.length())
                : decodeByteaEscapeFormat(text));
    }

    static Byte[] box(final byte[] bytes) {
        final Byte[] boxed = new Byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            boxed[i] = bytes[i];
        }
        return boxed;
    }

    private byte[] decodeByteaEscapeFormat(final String text) {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream(text.length());
        int i = 0;
        while (i < text.length()) {
            final char c = text.charAt(i);
            if (c != '\\') {
                bytes.write(c);
                i++;
            } else if (i + 1 < text.length() && text.charAt(i + 1) == '\\') {
                bytes.write('\\');
                i += 2;
            } else if (i + 3 < text.length()) {
                bytes.write(Integer.parseInt(text.substring(i + 1, i + 4), 8));
                i += 4;
            } else {
                throw new PgOutputException(String.format("Invalid bytea escape sequence at position %d in [%s]", i, text));
            }
        }
        return bytes.toByteArray();
    }
}
