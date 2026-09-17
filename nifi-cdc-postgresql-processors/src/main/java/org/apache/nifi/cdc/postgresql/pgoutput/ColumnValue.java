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

import java.util.Arrays;
import java.util.Objects;

/**
 * Value of one column in a tuple.
 *
 * @param kind kind of value
 * @param text textual representation of the value, only present when the kind is {@link Kind#TEXT}
 * @param binary binary representation of the value as produced by the send function of its type, only present when
 *               the kind is {@link Kind#BINARY}
 */
public record ColumnValue(Kind kind, String text, byte[] binary) {

    public static final ColumnValue NULL = new ColumnValue(Kind.NULL, null, null);
    public static final ColumnValue UNCHANGED_TOAST = new ColumnValue(Kind.UNCHANGED_TOAST, null, null);

    public static ColumnValue text(final String text) {
        return new ColumnValue(Kind.TEXT, text, null);
    }

    public static ColumnValue binary(final byte[] binary) {
        return new ColumnValue(Kind.BINARY, null, binary);
    }

    public boolean isNull() {
        return kind == Kind.NULL;
    }

    public boolean isUnchangedToast() {
        return kind == Kind.UNCHANGED_TOAST;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ColumnValue that && kind == that.kind && Objects.equals(text, that.text) && Arrays.equals(binary, that.binary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, text, Arrays.hashCode(binary));
    }

    @Override
    public String toString() {
        return switch (kind) {
            case NULL, UNCHANGED_TOAST -> kind.name();
            case TEXT -> String.format("TEXT[%s]", text);
            case BINARY -> String.format("BINARY[%d bytes]", binary.length);
        };
    }

    public enum Kind {
        /** SQL NULL. */
        NULL,
        /** The value was not sent because it is stored out of line (TOAST) and did not change. */
        UNCHANGED_TOAST,
        /** The value in text format. */
        TEXT,
        /** The value in the binary send format of its type. */
        BINARY
    }
}
