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

/**
 * Value of one column in a tuple.
 *
 * @param kind kind of value
 * @param text textual representation of the value, only present when the kind is {@link Kind#TEXT}
 */
public record ColumnValue(Kind kind, String text) {

    public static final ColumnValue NULL = new ColumnValue(Kind.NULL, null);
    public static final ColumnValue UNCHANGED_TOAST = new ColumnValue(Kind.UNCHANGED_TOAST, null);

    public static ColumnValue text(final String text) {
        return new ColumnValue(Kind.TEXT, text);
    }

    public boolean isNull() {
        return kind == Kind.NULL;
    }

    public boolean isUnchangedToast() {
        return kind == Kind.UNCHANGED_TOAST;
    }

    public enum Kind {
        /** SQL NULL. */
        NULL,
        /** The value was not sent because it is stored out of line (TOAST) and did not change. */
        UNCHANGED_TOAST,
        /** The value in text format. */
        TEXT
    }
}
