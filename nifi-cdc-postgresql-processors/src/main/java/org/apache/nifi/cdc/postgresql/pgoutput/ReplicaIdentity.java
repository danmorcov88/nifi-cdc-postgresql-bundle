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
 * Replica identity setting of a table, which determines what the old tuple of UPDATE and DELETE messages contains.
 */
public enum ReplicaIdentity {
    /** Primary key columns are sent when a key column changed or on DELETE. */
    DEFAULT('d'),
    /** No old tuple is ever sent. */
    NOTHING('n'),
    /** The whole old row is sent. */
    FULL('f'),
    /** The columns of the configured index are sent. */
    INDEX('i');

    private final char code;

    ReplicaIdentity(final char code) {
        this.code = code;
    }

    public char getCode() {
        return code;
    }

    public static ReplicaIdentity fromCode(final byte code) {
        for (final ReplicaIdentity identity : values()) {
            if (identity.code == code) {
                return identity;
            }
        }
        throw new PgOutputException(String.format("Unknown replica identity code [%c]", (char) code));
    }
}
