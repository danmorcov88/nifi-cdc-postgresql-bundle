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
 * An updated row.
 *
 * @param relationId OID of the table
 * @param oldTupleKind kind of the old tuple, or null when no old tuple was sent
 * @param oldTuple the row before the update, or null when the replica identity did not require sending it
 * @param newTuple the row after the update
 */
public record UpdateMessage(int relationId, OldTupleKind oldTupleKind, TupleData oldTuple, TupleData newTuple) implements PgOutputMessage {

    public boolean hasOldTuple() {
        return oldTuple != null;
    }
}
