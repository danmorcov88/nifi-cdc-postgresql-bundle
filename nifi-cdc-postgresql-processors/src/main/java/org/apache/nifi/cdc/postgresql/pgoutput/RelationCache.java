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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Remembers the latest Relation message received for each table so that Insert, Update, Delete and Truncate
 * messages, which only carry the table OID, can be interpreted. The server sends a Relation message before the
 * first change of a table in a replication session and again whenever the table definition changes, so the cache
 * always reflects the columns of the changes that follow.
 */
public class RelationCache {

    private final Map<Integer, RelationMessage> relations = new HashMap<>();

    public void put(final RelationMessage relation) {
        relations.put(relation.relationId(), relation);
    }

    public Optional<RelationMessage> find(final int relationId) {
        return Optional.ofNullable(relations.get(relationId));
    }

    /**
     * @throws PgOutputException when no Relation message was received for the OID
     */
    public RelationMessage get(final int relationId) {
        final RelationMessage relation = relations.get(relationId);
        if (relation == null) {
            throw new PgOutputException(String.format("No Relation message received for table OID [%d]", relationId));
        }
        return relation;
    }

    public int size() {
        return relations.size();
    }

    public void clear() {
        relations.clear();
    }
}
