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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationCacheTest {

    private static final int RELATION_ID = 16401;

    private final RelationCache cache = new RelationCache();

    @Test
    void testUnknownRelation() {
        assertTrue(cache.find(RELATION_ID).isEmpty());

        final PgOutputException exception = assertThrows(PgOutputException.class, () -> cache.get(RELATION_ID));

        assertTrue(exception.getMessage().contains(Integer.toString(RELATION_ID)));
    }

    @Test
    void testLatestRelationMessageWins() {
        final RelationMessage original = relation(List.of(new RelationColumn("id", true, PostgreSQLTypeMapper.OID_INT4, -1)));
        final RelationMessage altered = relation(List.of(
                new RelationColumn("id", true, PostgreSQLTypeMapper.OID_INT4, -1),
                new RelationColumn("added", false, PostgreSQLTypeMapper.OID_TEXT, -1)
        ));

        cache.put(original);
        assertEquals(original, cache.get(RELATION_ID));
        assertEquals(1, cache.size());

        cache.put(altered);
        assertEquals(altered, cache.get(RELATION_ID));
        assertEquals(altered, cache.find(RELATION_ID).orElseThrow());
        assertEquals(1, cache.size());
    }

    @Test
    void testClear() {
        cache.put(relation(List.of()));

        cache.clear();

        assertEquals(0, cache.size());
        assertTrue(cache.find(RELATION_ID).isEmpty());
    }

    private static RelationMessage relation(final List<RelationColumn> columns) {
        return new RelationMessage(RELATION_ID, "lab", "customers", ReplicaIdentity.DEFAULT, columns);
    }
}
