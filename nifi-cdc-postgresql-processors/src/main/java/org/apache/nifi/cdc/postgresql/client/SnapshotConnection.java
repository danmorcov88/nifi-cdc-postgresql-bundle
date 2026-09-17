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
package org.apache.nifi.cdc.postgresql.client;

import org.apache.nifi.cdc.postgresql.pgoutput.RelationMessage;

import java.sql.SQLException;
import java.util.List;

/**
 * A transaction that sees the database as it was when a replication slot was created, used to read the existing rows
 * of the published tables before streaming starts.
 */
public interface SnapshotConnection extends AutoCloseable {

    /**
     * @param publicationName publication whose tables are listed
     * @return the tables of the publication, described the way the Relation messages of pgoutput describe them
     */
    List<RelationMessage> listTables(String publicationName) throws SQLException;

    /**
     * Start reading all rows of a table. Values are read in their text representation, as the output function of
     * their type prints them, which is what pgoutput sends.
     *
     * @param table table to read, as returned by {@link #listTables(String)}
     * @param fetchSize number of rows fetched from the server at a time
     */
    SnapshotCursor openCursor(RelationMessage table, int fetchSize) throws SQLException;

    /**
     * End the transaction and close the connection.
     */
    @Override
    void close() throws SQLException;
}
