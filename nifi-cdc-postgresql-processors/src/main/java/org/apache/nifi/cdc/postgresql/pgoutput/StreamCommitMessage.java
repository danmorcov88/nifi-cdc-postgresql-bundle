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

import java.time.Instant;

/**
 * Commit of a streamed transaction, protocol version 2; sent after the last segment of the transaction.
 *
 * @param xid id of the top-level transaction
 * @param commitLsn position of the commit record
 * @param endLsn position right after the commit record
 * @param commitTime commit timestamp
 */
public record StreamCommitMessage(int xid, long commitLsn, long endLsn, Instant commitTime) implements PgOutputMessage {
}
