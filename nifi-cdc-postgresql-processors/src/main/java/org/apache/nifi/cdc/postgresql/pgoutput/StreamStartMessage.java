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
 * Start of a segment of a streamed (in-progress) transaction, protocol version 2. The messages that follow, up to the
 * Stream Stop message, belong to this transaction and carry the id of the (sub)transaction that made the change.
 *
 * @param xid id of the top-level transaction
 * @param firstSegment whether this is the first segment of the transaction
 */
public record StreamStartMessage(int xid, boolean firstSegment) implements PgOutputMessage {
}
