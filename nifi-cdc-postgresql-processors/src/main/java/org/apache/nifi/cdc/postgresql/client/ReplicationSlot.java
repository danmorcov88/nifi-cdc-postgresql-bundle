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

import org.postgresql.replication.LogSequenceNumber;

/**
 * State of a replication slot as reported by {@code pg_replication_slots}.
 *
 * @param name slot name
 * @param plugin output plugin the slot was created with
 * @param database database the slot belongs to
 * @param active whether another connection is currently streaming from the slot
 * @param confirmedFlushLsn position confirmed by the consumer, or null when the slot has never been consumed
 */
public record ReplicationSlot(String name, String plugin, String database, boolean active, LogSequenceNumber confirmedFlushLsn) {
}
