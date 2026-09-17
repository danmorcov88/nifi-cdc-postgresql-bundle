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
package org.apache.nifi.cdc.postgresql.processors;

import org.apache.nifi.components.DescribedValue;

/**
 * When a batch of change events is completed and its FlowFiles are transferred.
 */
public enum BatchStrategy implements DescribedValue {
    MAX_EVENTS("Max Events Per FlowFile", "Events of consecutive transactions are combined until the configured number of events is reached, "
            + "the configured wait time has elapsed or no further change is pending. Batches are always completed at a transaction boundary."),
    ONE_TRANSACTION("One Transaction Per FlowFile", "Every committed transaction is written on its own, which yields the smallest FlowFiles "
            + "and the lowest latency.");

    private final String displayName;
    private final String description;

    BatchStrategy(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
