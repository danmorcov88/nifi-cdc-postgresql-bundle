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
package org.apache.nifi.cdc.postgresql.event;

import org.apache.nifi.components.DescribedValue;

/**
 * How a column whose value was not sent by the server, because it is stored out of line (TOAST) and did not change,
 * is represented in the {@code after} image of an UPDATE event.
 */
public enum UnchangedToastStrategy implements DescribedValue {
    NULL("Null", "The field is set to null. The consumer cannot tell an unchanged value from a value set to null."),
    PLACEHOLDER("Placeholder", "The field is set to the configured placeholder text for string columns, and to null for other column types."),
    OMIT("Omit Field", "The field is left out of the record, so Record Writers that suppress missing values do not write it.");

    private final String displayName;
    private final String description;

    UnchangedToastStrategy(final String displayName, final String description) {
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
