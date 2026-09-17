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
 * Thrown when a value has no representation in the record data type the column is mapped to, for example
 * {@code NaN} in a numeric column or {@code infinity} in a date column.
 */
public class UnsupportedValueException extends IllegalArgumentException {

    private final String columnName;
    private final String text;

    public UnsupportedValueException(final RelationColumn column, final String text) {
        super(String.format("Value [%s] of column [%s] (type OID %d) has no representation in the mapped record type", text, column.name(), column.typeId()));
        this.columnName = column.name();
        this.text = text;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getText() {
        return text;
    }
}
