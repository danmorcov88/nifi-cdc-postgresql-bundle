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

import org.apache.nifi.cdc.postgresql.pgoutput.ColumnValue;
import org.apache.nifi.cdc.postgresql.pgoutput.PostgreSQLTypeMapper;
import org.apache.nifi.cdc.postgresql.pgoutput.RelationColumn;
import org.apache.nifi.cdc.postgresql.pgoutput.RelationMessage;
import org.apache.nifi.cdc.postgresql.pgoutput.TupleData;
import org.apache.nifi.cdc.postgresql.pgoutput.UnsupportedValueException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Builds change event records. Every event has the same top level fields; the {@code before} and {@code after}
 * images are records whose schema follows the columns of the table as described by the latest Relation message.
 * <p>
 * Values that cannot be represented in the mapped record type (for example {@code NaN} in a numeric column) are
 * written as null, and a warning is issued once per column so that the flow keeps running.
 */
public class ChangeEventRecordFactory {

    public static final String OPERATION = "operation";
    public static final String SCHEMA = "schema";
    public static final String TABLE = "table";
    public static final String LSN = "lsn";
    public static final String XID = "xid";
    public static final String COMMIT_TIMESTAMP = "commit_timestamp";
    public static final String BEFORE = "before";
    public static final String AFTER = "after";

    private final PostgreSQLTypeMapper typeMapper = new PostgreSQLTypeMapper();
    private final UnchangedToastStrategy unchangedToastStrategy;
    private final String unchangedToastPlaceholder;
    private final Consumer<String> warningHandler;
    private final Set<String> reportedColumns = new HashSet<>();

    /** Schemas keyed by relation description, so that a changed table definition yields a new schema. */
    private final Map<RelationMessage, EventSchemas> schemas = new HashMap<>();

    /**
     * @param unchangedToastStrategy how unchanged TOAST values are represented
     * @param unchangedToastPlaceholder placeholder for the {@link UnchangedToastStrategy#PLACEHOLDER} strategy
     * @param warningHandler receives a warning message the first time a column holds a value that cannot be represented
     */
    public ChangeEventRecordFactory(final UnchangedToastStrategy unchangedToastStrategy, final String unchangedToastPlaceholder,
                                    final Consumer<String> warningHandler) {
        this.unchangedToastStrategy = unchangedToastStrategy;
        this.unchangedToastPlaceholder = unchangedToastPlaceholder;
        this.warningHandler = warningHandler;
    }

    /**
     * @param relation table description
     * @return schema of the change event records of the table
     */
    public RecordSchema getEventSchema(final RelationMessage relation) {
        return getSchemas(relation).eventSchema();
    }

    /**
     * @param operation kind of change
     * @param relation table description matching the tuples
     * @param transaction transaction the change belongs to
     * @param lsn position of the change in the write-ahead log, in the {@code X/Y} notation
     * @param before old row image, or null when the server did not send one
     * @param after new row image, or null for DELETE and TRUNCATE
     * @return the change event record
     */
    public Record createRecord(final ChangeOperation operation, final RelationMessage relation, final TransactionInfo transaction, final String lsn,
                               final TupleData before, final TupleData after) {
        final EventSchemas eventSchemas = getSchemas(relation);
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(OPERATION, operation.getValue());
        values.put(SCHEMA, relation.namespace());
        values.put(TABLE, relation.name());
        values.put(LSN, lsn);
        values.put(XID, transaction.xid());
        values.put(COMMIT_TIMESTAMP, Timestamp.from(transaction.commitTime()));
        values.put(BEFORE, before == null ? null : createRowRecord(relation, eventSchemas.rowSchema(), before));
        values.put(AFTER, after == null ? null : createRowRecord(relation, eventSchemas.rowSchema(), after));
        return new MapRecord(eventSchemas.eventSchema(), values);
    }

    private Record createRowRecord(final RelationMessage relation, final RecordSchema rowSchema, final TupleData tuple) {
        final List<RelationColumn> columns = relation.columns();
        final List<ColumnValue> columnValues = tuple.columns();
        if (columnValues.size() != columns.size()) {
            throw new IllegalArgumentException(String.format("Tuple has %d columns but table %s.%s has %d columns",
                    columnValues.size(), relation.namespace(), relation.name(), columns.size()));
        }

        final Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            final RelationColumn column = columns.get(i);
            final ColumnValue value = columnValues.get(i);
            switch (value.kind()) {
                case NULL -> values.put(column.name(), null);
                case TEXT -> values.put(column.name(), convert(relation, column, value.text()));
                case UNCHANGED_TOAST -> putUnchangedToast(values, column);
            }
        }
        return new MapRecord(rowSchema, values);
    }

    private Object convert(final RelationMessage relation, final RelationColumn column, final String text) {
        try {
            return typeMapper.convert(column, text);
        } catch (final UnsupportedValueException e) {
            final String qualifiedColumn = String.format("%s.%s.%s", relation.namespace(), relation.name(), column.name());
            if (reportedColumns.add(qualifiedColumn)) {
                warningHandler.accept(String.format("Column %s holds the value [%s], which cannot be represented in the mapped record type; "
                        + "such values are written as null (reported once per column)", qualifiedColumn, e.getText()));
            }
            return null;
        }
    }

    private void putUnchangedToast(final Map<String, Object> values, final RelationColumn column) {
        switch (unchangedToastStrategy) {
            case NULL -> values.put(column.name(), null);
            case PLACEHOLDER -> {
                final boolean string = typeMapper.getDataType(column).getFieldType() == RecordFieldType.STRING;
                values.put(column.name(), string ? unchangedToastPlaceholder : null);
            }
            case OMIT -> {
                // the field is left unset
            }
        }
    }

    private EventSchemas getSchemas(final RelationMessage relation) {
        return schemas.computeIfAbsent(relation, this::createSchemas);
    }

    private EventSchemas createSchemas(final RelationMessage relation) {
        final List<RecordField> rowFields = new ArrayList<>(relation.columns().size());
        for (final RelationColumn column : relation.columns()) {
            rowFields.add(new RecordField(column.name(), typeMapper.getDataType(column), true));
        }
        final RecordSchema rowSchema = new SimpleRecordSchema(rowFields);

        final List<RecordField> eventFields = List.of(
                new RecordField(OPERATION, RecordFieldType.STRING.getDataType(), false),
                new RecordField(SCHEMA, RecordFieldType.STRING.getDataType(), false),
                new RecordField(TABLE, RecordFieldType.STRING.getDataType(), false),
                new RecordField(LSN, RecordFieldType.STRING.getDataType(), false),
                new RecordField(XID, RecordFieldType.LONG.getDataType(), false),
                new RecordField(COMMIT_TIMESTAMP, RecordFieldType.TIMESTAMP.getDataType(), false),
                new RecordField(BEFORE, RecordFieldType.RECORD.getRecordDataType(rowSchema), true),
                new RecordField(AFTER, RecordFieldType.RECORD.getRecordDataType(rowSchema), true)
        );
        return new EventSchemas(new SimpleRecordSchema(eventFields), rowSchema);
    }

    private record EventSchemas(RecordSchema eventSchema, RecordSchema rowSchema) {
    }
}
