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

import org.apache.nifi.cdc.postgresql.event.ChangeEventRecordFactory;
import org.apache.nifi.cdc.postgresql.event.TransactionInfo;
import org.apache.nifi.cdc.postgresql.pgoutput.RelationMessage;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.postgresql.replication.LogSequenceNumber;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Change events of one or more committed transactions, written as one FlowFile per table. A table gets a second
 * FlowFile in the same batch only when its definition changed in the middle of the batch. All FlowFiles of a batch
 * are transferred in the same session, so a transaction is never split across session commits.
 */
class EventBatch {

    private final ProcessSession session;
    private final RecordSetWriterFactory writerFactory;
    private final ChangeEventRecordFactory recordFactory;
    private final ComponentLog logger;
    private final String transitUri;

    private final Map<Integer, TableWriter> openWriters = new LinkedHashMap<>();
    private final List<TableWriter> completedWriters = new ArrayList<>();
    private int eventCount;

    EventBatch(final ProcessSession session, final RecordSetWriterFactory writerFactory, final ChangeEventRecordFactory recordFactory,
               final ComponentLog logger, final String transitUri) {
        this.session = session;
        this.writerFactory = writerFactory;
        this.recordFactory = recordFactory;
        this.logger = logger;
        this.transitUri = transitUri;
    }

    boolean isEmpty() {
        return eventCount == 0;
    }

    int getEventCount() {
        return eventCount;
    }

    /**
     * Close the FlowFile of a table whose definition changed, so that the following events get a FlowFile with the new schema.
     */
    void relationChanged(final RelationMessage relation) throws IOException {
        final TableWriter writer = openWriters.get(relation.relationId());
        if (writer != null && !writer.relation.equals(relation)) {
            openWriters.remove(relation.relationId());
            writer.finish();
            completedWriters.add(writer);
        }
    }

    void write(final RelationMessage relation, final Record record, final LogSequenceNumber lsn, final TransactionInfo transaction) throws IOException {
        TableWriter writer = openWriters.get(relation.relationId());
        if (writer == null) {
            writer = new TableWriter(relation);
            openWriters.put(relation.relationId(), writer);
        }
        writer.write(record, lsn, transaction);
        eventCount++;
    }

    /**
     * Complete all FlowFiles and transfer them to the relationship.
     */
    void transfer(final Relationship relationship) throws IOException {
        for (final TableWriter writer : openWriters.values()) {
            writer.finish();
            completedWriters.add(writer);
        }
        openWriters.clear();

        for (final TableWriter writer : completedWriters) {
            session.getProvenanceReporter().receive(writer.flowFile, transitUri);
            session.transfer(writer.flowFile, relationship);
        }
        completedWriters.clear();
    }

    /**
     * Discard everything written so far. Never throws, so that callers can rely on it in error handling.
     */
    void rollback() {
        for (final TableWriter writer : openWriters.values()) {
            writer.abort();
        }
        openWriters.clear();
        completedWriters.clear();
        try {
            session.rollback();
        } catch (final Exception e) {
            logger.warn("Rolling back session failed", e);
        }
    }

    private class TableWriter {

        private final RelationMessage relation;
        private final OutputStream outputStream;
        private final RecordSetWriter writer;
        private FlowFile flowFile;
        private int count;
        private LogSequenceNumber lastLsn;
        private TransactionInfo lastTransaction;

        private TableWriter(final RelationMessage relation) throws IOException {
            this.relation = relation;
            flowFile = session.create();
            final Map<String, String> attributes = Map.of(
                    CaptureChangePostgreSQL.ATTRIBUTE_SCHEMA, relation.namespace(),
                    CaptureChangePostgreSQL.ATTRIBUTE_TABLE, relation.name());
            flowFile = session.putAllAttributes(flowFile, attributes);
            outputStream = session.write(flowFile);
            try {
                final RecordSchema schema = writerFactory.getSchema(attributes, recordFactory.getEventSchema(relation));
                writer = writerFactory.createWriter(logger, schema, outputStream, flowFile);
                writer.beginRecordSet();
            } catch (final Exception e) {
                outputStream.close();
                throw new IOException(String.format("Record Writer initialization failed for table %s.%s", relation.namespace(), relation.name()), e);
            }
        }

        private void write(final Record record, final LogSequenceNumber lsn, final TransactionInfo transaction) throws IOException {
            writer.write(record);
            count++;
            lastLsn = lsn;
            lastTransaction = transaction;
        }

        private void finish() throws IOException {
            final WriteResult result;
            // resources close in reverse order: the writer must flush into the stream before the stream is closed
            try (outputStream; writer) {
                result = writer.finishRecordSet();
            }

            final Map<String, String> attributes = new HashMap<>(result.getAttributes());
            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
            attributes.put(CaptureChangePostgreSQL.ATTRIBUTE_RECORD_COUNT, Integer.toString(result.getRecordCount()));
            attributes.put(CaptureChangePostgreSQL.ATTRIBUTE_EVENT_COUNT, Integer.toString(count));
            attributes.put(CaptureChangePostgreSQL.ATTRIBUTE_LSN, lastLsn.asString());
            attributes.put(CaptureChangePostgreSQL.ATTRIBUTE_XID, Long.toString(lastTransaction.xid()));
            flowFile = session.putAllAttributes(flowFile, attributes);
        }

        private void abort() {
            try (outputStream; writer) {
                // closing releases the content claim; the session rollback removes the FlowFile
            } catch (final Exception e) {
                logger.debug("Closing Record Writer for table {}.{} failed during rollback", relation.namespace(), relation.name(), e);
            }
        }
    }
}
