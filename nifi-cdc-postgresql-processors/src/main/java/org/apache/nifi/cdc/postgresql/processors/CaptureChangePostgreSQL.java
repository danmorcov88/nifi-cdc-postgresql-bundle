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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.cdc.postgresql.client.ConnectionSettings;
import org.apache.nifi.cdc.postgresql.client.PgjdbcReplicationClient;
import org.apache.nifi.cdc.postgresql.client.ReplicationClient;
import org.apache.nifi.cdc.postgresql.client.ReplicationSlot;
import org.apache.nifi.cdc.postgresql.client.SSLContextRegistry;
import org.apache.nifi.cdc.postgresql.client.SSLMode;
import org.apache.nifi.cdc.postgresql.event.ChangeEventRecordFactory;
import org.apache.nifi.cdc.postgresql.event.ChangeOperation;
import org.apache.nifi.cdc.postgresql.event.TransactionInfo;
import org.apache.nifi.cdc.postgresql.event.UnchangedToastStrategy;
import org.apache.nifi.cdc.postgresql.pgoutput.BeginMessage;
import org.apache.nifi.cdc.postgresql.pgoutput.CommitMessage;
import org.apache.nifi.cdc.postgresql.pgoutput.DeleteMessage;
import org.apache.nifi.cdc.postgresql.pgoutput.InsertMessage;
import org.apache.nifi.cdc.postgresql.pgoutput.PgOutputDecoder;
import org.apache.nifi.cdc.postgresql.pgoutput.PgOutputMessage;
import org.apache.nifi.cdc.postgresql.pgoutput.RelationCache;
import org.apache.nifi.cdc.postgresql.pgoutput.RelationMessage;
import org.apache.nifi.cdc.postgresql.pgoutput.TruncateMessage;
import org.apache.nifi.cdc.postgresql.pgoutput.TupleData;
import org.apache.nifi.cdc.postgresql.pgoutput.UpdateMessage;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.ssl.SSLContextProvider;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

@TriggerSerially
@PrimaryNodeOnly
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "jdbc", "cdc", "postgresql", "replication", "transaction", "event"})
@CapabilityDescription("Retrieves Change Data Capture (CDC) events from a PostgreSQL database using logical replication with the built-in "
        + "pgoutput plugin. The processor reads INSERT, UPDATE, DELETE and TRUNCATE events for the tables of a publication and writes "
        + "them as records using the configured Record Writer, one FlowFile per table and batch. Each record has the fields operation, "
        + "schema, table, lsn, xid, commit_timestamp, before and after. The position confirmed to the PostgreSQL server never runs ahead "
        + "of the FlowFiles committed by the processor, so every change is delivered at least once.")
@Stateful(scopes = Scope.CLUSTER, description = "The Log Sequence Number (LSN) up to which changes have been written to FlowFiles is stored "
        + "so that the processor resumes from the same position after a restart or a change of the primary node.")
@WritesAttributes({
        @WritesAttribute(attribute = CaptureChangePostgreSQL.ATTRIBUTE_SCHEMA, description = "Schema of the table the events belong to"),
        @WritesAttribute(attribute = CaptureChangePostgreSQL.ATTRIBUTE_TABLE, description = "Name of the table the events belong to"),
        @WritesAttribute(attribute = CaptureChangePostgreSQL.ATTRIBUTE_LSN, description = "Log Sequence Number of the last event in the FlowFile"),
        @WritesAttribute(attribute = CaptureChangePostgreSQL.ATTRIBUTE_XID, description = "Transaction id of the last event in the FlowFile"),
        @WritesAttribute(attribute = CaptureChangePostgreSQL.ATTRIBUTE_EVENT_COUNT, description = "Number of change events in the FlowFile"),
        @WritesAttribute(attribute = CaptureChangePostgreSQL.ATTRIBUTE_RECORD_COUNT, description = "Number of records written to the FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "MIME type reported by the Record Writer")
})
public class CaptureChangePostgreSQL extends AbstractProcessor {

    public static final String ATTRIBUTE_SCHEMA = "cdc.schema";
    public static final String ATTRIBUTE_TABLE = "cdc.table";
    public static final String ATTRIBUTE_LSN = "cdc.lsn";
    public static final String ATTRIBUTE_XID = "cdc.xid";
    public static final String ATTRIBUTE_EVENT_COUNT = "cdc.event.count";
    public static final String ATTRIBUTE_RECORD_COUNT = "record.count";

    static final String STATE_LSN = "lsn";

    static final int MINIMUM_SERVER_VERSION = 140000;
    static final String REQUIRED_WAL_LEVEL = "logical";

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("Hostname or IP address of the PostgreSQL server")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("TCP port of the PostgreSQL server")
            .required(true)
            .defaultValue("5432")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("Database Name")
            .description("Name of the database whose changes are captured. Logical replication slots belong to one database.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Database user. The user must have the REPLICATION privilege (or be a superuser) and be allowed to open "
                    + "replication connections in pg_hba.conf.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password of the database user")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("Maximum time to wait for the connection to the server to be established")
            .required(true)
            .defaultValue("30 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SSL_MODE = new PropertyDescriptor.Builder()
            .name("SSL Mode")
            .description("TLS negotiation mode of the connection, as the sslmode connection parameter of PostgreSQL")
            .required(true)
            .allowableValues(SSLMode.class)
            .defaultValue(SSLMode.DISABLED)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("Provides the trust store used to verify the server certificate and, optionally, the key store used for client "
                    + "certificate authentication. Required for the Verify CA and Verify Full modes. When not set, the driver relies on "
                    + "its own certificate handling.")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .dependsOn(SSL_MODE, SSLMode.PREFERRED, SSLMode.REQUIRED, SSLMode.VERIFY_CA, SSLMode.VERIFY_FULL)
            .build();

    public static final PropertyDescriptor PUBLICATION_NAME = new PropertyDescriptor.Builder()
            .name("Publication Name")
            .description("Name of the publication that defines the tables whose changes are captured. The publication must be created "
                    + "beforehand, for example with CREATE PUBLICATION nifi_cdc FOR TABLE schema.table.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor REPLICATION_SLOT_NAME = new PropertyDescriptor.Builder()
            .name("Replication Slot Name")
            .description("Name of the logical replication slot that tracks the position of this processor on the server. The name may contain "
                    + "lower case letters, digits and underscores. The slot must use the pgoutput plugin and must not be used by another consumer. "
                    + "The processor never drops the slot; an unused slot retains write-ahead log on the server and must be dropped by an administrator.")
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("[a-z0-9_]{1,63}")))
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor CREATE_REPLICATION_SLOT = new PropertyDescriptor.Builder()
            .name("Create Replication Slot")
            .description("Whether to create the replication slot when it does not exist. When false, the processor fails to start until the slot "
                    + "has been created with pg_create_logical_replication_slot(name, 'pgoutput').")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor SCHEMA_NAME_PATTERN = new PropertyDescriptor.Builder()
            .name("Schema Name Pattern")
            .description("Regular expression that the schema of a table must match for its changes to be written. When not set, changes of all "
                    + "schemas in the publication are written.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME_PATTERN = new PropertyDescriptor.Builder()
            .name("Table Name Pattern")
            .description("Regular expression that the name of a table must match for its changes to be written. When not set, changes of all "
                    + "tables in the publication are written.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Record Writer used to serialize the change events. The schema of the events is derived from the table definition sent "
                    + "by the server, so the writer is usually configured to inherit the record schema.")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    public static final PropertyDescriptor BATCH_STRATEGY = new PropertyDescriptor.Builder()
            .name("Batch Strategy")
            .description("Determines when the FlowFiles of a batch of change events are completed and transferred. A batch always ends at a "
                    + "transaction boundary and contains one FlowFile per table.")
            .required(true)
            .allowableValues(BatchStrategy.class)
            .defaultValue(BatchStrategy.MAX_EVENTS)
            .build();

    public static final PropertyDescriptor EVENTS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("Events Per FlowFile")
            .description("Number of change events after which a batch is completed at the next transaction boundary. A transaction is never "
                    + "split, so a FlowFile can contain more events than this number.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(BATCH_STRATEGY, BatchStrategy.MAX_EVENTS)
            .build();

    public static final PropertyDescriptor MAX_BATCH_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("Max Batch Wait Time")
            .description("Maximum time a batch stays open after its first event while changes keep arriving, when Events Per FlowFile "
                    + "is not reached. A batch is completed as soon as no further change is pending, so this limit only applies under "
                    + "continuous load.")
            .required(true)
            .defaultValue("10 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(BATCH_STRATEGY, BatchStrategy.MAX_EVENTS)
            .build();

    public static final PropertyDescriptor UNCHANGED_TOAST_STRATEGY = new PropertyDescriptor.Builder()
            .name("Unchanged TOAST Value Strategy")
            .description("How to represent a column of the after image of an UPDATE whose value was not sent by the server because it is "
                    + "stored out of line (TOAST) and did not change. Setting REPLICA IDENTITY FULL on the table makes the server send "
                    + "all values.")
            .required(true)
            .allowableValues(UnchangedToastStrategy.class)
            .defaultValue(UnchangedToastStrategy.NULL)
            .build();

    public static final PropertyDescriptor UNCHANGED_TOAST_PLACEHOLDER = new PropertyDescriptor.Builder()
            .name("Unchanged TOAST Placeholder")
            .description("Text written for string columns whose value was not sent because it is an unchanged TOAST value")
            .required(true)
            .defaultValue("__unchanged_toast_value__")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(UNCHANGED_TOAST_STRATEGY, UnchangedToastStrategy.PLACEHOLDER)
            .build();

    public static final PropertyDescriptor WAL_RETENTION_WARNING_THRESHOLD = new PropertyDescriptor.Builder()
            .name("WAL Retention Warning Threshold")
            .description("Amount of write-ahead log retained on the server for the replication slot above which the processor logs a "
                    + "warning bulletin. A slot retains WAL until its consumer confirms it, so a stopped or lagging processor can fill the "
                    + "disk of the server. The check runs once per minute on a separate connection. Leave empty to disable the check.")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing change events, one FlowFile per table and batch")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            HOSTNAME,
            PORT,
            DATABASE_NAME,
            USERNAME,
            PASSWORD,
            CONNECTION_TIMEOUT,
            SSL_MODE,
            SSL_CONTEXT_SERVICE,
            PUBLICATION_NAME,
            REPLICATION_SLOT_NAME,
            CREATE_REPLICATION_SLOT,
            SCHEMA_NAME_PATTERN,
            TABLE_NAME_PATTERN,
            RECORD_WRITER,
            BATCH_STRATEGY,
            EVENTS_PER_FLOWFILE,
            MAX_BATCH_WAIT_TIME,
            UNCHANGED_TOAST_STRATEGY,
            UNCHANGED_TOAST_PLACEHOLDER,
            WAL_RETENTION_WARNING_THRESHOLD
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    /** How often the driver reports the confirmed position to the server; must stay well below wal_sender_timeout (default 60 seconds). */
    private static final Duration STATUS_INTERVAL = Duration.ofSeconds(10);
    /** Pause between polls of the replication stream while no message is pending. */
    private static final long POLL_INTERVAL_MILLIS = 50;
    /** Minimum time between state updates that only record progress without writing FlowFiles. */
    private static final long IDLE_STATE_UPDATE_INTERVAL_MILLIS = STATUS_INTERVAL.toMillis();
    private static final long INVALID_LSN = LogSequenceNumber.INVALID_LSN.asLong();
    private static final long BYTES_PER_MEGABYTE = 1024 * 1024;
    /** How often the retained write-ahead log is measured. */
    private static final long WAL_RETENTION_CHECK_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);
    /** Minimum time between repeated warnings while the same kind of failure persists. */
    private static final long REPEATED_FAILURE_LOG_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);

    private final PgOutputDecoder decoder = new PgOutputDecoder();
    private final RelationCache relationCache = new RelationCache();

    /**
     * Position up to which changes are durable in NiFi: the end of the last transaction written to committed FlowFiles,
     * or a position reached while no transaction was in progress. Updated from session commit callbacks, applied to the
     * replication stream by the processor thread.
     */
    private final AtomicLong confirmedLsn = new AtomicLong(INVALID_LSN);

    private volatile ReplicationClient client;
    private volatile PGReplicationStream stream;
    private ConnectionSettings connectionSettings;
    private Long walRetentionWarningThreshold;
    private long lastWalRetentionCheckMillis;
    private long lastFailureLogMillis;
    private boolean failing;
    private ChangeEventRecordFactory recordFactory;
    private RecordSetWriterFactory writerFactory;
    private String publicationName;
    private String slotName;
    private String transitUri;
    private Pattern schemaNamePattern;
    private Pattern tableNamePattern;
    private BatchStrategy batchStrategy;
    private int eventsPerFlowFile;
    private long maxBatchWaitMillis;
    private String sslContextIdentifier;
    private long streamFlushedLsn = INVALID_LSN;
    private long stateLsn = INVALID_LSN;
    private long lastIdleStateUpdateMillis;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();
        final SSLMode sslMode = context.getProperty(SSL_MODE).asAllowableValue(SSLMode.class);
        if (sslMode.isVerifying() && !context.getProperty(SSL_CONTEXT_SERVICE).isSet()) {
            results.add(new ValidationResult.Builder()
                    .subject(SSL_CONTEXT_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation(String.format("required when %s is %s", SSL_MODE.getDisplayName(), sslMode.getDisplayName()))
                    .build());
        }
        return results;
    }

    @OnScheduled
    public void setup(final ProcessContext context) throws IOException {
        publicationName = context.getProperty(PUBLICATION_NAME).evaluateAttributeExpressions().getValue();
        slotName = context.getProperty(REPLICATION_SLOT_NAME).evaluateAttributeExpressions().getValue();
        writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        schemaNamePattern = compilePattern(context.getProperty(SCHEMA_NAME_PATTERN).getValue());
        tableNamePattern = compilePattern(context.getProperty(TABLE_NAME_PATTERN).getValue());
        batchStrategy = context.getProperty(BATCH_STRATEGY).asAllowableValue(BatchStrategy.class);
        eventsPerFlowFile = batchStrategy == BatchStrategy.MAX_EVENTS
                ? context.getProperty(EVENTS_PER_FLOWFILE).evaluateAttributeExpressions().asInteger() : 1;
        maxBatchWaitMillis = batchStrategy == BatchStrategy.MAX_EVENTS
                ? context.getProperty(MAX_BATCH_WAIT_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS) : 0;
        final UnchangedToastStrategy toastStrategy = context.getProperty(UNCHANGED_TOAST_STRATEGY).asAllowableValue(UnchangedToastStrategy.class);
        final String toastPlaceholder = toastStrategy == UnchangedToastStrategy.PLACEHOLDER
                ? context.getProperty(UNCHANGED_TOAST_PLACEHOLDER).getValue() : null;
        recordFactory = new ChangeEventRecordFactory(toastStrategy, toastPlaceholder, message -> getLogger().warn(message));

        walRetentionWarningThreshold = context.getProperty(WAL_RETENTION_WARNING_THRESHOLD).isSet()
                ? context.getProperty(WAL_RETENTION_WARNING_THRESHOLD).evaluateAttributeExpressions().asDataSize(DataUnit.B).longValue() : null;
        lastWalRetentionCheckMillis = 0;
        failing = false;

        final ConnectionSettings settings = createConnectionSettings(context);
        connectionSettings = settings;
        transitUri = String.format("postgresql://%s:%d/%s", settings.hostname(), settings.port(), settings.database());

        final ReplicationClient newClient = createReplicationClient(settings);
        try {
            newClient.connect();
            validateServer(newClient, settings);
            final boolean slotCreated = prepareReplicationSlot(newClient, context, settings);
            loadState(context, slotCreated);
        } catch (final ProcessException e) {
            closeQuietly(newClient);
            unregisterSslContext();
            throw e;
        } catch (final Exception e) {
            closeQuietly(newClient);
            unregisterSslContext();
            throw new ProcessException(String.format("Preparing logical replication from %s failed: %s", transitUri, e.getMessage()), e);
        }
        client = newClient;
    }

    @OnStopped
    public void stop() {
        closeStream();
        closeClient();
        unregisterSslContext();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final PGReplicationStream currentStream;
        try {
            currentStream = getOrOpenStream();
        } catch (final Exception e) {
            closeStream();
            closeClient();
            logFailure(String.format("Connecting to %s for logical replication failed", transitUri), e);
            context.yield();
            return;
        }
        checkWalRetention();

        // Messages taken from the stream are either committed to FlowFiles or the stream is discarded and reopened
        // from the confirmed position, so that the confirmed position never runs ahead of the FlowFiles.
        final EventBatch batch = new EventBatch(session, writerFactory, recordFactory, getLogger(), transitUri);
        final BatchReader reader = new BatchReader(currentStream, batch);
        boolean completed = false;
        try {
            reader.read();
            if (reader.isTransactionInProgress()) {
                // stopped in the middle of a transaction: the transaction is sent again after the restart
                return;
            }

            if (batch.isEmpty()) {
                confirmIdleProgress(session, currentStream, Math.max(reader.getLastCommitEndLsn(), currentStream.getLastReceiveLSN().asLong()));
                context.yield();
            } else {
                batch.transfer(REL_SUCCESS);
                final long batchEndLsn = reader.getLastCommitEndLsn();
                session.setState(Map.of(STATE_LSN, LogSequenceNumber.valueOf(batchEndLsn).asString()), Scope.CLUSTER);
                stateLsn = batchEndLsn;
                session.commitAsync(() -> confirmedLsn.accumulateAndGet(batchEndLsn, Math::max), this::onCommitFailure);
            }
            completed = true;
            failing = false;
        } catch (final Exception e) {
            logFailure(String.format("Processing changes from %s failed; the connection will be reopened from the last confirmed position", transitUri), e);
            context.yield();
        } finally {
            if (!completed) {
                batch.rollback();
                closeStream();
                closeClient();
            }
        }
    }

    /**
     * Factory method for the replication client, overridable for tests.
     */
    protected ReplicationClient createReplicationClient(final ConnectionSettings settings) {
        return new PgjdbcReplicationClient(settings);
    }

    private ConnectionSettings createConnectionSettings(final ProcessContext context) {
        final SSLMode sslMode = context.getProperty(SSL_MODE).asAllowableValue(SSLMode.class);
        final SSLContextProvider sslContextProvider = sslMode == SSLMode.DISABLED
                ? null : context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        if (sslContextProvider != null) {
            sslContextIdentifier = getIdentifier();
            SSLContextRegistry.register(sslContextIdentifier, sslContextProvider.createContext());
        }
        return new ConnectionSettings(
                context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(PORT).evaluateAttributeExpressions().asInteger(),
                context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(USERNAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue(),
                Duration.ofMillis(context.getProperty(CONNECTION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS)),
                sslMode,
                sslContextIdentifier,
                String.format("Apache NiFi CaptureChangePostgreSQL %s", getIdentifier())
        );
    }

    private void validateServer(final ReplicationClient replicationClient, final ConnectionSettings settings) throws SQLException {
        final int serverVersion = replicationClient.getServerVersionNumber();
        if (serverVersion < MINIMUM_SERVER_VERSION) {
            throw new ProcessException(String.format("PostgreSQL server version %d is not supported; version %d or later is required",
                    serverVersion, MINIMUM_SERVER_VERSION));
        }
        final String walLevel = replicationClient.getWalLevel();
        if (!REQUIRED_WAL_LEVEL.equals(walLevel)) {
            throw new ProcessException(String.format("Server setting wal_level is [%s] but must be [%s]", walLevel, REQUIRED_WAL_LEVEL));
        }
        if (!replicationClient.hasReplicationPrivilege()) {
            throw new ProcessException(String.format("User [%s] does not have the REPLICATION privilege", settings.username()));
        }
        if (!replicationClient.publicationExists(publicationName)) {
            throw new ProcessException(String.format("Publication [%s] does not exist in database [%s]", publicationName, settings.database()));
        }
    }

    /**
     * @return whether the slot was created by this call
     */
    private boolean prepareReplicationSlot(final ReplicationClient replicationClient, final ProcessContext context, final ConnectionSettings settings)
            throws SQLException {
        final Optional<ReplicationSlot> existingSlot = replicationClient.findReplicationSlot(slotName);
        if (existingSlot.isEmpty()) {
            if (!context.getProperty(CREATE_REPLICATION_SLOT).asBoolean()) {
                throw new ProcessException(String.format("Replication slot [%s] does not exist and %s is false", slotName,
                        CREATE_REPLICATION_SLOT.getDisplayName()));
            }
            final LogSequenceNumber consistentPoint = replicationClient.createReplicationSlot(slotName);
            getLogger().info("Created replication slot [{}] with plugin [{}] at position {}", slotName, PgjdbcReplicationClient.OUTPUT_PLUGIN, consistentPoint);
            return true;
        }

        final ReplicationSlot slot = existingSlot.get();
        if (!PgjdbcReplicationClient.OUTPUT_PLUGIN.equals(slot.plugin())) {
            throw new ProcessException(String.format("Replication slot [%s] uses plugin [%s] but [%s] is required", slotName, slot.plugin(),
                    PgjdbcReplicationClient.OUTPUT_PLUGIN));
        }
        if (!settings.database().equals(slot.database())) {
            throw new ProcessException(String.format("Replication slot [%s] belongs to database [%s], not [%s]", slotName, slot.database(),
                    settings.database()));
        }
        if (slot.active()) {
            throw new ProcessException(String.format("Replication slot [%s] is in use by another connection", slotName));
        }
        getLogger().info("Using replication slot [{}] with confirmed position {}", slotName, slot.confirmedFlushLsn());
        return false;
    }

    private void loadState(final ProcessContext context, final boolean slotCreated) throws IOException {
        final StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
        final String storedLsn = stateMap.get(STATE_LSN);
        if (storedLsn == null) {
            stateLsn = INVALID_LSN;
        } else if (slotCreated) {
            getLogger().warn("Ignoring stored position {} because replication slot [{}] was just created; streaming starts at the slot position",
                    storedLsn, slotName);
            context.getStateManager().clear(Scope.CLUSTER);
            stateLsn = INVALID_LSN;
        } else {
            stateLsn = LogSequenceNumber.valueOf(storedLsn).asLong();
            getLogger().info("Resuming from stored position {}", storedLsn);
        }
        confirmedLsn.set(stateLsn);
        streamFlushedLsn = INVALID_LSN;
    }

    /**
     * Returns the open stream, reconnecting and reopening it from the confirmed position when necessary.
     */
    private PGReplicationStream getOrOpenStream() throws SQLException {
        if (client == null) {
            final ReplicationClient newClient = createReplicationClient(connectionSettings);
            newClient.connect();
            client = newClient;
        }
        if (stream == null) {
            final long startLsn = confirmedLsn.get();
            final LogSequenceNumber startPosition = startLsn == INVALID_LSN ? null : LogSequenceNumber.valueOf(startLsn);
            relationCache.clear();
            stream = client.startReplicationStream(slotName, publicationName, startPosition, STATUS_INTERVAL);
            streamFlushedLsn = INVALID_LSN;
            getLogger().info("Started replication stream from {} on slot [{}] at position {}", transitUri, slotName,
                    startPosition == null ? "of the slot" : startPosition);
        }
        applyConfirmedLsn(stream);
        return stream;
    }

    /**
     * Report the confirmed position to the server, so that it can release write-ahead log.
     */
    private void applyConfirmedLsn(final PGReplicationStream currentStream) {
        final long lsn = confirmedLsn.get();
        if (lsn != INVALID_LSN && lsn > streamFlushedLsn) {
            final LogSequenceNumber position = LogSequenceNumber.valueOf(lsn);
            currentStream.setAppliedLSN(position);
            currentStream.setFlushedLSN(position);
            streamFlushedLsn = lsn;
        }
    }

    /**
     * Record progress made while no change was written: transactions without matching events and keepalive positions.
     * The position is reported to the server right away and stored in state at a limited rate.
     */
    private void confirmIdleProgress(final ProcessSession session, final PGReplicationStream currentStream, final long lsn) throws IOException {
        if (lsn == INVALID_LSN || lsn <= confirmedLsn.get()) {
            return;
        }
        confirmedLsn.set(lsn);
        applyConfirmedLsn(currentStream);

        final long now = System.currentTimeMillis();
        if (lsn > stateLsn && now - lastIdleStateUpdateMillis >= IDLE_STATE_UPDATE_INTERVAL_MILLIS) {
            session.setState(Map.of(STATE_LSN, LogSequenceNumber.valueOf(lsn).asString()), Scope.CLUSTER);
            session.commitAsync();
            stateLsn = lsn;
            lastIdleStateUpdateMillis = now;
        }
    }

    private void onCommitFailure(final Throwable failure) {
        getLogger().error("Committing change events from {} failed; the connection will be reopened from the last confirmed position",
                transitUri, failure);
        closeStream();
        closeClient();
    }

    /**
     * Logs a failure as an error the first time, then as a warning at a limited rate while triggers keep failing, so that a
     * server outage or a change that cannot be written does not flood the bulletin board. Every trigger retries.
     */
    private void logFailure(final String message, final Exception failure) {
        final long now = System.currentTimeMillis();
        if (!failing) {
            failing = true;
            lastFailureLogMillis = now;
            getLogger().error("{}; retrying", message, failure);
        } else if (now - lastFailureLogMillis >= REPEATED_FAILURE_LOG_INTERVAL_MILLIS) {
            lastFailureLogMillis = now;
            getLogger().warn("{}; still retrying: {}", message, failure.toString());
        } else {
            getLogger().debug("{}", message, failure);
        }
    }

    private void checkWalRetention() {
        final long now = System.currentTimeMillis();
        if (walRetentionWarningThreshold == null || now - lastWalRetentionCheckMillis < WAL_RETENTION_CHECK_INTERVAL_MILLIS) {
            return;
        }
        lastWalRetentionCheckMillis = now;
        try {
            final long retainedBytes = client.getRetainedWalBytes(slotName);
            if (retainedBytes > walRetentionWarningThreshold) {
                getLogger().warn("Replication slot [{}] on {} retains {} MB of write-ahead log, above the threshold of {} MB; "
                                + "the server cannot recycle this log until the processor confirms the changes",
                        slotName, transitUri, retainedBytes / BYTES_PER_MEGABYTE, walRetentionWarningThreshold / BYTES_PER_MEGABYTE);
            }
        } catch (final Exception e) {
            getLogger().debug("Measuring the write-ahead log retained by replication slot [{}] failed", slotName, e);
        }
    }

    private void closeStream() {
        final PGReplicationStream currentStream = stream;
        stream = null;
        relationCache.clear();
        if (currentStream == null) {
            return;
        }
        try {
            applyConfirmedLsn(currentStream);
            currentStream.forceUpdateStatus();
        } catch (final Exception e) {
            getLogger().debug("Reporting confirmed position before closing the replication stream failed", e);
        }
        try {
            currentStream.close();
        } catch (final Exception e) {
            getLogger().debug("Closing replication stream failed", e);
        }
    }

    private void closeClient() {
        closeQuietly(client);
        client = null;
    }

    private void closeQuietly(final ReplicationClient replicationClient) {
        if (replicationClient == null) {
            return;
        }
        try {
            replicationClient.close();
        } catch (final Exception e) {
            getLogger().debug("Closing replication connection failed", e);
        }
    }

    private void unregisterSslContext() {
        if (sslContextIdentifier != null) {
            SSLContextRegistry.unregister(sslContextIdentifier);
            sslContextIdentifier = null;
        }
    }

    private boolean isCaptured(final RelationMessage relation) {
        return (schemaNamePattern == null || schemaNamePattern.matcher(relation.namespace()).matches())
                && (tableNamePattern == null || tableNamePattern.matcher(relation.name()).matches());
    }

    private static Pattern compilePattern(final String regex) {
        return regex == null || regex.isBlank() ? null : Pattern.compile(regex);
    }

    /**
     * Reads messages from the stream into a batch until the batch is complete, no more messages are pending between
     * transactions or the processor is stopped.
     */
    private class BatchReader {

        private final PGReplicationStream currentStream;
        private final EventBatch batch;
        private TransactionInfo transaction;
        private long lastCommitEndLsn = INVALID_LSN;
        private long batchDeadlineMillis;

        private BatchReader(final PGReplicationStream currentStream, final EventBatch batch) {
            this.currentStream = currentStream;
            this.batch = batch;
        }

        boolean isTransactionInProgress() {
            return transaction != null;
        }

        long getLastCommitEndLsn() {
            return lastCommitEndLsn;
        }

        void read() throws SQLException, IOException, InterruptedException {
            while (isScheduled()) {
                final ByteBuffer buffer = currentStream.readPending();
                if (buffer == null) {
                    if (transaction == null) {
                        // nothing pending between transactions: complete what has been read instead of waiting for more
                        return;
                    }
                    Thread.sleep(POLL_INTERVAL_MILLIS);
                    continue;
                }

                final LogSequenceNumber messageLsn = currentStream.getLastReceiveLSN();
                if (handle(decoder.decode(buffer), messageLsn)) {
                    return;
                }
            }
        }

        /**
         * @return whether the batch is complete
         */
        private boolean handle(final PgOutputMessage message, final LogSequenceNumber lsn) throws IOException {
            switch (message) {
                case BeginMessage begin -> {
                    if (transaction != null) {
                        throw new IllegalStateException("Begin message received while a transaction is in progress");
                    }
                    transaction = new TransactionInfo(Integer.toUnsignedLong(begin.xid()), begin.commitTime());
                }
                case RelationMessage relation -> {
                    relationCache.put(relation);
                    batch.relationChanged(relation);
                }
                case InsertMessage insert -> change(ChangeOperation.INSERT, insert.relationId(), lsn, null, insert.newTuple());
                case UpdateMessage update -> change(ChangeOperation.UPDATE, update.relationId(), lsn, update.oldTuple(), update.newTuple());
                case DeleteMessage delete -> change(ChangeOperation.DELETE, delete.relationId(), lsn, delete.oldTuple(), null);
                case TruncateMessage truncate -> {
                    for (final int relationId : truncate.relationIds()) {
                        change(ChangeOperation.TRUNCATE, relationId, lsn, null, null);
                    }
                }
                case CommitMessage commit -> {
                    if (transaction == null) {
                        throw new IllegalStateException("Commit message received outside of a transaction");
                    }
                    transaction = null;
                    lastCommitEndLsn = commit.endLsn();
                    return !batch.isEmpty()
                            && (batch.getEventCount() >= eventsPerFlowFile || System.currentTimeMillis() >= batchDeadlineMillis);
                }
                default -> {
                    // Type messages: the type mapper works with OIDs of built-in types, user-defined types are written as strings.
                    // Origin messages: changes replicated from another node are treated like local changes.
                }
            }
            return false;
        }

        private void change(final ChangeOperation operation, final int relationId, final LogSequenceNumber lsn, final TupleData before,
                            final TupleData after) throws IOException {
            if (transaction == null) {
                throw new IllegalStateException(String.format("%s message received outside of a transaction", operation));
            }
            final RelationMessage relation = relationCache.get(relationId);
            if (!isCaptured(relation)) {
                return;
            }
            if (batch.isEmpty()) {
                batchDeadlineMillis = System.currentTimeMillis() + maxBatchWaitMillis;
            }
            final Record record = recordFactory.createRecord(operation, relation, transaction, lsn.asString(), before, after);
            batch.write(relation, record, lsn, transaction);
        }
    }
}
