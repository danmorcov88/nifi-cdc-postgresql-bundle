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

import org.apache.nifi.cdc.postgresql.pgoutput.PgOutputDecoder;
import org.apache.nifi.logging.ComponentLog;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Streamed transactions in progress on the replication stream: the segment currently open and the spool of every
 * transaction whose commit or abort has not arrived yet. The spools live in a temporary directory that is created on
 * first use and removed with {@link #close()}.
 */
class StreamedTransactions implements Closeable {

    private static final String DIRECTORY_PREFIX = "nifi-cdc-postgresql-";

    private final long maxBytesPerTransaction;
    private final ComponentLog logger;
    private final Path baseDirectory;
    private final Map<Integer, StreamedTransactionSpool> spools = new HashMap<>();
    private Path directory;
    private StreamedTransactionSpool openSegment;

    /**
     * @param maxBytesPerTransaction maximum size of the spool of one transaction
     * @param logger component log
     * @param baseDirectory where the temporary directory is created, or null for the temporary directory of the JVM
     */
    StreamedTransactions(final long maxBytesPerTransaction, final ComponentLog logger, final Path baseDirectory) {
        this.maxBytesPerTransaction = maxBytesPerTransaction;
        this.logger = logger;
        this.baseDirectory = baseDirectory;
    }

    boolean isSegmentOpen() {
        return openSegment != null;
    }

    int getTransactionCount() {
        return spools.size();
    }

    /**
     * Handle a Stream Start message: the messages that follow belong to the transaction.
     */
    void beginSegment(final int xid, final boolean firstSegment) throws IOException {
        if (openSegment != null) {
            throw new IllegalStateException(String.format("Stream Start of transaction %d received while a segment of transaction %d is open",
                    Integer.toUnsignedLong(xid), Integer.toUnsignedLong(openSegment.getXid())));
        }
        StreamedTransactionSpool spool = spools.get(xid);
        if (firstSegment) {
            if (spool != null) {
                // the server starts a transaction over from its first segment after a reconnection
                closeQuietly(spools.remove(xid));
            }
            if (directory == null) {
                directory = baseDirectory == null ? Files.createTempDirectory(DIRECTORY_PREFIX) : Files.createTempDirectory(baseDirectory, DIRECTORY_PREFIX);
            }
            spool = new StreamedTransactionSpool(xid, directory, maxBytesPerTransaction);
            spools.put(xid, spool);
        } else if (spool == null) {
            throw new IllegalStateException(String.format("Stream segment of transaction %d received without its first segment", Integer.toUnsignedLong(xid)));
        }
        openSegment = spool;
    }

    /**
     * Handle a Stream Stop message.
     */
    void endSegment() {
        if (openSegment == null) {
            throw new IllegalStateException("Stream Stop received while no segment is open");
        }
        openSegment = null;
    }

    /**
     * Spool a message received inside the open segment.
     *
     * @param lsn position the message was received at
     * @param message raw message positioned at the type byte, consumed by this call
     */
    void append(final long lsn, final ByteBuffer message) throws IOException {
        if (openSegment == null) {
            throw new IllegalStateException("Streamed change received while no segment is open");
        }
        openSegment.append(lsn, PgOutputDecoder.streamedTransactionId(message), message);
    }

    /**
     * Handle a Stream Abort message: discard the transaction or the changes of one of its subtransactions.
     */
    void abort(final int xid, final int subTransactionXid) throws IOException {
        final StreamedTransactionSpool spool = spools.get(xid);
        if (spool == null) {
            // a transaction without spooled changes has nothing to discard
            return;
        }
        if (subTransactionXid == xid) {
            spools.remove(xid);
            spool.close();
        } else {
            spool.abortSubTransaction(subTransactionXid);
        }
    }

    /**
     * Take the spool of a committed transaction for replay. The caller closes it.
     */
    StreamedTransactionSpool remove(final int xid) {
        final StreamedTransactionSpool spool = spools.remove(xid);
        if (spool == null) {
            throw new IllegalStateException(String.format("Stream Commit of unknown transaction %d", Integer.toUnsignedLong(xid)));
        }
        return spool;
    }

    /**
     * Discard every transaction in progress; the server sends them again from their first segment after a reconnection.
     */
    void clear() {
        openSegment = null;
        final Iterator<StreamedTransactionSpool> iterator = spools.values().iterator();
        while (iterator.hasNext()) {
            closeQuietly(iterator.next());
            iterator.remove();
        }
    }

    @Override
    public void close() throws IOException {
        clear();
        if (directory != null) {
            Files.deleteIfExists(directory);
            directory = null;
        }
    }

    private void closeQuietly(final StreamedTransactionSpool spool) {
        try {
            spool.close();
        } catch (final IOException e) {
            logger.debug("Deleting the spool of streamed transaction {} failed", Integer.toUnsignedLong(spool.getXid()), e);
        }
    }
}
