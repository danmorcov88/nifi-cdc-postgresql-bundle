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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Raw pgoutput messages of one streamed transaction, appended to a temporary file as the segments of the transaction
 * arrive and replayed once the server commits it. Every message is stored with the position it was received at:
 * {@code [lsn Int64][length Int32][message bytes]}.
 * <p>
 * The first message of every subtransaction marks an offset, so that the abort of a subtransaction can truncate
 * the file back to the point where that subtransaction started, exactly as the apply worker of PostgreSQL does.
 * Changes made after a rolled back subtransaction arrive after the abort and are appended again.
 */
class StreamedTransactionSpool implements Closeable {

    private static final int HEADER_LENGTH = Long.BYTES + Integer.BYTES;
    private static final int BUFFER_SIZE = 64 * 1024;

    private final int xid;
    private final long maxBytes;
    private final Path file;
    private final FileChannel channel;
    private final ByteBuffer staging = ByteBuffer.allocate(BUFFER_SIZE);
    private final List<SubTransaction> subTransactions = new ArrayList<>();
    private long size;
    private int messageCount;

    /**
     * @param xid id of the top-level transaction
     * @param directory directory for the temporary file
     * @param maxBytes maximum size of the spooled messages, beyond which {@link #append} fails
     */
    StreamedTransactionSpool(final int xid, final Path directory, final long maxBytes) throws IOException {
        this.xid = xid;
        this.maxBytes = maxBytes;
        file = Files.createTempFile(directory, String.format("xid-%d-", Integer.toUnsignedLong(xid)), ".spool");
        channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    int getXid() {
        return xid;
    }

    long getSize() {
        return size;
    }

    int getMessageCount() {
        return messageCount;
    }

    /**
     * @param lsn position the message was received at
     * @param subTransactionXid id of the (sub)transaction that made the change, as carried by the message
     * @param message raw message, consumed by this call
     */
    void append(final long lsn, final int subTransactionXid, final ByteBuffer message) throws IOException {
        final int length = message.remaining();
        if (size + HEADER_LENGTH + length > maxBytes) {
            throw new IOException(String.format("Streamed transaction %d exceeds the configured maximum size of %d bytes; the server keeps sending it "
                    + "until the processor accepts it, so the limit must be raised", Integer.toUnsignedLong(xid), maxBytes));
        }
        if (subTransactionXid != xid && subTransactions.stream().noneMatch(subTransaction -> subTransaction.xid == subTransactionXid)) {
            subTransactions.add(new SubTransaction(subTransactionXid, size, messageCount));
        }

        if (staging.remaining() < HEADER_LENGTH) {
            flush();
        }
        staging.putLong(lsn).putInt(length);
        if (length > staging.remaining()) {
            flush();
        }
        if (length > staging.capacity()) {
            while (message.hasRemaining()) {
                channel.write(message);
            }
        } else {
            staging.put(message);
        }
        size += HEADER_LENGTH + length;
        messageCount++;
    }

    /**
     * Discard the messages of a subtransaction and everything received after its first message.
     */
    void abortSubTransaction(final int subTransactionXid) throws IOException {
        for (int i = 0; i < subTransactions.size(); i++) {
            final SubTransaction subTransaction = subTransactions.get(i);
            if (subTransaction.xid == subTransactionXid) {
                flush();
                channel.truncate(subTransaction.offset);
                channel.position(subTransaction.offset);
                size = subTransaction.offset;
                messageCount = subTransaction.messageCount;
                subTransactions.subList(i, subTransactions.size()).clear();
                return;
            }
        }
        // a subtransaction without changes has nothing to discard
    }

    /**
     * Hand every spooled message to the handler, in the order they were received.
     */
    void replay(final MessageHandler handler) throws IOException {
        flush();
        channel.position(0);
        // the streams are not closed, closing them would close the channel; the spool is closed after the replay
        final DataInputStream input = new DataInputStream(new BufferedInputStream(Channels.newInputStream(channel), BUFFER_SIZE));
        for (int i = 0; i < messageCount; i++) {
            final long lsn = input.readLong();
            final int length = input.readInt();
            final byte[] bytes = input.readNBytes(length);
            if (bytes.length != length) {
                throw new EOFException(String.format("Spool of streamed transaction %d is truncated", Integer.toUnsignedLong(xid)));
            }
            handler.handle(lsn, ByteBuffer.wrap(bytes));
        }
    }

    /**
     * Close and delete the file.
     */
    @Override
    public void close() throws IOException {
        try {
            channel.close();
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private void flush() throws IOException {
        staging.flip();
        while (staging.hasRemaining()) {
            channel.write(staging);
        }
        staging.clear();
    }

    interface MessageHandler {

        /**
         * @param lsn position the message was received at
         * @param message raw message, positioned at the type byte
         */
        void handle(long lsn, ByteBuffer message) throws IOException;
    }

    /**
     * @param xid subtransaction id
     * @param offset size of the spool before the first message of the subtransaction
     * @param messageCount number of messages before the first message of the subtransaction
     */
    private record SubTransaction(int xid, long offset, int messageCount) {
    }
}
