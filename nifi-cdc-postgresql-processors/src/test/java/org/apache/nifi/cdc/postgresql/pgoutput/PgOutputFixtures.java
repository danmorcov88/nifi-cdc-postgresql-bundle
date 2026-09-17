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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;

/**
 * Loads pgoutput messages recorded by docker/capture-fixtures.sh from src/test/resources/pgoutput.
 */
public final class PgOutputFixtures {

    public static final List<String> SERVERS = List.of("pg14", "pg18");

    private PgOutputFixtures() {
    }

    /**
     * @param server fixture directory, one of {@link #SERVERS}
     * @param scenario fixture name without extension
     * @return one buffer per recorded message, in the order the server produced them
     */
    public static List<ByteBuffer> load(final String server, final String scenario) {
        final String resource = String.format("/pgoutput/%s/%s.hex", server, scenario);
        try (InputStream stream = PgOutputFixtures.class.getResourceAsStream(resource)) {
            if (stream == null) {
                throw new IllegalArgumentException("Fixture not found: " + resource);
            }
            final String content = new String(stream.readAllBytes(), StandardCharsets.US_ASCII);
            return content.lines()
                    .filter(line -> !line.isBlank())
                    .map(line -> ByteBuffer.wrap(HexFormat.of().parseHex(line.strip())))
                    .toList();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<PgOutputMessage> decode(final String server, final String scenario) {
        final PgOutputDecoder decoder = new PgOutputDecoder();
        return load(server, scenario).stream().map(decoder::decode).toList();
    }
}
