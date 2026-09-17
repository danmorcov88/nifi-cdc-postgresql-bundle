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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLContext;

/**
 * Hands NiFi SSL contexts to the PostgreSQL JDBC driver, which instantiates its {@code sslfactory} class by name
 * and can only pass connection properties to it. A processor registers its context under an identifier, passes the
 * identifier as a connection property and unregisters the context when stopped.
 */
public final class SSLContextRegistry {

    private static final Map<String, SSLContext> CONTEXTS = new ConcurrentHashMap<>();

    private SSLContextRegistry() {
    }

    public static void register(final String identifier, final SSLContext sslContext) {
        CONTEXTS.put(identifier, sslContext);
    }

    public static void unregister(final String identifier) {
        CONTEXTS.remove(identifier);
    }

    /**
     * @throws IllegalArgumentException when no context is registered under the identifier
     */
    public static SSLContext get(final String identifier) {
        final SSLContext sslContext = CONTEXTS.get(identifier);
        if (sslContext == null) {
            throw new IllegalArgumentException(String.format("No SSL context registered for identifier [%s]", identifier));
        }
        return sslContext;
    }
}
