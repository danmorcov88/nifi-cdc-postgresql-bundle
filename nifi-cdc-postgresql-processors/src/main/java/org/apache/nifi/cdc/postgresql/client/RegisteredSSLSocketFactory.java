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

import org.postgresql.ssl.WrappedFactory;

import java.util.Properties;

/**
 * {@code sslfactory} implementation for the PostgreSQL JDBC driver that uses the SSL context registered in
 * {@link SSLContextRegistry} under the identifier found in the {@link #SSL_CONTEXT_IDENTIFIER_PROPERTY} connection property.
 */
public class RegisteredSSLSocketFactory extends WrappedFactory {

    public static final String SSL_CONTEXT_IDENTIFIER_PROPERTY = "nifi.ssl.context.identifier";

    public RegisteredSSLSocketFactory(final Properties properties) {
        final String identifier = properties.getProperty(SSL_CONTEXT_IDENTIFIER_PROPERTY);
        if (identifier == null) {
            throw new IllegalArgumentException(String.format("Connection property [%s] not set", SSL_CONTEXT_IDENTIFIER_PROPERTY));
        }
        factory = SSLContextRegistry.get(identifier).getSocketFactory();
    }
}
