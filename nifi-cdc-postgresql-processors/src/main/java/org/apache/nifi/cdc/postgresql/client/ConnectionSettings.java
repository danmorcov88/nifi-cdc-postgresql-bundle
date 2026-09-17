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

import org.postgresql.PGProperty;

import java.time.Duration;
import java.util.Properties;

/**
 * Parameters of a PostgreSQL replication connection.
 *
 * @param hostname server hostname
 * @param port server port
 * @param database database name
 * @param username user with the REPLICATION privilege
 * @param password password of the user
 * @param connectionTimeout timeout for establishing the connection
 * @param sslMode TLS negotiation mode
 * @param sslContextIdentifier identifier of a context registered in {@link SSLContextRegistry}, or null to use the driver defaults
 * @param applicationName value reported in {@code pg_stat_activity.application_name}
 */
public record ConnectionSettings(String hostname, int port, String database, String username, String password, Duration connectionTimeout,
                                 SSLMode sslMode, String sslContextIdentifier, String applicationName) {

    /** Oldest server version the driver may assume; required for the replication protocol. */
    private static final String MINIMUM_ASSUMED_SERVER_VERSION = "9.4";

    public String getJdbcUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s", hostname, port, database);
    }

    /**
     * @return driver properties for a logical replication connection ({@code replication=database}, simple query protocol)
     */
    public Properties getReplicationProperties() {
        final Properties properties = new Properties();
        PGProperty.USER.set(properties, username);
        if (password != null) {
            PGProperty.PASSWORD.set(properties, password);
        }
        PGProperty.APPLICATION_NAME.set(properties, applicationName);
        PGProperty.CONNECT_TIMEOUT.set(properties, (int) connectionTimeout.toSeconds());
        PGProperty.LOGIN_TIMEOUT.set(properties, (int) connectionTimeout.toSeconds());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, MINIMUM_ASSUMED_SERVER_VERSION);
        PGProperty.REPLICATION.set(properties, "database");
        PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
        PGProperty.SSL_MODE.set(properties, sslMode.getParameterValue());
        if (sslContextIdentifier != null) {
            PGProperty.SSL_FACTORY.set(properties, RegisteredSSLSocketFactory.class.getName());
            properties.setProperty(RegisteredSSLSocketFactory.SSL_CONTEXT_IDENTIFIER_PROPERTY, sslContextIdentifier);
        }
        return properties;
    }
}
