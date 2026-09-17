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

import org.apache.nifi.components.DescribedValue;

/**
 * TLS negotiation modes of the PostgreSQL JDBC driver ({@code sslmode} connection parameter).
 */
public enum SSLMode implements DescribedValue {
    DISABLED("disable", "Disabled", "Connect without TLS."),
    PREFERRED("prefer", "Preferred", "Use TLS when the server supports it, otherwise connect without TLS. The server certificate is not verified."),
    REQUIRED("require", "Required", "Require TLS. The server certificate is not verified unless an SSL Context Service is configured."),
    VERIFY_CA("verify-ca", "Verify CA", "Require TLS and verify that the server certificate is issued by a trusted certificate authority."),
    VERIFY_FULL("verify-full", "Verify Full", "Require TLS, verify the certificate authority and verify that the server hostname matches the certificate.");

    private final String parameterValue;
    private final String displayName;
    private final String description;

    SSLMode(final String parameterValue, final String displayName, final String description) {
        this.parameterValue = parameterValue;
        this.displayName = displayName;
        this.description = description;
    }

    /**
     * @return value of the {@code sslmode} connection parameter
     */
    public String getParameterValue() {
        return parameterValue;
    }

    public boolean isVerifying() {
        return this == VERIFY_CA || this == VERIFY_FULL;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
