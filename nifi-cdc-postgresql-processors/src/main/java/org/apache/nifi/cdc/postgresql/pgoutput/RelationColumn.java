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

/**
 * A column of a relation.
 *
 * @param name column name
 * @param key whether the column is part of the replica identity (primary key or identity index)
 * @param typeId OID of the column data type
 * @param typeModifier type modifier of the column (for example precision and scale of a numeric), or -1
 */
public record RelationColumn(String name, boolean key, int typeId, int typeModifier) {
}
