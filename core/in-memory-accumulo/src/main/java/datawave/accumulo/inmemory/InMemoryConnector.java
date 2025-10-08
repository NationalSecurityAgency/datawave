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
package datawave.accumulo.inmemory;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;

public class InMemoryConnector {

    String username;
    private final InMemoryAccumulo acu;

     InMemoryConnector(String username, InMemoryInstance instance) throws AccumuloSecurityException {

        this.username = username;
        this.acu = instance.acu;
        if (!acu.users.containsKey(username)) {
            InMemoryUser user = new InMemoryUser(username, new PasswordToken(new byte[0]), Authorizations.EMPTY);
            user.permissions.add(SystemPermission.SYSTEM);
            acu.users.put(user.name, user);
        }
    }

    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
        if (acu.tables.get(tableName) == null)
            throw new TableNotFoundException(tableName, tableName, "no such table");
        return acu.createBatchScanner(tableName, authorizations);
    }

    public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
                    int maxWriteThreads) throws TableNotFoundException {
        if (acu.tables.get(tableName) == null)
            throw new TableNotFoundException(tableName, tableName, "no such table");
        return new InMemoryBatchDeleter(acu, tableName, authorizations);
    }

    public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, BatchWriterConfig config)
                    throws TableNotFoundException {
        return createBatchDeleter(tableName, authorizations, numQueryThreads, config.getMaxMemory(), config.getMaxLatency(TimeUnit.MILLISECONDS),
                        config.getMaxWriteThreads());
    }

    public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
        if (acu.tables.get(tableName) == null)
            throw new TableNotFoundException(tableName, tableName, "no such table");
        return new InMemoryBatchWriter(acu, tableName);
    }

    public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config) throws TableNotFoundException {
        return createBatchWriter(tableName, config.getMaxMemory(), config.getMaxLatency(TimeUnit.MILLISECONDS), config.getMaxWriteThreads());
    }

    public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads) {
        return new InMemoryMultiTableBatchWriter(acu);
    }

    public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
        return createMultiTableBatchWriter(config.getMaxMemory(), config.getMaxLatency(TimeUnit.MILLISECONDS), config.getMaxWriteThreads());
    }

    public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
        InMemoryTable table = acu.tables.get(tableName);
        if (table == null)
            throw new TableNotFoundException(tableName, tableName, "no such table");
        return new InMemoryScanner(table, authorizations);
    }

    public String whoami() {
        return username;
    }

    public TableOperations tableOperations() {
        return new InMemoryTableOperations(acu, username);
    }

    public SecurityOperations securityOperations() {
        return new InMemorySecurityOperations(acu);
    }

    public InstanceOperations instanceOperations() {
        return new InMemoryInstanceOperations(acu);
    }

    public NamespaceOperations namespaceOperations() {
        return new InMemoryNamespaceOperations(acu, username);
    }

    public ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config) throws TableNotFoundException {
        // TODO add implementation
        throw new UnsupportedOperationException();
    }

}
