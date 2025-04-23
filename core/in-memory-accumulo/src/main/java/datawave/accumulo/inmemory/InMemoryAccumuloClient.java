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

import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
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
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.singletons.SingletonReservation;

public class InMemoryAccumuloClient extends ClientContext implements AccumuloClient {
    
    String username;
    private final InMemoryAccumulo acu;
    
    public InMemoryAccumuloClient(String username, InMemoryInstance instance) throws AccumuloSecurityException {
        this(new Credentials(username, new PasswordToken(new byte[0])), instance.acu);
    }
    
    public InMemoryAccumuloClient(Credentials credentials, InMemoryAccumulo acu) throws AccumuloSecurityException {
        super(SingletonReservation.noop(), new InMemoryClientInfo(credentials), DefaultConfiguration.getInstance(), null);
        if (credentials.getToken().isDestroyed())
            throw new AccumuloSecurityException(credentials.getPrincipal(), SecurityErrorCode.TOKEN_EXPIRED);
        this.username = credentials.getPrincipal();
        this.acu = acu;
        if (!acu.users.containsKey(username)) {
            InMemoryUser user = new InMemoryUser(username, new PasswordToken(new byte[0]), Authorizations.EMPTY);
            user.permissions.add(SystemPermission.SYSTEM);
            acu.users.put(user.name, user);
        }
    }
    
    @Override
    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
        if (acu.tables.get(tableName) == null)
            throw new TableNotFoundException(tableName, tableName, "no such table");
        return acu.createBatchScanner(tableName, authorizations);
    }
    
    @Override
    public BatchScanner createBatchScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
        return createBatchScanner(tableName, authorizations, 1);
    }
    
    @Override
    public BatchScanner createBatchScanner(String tableName) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
        return createBatchScanner(tableName, securityOperations().getUserAuthorizations(username));
    }
    
    @Override
    public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, BatchWriterConfig config)
                    throws TableNotFoundException {
        return createBatchDeleter(tableName, authorizations, numQueryThreads);
    }
    
    @Override
    public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
        if (acu.tables.get(tableName) == null)
            throw new TableNotFoundException(tableName, tableName, "no such table");
        return new InMemoryBatchDeleter(acu, tableName, authorizations);
    }
    
    @Override
    public BatchWriter createBatchWriter(String tableName) throws TableNotFoundException {
        if (acu.tables.get(tableName) == null)
            throw new TableNotFoundException(tableName, tableName, "no such table");
        return new InMemoryBatchWriter(acu, tableName);
    }
    
    @Override
    public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config) throws TableNotFoundException {
        return createBatchWriter(tableName);
    }
    
    @Override
    public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
        return createMultiTableBatchWriter();
    }
    
    @Override
    public MultiTableBatchWriter createMultiTableBatchWriter() {
        return new InMemoryMultiTableBatchWriter(acu);
    }
    
    @Override
    public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
        InMemoryTable table = acu.tables.get(tableName);
        if (table == null)
            throw new TableNotFoundException(tableName, tableName, "no such table");
        return new InMemoryScanner(table, authorizations);
    }
    
    @Override
    public Scanner createScanner(String tableName) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
        return createScanner(tableName, securityOperations().getUserAuthorizations(username));
    }
    
    @Override
    public String whoami() {
        return username;
    }
    
    @Override
    public TableOperations tableOperations() {
        return new InMemoryTableOperations(acu, username);
    }
    
    @Override
    public SecurityOperations securityOperations() {
        return new InMemorySecurityOperations(acu);
    }
    
    @Override
    public InstanceOperations instanceOperations() {
        return new InMemoryInstanceOperations(acu);
    }
    
    @Override
    public NamespaceOperations namespaceOperations() {
        return new InMemoryNamespaceOperations(acu, username);
    }
    
    @Override
    public ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config) {
        // TODO add implementation
        throw new UnsupportedOperationException();
    }
    
    @Override
    public ReplicationOperations replicationOperations() {
        // TODO add implementation
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Properties properties() {
        return new Properties();
    }
    
    @Override
    public void close() {}
    
}
