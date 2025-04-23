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

import java.util.EnumSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

class InMemorySecurityOperations implements SecurityOperations {
    
    final private InMemoryAccumulo acu;
    
    InMemorySecurityOperations(InMemoryAccumulo acu) {
        this.acu = acu;
    }
    
    @Override
    public void createLocalUser(String principal, PasswordToken password) throws AccumuloException, AccumuloSecurityException {
        this.acu.users.put(principal, new InMemoryUser(principal, password, new Authorizations()));
    }
    
    @Override
    public void dropLocalUser(String principal) throws AccumuloException, AccumuloSecurityException {
        this.acu.users.remove(principal);
    }
    
    @Override
    public boolean authenticateUser(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
        InMemoryUser user = acu.users.get(principal);
        if (user == null)
            return false;
        return user.token.equals(token);
    }
    
    @Override
    public void changeLocalUserPassword(String principal, PasswordToken token) throws AccumuloException, AccumuloSecurityException {
        InMemoryUser user = acu.users.get(principal);
        if (user != null)
            user.token = token.clone();
        else
            throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
    }
    
    @Override
    public void changeUserAuthorizations(String principal, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
        InMemoryUser user = acu.users.get(principal);
        if (user != null)
            user.authorizations = authorizations;
        else
            throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
    }
    
    @Override
    public Authorizations getUserAuthorizations(String principal) throws AccumuloException, AccumuloSecurityException {
        InMemoryUser user = acu.users.get(principal);
        if (user != null)
            return user.authorizations;
        else
            throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
    }
    
    @Override
    public boolean hasSystemPermission(String principal, SystemPermission perm) throws AccumuloException, AccumuloSecurityException {
        InMemoryUser user = acu.users.get(principal);
        if (user != null)
            return user.permissions.contains(perm);
        else
            throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
    }
    
    @Override
    public boolean hasTablePermission(String principal, String tableName, TablePermission perm) throws AccumuloException, AccumuloSecurityException {
        InMemoryTable table = acu.tables.get(tableName);
        if (table == null)
            throw new AccumuloSecurityException(tableName, SecurityErrorCode.TABLE_DOESNT_EXIST);
        EnumSet<TablePermission> perms = table.userPermissions.get(principal);
        if (perms == null)
            return false;
        return perms.contains(perm);
    }
    
    @Override
    public boolean hasNamespacePermission(String principal, String namespace, NamespacePermission permission)
                    throws AccumuloException, AccumuloSecurityException {
        InMemoryNamespace mockNamespace = acu.namespaces.get(namespace);
        if (mockNamespace == null)
            throw new AccumuloSecurityException(namespace, SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
        EnumSet<NamespacePermission> perms = mockNamespace.userPermissions.get(principal);
        if (perms == null)
            return false;
        return perms.contains(permission);
    }
    
    @Override
    public void grantSystemPermission(String principal, SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
        InMemoryUser user = acu.users.get(principal);
        if (user != null)
            user.permissions.add(permission);
        else
            throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
    }
    
    @Override
    public void grantTablePermission(String principal, String tableName, TablePermission permission) throws AccumuloException, AccumuloSecurityException {
        if (acu.users.get(principal) == null)
            throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
        InMemoryTable table = acu.tables.get(tableName);
        if (table == null)
            throw new AccumuloSecurityException(tableName, SecurityErrorCode.TABLE_DOESNT_EXIST);
        EnumSet<TablePermission> perms = table.userPermissions.get(principal);
        if (perms == null)
            table.userPermissions.put(principal, EnumSet.of(permission));
        else
            perms.add(permission);
    }
    
    @Override
    public void grantNamespacePermission(String principal, String namespace, NamespacePermission permission)
                    throws AccumuloException, AccumuloSecurityException {
        if (acu.users.get(principal) == null)
            throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
        InMemoryNamespace mockNamespace = acu.namespaces.get(namespace);
        if (mockNamespace == null)
            throw new AccumuloSecurityException(namespace, SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
        EnumSet<NamespacePermission> perms = mockNamespace.userPermissions.get(principal);
        if (perms == null)
            mockNamespace.userPermissions.put(principal, EnumSet.of(permission));
        else
            perms.add(permission);
    }
    
    @Override
    public void revokeSystemPermission(String principal, SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
        InMemoryUser user = acu.users.get(principal);
        if (user != null)
            user.permissions.remove(permission);
        else
            throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
    }
    
    @Override
    public void revokeTablePermission(String principal, String tableName, TablePermission permission) throws AccumuloException, AccumuloSecurityException {
        if (acu.users.get(principal) == null)
            throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
        InMemoryTable table = acu.tables.get(tableName);
        if (table == null)
            throw new AccumuloSecurityException(tableName, SecurityErrorCode.TABLE_DOESNT_EXIST);
        EnumSet<TablePermission> perms = table.userPermissions.get(principal);
        if (perms != null)
            perms.remove(permission);
        
    }
    
    @Override
    public void revokeNamespacePermission(String principal, String namespace, NamespacePermission permission)
                    throws AccumuloException, AccumuloSecurityException {
        if (acu.users.get(principal) == null)
            throw new AccumuloSecurityException(principal, SecurityErrorCode.USER_DOESNT_EXIST);
        InMemoryNamespace mockNamespace = acu.namespaces.get(namespace);
        if (mockNamespace == null)
            throw new AccumuloSecurityException(namespace, SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
        EnumSet<NamespacePermission> perms = mockNamespace.userPermissions.get(principal);
        if (perms != null)
            perms.remove(permission);
        
    }
    
    @Override
    public Set<String> listLocalUsers() throws AccumuloException, AccumuloSecurityException {
        return acu.users.keySet();
    }
    
    @Override
    public DelegationToken getDelegationToken(DelegationTokenConfig cfg) throws AccumuloException, AccumuloSecurityException {
        return null;
    }
    
}
