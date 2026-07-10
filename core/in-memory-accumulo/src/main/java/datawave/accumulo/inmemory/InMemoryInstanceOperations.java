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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InMemoryInstanceOperations implements InstanceOperations {
    private static final Logger log = LoggerFactory.getLogger(InMemoryInstanceOperations.class);
    InMemoryAccumulo acu;

    public InMemoryInstanceOperations(InMemoryAccumulo acu) {
        this.acu = acu;
    }

    @Override
    public Set<String> getScanServers() {
        return new HashSet<>();
    }

    @Override
    public void setProperty(String property, String value) throws AccumuloException, AccumuloSecurityException {
        acu.setProperty(property, value);
    }

    @Override
    public Map<String,String> modifyProperties(Consumer<Map<String,String>> mapMutator)
                    throws AccumuloException, AccumuloSecurityException, IllegalArgumentException, ConcurrentModificationException {
        mapMutator.accept(acu.systemProperties);
        return acu.systemProperties;
    }

    @Override
    public void removeProperty(String property) throws AccumuloException, AccumuloSecurityException {
        acu.removeProperty(property);
    }

    @Override
    public Map<String,String> getSystemConfiguration() throws AccumuloException, AccumuloSecurityException {
        return acu.systemProperties;
    }

    @Override
    public Map<String,String> getSiteConfiguration() throws AccumuloException, AccumuloSecurityException {
        return acu.systemProperties;
    }

    @Override
    public Map<String,String> getSystemProperties() throws AccumuloException, AccumuloSecurityException {
        return Map.of();
    }

    @Override
    public List<String> getManagerLocations() {
        return null;
    }

    @Override
    public Set<String> getCompactors() {
        return Set.of();
    }

    @Override
    public List<String> getTabletServers() {
        return new ArrayList<>();
    }

    @Override
    public ServerId getServer(ServerId.Type type, ResourceGroupId resourceGroupId, String s, int i) {
        return null;
    }

    @Override
    public Set<ServerId> getServers(ServerId.Type type) {
        return Set.of();
    }

    @Override
    public Set<ServerId> getServers(ServerId.Type type, Predicate<ResourceGroupId> predicate, BiPredicate<String,Integer> biPredicate) {
        return Set.of();
    }

    @Override
    public List<ActiveScan> getActiveScans(String tserver) throws AccumuloException, AccumuloSecurityException {
        return new ArrayList<>();
    }

    @Override
    public List<ActiveScan> getActiveScans(Collection<ServerId> collection) throws AccumuloException, AccumuloSecurityException {
        return List.of();
    }

    @Override
    public boolean testClassLoad(String className, String asTypeName) throws AccumuloException, AccumuloSecurityException {
        try {
            Class.forName(className).asSubclass(Class.forName(asTypeName));
        } catch (ClassNotFoundException | ClassCastException e) {
            log.warn("Could not find class named '" + className + "' in testClassLoad.", e);
            return false;
        }
        return true;
    }

    @Override
    public List<ActiveCompaction> getActiveCompactions(String tserver) throws AccumuloException, AccumuloSecurityException {
        return new ArrayList<>();
    }

    @Override
    public List<ActiveCompaction> getActiveCompactions() throws AccumuloException, AccumuloSecurityException {
        return new ArrayList<>();
    }

    @Override
    public List<ActiveCompaction> getActiveCompactions(Collection<ServerId> collection) throws AccumuloException, AccumuloSecurityException {
        return List.of();
    }

    @Override
    public void ping(String tserver) throws AccumuloException {

    }

    @Override
    public void ping(ServerId serverId) throws AccumuloException {

    }

    @Override
    public void waitForBalance() throws AccumuloException {}

    @Override
    public InstanceId getInstanceId() {
        return InstanceId.of("in-memory-instance");
    }

    @Override
    public Duration getManagerTime() throws AccumuloException, AccumuloSecurityException {
        return null;
    }
}
