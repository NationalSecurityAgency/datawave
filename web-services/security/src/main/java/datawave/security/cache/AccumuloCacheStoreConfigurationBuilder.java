package datawave.security.cache;

import static datawave.security.cache.AccumuloCacheStoreConfiguration.INSTANCE_NAME;
import static datawave.security.cache.AccumuloCacheStoreConfiguration.ZOOKEEPERS;
import static datawave.security.cache.AccumuloCacheStoreConfiguration.USERNAME;
import static datawave.security.cache.AccumuloCacheStoreConfiguration.PASSWORD;
import static datawave.security.cache.AccumuloCacheStoreConfiguration.TABLE_NAME;
import static datawave.security.cache.AccumuloCacheStoreConfiguration.AUTHORIZATIONS;
import static datawave.security.cache.AccumuloCacheStoreConfiguration.WRITE_THREADS;
import static datawave.security.cache.AccumuloCacheStoreConfiguration.MAX_LATENCY;
import static datawave.security.cache.AccumuloCacheStoreConfiguration.MAX_MEMORY;
import static datawave.security.cache.AccumuloCacheStoreConfiguration.AGEOFF_TTL;
import static datawave.security.cache.AccumuloCacheStoreConfiguration.AGEOFF_PRIORITY;

import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class AccumuloCacheStoreConfigurationBuilder extends
                AbstractStoreConfigurationBuilder<AccumuloCacheStoreConfiguration,AccumuloCacheStoreConfigurationBuilder> {
    
    public AccumuloCacheStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
        super(builder, AccumuloCacheStoreConfiguration.attributeDefinitionSet());
    }
    
    @Override
    public AccumuloCacheStoreConfigurationBuilder self() {
        return this;
    }
    
    public AccumuloCacheStoreConfigurationBuilder instanceName(String instanceName) {
        attributes.attribute(INSTANCE_NAME).set(instanceName);
        return this;
    }
    
    public AccumuloCacheStoreConfigurationBuilder zookeepers(String zookeepers) {
        attributes.attribute(ZOOKEEPERS).set(zookeepers);
        return this;
    }
    
    public AccumuloCacheStoreConfigurationBuilder username(String username) {
        attributes.attribute(USERNAME).set(username);
        return this;
    }
    
    public AccumuloCacheStoreConfigurationBuilder password(String password) {
        attributes.attribute(PASSWORD).set(password);
        return this;
    }
    
    public AccumuloCacheStoreConfigurationBuilder tableName(String tableName) {
        attributes.attribute(TABLE_NAME).set(tableName);
        return this;
    }
    
    public AccumuloCacheStoreConfigurationBuilder auths(List<String> auths) {
        attributes.attribute(AUTHORIZATIONS).set(auths);
        return this;
    }
    
    public AccumuloCacheStoreConfigurationBuilder writeThreads(int writeThreads) {
        attributes.attribute(WRITE_THREADS).set(writeThreads);
        return this;
    }
    
    public AccumuloCacheStoreConfigurationBuilder maxLatency(long maxLatency) {
        attributes.attribute(MAX_LATENCY).set(maxLatency);
        return this;
    }
    
    public AccumuloCacheStoreConfigurationBuilder maxMemory(long maxMemory) {
        attributes.attribute(MAX_MEMORY).set(maxMemory);
        return this;
    }
    
    public AccumuloCacheStoreConfigurationBuilder ageoffTTLhours(int ageoffTTLhours) {
        attributes.attribute(AGEOFF_TTL).set(ageoffTTLhours);
        return this;
    }
    
    public AccumuloCacheStoreConfigurationBuilder ageoffPriority(int ageoffPriority) {
        attributes.attribute(AGEOFF_PRIORITY).set(ageoffPriority);
        return this;
    }
    
    @Override
    public AccumuloCacheStoreConfiguration create() {
        return new AccumuloCacheStoreConfiguration(attributes.protect(), async.create(), singletonStore.create());
    }
    
    @Override
    public Builder<?> read(AccumuloCacheStoreConfiguration template) {
        super.read(template);
        return this;
    }
}
