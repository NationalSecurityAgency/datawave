package datawave.security.cache;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;

@BuiltBy(AccumuloCacheStoreConfigurationBuilder.class)
@ConfigurationFor(AccumuloCacheStore.class)
public class AccumuloCacheStoreConfiguration extends AbstractStoreConfiguration {
    public static final AttributeDefinition<String> INSTANCE_NAME = AttributeDefinition.builder("instanceName", null, String.class).immutable().build();
    public static final AttributeDefinition<String> ZOOKEEPERS = AttributeDefinition.builder("zookeeperHosts", null, String.class).immutable().build();
    public static final AttributeDefinition<String> USERNAME = AttributeDefinition.builder("username", null, String.class).immutable().build();
    public static final AttributeDefinition<String> PASSWORD = AttributeDefinition.builder("password", null, String.class).immutable().build();
    public static final AttributeDefinition<String> TABLE_NAME = AttributeDefinition.builder("tableName", "AuthorizationServiceCache").immutable().build();
    public static final AttributeDefinition<List<String>> AUTHORIZATIONS = AttributeDefinition.builder("authorizations", (List<String>) new ArrayList<String>())
                    .immutable().build();
    public static final AttributeDefinition<Integer> WRITE_THREADS = AttributeDefinition.builder("writeThreads", 4).immutable().build();
    public static final AttributeDefinition<Long> MAX_LATENCY = AttributeDefinition.builder("maxLatencySeconds", 5L).immutable().build();
    public static final AttributeDefinition<Long> MAX_MEMORY = AttributeDefinition.builder("maxMemoryBytes", 262144L).immutable().build();
    public static final AttributeDefinition<Integer> AGEOFF_TTL = AttributeDefinition.builder("ageoffTTLhours", 24).immutable().build();
    public static final AttributeDefinition<Integer> AGEOFF_PRIORITY = AttributeDefinition.builder("ageoffPriority", 19).immutable().build();

    public static AttributeSet attributeDefinitionSet() {
        return new AttributeSet(AccumuloCacheStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), INSTANCE_NAME, ZOOKEEPERS, USERNAME,
                        PASSWORD, TABLE_NAME, AUTHORIZATIONS, WRITE_THREADS, MAX_LATENCY, MAX_MEMORY, AGEOFF_TTL, AGEOFF_PRIORITY);
    }

    private Attribute<String> instanceName;
    private Attribute<String> zookeepers;
    private Attribute<String> username;
    private Attribute<String> password;
    private Attribute<String> tableName;
    private Attribute<List<String>> auths;
    private Attribute<Integer> writeThreads;
    private Attribute<Long> maxLatency;
    private Attribute<Long> maxMemory;
    private Attribute<Integer> ageoffTTLhours;
    private Attribute<Integer> ageoffPriority;

    public AccumuloCacheStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
        super(attributes, async, singletonStore);
        instanceName = attributes.attribute(INSTANCE_NAME);
        zookeepers = attributes.attribute(ZOOKEEPERS);
        username = attributes.attribute(USERNAME);
        password = attributes.attribute(PASSWORD);
        tableName = attributes.attribute(TABLE_NAME);
        auths = attributes.attribute(AUTHORIZATIONS);
        writeThreads = attributes.attribute(WRITE_THREADS);
        maxLatency = attributes.attribute(MAX_LATENCY);
        maxMemory = attributes.attribute(MAX_MEMORY);
        ageoffTTLhours = attributes.attribute(AGEOFF_TTL);
        ageoffPriority = attributes.attribute(AGEOFF_PRIORITY);
    }

    public String instanceName() {
        return instanceName.get();
    }

    public String zookeepers() {
        return zookeepers.get();
    }

    public String username() {
        return username.get();
    }

    public String password() {
        return password.get();
    }

    public String tableName() {
        return tableName.get();
    }

    public List<String> auths() {
        return auths.get();
    }

    public int writeThreads() {
        return writeThreads.get();
    }

    public long maxLatency() {
        return maxLatency.get();
    }

    public long maxMemory() {
        return maxMemory.get();
    }

    public int ageoffTTLhours() {
        return ageoffTTLhours.get();
    }

    public int ageoffPriority() {
        return ageoffPriority.get();
    }

    @Override
    @SuppressWarnings("RedundantIfStatement")
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        AccumuloCacheStoreConfiguration that = (AccumuloCacheStoreConfiguration) o;

        if (instanceName != null ? !instanceName.equals(that.instanceName) : that.instanceName != null)
            return false;
        if (zookeepers != null ? !zookeepers.equals(that.zookeepers) : that.zookeepers != null)
            return false;
        if (username != null ? !username.equals(that.username) : that.username != null)
            return false;
        if (password != null ? !password.equals(that.password) : that.password != null)
            return false;
        if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null)
            return false;
        if (auths != null ? !auths.equals(that.auths) : that.auths != null)
            return false;
        if (writeThreads != null ? !writeThreads.equals(that.writeThreads) : that.writeThreads != null)
            return false;
        if (maxLatency != null ? !maxLatency.equals(that.maxLatency) : that.maxLatency != null)
            return false;
        if (maxMemory != null ? !maxMemory.equals(that.maxMemory) : that.maxMemory != null)
            return false;
        if (ageoffTTLhours != null ? !ageoffTTLhours.equals(that.ageoffTTLhours) : that.ageoffTTLhours != null)
            return false;
        if (ageoffPriority != null ? !ageoffPriority.equals(that.ageoffPriority) : that.ageoffPriority != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (instanceName != null ? instanceName.hashCode() : 0);
        result = 31 * result + (zookeepers != null ? zookeepers.hashCode() : 0);
        result = 31 * result + (username != null ? username.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
        result = 31 * result + (auths != null ? auths.hashCode() : 0);
        result = 31 * result + (writeThreads != null ? writeThreads.hashCode() : 0);
        result = 31 * result + (maxLatency != null ? maxLatency.hashCode() : 0);
        result = 31 * result + (maxMemory != null ? maxMemory.hashCode() : 0);
        result = 31 * result + (ageoffTTLhours != null ? ageoffTTLhours.hashCode() : 0);
        result = 31 * result + (ageoffPriority != null ? ageoffPriority.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AccumuloCacheStoreConfiguration [attributes=" + attributes + "]";
    }
}
