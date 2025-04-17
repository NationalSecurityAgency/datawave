package datawave.microservice.query.messaging.config;

import static datawave.microservice.query.messaging.config.MessagingProperties.AutoOffsetReset.EARLIEST;
import static datawave.microservice.query.messaging.hazelcast.HazelcastQueryResultsManager.HAZELCAST;
import static datawave.microservice.query.messaging.kafka.KafkaQueryResultsManager.KAFKA;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "datawave.query.messaging")
public class MessagingProperties {
    // rabbit, kafka, or hazelcast
    @NotEmpty
    private String backend = KAFKA;
    
    // the number of concurrent listeners to use per query (applicable to kafka and rabbitmq only)
    @Positive
    private int concurrency = 1;
    
    @Valid
    private KafkaProperties kafka = new KafkaProperties();
    
    @Valid
    private RabbitMQProperties rabbitmq = new RabbitMQProperties();
    
    @Valid
    private HazelcastProperties hazelcast = new HazelcastProperties();
    
    private ClaimCheckProperties claimCheck = new ClaimCheckProperties();
    
    public String getBackend() {
        return backend;
    }
    
    public void setBackend(String backend) {
        this.backend = backend;
    }
    
    public int getConcurrency() {
        return concurrency;
    }
    
    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }
    
    public KafkaProperties getKafka() {
        return kafka;
    }
    
    public void setKafka(KafkaProperties kafka) {
        this.kafka = kafka;
    }
    
    public RabbitMQProperties getRabbitmq() {
        return rabbitmq;
    }
    
    public void setRabbitmq(RabbitMQProperties rabbitmq) {
        this.rabbitmq = rabbitmq;
    }
    
    public HazelcastProperties getHazelcast() {
        return hazelcast;
    }
    
    public void setHazelcast(HazelcastProperties hazelcast) {
        this.hazelcast = hazelcast;
    }
    
    public ClaimCheckProperties getClaimCheck() {
        return claimCheck;
    }
    
    public void setClaimCheck(ClaimCheckProperties claimCheck) {
        this.claimCheck = claimCheck;
    }
    
    public static final class KafkaProperties {
        // max time to block in the consumer waiting for records
        @PositiveOrZero
        private long pollTimeoutMillis = 500L;
        
        // sleep interval between consumer.poll calls
        @PositiveOrZero
        private long idleBetweenPollsMillis = 0L;
        
        private int partitions = -1;
        
        private int replicas = -1;
        
        private boolean useDedicatedInstance = false;
        private KafkaInstanceSettings instanceSettings = new KafkaInstanceSettings();
        
        public long getPollTimeoutMillis() {
            return pollTimeoutMillis;
        }
        
        public void setPollTimeoutMillis(long pollTimeoutMillis) {
            this.pollTimeoutMillis = pollTimeoutMillis;
        }
        
        public long getIdleBetweenPollsMillis() {
            return idleBetweenPollsMillis;
        }
        
        public void setIdleBetweenPollsMillis(long idleBetweenPollsMillis) {
            this.idleBetweenPollsMillis = idleBetweenPollsMillis;
        }
        
        public int getPartitions() {
            return partitions;
        }
        
        public void setPartitions(int partitions) {
            this.partitions = partitions;
        }
        
        public int getReplicas() {
            return replicas;
        }
        
        public void setReplicas(int replicas) {
            this.replicas = replicas;
        }
        
        public boolean isUseDedicatedInstance() {
            return useDedicatedInstance;
        }
        
        public void setUseDedicatedInstance(boolean useDedicatedInstance) {
            this.useDedicatedInstance = useDedicatedInstance;
        }
        
        public KafkaInstanceSettings getInstanceSettings() {
            return instanceSettings;
        }
        
        public void setInstanceSettings(KafkaInstanceSettings instanceSettings) {
            this.instanceSettings = instanceSettings;
        }
    }
    
    public final static class RabbitMQProperties {
        // whether the queues should be persisted to disk
        @NotNull
        private boolean durable = true;
        
        // the maximum message size to allow before using a claim check
        @Positive
        private long maxMessageSizeBytes = 536870912L;
        
        private boolean useDedicatedInstance = false;
        private RabbitMQInstanceSettings instanceSettings = new RabbitMQInstanceSettings();
        
        public boolean isDurable() {
            return durable;
        }
        
        public void setDurable(boolean durable) {
            this.durable = durable;
        }
        
        public long getMaxMessageSizeBytes() {
            return maxMessageSizeBytes;
        }
        
        public void setMaxMessageSizeBytes(long maxMessageSizeBytes) {
            this.maxMessageSizeBytes = maxMessageSizeBytes;
        }
        
        public boolean isUseDedicatedInstance() {
            return useDedicatedInstance;
        }
        
        public void setUseDedicatedInstance(boolean useDedicatedInstance) {
            this.useDedicatedInstance = useDedicatedInstance;
        }
        
        public RabbitMQInstanceSettings getInstanceSettings() {
            return instanceSettings;
        }
        
        public void setInstanceSettings(RabbitMQInstanceSettings instanceSettings) {
            this.instanceSettings = instanceSettings;
        }
    }
    
    public final static class RabbitMQInstanceSettings {
        private String host = null;
        private int port = 5672;
        private String username = null;
        private String password = null;
        private String virtualHost = null;
        private CachingConnectionFactory.ConfirmType publisherConfirmType = CachingConnectionFactory.ConfirmType.SIMPLE;
        private boolean publisherConfirms = true;
        private boolean publisherReturns = true;
        
        public String getHost() {
            return host;
        }
        
        public void setHost(String host) {
            this.host = host;
        }
        
        public int getPort() {
            return port;
        }
        
        public void setPort(int port) {
            this.port = port;
        }
        
        public String getUsername() {
            return username;
        }
        
        public void setUsername(String username) {
            this.username = username;
        }
        
        public String getPassword() {
            return password;
        }
        
        public void setPassword(String password) {
            this.password = password;
        }
        
        public String getVirtualHost() {
            return virtualHost;
        }
        
        public void setVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
        }
        
        public CachingConnectionFactory.ConfirmType getPublisherConfirmType() {
            return publisherConfirmType;
        }
        
        public void setPublisherConfirmType(CachingConnectionFactory.ConfirmType publisherConfirmType) {
            this.publisherConfirmType = publisherConfirmType;
        }
        
        public boolean isPublisherConfirms() {
            return publisherConfirms;
        }
        
        public void setPublisherConfirms(boolean publisherConfirms) {
            this.publisherConfirms = publisherConfirms;
        }
        
        public boolean isPublisherReturns() {
            return publisherReturns;
        }
        
        public void setPublisherReturns(boolean publisherReturns) {
            this.publisherReturns = publisherReturns;
        }
    }
    
    public final static class KafkaInstanceSettings {
        private String bootstrapServers = null;
        private AutoOffsetReset autoOffsetReset = EARLIEST;
        private Boolean enableAutoCommit = false;
        private Boolean allowAutoCreateTopics = false;
        
        public String getBootstrapServers() {
            return bootstrapServers;
        }
        
        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }
        
        public AutoOffsetReset getAutoOffsetReset() {
            return autoOffsetReset;
        }
        
        public void setAutoOffsetReset(AutoOffsetReset autoOffsetReset) {
            this.autoOffsetReset = autoOffsetReset;
        }
        
        public Boolean isEnableAutoCommit() {
            return enableAutoCommit;
        }
        
        public void setEnableAutoCommit(Boolean enableAutoCommit) {
            this.enableAutoCommit = enableAutoCommit;
        }
        
        public Boolean isAllowAutoCreateTopics() {
            return allowAutoCreateTopics;
        }
        
        public void setAllowAutoCreateTopics(Boolean allowAutoCreateTopics) {
            this.allowAutoCreateTopics = allowAutoCreateTopics;
        }
    }
    
    public enum AutoOffsetReset {
        EARLIEST, LATEST, NONE, ANYTHING
    }
    
    public final static class HazelcastProperties {
        // the number of backups to keep for each queue
        @PositiveOrZero
        private int backupCount = 1;
        
        public int getBackupCount() {
            return backupCount;
        }
        
        public void setBackupCount(int backupCount) {
            this.backupCount = backupCount;
        }
    }
    
    public final static class ClaimCheckProperties {
        // whether claim check should be used for large messages
        private boolean enabled = true;
        
        // the backend to use for a claim check
        private String backend = HAZELCAST;
        
        public boolean isEnabled() {
            return enabled;
        }
        
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
        
        public String getBackend() {
            return backend;
        }
        
        public void setBackend(String backend) {
            this.backend = backend;
        }
    }
}
