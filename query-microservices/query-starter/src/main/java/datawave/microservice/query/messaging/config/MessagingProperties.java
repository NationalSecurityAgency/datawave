package datawave.microservice.query.messaging.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;
import java.util.concurrent.TimeUnit;

import static datawave.microservice.query.messaging.hazelcast.HazelcastQueryResultsManager.HAZELCAST;
import static datawave.microservice.query.messaging.kafka.KafkaQueryResultsManager.KAFKA;

@Validated
@ConfigurationProperties(prefix = "query.messaging")
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
        
        @PositiveOrZero
        private long sendTimeout = 10;
        
        @NotNull
        private TimeUnit sendTimeUnit = TimeUnit.SECONDS;
        
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
        
        public long getSendTimeout() {
            return sendTimeout;
        }
        
        public long getSendTimeoutMillis() {
            return sendTimeUnit.toMillis(sendTimeout);
        }
        
        public void setSendTimeout(long sendTimeout) {
            this.sendTimeout = sendTimeout;
        }
        
        public TimeUnit getSendTimeUnit() {
            return sendTimeUnit;
        }
        
        public void setSendTimeUnit(TimeUnit sendTimeUnit) {
            this.sendTimeUnit = sendTimeUnit;
        }
    }
    
    public final static class RabbitMQProperties {
        // whether the queues should be persisted to disk
        @NotNull
        private boolean durable = true;
        
        // the maximum message size to allow before using a claim check
        @Positive
        private long maxMessageSizeBytes = 536870912L;
        
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
