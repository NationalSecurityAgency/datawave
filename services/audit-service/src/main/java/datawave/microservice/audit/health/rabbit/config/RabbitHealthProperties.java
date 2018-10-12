package datawave.microservice.audit.health.rabbit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ConfigurationProperties(prefix = "audit.health.rabbit")
public class RabbitHealthProperties {
    private boolean enabled = false;
    
    private long healthyPollIntervalMillis = TimeUnit.SECONDS.toMillis(30);
    private long unhealthyPollIntervalMillis = TimeUnit.SECONDS.toMillis(5);
    
    private boolean attemptRecovery = true;
    private boolean fixMissing = true;
    private boolean fixInvalid = true;
    
    private boolean includeQueueSizeStats = true;
    
    private ClusterProperties cluster = new ClusterProperties();
    private ManagementProperties management = new ManagementProperties();
    
    private List<QueueProperties> queues = new ArrayList<>();
    private List<ExchangeProperties> exchanges = new ArrayList<>();
    private List<BindingProperties> bindings = new ArrayList<>();
    
    public static class ClusterProperties {
        private int expectedNodes = 3;
        private int numChecksBeforeFailure = 2;
        private boolean failIfNodeMissing = true;
        
        public int getExpectedNodes() {
            return expectedNodes;
        }
        
        public void setExpectedNodes(int expectedNodes) {
            this.expectedNodes = expectedNodes;
        }
        
        public int getNumChecksBeforeFailure() {
            return numChecksBeforeFailure;
        }
        
        public void setNumChecksBeforeFailure(int numChecksBeforeFailure) {
            this.numChecksBeforeFailure = numChecksBeforeFailure;
        }
        
        public boolean isFailIfNodeMissing() {
            return failIfNodeMissing;
        }
        
        public void setFailIfNodeMissing(boolean failIfNodeMissing) {
            this.failIfNodeMissing = failIfNodeMissing;
        }
    }
    
    public static class ManagementProperties {
        private String scheme = "http";
        private String host = "";
        private String username = "";
        private String password = "";
        private int port = 15672;
        private String uri = "/api/";
        
        public String getScheme() {
            return scheme;
        }
        
        public void setScheme(String scheme) {
            this.scheme = scheme;
        }
        
        public String getHost() {
            return host;
        }
        
        public void setHost(String host) {
            this.host = host;
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
        
        public int getPort() {
            return port;
        }
        
        public void setPort(int port) {
            this.port = port;
        }
        
        public String getUri() {
            return uri;
        }
        
        public void setUri(String uri) {
            this.uri = uri;
        }
    }
    
    public static class QueueProperties {
        private String name;
        private boolean durable;
        private boolean exclusive;
        private boolean autoDelete;
        private Map<String,Object> arguments;
        
        public String getName() {
            return name;
        }
        
        public void setName(String name) {
            this.name = name;
        }
        
        public boolean isDurable() {
            return durable;
        }
        
        public void setDurable(boolean durable) {
            this.durable = durable;
        }
        
        public boolean isExclusive() {
            return exclusive;
        }
        
        public void setExclusive(boolean exclusive) {
            this.exclusive = exclusive;
        }
        
        public boolean isAutoDelete() {
            return autoDelete;
        }
        
        public void setAutoDelete(boolean autoDelete) {
            this.autoDelete = autoDelete;
        }
        
        public Map<String,Object> getArguments() {
            return arguments;
        }
        
        public void setArguments(Map<String,Object> arguments) {
            this.arguments = arguments;
        }
    }
    
    public static class ExchangeProperties {
        private String name;
        private String type;
        private boolean durable;
        private boolean autoDelete;
        private boolean internal;
        private boolean delayed;
        
        public String getName() {
            return name;
        }
        
        public void setName(String name) {
            this.name = name;
        }
        
        public String getType() {
            return type;
        }
        
        public void setType(String type) {
            this.type = type;
        }
        
        public boolean isDurable() {
            return durable;
        }
        
        public void setDurable(boolean durable) {
            this.durable = durable;
        }
        
        public boolean isAutoDelete() {
            return autoDelete;
        }
        
        public void setAutoDelete(boolean autoDelete) {
            this.autoDelete = autoDelete;
        }
        
        public boolean isInternal() {
            return internal;
        }
        
        public void setInternal(boolean internal) {
            this.internal = internal;
        }
        
        public boolean isDelayed() {
            return delayed;
        }
        
        public void setDelayed(boolean delayed) {
            this.delayed = delayed;
        }
    }
    
    public static class BindingProperties {
        private String destination;
        private String destinationType;
        private String exchange;
        private String routingKey;
        private Map<String,Object> arguments;
        
        public String getDestination() {
            return destination;
        }
        
        public void setDestination(String destination) {
            this.destination = destination;
        }
        
        public String getDestinationType() {
            return destinationType;
        }
        
        public void setDestinationType(String destinationType) {
            this.destinationType = destinationType;
        }
        
        public String getExchange() {
            return exchange;
        }
        
        public void setExchange(String exchange) {
            this.exchange = exchange;
        }
        
        public String getRoutingKey() {
            return routingKey;
        }
        
        public void setRoutingKey(String routingKey) {
            this.routingKey = routingKey;
        }
        
        public Map<String,Object> getArguments() {
            return arguments;
        }
        
        public void setArguments(Map<String,Object> arguments) {
            this.arguments = arguments;
        }
    }
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public long getHealthyPollIntervalMillis() {
        return healthyPollIntervalMillis;
    }
    
    public void setHealthyPollIntervalMillis(long healthyPollIntervalMillis) {
        this.healthyPollIntervalMillis = healthyPollIntervalMillis;
    }
    
    public long getUnhealthyPollIntervalMillis() {
        return unhealthyPollIntervalMillis;
    }
    
    public void setUnhealthyPollIntervalMillis(long unhealthyPollIntervalMillis) {
        this.unhealthyPollIntervalMillis = unhealthyPollIntervalMillis;
    }
    
    public boolean isAttemptRecovery() {
        return attemptRecovery;
    }
    
    public void setAttemptRecovery(boolean attemptRecovery) {
        this.attemptRecovery = attemptRecovery;
    }
    
    public boolean isFixMissing() {
        return fixMissing;
    }
    
    public void setFixMissing(boolean fixMissing) {
        this.fixMissing = fixMissing;
    }
    
    public boolean isFixInvalid() {
        return fixInvalid;
    }
    
    public void setFixInvalid(boolean fixInvalid) {
        this.fixInvalid = fixInvalid;
    }
    
    public boolean isIncludeQueueSizeStats() {
        return includeQueueSizeStats;
    }
    
    public void setIncludeQueueSizeStats(boolean includeQueueSizeStats) {
        this.includeQueueSizeStats = includeQueueSizeStats;
    }
    
    public ClusterProperties getCluster() {
        return cluster;
    }
    
    public void setCluster(ClusterProperties cluster) {
        this.cluster = cluster;
    }
    
    public ManagementProperties getManagement() {
        return management;
    }
    
    public void setManagement(ManagementProperties management) {
        this.management = management;
    }
    
    public List<QueueProperties> getQueues() {
        return queues;
    }
    
    public void setQueues(List<QueueProperties> queues) {
        this.queues = queues;
    }
    
    public List<ExchangeProperties> getExchanges() {
        return exchanges;
    }
    
    public void setExchanges(List<ExchangeProperties> exchanges) {
        this.exchanges = exchanges;
    }
    
    public List<BindingProperties> getBindings() {
        return bindings;
    }
    
    public void setBindings(List<BindingProperties> bindings) {
        this.bindings = bindings;
    }
}
