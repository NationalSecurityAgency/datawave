package datawave.microservice.annotation.health.rabbit.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

@EnableConfigurationProperties(RabbitHealthProperties.class)
@ConfigurationProperties(prefix = "annotation.health.rabbit")
@Getter
@Setter
public class RabbitHealthProperties {
    private boolean enabled = false;

    private long healthyPollIntervalMillis = TimeUnit.SECONDS.toMillis(30);
    private long unhealthyPollIntervalMillis = TimeUnit.SECONDS.toMillis(5);

    private boolean attemptRecovery = true;
    private boolean fixMissing = true;
    private boolean fixInvalid = true;

    private boolean includeQueueSizeStats = true;

    private final ClusterProperties cluster = new ClusterProperties();
    private final ManagementProperties management = new ManagementProperties();

    private final List<QueueProperties> queues = new ArrayList<>();
    private final List<ExchangeProperties> exchanges = new ArrayList<>();
    private final List<BindingProperties> bindings = new ArrayList<>();

    @Getter
    @Setter
    public static class ClusterProperties {
        private int expectedNodes = 3;
        private int numChecksBeforeFailure = 2;
        private boolean failIfNodeMissing = true;
    }

    @Getter
    @Setter
    public static class ManagementProperties {
        private String scheme = "http";
        private String host = "";
        private String username = "";
        private String password = "";
        private int port = 15672;
        private String uri = "/api/";
    }

    @Getter
    @Setter
    public static class QueueProperties {
        private String name;
        private boolean durable;
        private boolean exclusive;
        private boolean autoDelete;
        private Map<String,Object> arguments;
    }

    @Getter
    @Setter
    public static class ExchangeProperties {
        private String name;
        private String type;
        private boolean durable;
        private boolean autoDelete;
        private boolean internal;
        private boolean delayed;
    }

    @Getter
    @Setter
    public static class BindingProperties {
        private String destination;
        private String destinationType;
        private String source;
        private String routingKey;
        private Map<String,Object> arguments;
    }
}
