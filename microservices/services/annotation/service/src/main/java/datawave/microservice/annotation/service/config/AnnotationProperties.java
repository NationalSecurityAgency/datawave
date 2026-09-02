package datawave.microservice.annotation.service.config;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.PositiveOrZero;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import datawave.core.common.connection.AccumuloConnectionFactory;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "annotation")
@Component
public class AnnotationProperties {
    private int maxConnections;

    private String annotationTableName = "annotation";

    private String annotationSourceTableName = "annotationSource";

    private String truthmarkTableName = "truthmark";

    private String truthmarkSourceTableName = "truthmarkSource";

    private String connPoolName;

    private boolean enableInternalIdLookup;

    /**
     * Metadata keys to mask (remove) from annotation sources when injecting them into annotations. Mirrors the legacy
     * {@code AnnotationConfig.getMaskSourceMetadata()} default of {@code ["visibility"]}.
     */
    private List<String> maskSourceMetadata = List.of("visibility");

    /**
     * The name of the shard table used for internal document identifier lookups.
     */
    private String shardTableName = "shard";

    private AccumuloConnectionFactory.Priority priority;

    private boolean annotationAckEnabled = true;
    private long annotationAckTimeoutMillis = 500L;

    private List<String> fsConfigResources;

    private String systemFrom;

    @Valid
    private Retry retry = new Retry();

    @Validated
    @Getter
    @Setter
    public static class Retry {
        @PositiveOrZero
        private int maxAttempts = 10;

        @PositiveOrZero
        private long failTimeoutMillis = TimeUnit.MINUTES.toMillis(5);

        @PositiveOrZero
        private long backoffIntervalMillis = TimeUnit.SECONDS.toMillis(5);

        public boolean noTimeout(long startTime, long currentTime) {
            return (currentTime - startTime) < failTimeoutMillis;
        }

        public boolean hasAttemptsRemaining(int attempts) {
            return attempts < maxAttempts;
        }
    }
}
