package datawave.microservice.annotation.writers.accumulo.config;

import java.util.concurrent.TimeUnit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

@EnableConfigurationProperties(AccumuloAnnotationWriterProperties.class)
@ConfigurationProperties(prefix = "annotation.writers.accumulo")
@Getter
@Setter
public class AccumuloAnnotationWriterProperties {

    private String annotationTableName = "annotation";
    private String annotationSourceTableName = "annotationSource";
    private int concurrency = 1;

    private Accumulo accumuloConfig = new Accumulo();
    private Health health = new Health();

    @Getter
    @Setter
    public static class Accumulo {
        private String zookeepers;
        private String instanceName;
        private String username;
        private String password;
    }

    @Getter
    @Setter
    public static class Health {
        private Long hungTimeout = 5L;
        private TimeUnit hungTimeoutUnit = TimeUnit.MINUTES;
        // The minimum percentage of hung audit consumers (expressed as a number
        // between 0 and 1, inclusive) required to mark the service as down
        private double percentHungFailureThreshold = 0.5;

        public Long getHungAnnotationWriterTimeoutMillis() {
            return hungTimeoutUnit.toMillis(hungTimeout);
        }

    }
}
