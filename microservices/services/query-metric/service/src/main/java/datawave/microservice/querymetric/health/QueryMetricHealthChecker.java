package datawave.microservice.querymetric.health;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;

/**
 * Health indicator for the query metric service. Verifies Accumulo connectivity and that the configured query metric tables exist.
 */
@Component
@ConditionalOnProperty(name = "datawave.query.metric.health.enabled", havingValue = "true", matchIfMissing = true)
public class QueryMetricHealthChecker implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(QueryMetricHealthChecker.class);

    private final AccumuloConnectionFactory connectionFactory;
    private final QueryMetricHandlerProperties handlerProperties;

    public QueryMetricHealthChecker(AccumuloConnectionFactory connectionFactory, QueryMetricHandlerProperties handlerProperties) {
        this.connectionFactory = connectionFactory;
        this.handlerProperties = handlerProperties;
    }

    @Override
    public Health health() {
        Health.Builder builder = new Health.Builder();

        String shardTable = handlerProperties.getShardTableName();
        String indexTable = handlerProperties.getIndexTableName();
        String reverseIndexTable = handlerProperties.getReverseIndexTableName();
        String metadataTable = handlerProperties.getMetadataTableName();

        builder.withDetail("shardTable", shardTable);
        builder.withDetail("indexTable", indexTable);
        builder.withDetail("reverseIndexTable", reverseIndexTable);
        builder.withDetail("metadataTable", metadataTable);

        AccumuloClient client = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(null, null, AccumuloConnectionFactory.Priority.ADMIN, trackingMap);

            Map<String,Boolean> tableStatus = new LinkedHashMap<>();
            tableStatus.put(shardTable, client.tableOperations().exists(shardTable));
            tableStatus.put(indexTable, client.tableOperations().exists(indexTable));
            tableStatus.put(reverseIndexTable, client.tableOperations().exists(reverseIndexTable));
            tableStatus.put(metadataTable, client.tableOperations().exists(metadataTable));

            builder.withDetail("tableStatus", tableStatus);

            boolean allExist = tableStatus.values().stream().allMatch(Boolean::booleanValue);
            if (allExist) {
                builder.up();
            } else {
                builder.down();
                builder.withDetail("error", "One or more query metric tables do not exist");
            }
        } catch (Exception e) {
            log.warn("Query metric health check failed", e);
            builder.down(e);
        } finally {
            if (client != null) {
                try {
                    connectionFactory.returnClient(client);
                } catch (Exception e) {
                    log.warn("Error returning Accumulo client during health check", e);
                }
            }
        }

        return builder.build();
    }
}
