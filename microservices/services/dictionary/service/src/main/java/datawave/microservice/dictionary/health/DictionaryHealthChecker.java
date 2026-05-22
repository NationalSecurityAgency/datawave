package datawave.microservice.dictionary.health;

import org.apache.accumulo.core.client.AccumuloClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import datawave.microservice.dictionary.config.DataDictionaryProperties;

@Component
public class DictionaryHealthChecker implements HealthIndicator {

    private final AccumuloClient accumuloClient;
    private final DataDictionaryProperties dataDictionaryProperties;

    public DictionaryHealthChecker(@Qualifier("warehouse") AccumuloClient accumuloClient,
                    DataDictionaryProperties dataDictionaryProperties) {
        this.accumuloClient = accumuloClient;
        this.dataDictionaryProperties = dataDictionaryProperties;
    }

    @Override
    public Health health() {
        Health.Builder builder = new Health.Builder();
        String metadataTable = dataDictionaryProperties.getMetadataTableName();
        String modelTable = dataDictionaryProperties.getModelTableName();

        builder.withDetail("metadataTable", metadataTable);
        builder.withDetail("modelTable", modelTable);

        try {
            boolean metadataExists = accumuloClient.tableOperations().exists(metadataTable);
            boolean modelExists = accumuloClient.tableOperations().exists(modelTable);

            builder.withDetail("metadataTableExists", metadataExists);
            builder.withDetail("modelTableExists", modelExists);

            if (metadataExists && modelExists) {
                builder.up();
            } else {
                builder.down();
                if (!metadataExists) {
                    builder.withDetail("error", "Metadata table '" + metadataTable + "' does not exist");
                }
                if (!modelExists) {
                    builder.withDetail("error", "Model table '" + modelTable + "' does not exist");
                }
            }
        } catch (Exception e) {
            builder.down(e);
        }

        return builder.build();
    }
}
