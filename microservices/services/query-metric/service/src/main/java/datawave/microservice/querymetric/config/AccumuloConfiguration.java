package datawave.microservice.querymetric.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import datawave.core.common.connection.AccumuloClientPool;
import datawave.core.common.connection.AccumuloClientPoolFactory;
import datawave.microservice.config.accumulo.AccumuloProperties;
import datawave.microservice.config.cluster.ClusterProperties;
import datawave.microservice.querymetric.factory.WrappedAccumuloClientPoolFactory;

@Configuration
@EnableConfigurationProperties({AccumuloConfiguration.WarehouseClusterProperties.class})
public class AccumuloConfiguration {
    
    @ConfigurationProperties(prefix = "warehouse-cluster")
    public static class WarehouseClusterProperties extends ClusterProperties {
        
    }
    
    @Bean
    @Lazy
    @Qualifier("warehouse")
    @ConditionalOnMissingBean
    public AccumuloProperties warehouseAccumuloProperties(WarehouseClusterProperties warehouseProperties) {
        return warehouseProperties.getAccumulo();
    }
    
    @Bean
    @Lazy
    @Qualifier("warehouse")
    @ConditionalOnMissingBean
    public AccumuloClientPool accumuloClientPool(@Qualifier("warehouse") AccumuloClientPoolFactory accumuloClientPoolFactory,
                    QueryMetricHandlerProperties queryMetricHandlerProperties) {
        AccumuloClientPool pool = new AccumuloClientPool(new WrappedAccumuloClientPoolFactory(accumuloClientPoolFactory));
        pool.setMaxTotal(queryMetricHandlerProperties.getAccumuloClientPoolSize());
        return pool;
    }
    
    @Bean
    @Lazy
    @Qualifier("warehouse-wrapped")
    public AccumuloClientPool accumuloClientPoolWrapped(@Qualifier("warehouse") AccumuloClientPoolFactory accumuloClientPoolFactory) throws Exception {
        return new AccumuloClientPool(new WrappedAccumuloClientPoolFactory(accumuloClientPoolFactory));
    }
    
    @Bean
    @Lazy
    @Qualifier("warehouse")
    @ConditionalOnMissingBean
    public AccumuloClientPoolFactory warehouseInstance(@Qualifier("warehouse") AccumuloProperties accumuloProperties) {
        return new AccumuloClientPoolFactory(accumuloProperties.getUsername(), accumuloProperties.getPassword(), accumuloProperties.getZookeepers(),
                        accumuloProperties.getInstanceName());
    }
}
