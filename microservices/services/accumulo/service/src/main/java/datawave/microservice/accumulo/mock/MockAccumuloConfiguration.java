package datawave.microservice.accumulo.mock;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.config.accumulo.AccumuloProperties;

/**
 * Mock profile setup provided for in-memory usage, for both junit and integration dev/test contexts as needed
 */
@Configuration
@Profile("mock")
public class MockAccumuloConfiguration {
    
    @Bean
    @Qualifier("warehouse")
    public InMemoryInstance warehouseInstance(@Qualifier("warehouse") AccumuloProperties warehouseProperties) {
        return new InMemoryInstance(warehouseProperties.getInstanceName());
    }
    
    @Bean
    @Qualifier("warehouse")
    public AccumuloClient warehouseClient(@Qualifier("warehouse") AccumuloProperties warehouseProperties) throws AccumuloSecurityException {
        return new InMemoryAccumuloClient(warehouseProperties.getUsername(), new InMemoryInstance(warehouseProperties.getInstanceName()));
    }
}
