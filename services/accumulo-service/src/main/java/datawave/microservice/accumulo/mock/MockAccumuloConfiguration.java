package datawave.microservice.accumulo.mock;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.config.accumulo.AccumuloProperties;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;

/**
 * Mock profile setup provided for in-memory usage, for both junit and integration dev/test contexts as needed
 */
@Configuration
@Profile("mock")
public class MockAccumuloConfiguration {
    
    @Bean
    @Qualifier("warehouse")
    public Instance warehouseInstance(@Qualifier("warehouse") AccumuloProperties warehouseProperties) {
        return new InMemoryInstance(warehouseProperties.getInstanceName());
    }
    
    @Bean
    @Qualifier("warehouse")
    //@formatter:off
    public Connector warehouseConnector(
        @Qualifier("warehouse") AccumuloProperties warehouseProperties,
        @Qualifier("warehouse") Instance warehouseInstance) throws AccumuloSecurityException, AccumuloException {
        Connector connector = warehouseInstance.getConnector(
            warehouseProperties.getUsername(),
            new PasswordToken(warehouseProperties.getPassword()));
        return connector;
    }
    //@formatter:on
}
