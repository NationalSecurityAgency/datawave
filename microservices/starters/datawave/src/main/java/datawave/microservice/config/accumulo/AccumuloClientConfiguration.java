package datawave.microservice.config.accumulo;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * A {@link Configuration} that produces Accumulo {@link AccumuloClient}s for either the warehouse or metrics Accumulo instance. These two clients are produced
 * in a {@link Lazy} fashion, so no attempt to produce them will occur unless there is another bean that attempts to inject a connector using either the
 * "warehouse" or "metrics" {@link Qualifier}. If you wish to use one of these connectors, then your service must produce an {@link AccumuloProperties} using a
 * {@link Qualifier} of either "warehouse" or "metrics".
 */
@Configuration
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
public class AccumuloClientConfiguration {
    @Bean
    @Lazy
    @Qualifier("warehouse")
    @ConditionalOnMissingBean
    public AccumuloClient warehouseClient(@Qualifier("warehouse") AccumuloProperties accumuloProperties) throws AccumuloSecurityException, AccumuloException {
        return Accumulo.newClient().to(accumuloProperties.getInstanceName(), accumuloProperties.getZookeepers())
                        .as(accumuloProperties.getUsername(), new PasswordToken(accumuloProperties.getPassword())).build();
    }
    
    @Bean
    @Lazy
    @Qualifier("metrics")
    @ConditionalOnMissingBean
    public AccumuloClient metricsClient(@Qualifier("metrics") AccumuloProperties accumuloProperties) throws AccumuloSecurityException, AccumuloException {
        return Accumulo.newClient().to(accumuloProperties.getInstanceName(), accumuloProperties.getZookeepers())
                        .as(accumuloProperties.getUsername(), new PasswordToken(accumuloProperties.getPassword())).build();
    }
}
