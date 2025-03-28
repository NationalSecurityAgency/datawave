package datawave.microservice.querymetric.config;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloClientPool;
import datawave.core.common.connection.AccumuloClientPoolFactory;
import datawave.microservice.config.accumulo.AccumuloProperties;

@ImportAutoConfiguration({RefreshAutoConfiguration.class})
@AutoConfigureCache(cacheProvider = CacheType.HAZELCAST)
@ComponentScan(basePackages = "datawave.microservice")
@Profile("QueryMetricTest")
@Configuration
public class QueryMetricTestConfiguration {
    
    public QueryMetricTestConfiguration() {}
    
    @Bean
    @Lazy
    @Qualifier("warehouse")
    public AccumuloClientPool accumuloClientPool(@Qualifier("warehouse") AccumuloProperties accumuloProperties) throws Exception {
        return new AccumuloClientPool(new InMemoryAccumuloClientPoolFactory(accumuloProperties));
    }
    
    @Bean
    @Lazy
    @Qualifier("warehouse")
    public AccumuloClientPoolFactory warehouseInstance(AccumuloProperties accumuloProperties) throws Exception {
        return new InMemoryAccumuloClientPoolFactory(accumuloProperties);
    }
    
    public class InMemoryAccumuloClientPoolFactory extends AccumuloClientPoolFactory {
        
        private AccumuloProperties accumuloProperties;
        
        public InMemoryAccumuloClientPoolFactory(AccumuloProperties accumuloProperties) throws Exception {
            super(accumuloProperties.getUsername(), accumuloProperties.getPassword(), "mock", "mock");
            this.accumuloProperties = accumuloProperties;
            try {
                AccumuloClient accumuloClient = new InMemoryAccumuloClient(accumuloProperties.getUsername(),
                                new InMemoryInstance(accumuloProperties.getInstanceName()));
                accumuloClient.securityOperations().changeUserAuthorizations(accumuloClient.whoami(), new Authorizations("PUBLIC", "A", "B", "C"));
            } catch (AccumuloSecurityException e) {
                e.printStackTrace();
            }
        }
        
        public PooledObject<AccumuloClient> makeObject() throws Exception {
            return new DefaultPooledObject(
                            new InMemoryAccumuloClient(this.accumuloProperties.getUsername(), new InMemoryInstance(accumuloProperties.getInstanceName())));
        }
    }
}
