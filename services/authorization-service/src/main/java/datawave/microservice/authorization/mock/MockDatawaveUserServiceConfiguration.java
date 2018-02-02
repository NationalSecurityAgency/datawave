package datawave.microservice.authorization.mock;

import datawave.microservice.cached.CacheInspector;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Configuration to supply beans for the {@link MockDatawaveUserService}. This configuration is only active when the "mock" profile is selected. This profile is
 * used for testing and development and should <strong>not</strong> be used in production.
 */
@Configuration
@EnableCaching
@Profile("mock")
public class MockDatawaveUserServiceConfiguration {
    @Bean
    public MockDatawaveUserService mockDatawaveUserService(MockDatawaveUserLookup mockDatawaveUserLookup, CacheInspector cacheInspector) {
        return new MockDatawaveUserService(mockDatawaveUserLookup, cacheInspector);
    }
    
    @Bean
    public MockDatawaveUserLookup mockDatawaveUserLookup(MockDULProperties mockDULProperties) {
        return new MockDatawaveUserLookup(mockDULProperties);
    }
    
    @Bean
    public MockDULProperties mockDULProperties() {
        return new MockDULProperties();
    }
}
