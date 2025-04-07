package datawave.microservice.authorization.mock;

import java.util.function.Function;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import datawave.microservice.cached.CacheInspector;

/**
 * Configuration to supply beans for the {@link MockDatawaveUserService}. This configuration is only active when the "mock" profile is selected. This profile is
 * used for testing and development and should <strong>not</strong> be used in production.
 */
@Configuration
@EnableCaching
@Profile("mock")
public class MockDatawaveUserServiceConfiguration {
    @Bean
    public MockDatawaveUserService mockDatawaveUserService(MockDatawaveUserLookup mockDatawaveUserLookup, CacheManager cacheManager,
                    @Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory) {
        return new MockDatawaveUserService(mockDatawaveUserLookup, cacheInspectorFactory.apply(cacheManager));
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
