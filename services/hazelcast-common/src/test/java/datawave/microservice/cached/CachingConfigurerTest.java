package datawave.microservice.cached;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTestContextBootstrapper;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.BootstrapWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@DirtiesContext
@AutoConfigureCache(cacheProvider = CacheType.SIMPLE)
@BootstrapWith(SpringBootTestContextBootstrapper.class)
@ContextConfiguration(classes = CachingConfigurerTest.CachingConfiguration.class)
public class CachingConfigurerTest {
    public static final Object ERROR_KEY = new Object();
    
    @Autowired
    private TestService testService;
    
    @Autowired
    private TestCache testCache;
    
    @Test
    public void testCacheGetError() {
        // Cache some stuff...
        testService.get("foo");
        testService.get("bar");
        
        assertEquals("foo", testCache.get("foo").get());
        assertEquals("bar", testCache.get("bar").get());
        try {
            testService.get(ERROR_KEY);
        } catch (RuntimeException e) {
            assertEquals("Simulated error for testing.", e.getMessage());
        }
        assertTrue("Cache should have been cleared after a get error, but it wasn't.", testCache.getNativeCache().isEmpty());
    }
    
    @EnableCaching
    @ComponentScan(basePackages = "datawave.microservice")
    public static class CachingConfiguration {
        @Bean
        public CacheManager cacheManager(TestCache testCache) {
            SimpleCacheManager simpleCacheManager = new SimpleCacheManager();
            simpleCacheManager.setCaches(Collections.singletonList(testCache));
            return simpleCacheManager;
        }
        
        @Bean
        public TestCache testCache() {
            return new TestCache("testCache");
        }
        
        @Bean
        public TestService testService() {
            return new TestService();
        }
    }
    
    public static class TestService {
        @Cacheable("testCache")
        public Object get(Object key) {
            if (key == ERROR_KEY) {
                throw new RuntimeException("Simulated error for testing.");
            }
            return String.valueOf(key);
        }
    }
    
    public static class TestCache extends ConcurrentMapCache {
        public TestCache(String name) {
            super(name);
        }
        
        @Override
        public ValueWrapper get(Object key) {
            if (key == ERROR_KEY) {
                throw new RuntimeException("This should cause the cache to be cleared!");
            }
            return super.get(key);
        }
    }
}
