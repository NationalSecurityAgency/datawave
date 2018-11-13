package datawave.microservice.cached;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTestContextBootstrapper;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
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

import static org.junit.Assert.assertTrue;

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
    
    @Before
    public void setup() {
        TestCache.putError = false;
        TestCache.evictError = false;
        TestCache.clearError = false;
    }
    
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
    
    @Test
    public void testCachePutError() {
        TestCache.putError = true;
        
        Object response = testService.get("putFailed");
        assertEquals("putFailed", response);
        
        assertNull("Object should not have been cached", testCache.get("putFailed"));
    }
    
    @Test
    public void testCacheEvictError() {
        TestCache.evictError = true;
        
        testService.get("foo");
        assertEquals("foo", testCache.get("foo").get());
        
        testService.evict("foo");
        
        // Make sure it is still cached, but we got here if the evict call didn't fully fail
        assertEquals("foo", testCache.get("foo").get());
    }
    
    @Test
    public void testCacheClearError() {
        TestCache.clearError = true;
        
        testService.get("foo");
        assertEquals("foo", testCache.get("foo").get());
        
        testService.clear();
        
        // Make sure it is still cached, but we got here if the evict call didn't fully fail
        assertEquals("foo", testCache.get("foo").get());
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
    
    @CacheConfig(cacheNames = "testCache")
    public static class TestService {
        @Cacheable
        public Object get(Object key) {
            if (key == ERROR_KEY) {
                throw new RuntimeException("Simulated error for testing.");
            }
            return String.valueOf(key);
        }
        
        @CacheEvict
        public void evict(Object key) {
            // do nothing
        }
        
        @CacheEvict(allEntries = true)
        public void clear() {
            // do nothing
        }
    }
    
    public static class TestCache extends ConcurrentMapCache {
        public static boolean putError;
        public static boolean evictError;
        public static boolean clearError;
        
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
        
        @Override
        public void put(Object key, Object value) {
            if (putError)
                throw new RuntimeException("Configured error for put on " + key + " -> " + value);
            super.put(key, value);
        }
        
        @Override
        public ValueWrapper putIfAbsent(Object key, Object value) {
            if (putError)
                throw new RuntimeException("Configured error for put on " + key + " -> " + value);
            return super.putIfAbsent(key, value);
        }
        
        @Override
        public void evict(Object key) {
            if (evictError)
                throw new RuntimeException("Configured error for evict on " + key);
            super.evict(key);
        }
        
        @Override
        public void clear() {
            if (clearError)
                throw new RuntimeException("Configured error for clear");
            super.clear();
        }
    }
}
