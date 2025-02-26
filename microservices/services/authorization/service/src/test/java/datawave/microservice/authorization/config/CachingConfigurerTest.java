package datawave.microservice.authorization.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import datawave.microservice.authorization.AuthorizationTestUserService;
import datawave.microservice.cached.CacheInspector;
import datawave.security.authorization.CachedDatawaveUserService;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"CachingConfigurerTest"})
@DirtiesContext
public class CachingConfigurerTest {
    public static final Object ERROR_KEY = new Object();
    
    @Autowired
    private TestService testService;
    
    @Autowired
    private TestCache testCache;
    
    @BeforeEach
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
        assertTrue(testCache.getNativeCache().isEmpty(), "Cache should have been cleared after a get error, but it wasn't.");
    }
    
    @Test
    public void testCachePutError() {
        TestCache.putError = true;
        
        Object response = testService.get("putFailed");
        assertEquals("putFailed", response);
        
        assertNull(testCache.get("putFailed"), "Object should not have been cached");
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
    
    // @EnableCaching
    @ImportAutoConfiguration({RefreshAutoConfiguration.class})
    @AutoConfigureCache(cacheProvider = CacheType.HAZELCAST)
    @ComponentScan(basePackages = "datawave.microservice")
    @Profile("CachingConfigurerTest")
    @Configuration
    public static class CachingConfiguration {
        @Bean
        public CachedDatawaveUserService cachedDatawaveUserService(CacheManager cacheManager,
                        @Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory) {
            return new AuthorizationTestUserService(Collections.EMPTY_MAP, true);
        }
        
        @Bean
        public HazelcastInstance testHazelcastInstance() {
            Config config = new Config();
            config.setClusterName(UUID.randomUUID().toString());
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            return Hazelcast.newHazelcastInstance(config);
        }
        
        @Bean
        public BusProperties busProperties() {
            return new BusProperties();
        }
        
        @Bean
        @Primary
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
        
        @Bean
        public CacheInspector testCacheInspector() {
            return new CacheInspector() {
                @Override
                public <T> T list(String cacheName, Class<T> cacheObjectType, String key) {
                    throw new UnsupportedOperationException();
                }
                
                @Override
                public <T> List<? extends T> listAll(String cacheName, Class<T> cacheObjectType) {
                    throw new UnsupportedOperationException();
                }
                
                @Override
                public <T> List<? extends T> listMatching(String cacheName, Class<T> cacheObjectType, String substring) {
                    throw new UnsupportedOperationException();
                }
                
                @Override
                public <T> int evictMatching(String cacheName, Class<T> cacheObjectType, String substring) {
                    throw new UnsupportedOperationException();
                }
            };
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
