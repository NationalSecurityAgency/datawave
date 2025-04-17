package datawave.microservice.cached;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.cache.HazelcastCacheManager;

import datawave.autoconfigure.DatawaveCacheAutoConfiguration;

@DirtiesContext
@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
                classes = {HazelcastCacheInspectorTest.InspectorConfiguration.class, DatawaveCacheAutoConfiguration.class})
public class HazelcastCacheInspectorTest {
    private static final String CACHE_NAME = "cacheinspector-test";
    
    @Autowired
    private CacheManager cacheManager;
    
    @Autowired
    @Qualifier("cacheInspectorFactory")
    private Function<CacheManager,CacheInspector> cacheInspectorFactory;
    
    private CacheInspector cacheInspector;
    
    private Cache cache;
    
    @BeforeEach
    public void setup() {
        cacheInspector = cacheInspectorFactory.apply(cacheManager);
        
        cache = cacheManager.getCache(CACHE_NAME);
        cache.clear();
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3a", "value3a");
        cache.put("key3b", "value3b");
        cache.put("key4", "value4");
        cache.put("key5", "value5");
    }
    
    @Test
    public void testCacheManagerType() {
        assertTrue(cacheManager instanceof HazelcastCacheManager);
    }
    
    @Test
    public void testGet() {
        String value = cacheInspector.list(CACHE_NAME, String.class, "key3a");
        assertEquals("value3a", value);
    }
    
    @Test
    public void testListAll() {
        List<? extends String> users = cacheInspector.listAll(CACHE_NAME, String.class);
        users.sort(String::compareTo);
        assertEquals(Arrays.asList("value1", "value2", "value3a", "value3b", "value4", "value5"), users);
    }
    
    @Test
    public void testListMatching() {
        // Force a specific value into memory (cache only allows 1 in memory at a time)
        cache.get("key4");
        
        // List matching users, but not in memory
        List<? extends String> users = cacheInspector.listMatching(CACHE_NAME, String.class, "ey3");
        users.sort(String::compareTo);
        assertEquals(Arrays.asList("value3a", "value3b"), users);
    }
    
    @Test
    public void testEvictMatching() {
        List<? extends String> users = cacheInspector.listAll(CACHE_NAME, String.class);
        users.sort(String::compareTo);
        assertEquals(Arrays.asList("value1", "value2", "value3a", "value3b", "value4", "value5"), users);
        
        cacheInspector.evictMatching(CACHE_NAME, String.class, "ey3");
        
        users = cacheInspector.listAll(CACHE_NAME, String.class);
        users.sort(String::compareTo);
        assertEquals(Arrays.asList("value1", "value2", "value4", "value5"), users);
    }
    
    @ComponentScan(basePackages = "datawave.microservice")
    public static class InspectorConfiguration {
        @Bean
        public HazelcastInstance hazelcastInstance() {
            Config config = new Config();
            config.setClusterName(UUID.randomUUID().toString());
            config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            return Hazelcast.newHazelcastInstance(config);
        }
    }
}
