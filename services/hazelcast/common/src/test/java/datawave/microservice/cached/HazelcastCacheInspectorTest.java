package datawave.microservice.cached;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTestContextBootstrapper;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.BootstrapWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@DirtiesContext
@AutoConfigureCache(cacheProvider = CacheType.HAZELCAST)
@BootstrapWith(SpringBootTestContextBootstrapper.class)
@ContextConfiguration(classes = HazelcastCacheInspectorTest.InspectorConfiguration.class)
public class HazelcastCacheInspectorTest {
    private static final String CACHE_NAME = "cacheinspector-test";
    
    @Autowired
    private CacheInspector cacheInspector;
    
    @Autowired
    private CacheManager cacheManager;
    
    private Cache cache;
    
    @Before
    public void setup() {
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
            return Hazelcast.newHazelcastInstance(config);
        }
    }
}
