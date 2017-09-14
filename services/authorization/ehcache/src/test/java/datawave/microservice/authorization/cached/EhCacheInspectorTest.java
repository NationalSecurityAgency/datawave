package datawave.microservice.authorization.cached;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTestContextBootstrapper;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.ehcache.EhCacheCacheManager;
import org.springframework.cache.ehcache.EhCacheManagerUtils;
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
@AutoConfigureCache(cacheProvider = CacheType.SIMPLE)
@BootstrapWith(SpringBootTestContextBootstrapper.class)
@ContextConfiguration(classes = EhCacheInspectorTest.InspectorConfiguration.class)
public class EhCacheInspectorTest {
    private static final String CACHE_NAME = "datawaveUsers-test";
    
    @Autowired
    private EhCacheInspector ehCacheInspector;
    
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
        String value = ehCacheInspector.list(CACHE_NAME, String.class, "key3a");
        assertEquals("value3a", value);
    }
    
    @Test
    public void testListInMemory() {
        // Force a specific value into memory (cache only allows 1 in memory at a time)
        cache.get("key5");
        assertEquals(Collections.singletonList("value5"), ehCacheInspector.listAll(CACHE_NAME, String.class, true));
    }
    
    @Test
    public void testListOnDisk() {
        List<? extends String> users = ehCacheInspector.listAll(CACHE_NAME, String.class, false);
        users.sort(String::compareTo);
        assertEquals(Arrays.asList("value1", "value2", "value3a", "value3b", "value4", "value5"), users);
    }
    
    @Test
    public void testListMatching() {
        // Force a specific value into memory (cache only allows 1 in memory at a time)
        cache.get("key4");
        
        // List matching user in memory
        List<? extends String> users = ehCacheInspector.listMatching(CACHE_NAME, String.class, "key", true);
        assertEquals(Collections.singletonList("value4"), users);
        
        // List matching user on disk
        users = ehCacheInspector.listMatching(CACHE_NAME, String.class, "key", false);
        users.sort(String::compareTo);
        assertEquals(Arrays.asList("value1", "value2", "value3a", "value3b", "value4", "value5"), users);
        
        // List matching user on disk, but not in memory
        users = ehCacheInspector.listMatching(CACHE_NAME, String.class, "ey3", false);
        users.sort(String::compareTo);
        assertEquals(Arrays.asList("value3a", "value3b"), users);
    }
    
    @Test
    public void testEvictMatching() {
        List<? extends String> users = ehCacheInspector.listAll(CACHE_NAME, String.class, false);
        users.sort(String::compareTo);
        assertEquals(Arrays.asList("value1", "value2", "value3a", "value3b", "value4", "value5"), users);
        
        ehCacheInspector.evictMatching(CACHE_NAME, String.class, "ey3");
        
        users = ehCacheInspector.listAll(CACHE_NAME, String.class, false);
        users.sort(String::compareTo);
        assertEquals(Arrays.asList("value1", "value2", "value4", "value5"), users);
    }
    
    @ComponentScan(basePackages = "datawave.microservice")
    public static class InspectorConfiguration {
        @Bean
        public EhCacheInspector cacheInspector() {
            return new EhCacheInspector();
        }
        
        @Bean
        public CacheManager cacheManager() {
            return new EhCacheCacheManager(EhCacheManagerUtils.buildCacheManager());
        }
    }
}
