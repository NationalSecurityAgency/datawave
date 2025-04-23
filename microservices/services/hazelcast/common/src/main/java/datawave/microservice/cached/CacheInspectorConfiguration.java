package datawave.microservice.cached;

import java.util.List;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.spring.cache.HazelcastCacheManager;

/**
 * Provides an instance of a {@link CacheInspector}.
 */
@Configuration
public class CacheInspectorConfiguration {
    
    @Bean
    @Qualifier("cacheInspectorFactory")
    public Function<CacheManager,CacheInspector> cacheInspectorFactory() {
        return cacheManager -> {
            if (cacheManager instanceof HazelcastCacheManager)
                return new HazelcastCacheInspector(cacheManager);
            else if (cacheManager instanceof CaffeineCacheManager)
                return new CaffeineCacheInspector(cacheManager);
            else if (cacheManager instanceof ConcurrentMapCacheManager)
                return new ConcurrentMapCacheInspector(cacheManager);
            else
                return new UnsupportedOperationCacheInspector();
        };
    }
    
    private static class UnsupportedOperationCacheInspector implements CacheInspector {
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
    }
}
