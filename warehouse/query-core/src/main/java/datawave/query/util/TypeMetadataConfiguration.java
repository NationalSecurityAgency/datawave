package datawave.query.util;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.deltaspike.core.api.config.Configuration;
import org.apache.log4j.Logger;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.TimeUnit;

@Configuration
@EnableCaching
public class TypeMetadataConfiguration {
    private static final org.apache.log4j.Logger log = Logger.getLogger(TypeMetadataConfiguration.class);
    
    @Bean(name = "evictionMetadataCache")
    public CacheManager typeMetadataCacheManager() {
        log.warn("*** TypeMetadataConfiguration: Initializing caches! *** ");
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager("loadAllFields");
        caffeineCacheManager.setCaffeine(caffeineCacheBuilder());
        return caffeineCacheManager;
    }
    
    @Bean(name = "caffeineCacheBuilder")
    Caffeine<Object,Object> caffeineCacheBuilder() {
        return Caffeine.newBuilder().initialCapacity(50).maximumSize(100).expireAfterWrite(3, TimeUnit.MINUTES).weakKeys().recordStats();
    }
}
