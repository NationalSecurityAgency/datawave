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
    
    @Bean(name = "pcagbuMetadataTypeCacheThingy")
    public CacheManager typeMetadataCacheManager() {
        log.warn("*** TypeMetadataConfiguration: Initializing caches! *** ");
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager("getTypeDescription");
        caffeineCacheManager.setCaffeine(caffeineCacheBuilder());
        return caffeineCacheManager;
    }
    
    @Bean(name = "caffeineCacheBuilder")
    Caffeine<Object,Object> caffeineCacheBuilder() {
        return Caffeine.newBuilder().initialCapacity(100).maximumSize(500).expireAfterAccess(10, TimeUnit.MINUTES).expireAfterWrite(10, TimeUnit.MINUTES)
                        .recordStats();
    }
}
