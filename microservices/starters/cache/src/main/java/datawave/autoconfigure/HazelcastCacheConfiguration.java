package datawave.autoconfigure;

import java.io.IOException;

import org.springframework.boot.autoconfigure.cache.CacheManagerCustomizers;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.cache.HazelcastCacheManager;

@Configuration
@ConditionalOnClass({HazelcastInstance.class, HazelcastCacheManager.class})
@ConditionalOnMissingBean(name = "cacheManager")
@Conditional(CacheCondition.class)
@ConditionalOnSingleCandidate(HazelcastInstance.class)
public class HazelcastCacheConfiguration {
    private final CacheManagerCustomizers customizers;
    
    HazelcastCacheConfiguration(CacheManagerCustomizers customizers) {
        this.customizers = customizers;
    }
    
    @Bean
    @Primary
    public HazelcastCacheManager cacheManager(HazelcastInstance existingHazelcastInstance) throws IOException {
        HazelcastCacheManager cacheManager = new HazelcastCacheManager(existingHazelcastInstance);
        return this.customizers.customize(cacheManager);
    }
}
