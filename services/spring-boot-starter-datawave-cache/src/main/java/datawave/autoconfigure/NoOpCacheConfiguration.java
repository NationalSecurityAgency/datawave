package datawave.autoconfigure;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.support.NoOpCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@ConditionalOnMissingBean(name = "cacheManager")
@Conditional(CacheCondition.class)
public class NoOpCacheConfiguration {
    @Bean
    @Primary
    public NoOpCacheManager cacheManager() {
        return new NoOpCacheManager();
    }
}
