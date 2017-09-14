package datawave.microservice.authorization.cached;

import net.sf.ehcache.Ehcache;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EhCacheInspectorConfiguration {
    @Bean
    @ConditionalOnClass(Ehcache.class)
    public EhCacheInspector cacheInspector() {
        return new EhCacheInspector();
    }
}
