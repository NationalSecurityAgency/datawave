package datawave.autoconfigure;

import static org.springframework.util.Assert.notNull;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.cache.CacheAutoConfiguration;
import org.springframework.boot.autoconfigure.cache.CacheManagerCustomizer;
import org.springframework.boot.autoconfigure.cache.CacheManagerCustomizers;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.couchbase.CouchbaseAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.interceptor.CacheAspectSupport;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

import datawave.autoconfigure.DatawaveCacheAutoConfiguration.CacheConfigurationImportSelector;

@Configuration
@ConditionalOnClass(CacheManager.class)
@ConditionalOnBean(CacheAspectSupport.class)
@ConditionalOnMissingBean(name = {"cacheManager", "cacheResolver"})
@EnableConfigurationProperties(CacheProperties.class)
@AutoConfigureBefore({CacheAutoConfiguration.class, HibernateJpaAutoConfiguration.class})
@AutoConfigureAfter({CouchbaseAutoConfiguration.class, HazelcastAutoConfiguration.class, RedisAutoConfiguration.class})
@Import(CacheConfigurationImportSelector.class)
public class DatawaveCacheAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public CacheManagerCustomizers cacheManagerCustomizers(ObjectProvider<List<CacheManagerCustomizer<?>>> customizers) {
        return new CacheManagerCustomizers(customizers.getIfAvailable());
    }
    
    @Bean
    public CacheManagerValidator cacheAutoConfigurationValidator(CacheProperties cacheProperties, ObjectProvider<CacheManager> cacheManager) {
        return new CacheManagerValidator(cacheProperties, cacheManager);
    }
    
    /**
     * Bean used to validate that a CacheManager exists and provide a more meaningful exception.
     */
    static class CacheManagerValidator implements InitializingBean {
        
        private final CacheProperties cacheProperties;
        
        private final ObjectProvider<CacheManager> cacheManager;
        
        CacheManagerValidator(CacheProperties cacheProperties, ObjectProvider<CacheManager> cacheManager) {
            this.cacheProperties = cacheProperties;
            this.cacheManager = cacheManager;
        }
        
        @Override
        public void afterPropertiesSet() {
            notNull(this.cacheManager.getIfAvailable(), () -> "No cache manager could be auto-configured, check your configuration " + "(caching type is '"
                            + this.cacheProperties.getType() + "')");
        }
    }
    
    /**
     * {@link ImportSelector} to add {@link CacheType} configuration classes with the exception of those listed as unsupported.
     */
    static class CacheConfigurationImportSelector implements ImportSelector {
        
        @Override
        public String[] selectImports(AnnotationMetadata importingClassMetadata) {
            // Filter out unsupported cache types
            EnumSet<CacheType> unsupported = EnumSet.of(CacheType.JCACHE, CacheType.INFINISPAN, CacheType.COUCHBASE, CacheType.REDIS);
            CacheType[] types = Arrays.stream(CacheType.values()).filter(t -> !unsupported.contains(t)).toArray(CacheType[]::new);
            String[] imports = new String[types.length];
            for (int i = 0; i < types.length; i++) {
                imports[i] = CacheConfigurations.getConfigurationClass(types[i]);
            }
            return imports;
        }
        
    }
}
