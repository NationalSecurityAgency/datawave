package datawave.microservice.metadata.config;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.cache.interceptor.CacheResolver;
import org.springframework.cache.interceptor.SimpleCacheResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import com.github.benmanes.caffeine.cache.CaffeineSpec;

import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.util.AllFieldMetadataHelper;
import datawave.query.util.MetadataCacheManager;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.TypeMetadataHelper;

@EnableCaching
@Configuration
@ConditionalOnProperty(name = "datawave.metadata.enabled", havingValue = "true", matchIfMissing = true)
public class MetadataHelperConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public MetadataHelperProperties metadataHelperProperties() {
        return new MetadataHelperProperties();
    }
    
    @Bean
    @ConditionalOnMissingBean
    public MetadataHelperFactory metadataHelperFactory(BeanFactory beanFactory, TypeMetadataHelper.Factory typeMetadataHelperFactory) {
        return new MetadataHelperFactory(beanFactory, typeMetadataHelperFactory);
    }
    
    @Bean
    @ConditionalOnMissingBean
    public TypeMetadataHelper.Factory typeMetadataHelperFactory(BeanFactory beanFactory) {
        return new TypeMetadataHelper.Factory(beanFactory);
    }
    
    @Bean
    @ConditionalOnMissingBean
    public MetadataCacheManager metadataCacheManager(@Qualifier("metadataHelperCacheManager") CacheManager cacheManager) {
        return new MetadataCacheManager(cacheManager);
    }
    
    @Bean
    @Scope("prototype")
    @ConditionalOnMissingBean
    public MetadataHelper metadataHelper(AllFieldMetadataHelper allFieldMetadataHelper, @Qualifier("allMetadataAuths") Set<Authorizations> allMetadataAuths,
                    AccumuloClient accumuloClient, String metadataTableName, Set<Authorizations> auths, Set<Authorizations> fullUserAuths) {
        return new MetadataHelper(allFieldMetadataHelper, allMetadataAuths, accumuloClient, metadataTableName, auths, fullUserAuths);
    }
    
    @Bean
    @Scope("prototype")
    @ConditionalOnMissingBean
    public AllFieldMetadataHelper allFieldMetadataHelper(TypeMetadataHelper typeMetadataHelper, CompositeMetadataHelper compositeMetadataHelper,
                    AccumuloClient accumuloClient, String metadataTableName, Set<Authorizations> auths, Set<Authorizations> fullUserAuths) {
        return new AllFieldMetadataHelper(typeMetadataHelper, compositeMetadataHelper, accumuloClient, metadataTableName, auths, fullUserAuths);
    }
    
    @Bean
    @Scope("prototype")
    @ConditionalOnMissingBean
    public CompositeMetadataHelper compositeMetadataHelper(AccumuloClient accumuloClient, String metadataTableName, Set<Authorizations> auths) {
        return new CompositeMetadataHelper(accumuloClient, metadataTableName, auths);
    }
    
    @Bean
    @Scope("prototype")
    @ConditionalOnMissingBean
    public TypeMetadataHelper typeMetadataHelper(@Qualifier("typeSubstitutions") Map<String,String> typeSubstitutions,
                    @Qualifier("allMetadataAuths") Set<Authorizations> allMetadataAuths, AccumuloClient accumuloClient, String metadataTableName,
                    Set<Authorizations> auths, boolean useTypeSubstitution) {
        return new TypeMetadataHelper(typeSubstitutions, allMetadataAuths, accumuloClient, metadataTableName, auths, useTypeSubstitution);
    }
    
    @Bean(name = "typeSubstitutions")
    @ConditionalOnMissingBean(name = "typeSubstitutions")
    public Map<String,String> typeSubstitutions(MetadataHelperProperties metadataHelperProperties) {
        return metadataHelperProperties.getTypeSubstitutions();
    }
    
    @Bean(name = "allMetadataAuths")
    @ConditionalOnMissingBean(name = "allMetadataAuths")
    public Set<Authorizations> allMetadataAuths(MetadataHelperProperties metadataHelperProperties) {
        return metadataHelperProperties.getAllMetadataAuths();
    }
    
    @Bean(name = "metadataHelperCacheManager")
    @ConditionalOnMissingBean(name = "metadataHelperCacheManager")
    public CacheManager metadataHelperCacheManager() {
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeineSpec(CaffeineSpec.parse("maximumSize=100, expireAfterAccess=24h, expireAfterWrite=24h"));
        return caffeineCacheManager;
    }
    
    @Bean(name = "metadataHelperCacheResolver")
    @ConditionalOnMissingBean(name = "metadataHelperCacheManager")
    public CacheResolver metadataHelperCacheResolver(@Qualifier("metadataHelperCacheManager") CacheManager metadataHelperCacheManager) {
        return new SimpleCacheResolver(metadataHelperCacheManager);
    }
}
