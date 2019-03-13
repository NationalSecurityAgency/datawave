package datawave.microservice.metadata.config;

import com.github.benmanes.caffeine.cache.CaffeineSpec;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.util.AllFieldMetadataHelper;
import datawave.query.util.MetadataCacheManager;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.TypeMetadataHelper;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.Map;
import java.util.Set;

@Configuration
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
                    Connector connector, String metadataTableName, Set<Authorizations> auths, Set<Authorizations> fullUserAuths) {
        return new MetadataHelper(allFieldMetadataHelper, allMetadataAuths, connector, metadataTableName, auths, fullUserAuths);
    }
    
    @Bean
    @Scope("prototype")
    @ConditionalOnMissingBean
    public AllFieldMetadataHelper allFieldMetadataHelper(TypeMetadataHelper typeMetadataHelper, CompositeMetadataHelper compositeMetadataHelper,
                    Connector connector, String metadataTableName, Set<Authorizations> auths, Set<Authorizations> fullUserAuths) {
        return new AllFieldMetadataHelper(typeMetadataHelper, compositeMetadataHelper, connector, metadataTableName, auths, fullUserAuths);
    }
    
    @Bean
    @Scope("prototype")
    @ConditionalOnMissingBean
    public CompositeMetadataHelper compositeMetadataHelper(Connector connector, String metadataTableName, Set<Authorizations> auths) {
        return new CompositeMetadataHelper(connector, metadataTableName, auths);
    }
    
    @Bean
    @Scope("prototype")
    @ConditionalOnMissingBean
    public TypeMetadataHelper typeMetadataHelper(@Qualifier("typeSubstitutions") Map<String,String> typeSubstitutions,
                    @Qualifier("allMetadataAuths") Set<Authorizations> allMetadataAuths, Connector connector, String metadataTableName,
                    Set<Authorizations> auths, boolean useTypeSubstitution) {
        return new TypeMetadataHelper(typeSubstitutions, allMetadataAuths, connector, metadataTableName, auths, useTypeSubstitution);
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
}
