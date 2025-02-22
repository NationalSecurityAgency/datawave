package datawave.microservice.query.cachedresults.config;

import java.util.function.Function;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.context.annotation.RequestScope;

import com.hazelcast.spring.cache.HazelcastCacheManager;

import datawave.core.query.cachedresults.CachedResultsQueryParameters;
import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import datawave.microservice.cached.CacheInspector;
import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.cached.LockableHazelcastCacheInspector;
import datawave.microservice.cached.UniversalLockableCacheInspector;
import datawave.microservice.query.cachedresults.status.cache.CachedResultsQueryCache;
import datawave.microservice.query.cachedresults.status.cache.CachedResultsQueryStatusCache;
import datawave.microservice.query.cachedresults.status.cache.DefinedQueryIdByAliasCache;
import datawave.microservice.query.cachedresults.status.cache.DefinedQueryIdByCachedQueryIdCache;
import datawave.microservice.query.cachedresults.status.cache.DefinedQueryIdByViewCache;

@Configuration
@EnableConfigurationProperties(CachedResultsQueryProperties.class)
@ConditionalOnProperty(name = "datawave.query.cached-results.enabled", havingValue = "true", matchIfMissing = true)
public class CachedResultsQueryConfiguration {
    
    @Bean
    @ConfigurationProperties("spring.datasource.cached-results")
    public DataSourceProperties cachedResultsDataSourceProperties() {
        return new DataSourceProperties();
    }
    
    @Bean
    @ConfigurationProperties("spring.datasource.cached-results.hikari")
    public DataSource cachedResultsDataSource() {
        // @formatter:off
        return cachedResultsDataSourceProperties()
                .initializeDataSourceBuilder()
                .build();
        // @formatter:on
    }
    
    @Bean
    public JdbcTemplate cachedResultsJdbcTemplate() {
        return new JdbcTemplate(cachedResultsDataSource());
    }
    
    @Bean
    @ConditionalOnMissingBean
    @RequestScope
    public CachedResultsQueryParameters cachedResultsQueryParameters() {
        CachedResultsQueryParameters cachedResultsQueryParameters = new CachedResultsQueryParameters();
        cachedResultsQueryParameters.clear();
        return cachedResultsQueryParameters;
    }
    
    @Bean
    @ConditionalOnMissingBean
    @RequestScope
    public SecurityMarking securityMarking() {
        SecurityMarking securityMarking = new ColumnVisibilitySecurityMarking();
        securityMarking.clear();
        return securityMarking;
    }
    
    @Bean
    public DefinedQueryIdByCachedQueryIdCache cachedResultsQueryIdByCachedQueryIdCache(
                    @Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory, CacheManager cacheManager) {
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        return new DefinedQueryIdByCachedQueryIdCache(lockableCacheInspector);
    }
    
    @Bean
    public DefinedQueryIdByAliasCache cachedResultsQueryIdByAliasCache(
                    @Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory, CacheManager cacheManager) {
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        return new DefinedQueryIdByAliasCache(lockableCacheInspector);
    }
    
    @Bean
    public DefinedQueryIdByViewCache cachedResultsQueryIdByViewCache(
                    @Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory, CacheManager cacheManager) {
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        return new DefinedQueryIdByViewCache(lockableCacheInspector);
    }
    
    @Bean
    public CachedResultsQueryStatusCache cachedResultsStatusCache(
                    @Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory, CacheManager cacheManager) {
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        return new CachedResultsQueryStatusCache(lockableCacheInspector);
    }
    
    @Bean
    public CachedResultsQueryCache cachedResultsQueryCache(CachedResultsQueryProperties cachedResultsQueryProperties,
                    DefinedQueryIdByCachedQueryIdCache definedQueryIdByCachedQueryIdCache, DefinedQueryIdByAliasCache definedQueryIdByAliasCache,
                    DefinedQueryIdByViewCache definedQueryIdByViewCache, CachedResultsQueryStatusCache cachedResultsStatusCache) {
        return new CachedResultsQueryCache(cachedResultsQueryProperties, cachedResultsStatusCache, definedQueryIdByCachedQueryIdCache,
                        definedQueryIdByAliasCache, definedQueryIdByViewCache);
    }
}
