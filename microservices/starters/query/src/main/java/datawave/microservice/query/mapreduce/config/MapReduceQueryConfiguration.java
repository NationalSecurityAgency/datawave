package datawave.microservice.query.mapreduce.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

import com.hazelcast.spring.cache.HazelcastCacheManager;

import datawave.microservice.cached.CacheInspector;
import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.cached.LockableHazelcastCacheInspector;
import datawave.microservice.cached.UniversalLockableCacheInspector;
import datawave.microservice.query.mapreduce.jobs.BulkResultsJob;
import datawave.microservice.query.mapreduce.jobs.MapReduceJob;
import datawave.microservice.query.mapreduce.jobs.OozieJob;
import datawave.microservice.query.mapreduce.status.MapReduceQueryCache;
import datawave.microservice.query.mapreduce.status.cache.MapReduceQueryIdByJobIdCache;
import datawave.microservice.query.mapreduce.status.cache.MapReduceQueryIdByUsernameCache;
import datawave.microservice.query.mapreduce.status.cache.MapReduceQueryStatusCache;

@Configuration
@EnableCaching
@ConditionalOnProperty(name = MapReduceQueryProperties.PREFIX + ".enabled", havingValue = "true", matchIfMissing = true)
public class MapReduceQueryConfiguration {
    
    @Autowired
    private ApplicationContext applicationContext;
    
    @Bean
    public MapReduceQueryIdByJobIdCache mapReduceQueryIdByJobIdCache(
                    @Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory,
                    @Qualifier("cacheManager") CacheManager cacheManager) {
        LockableCacheInspector lockableCacheInspector = null;
        if (cacheManager instanceof HazelcastCacheManager) {
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        } else {
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        }
        return new MapReduceQueryIdByJobIdCache(lockableCacheInspector);
    }
    
    @Bean
    public MapReduceQueryIdByUsernameCache mapReduceQueryIdByUsernameCache(
                    @Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory,
                    @Qualifier("cacheManager") CacheManager cacheManager) {
        LockableCacheInspector lockableCacheInspector = null;
        if (cacheManager instanceof HazelcastCacheManager) {
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        } else {
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        }
        return new MapReduceQueryIdByUsernameCache(lockableCacheInspector);
    }
    
    @Bean
    public MapReduceQueryStatusCache mapReduceQueryStatusCache(@Qualifier("cacheInspectorFactory") Function<CacheManager,CacheInspector> cacheInspectorFactory,
                    @Qualifier("cacheManager") CacheManager cacheManager) {
        LockableCacheInspector lockableCacheInspector = null;
        if (cacheManager instanceof HazelcastCacheManager) {
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        } else {
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        }
        return new MapReduceQueryStatusCache(lockableCacheInspector);
    }
    
    @Bean
    public MapReduceQueryCache mapReduceQueryCache(MapReduceQueryStatusCache queryStatusCache, MapReduceQueryIdByJobIdCache queryIdByJobIdCache,
                    MapReduceQueryIdByUsernameCache queryIdByUsernameCache) {
        return new MapReduceQueryCache(queryStatusCache, queryIdByJobIdCache, queryIdByUsernameCache);
    }
    
    @Bean
    public Map<String,String> propertiesMap() {
        Environment env = applicationContext.getEnvironment();
        Map<String,String> propertiesMap = new HashMap<>();
        
        // @formatter:off
        ((ConfigurableEnvironment) applicationContext.getEnvironment()).getPropertySources().stream()
                .filter(ps -> ps instanceof EnumerablePropertySource)
                .map(ps -> ((EnumerablePropertySource<?>) ps).getPropertyNames())
                .flatMap(Arrays::stream).distinct()
                .forEach(prop -> putProperty(propertiesMap, prop));
        // @formatter:on
        
        return propertiesMap;
    }
    
    private void putProperty(Map<String,String> propertiesMap, String prop) {
        Environment env = applicationContext.getEnvironment();
        try {
            propertiesMap.put(prop, env.getProperty(prop));
        } catch (Exception e) {
            // ignoring property
        }
    }
    
    @ConditionalOnMapReduceJob("BulkResultsJob")
    @Bean("BulkResultsJob")
    public Supplier<MapReduceJob> bulkResultsJob(MapReduceQueryProperties mapReduceQueryProperties, Map<String,String> propertiesMap) {
        return () -> new BulkResultsJob(mapReduceQueryProperties, new HashMap<>(propertiesMap));
    }
    
    @ConditionalOnMapReduceJob("OozieJob")
    @Bean("OozieJob")
    public Supplier<MapReduceJob> oozieJob(MapReduceQueryProperties mapReduceQueryProperties) {
        return () -> new OozieJob(mapReduceQueryProperties);
    }
    
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Conditional(MapReduceJobCondition.class)
    public static @interface ConditionalOnMapReduceJob {
        String value();
    }
    
    public static class MapReduceJobCondition implements Condition {
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            Map<String,Object> attributes = metadata.getAnnotationAttributes(ConditionalOnMapReduceJob.class.getName());
            String value = (String) attributes.get("value");
            MapReduceQueryProperties mrQueryProperties = Binder.get(context.getEnvironment())
                            .bind(MapReduceQueryProperties.PREFIX, MapReduceQueryProperties.class).orElse(null);
            return value != null && mrQueryProperties != null && mrQueryProperties.getJobs().containsKey(value);
        }
    }
}
