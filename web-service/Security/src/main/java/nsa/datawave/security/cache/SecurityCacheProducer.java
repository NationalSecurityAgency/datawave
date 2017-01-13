package nsa.datawave.security.cache;

import java.security.Principal;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;

import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A producer class to build caches used by the security/authorization subsystem.
 */
@Singleton
public class SecurityCacheProducer {
    private EmbeddedCacheManager cacheManager = new DefaultCacheManager(new GlobalConfigurationBuilder().globalJmxStatistics().enable().build());
    
    @Produces
    @Singleton
    @PrincipalsCache
    public Cache<String,Principal> producePrincipalsCache(@ConfigProperty(name = "dw.warehouse.instanceName") String instanceName, @ConfigProperty(
                    name = "dw.warehouse.zookeepers") String zookeepers, @ConfigProperty(name = "dw.warehouse.accumulo.userName") String username,
                    @ConfigProperty(name = "dw.warehouse.accumulo.password") String password, @ConfigProperty(name = "dw.security.cache.tableName",
                                    defaultValue = "AuthorizationServiceCache") String tableName,
                    @ConfigProperty(name = "dw.security.cache.authorizations") List<String> auths, @ConfigProperty(name = "dw.security.cache.writer.threads",
                                    defaultValue = "4") int writeThreads, @ConfigProperty(name = "dw.security.cache.writer.maxLatency.seconds",
                                    defaultValue = "5") long maxLatency, @ConfigProperty(name = "dw.security.cache.writer.maxMemory.bytes",
                                    defaultValue = "262144") long maxMemory,
                    @ConfigProperty(name = "dw.security.cache.ageoff.ttl.hours", defaultValue = "24") int ageoffTTL, @ConfigProperty(
                                    name = "dw.security.cache.writer.ageoff.iteratorPriority", defaultValue = "19") int ageoffPriority) {
        final String cacheName = "principals-cache";
        if (!cacheManager.cacheExists(cacheName)) {
            cacheManager.defineConfiguration(cacheName, new ConfigurationBuilder().jmxStatistics().enable().expiration().lifespan(ageoffTTL, TimeUnit.HOURS)
                            .enableReaper().wakeUpInterval(5, TimeUnit.MINUTES).persistence().addStore(AccumuloCacheStoreConfigurationBuilder.class)
                            .instanceName(instanceName).zookeepers(zookeepers).username(username).password(password).tableName(tableName).auths(auths)
                            .writeThreads(writeThreads).maxLatency(maxLatency).maxMemory(maxMemory).ageoffTTLhours(ageoffTTL).ageoffPriority(ageoffPriority)
                            .build());
        }
        return cacheManager.getCache(cacheName);
    }
    
    @PreDestroy
    public void shutdown() {
        cacheManager.stop();
    }
}
