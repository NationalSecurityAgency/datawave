package datawave.webservice.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

import com.github.benmanes.caffeine.cache.CaffeineSpec;

import datawave.webservice.DynamicWSRemoteUserOperationsBeanDefinitionRegistrar;
import datawave.webservice.query.result.event.ResponseObjectFactory;

@EnableCaching
@Configuration
@Profile("federation")
public class WSRemoteUserOperationsRegistrarConfiguration {
    @Bean
    public static DynamicWSRemoteUserOperationsBeanDefinitionRegistrar microserviceRemoteUserOperationsBeanDefinitionRegistrar(Environment environment,
                    ResponseObjectFactory responseObjectFactory) {
        return new DynamicWSRemoteUserOperationsBeanDefinitionRegistrar(environment, responseObjectFactory);
    }

    @Bean
    @ConditionalOnMissingBean(name = "remoteOperationsCacheManager")
    public CacheManager remoteOperationsCacheManager() {
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager("getRemoteUser", "listEffectiveAuthorizations");
        caffeineCacheManager.setCaffeineSpec(CaffeineSpec.parse("maximumSize=1000, expireAfterAccess=5m, expireAfterWrite=5m"));
        return caffeineCacheManager;
    }
}
