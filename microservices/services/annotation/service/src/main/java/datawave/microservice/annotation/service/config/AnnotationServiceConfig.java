package datawave.microservice.annotation.service.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.web.firewall.StrictHttpFirewall;

import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.common.config.AnnotationJsonConfiguration;
import datawave.microservice.annotation.config.AnnotationRole;
import datawave.microservice.annotation.config.ConditionalOnAnnotationRole;
import datawave.microservice.annotation.health.HealthChecker;
import datawave.microservice.annotation.service.AnnotationPublisher;

@Configuration
@EnableConfigurationProperties
@ConditionalOnAnnotationRole({AnnotationRole.API, AnnotationRole.COMBINED})
@Import({AnnotationJsonConfiguration.class, AnnotationProperties.class})
public class AnnotationServiceConfig {
    /**
     * The message source where REST updates enter the stream. Its standalone function name selects the configured {@code annotationSource-out-0} producer
     * binding.
     */
    @Bean
    public AnnotationSupplier annotationSource() {
        return new AnnotationSupplier();
    }

    @Bean
    public AnnotationPublisher annotationPublisher(AnnotationSupplier annotationSource, AnnotationProperties properties,
                    ObjectProvider<HealthChecker> healthChecker) {
        return new AnnotationPublisher(annotationSource, properties, healthChecker.getIfAvailable());
    }

    @Bean
    public StrictHttpFirewall httpFirewall() {
        // override the default strict http firewall to allow forward slashes. These are used in internal identifiers
        // as separators for shard/datatype/uid structures. We also allow shard:datatype:uid, but in some cases we
        // want to accept the slash variant which is returned by other services.
        StrictHttpFirewall firewall = new StrictHttpFirewall();
        firewall.setAllowUrlEncodedSlash(true);
        // this is due to strangeness in the StrictHttpFirewall, you must also allow encoded percents to allow encoded
        // slashes, arrived here via testing.
        firewall.setAllowUrlEncodedPercent(true);
        return firewall;
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService federatedReadExecutorService() {
        return Executors.newCachedThreadPool();
    }
}
