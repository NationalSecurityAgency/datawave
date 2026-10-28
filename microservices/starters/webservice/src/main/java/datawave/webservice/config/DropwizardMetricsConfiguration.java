package datawave.webservice.config;

import java.util.Collections;

import org.springframework.beans.factory.annotation.CustomAutowireConfigurer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Metric;

@Configuration
public class DropwizardMetricsConfiguration {

    @Bean(name = "metricsCDIProperties")
    @ConfigurationProperties("datawave.metrics")
    public DropwizardMetricsProperties metricsCDIProperties() {
        return new DropwizardMetricsProperties();
    }

    // used to @Inject Dropwizard metrics into Spring-created beans with a @Metric annotation
    // the other RemoteHttpService beans are created by CDI, but we need a RemoteDatawaveUserService
    // to use in creating a Supplier<ProxiedUserDetails> which is used in DefaultQueryLogicFactory
    @Bean
    public static CustomAutowireConfigurer metricQualifierConfigurer() {
        CustomAutowireConfigurer configurer = new CustomAutowireConfigurer();
        configurer.setCustomQualifierTypes(Collections.singleton(Metric.class));
        return configurer;
    }

    @Bean
    public MetricRegistry metricRegistry() {
        return new MetricRegistry();
    }

    // Counters used by the RemoteHttpService to track retries of remote calls to the user service
    // previously @Inject by CDI, but Spring will inject it as well since we have the CustomAutowireConfigurer above

    @Bean
    @Metric(name = "dw.remoteDatawaveUserService.retries", absolute = true)
    public Counter remoteDatawaveUserServiceRetryCounter(MetricRegistry registry) {
        return registry.counter("dw.remoteDatawaveUserService.retries");
    }

    @Bean
    @Metric(name = "dw.remoteDatawaveUserService.failures", absolute = true)
    public Counter remoteDatawaveUserServiceFailureCounter(MetricRegistry registry) {
        return registry.counter("dw.remoteDatawaveUserService.failures");
    }
}
