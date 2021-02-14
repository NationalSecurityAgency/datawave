package datawave.microservice.query.config;

import datawave.microservice.query.web.filter.BaseMethodStatsInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class QueryServiceConfig {
    @Bean
    public WebMvcConfigurer BaseMethodStatsInterceptorConfigurer(BaseMethodStatsInterceptor baseMethodStatsInterceptor) {
        return new WebMvcConfigurer() {
            @Override
            public void addInterceptors(InterceptorRegistry registry) {
                registry.addInterceptor(baseMethodStatsInterceptor);
            }
        };
    }
}
