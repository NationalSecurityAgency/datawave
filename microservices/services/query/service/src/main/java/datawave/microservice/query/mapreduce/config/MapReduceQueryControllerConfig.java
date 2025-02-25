package datawave.microservice.query.mapreduce.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@ConditionalOnProperty(name = MapReduceQueryProperties.PREFIX + ".enabled", havingValue = "true", matchIfMissing = true)
public class MapReduceQueryControllerConfig {
    @Autowired
    MapReduceQueryProperties mapReduceQueryProperties;
    
    @Bean
    public WebSecurityCustomizer ignoreMapReduceUpdateState() {
        return (web) -> web.ignoring().antMatchers("/v1/mapreduce/updateState");
    }
    
    @Bean
    public ThreadPoolTaskExecutor mvcTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(mapReduceQueryProperties.getExecutor().getCorePoolSize());
        taskExecutor.setMaxPoolSize(mapReduceQueryProperties.getExecutor().getMaxPoolSize());
        taskExecutor.setQueueCapacity(mapReduceQueryProperties.getExecutor().getQueueCapacity());
        taskExecutor.setThreadNamePrefix(mapReduceQueryProperties.getExecutor().getThreadNamePrefix());
        return taskExecutor;
    }
    
    @Bean
    public WebMvcConfigurer taskExecutorConfiguration() {
        return new WebMvcConfigurer() {
            @Override
            public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
                configurer.setTaskExecutor(mvcTaskExecutor());
            }
        };
    }
}
