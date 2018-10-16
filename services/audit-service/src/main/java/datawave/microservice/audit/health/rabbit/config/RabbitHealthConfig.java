package datawave.microservice.audit.health.rabbit.config;

import datawave.microservice.audit.health.HealthChecker;
import datawave.microservice.audit.health.rabbit.RabbitHealthChecker;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import javax.annotation.Resource;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Configuration for the RabbitMQ Health Checker. This configuration is activated via the 'audit.health.rabbit.enabled' property.
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties(RabbitHealthProperties.class)
@ConditionalOnProperty(name = "audit.health.rabbit.enabled", havingValue = "true")
public class RabbitHealthConfig implements SchedulingConfigurer {
    
    @Autowired
    RabbitHealthProperties rabbitHealthProperties;
    
    @Resource(name = "rabbitConnectionFactory")
    CachingConnectionFactory rabbitConnectionFactory;
    
    @Bean
    public HealthChecker healthChecker() {
        try {
            return new RabbitHealthChecker(rabbitHealthProperties, rabbitConnectionFactory);
        } catch (Exception e) {
            throw new RuntimeException("Unable to create rabbit health checker");
        }
    }
    
    @Bean
    public Runnable triggerTask() {
        return () -> {
            healthChecker().runHealthCheck();
            healthChecker().recover();
        };
    }
    
    @Bean
    public Trigger trigger() {
        return triggerContext -> {
            Calendar nextExecutionTime = new GregorianCalendar();
            Date lastActualExecutionTime = triggerContext.lastActualExecutionTime();
            nextExecutionTime.setTime(lastActualExecutionTime != null ? lastActualExecutionTime : new Date());
            nextExecutionTime.add(Calendar.MILLISECOND, Math.toIntExact(healthChecker().pollIntervalMillis()));
            return nextExecutionTime.getTime();
        };
    }
    
    @Bean(destroyMethod = "shutdown")
    public Executor taskExecutor() {
        return Executors.newScheduledThreadPool(2);
    }
    
    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.setScheduler(taskExecutor());
        taskRegistrar.addTriggerTask(triggerTask(), trigger());
    }
}
