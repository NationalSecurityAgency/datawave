package datawave.microservice.querymetric.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.querymetric.MetricUpdateEntryProcessorFactory;
import datawave.microservice.querymetric.handler.QueryMetricCombiner;

@Configuration
public class EntryProcessorConfiguration {
    
    @Bean
    MetricUpdateEntryProcessorFactory entryProcessorFactory(QueryMetricCombiner combiner) {
        return new MetricUpdateEntryProcessorFactory(combiner);
    }
}
