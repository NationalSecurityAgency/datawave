package datawave.microservice.annotation.common.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;

/** Provides transformer implementation for the {@link AccumuloAnnotationSerializer} */
@Configuration
public class AnnotationSerializerConfiguration {
    @Bean
    @ConditionalOnMissingBean(TimestampTransformer.class)
    public TimestampTransformer timestampTransformer() {
        return new DefaultTimestampTransformer();
    }

    @Bean
    @ConditionalOnMissingBean(VisibilityTransformer.class)
    public VisibilityTransformer visibilityTransformer() {
        return new DefaultVisibilityTransformer();
    }

    @Bean
    @ConditionalOnMissingBean(AccumuloAnnotationSerializer.class)
    public AccumuloAnnotationSerializer accumuloAnnotationSerializer(VisibilityTransformer visibilityTransformer, TimestampTransformer timestampTransformer) {
        return new AccumuloAnnotationSerializer(visibilityTransformer, timestampTransformer);
    }

    @Bean
    @ConditionalOnMissingBean(AccumuloAnnotationSourceSerializer.class)
    public AccumuloAnnotationSourceSerializer accumuloAnnotationSourceSerializer(VisibilityTransformer visibilityTransformer,
                    TimestampTransformer timestampTransformer) {
        return new AccumuloAnnotationSourceSerializer(visibilityTransformer, timestampTransformer);
    }

}
