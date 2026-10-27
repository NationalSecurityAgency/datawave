package datawave.microservice.annotation.messaging;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.MessageConverter;

/** Registers protobuf conversion support shared by annotation message producers and consumers. */
@Configuration
public class AnnotationMessagingConfiguration {
    @Bean
    public MessageConverter annotationProtobufMessageConverter() {
        return new AnnotationProtobufMessageConverter();
    }
}
