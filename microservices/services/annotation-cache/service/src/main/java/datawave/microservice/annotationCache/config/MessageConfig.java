package datawave.microservice.annotationCache.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.ProtobufMessageConverter;

/**
 * publish bean to manage protobuf serialization for messaging
 */
@Configuration
public class MessageConfig {
    // This is necessary to send messages in protobuf
    // when combined with content-type: application/x-protobuf
    @Bean
    public MessageConverter protobufMessageConverter() {
        return new ProtobufMessageConverter();
    }
}
