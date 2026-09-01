package datawave.microservice.annotationCache.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.util.MimeType;

import datawave.microservice.annotationCache.api.AnnotationMessageProto.AnnotationMessage;

/**
 * a message converter for AnnotationMessage class messages. Spring was having trouble serializing and deserializing Annotation objects without this
 */
@Configuration
public class SpecificProtobufConverterConfig {
    @Bean
    public AbstractMessageConverter explicitAnnotationConverter() {
        return new AbstractMessageConverter(MimeType.valueOf("application/x-protobuf")) {

            @Override
            protected boolean supports(Class<?> clazz) {
                // Explicitly declare that this converter owns the AnnotationMessage class target
                return AnnotationMessage.class.isAssignableFrom(clazz);
            }

            @Override
            protected Object convertFromInternal(Message<?> message, Class<?> targetClass, Object conversionHint) {
                try {
                    Object payload = message.getPayload();
                    if (payload instanceof byte[]) {
                        // Natively parse the pristine bytes sent by StreamBridge
                        return AnnotationMessage.parseFrom((byte[]) payload);
                    }
                } catch (Exception e) {
                    logger.error("Failed to parse bytes natively into AnnotationMessage target within converter: " + e.getMessage());
                }
                return null;
            }

            @Override
            protected Object convertToInternal(Object payload, org.springframework.messaging.MessageHeaders headers, Object conversionHint) {
                if (payload instanceof AnnotationMessage) {
                    return ((AnnotationMessage) payload).toByteArray();
                }
                return null;
            }
        };
    }
}
