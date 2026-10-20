package datawave.microservice.annotation.messaging;

import org.springframework.messaging.converter.ProtobufMessageConverter;

/**
 * DATAWAVE-packaged protobuf converter for annotation messaging.
 *
 * <p>
 * Spring Cloud Stream excludes most framework-provided {@code org.springframework.*} message converter beans from its custom converter collection. This
 * subclass preserves Spring's standard protobuf implementation while making the converter eligible for registration with the stream conversion service.
 * </p>
 */
public class AnnotationProtobufMessageConverter extends ProtobufMessageConverter {}
