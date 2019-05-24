package datawave.webservice.common.json;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Typically, implementors will configure a given {@link ObjectMapper} for compatibility with DataWave's REST API, e.g., for serialization/deserialization of
 * DataWave's jaxb-annotated response types, etc
 */
public interface ObjectMapperDecorator {
    ObjectMapper decorate(ObjectMapper mapper);
}
