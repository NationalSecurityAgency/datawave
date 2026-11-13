package datawave.webservice.common.json;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.v1.JacksonAnnotationDeserializer;
import datawave.annotation.util.v1.JacksonAnnotationSerializer;
import datawave.annotation.util.v1.JacksonSegmentDeserializer;
import datawave.annotation.util.v1.JacksonSegmentSerializer;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.ContextResolver;
import jakarta.ws.rs.ext.Provider;

/**
 * Configures JSON serialization via Jackson to honor JAXB annotations. This provider must be listed in the value of a {@code resteasy.providers} servlet
 * context parameter in the web.xml for each deployed WAR that is to use this provider (or this class needs to be on the WAR's WEB-INF/lib or WEB-INF/classes
 * classpath).
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
public class JacksonContextResolver implements ContextResolver<ObjectMapper> {
    private final ObjectMapper mapper;

    public JacksonContextResolver() {
        mapper = new ObjectMapper();
        mapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
        mapper.setAnnotationIntrospector(
                        AnnotationIntrospector.pair(new JacksonAnnotationIntrospector(), new JakartaXmlBindAnnotationIntrospector(mapper.getTypeFactory())));
        mapper.setSerializationInclusion(Include.NON_NULL);

        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(MultivaluedMap.class, new MultivaluedMapDeserializer());

        // Added for Annotation and Segment serialization and deserialization.
        simpleModule.addDeserializer(Annotation.class, new JacksonAnnotationDeserializer());
        simpleModule.addSerializer(Annotation.class, new JacksonAnnotationSerializer());
        simpleModule.addDeserializer(Segment.class, new JacksonSegmentDeserializer());
        simpleModule.addSerializer(Segment.class, new JacksonSegmentSerializer());

        mapper.registerModule(simpleModule);
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return mapper;
    }
}
