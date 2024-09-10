package datawave.webservice.common.json;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;

/**
 * Configures JSON serialization via Jackson to honor JAXB annotations. This provider must be listed in the value of a {@code resteasy.providers} servlet
 * context parameter in the web.xml for each deployed WAR that is to use this provider (or this class needs to be on the WAR's WEB-INF/lib or WEB-INF/classes
 * classpath).
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
public class JacksonContextResolver implements ContextResolver<ObjectMapper> {
    private ObjectMapper mapper;

    public JacksonContextResolver() {
        mapper = new ObjectMapper();
        mapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
        mapper.setAnnotationIntrospector(
                        AnnotationIntrospector.pair(new JacksonAnnotationIntrospector(), new JaxbAnnotationIntrospector(mapper.getTypeFactory())));
        mapper.setSerializationInclusion(Include.NON_NULL);
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return mapper;
    }
}
