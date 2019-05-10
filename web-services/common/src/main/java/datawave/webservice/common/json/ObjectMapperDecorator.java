package datawave.webservice.common.json;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import datawave.webservice.response.objects.DefaultKey;
import datawave.webservice.response.objects.KeyBase;

import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.interceptor.Interceptor;

/**
 * This class will configure a given {@link ObjectMapper} for compatibility with DataWave's typical use cases, e.g., serialization/deserialization of DataWave's
 * jaxb-annotated response types, etc
 */
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
public class ObjectMapperDecorator {
    
    public ObjectMapper decorate(ObjectMapper mapper) {
        
        if (null == mapper) {
            throw new IllegalArgumentException("mapper cannot be null");
        }
        mapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new JaxbAnnotationModule());
        
        SimpleModule module = new SimpleModule(KeyBase.class.getName());
        module.addAbstractTypeMapping(KeyBase.class, DefaultKey.class);
        mapper.registerModule(module);
        
        return mapper;
    }
}
