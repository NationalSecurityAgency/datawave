package datawave.webservice.common.json;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import datawave.microservice.querymetric.BaseQueryMetricListResponse;
import datawave.microservice.querymetric.QueryMetricsDetailListResponse;
import datawave.webservice.response.objects.DefaultKey;
import datawave.webservice.response.objects.KeyBase;

public class DefaultMapperDecorator implements ObjectMapperDecorator {
    
    @Override
    public ObjectMapper decorate(ObjectMapper mapper) {
        
        if (null == mapper) {
            throw new IllegalArgumentException("mapper cannot be null");
        }
        mapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new JaxbAnnotationModule());
        
        registerAbstractTypes(mapper);
        
        return mapper;
    }
    
    protected void registerAbstractTypes(ObjectMapper mapper) {
        SimpleModule module = new SimpleModule(KeyBase.class.getName());
        module.addAbstractTypeMapping(KeyBase.class, DefaultKey.class);
        module.addAbstractTypeMapping(BaseQueryMetricListResponse.class, QueryMetricsDetailListResponse.class);
        mapper.registerModule(module);
    }
}
