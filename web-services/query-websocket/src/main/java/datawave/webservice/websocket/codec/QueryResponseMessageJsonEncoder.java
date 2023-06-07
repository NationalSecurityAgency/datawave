package datawave.webservice.websocket.codec;

import java.io.IOException;
import java.io.Writer;

import javax.websocket.EncodeException;
import javax.websocket.Encoder;
import javax.websocket.EndpointConfig;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.websocket.messages.QueryResponseMessage;

/**
 * Encodes an object in the {@link BaseResponse} hierarchy into JSON, writing the results to the supplied stream.
 */
public class QueryResponseMessageJsonEncoder implements Encoder.TextStream<QueryResponseMessage> {
    private ObjectMapper mapper;
    
    @Override
    public void encode(QueryResponseMessage object, Writer writer) throws EncodeException, IOException {
        try (JsonGenerator jsonGenerator = mapper.getFactory().createGenerator(writer)) {
            jsonGenerator.writeObject(object);
        }
    }
    
    @Override
    public void init(EndpointConfig config) {
        mapper = new ObjectMapper();
        mapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
        mapper.setAnnotationIntrospector(
                        AnnotationIntrospector.pair(new JacksonAnnotationIntrospector(), new JaxbAnnotationIntrospector(mapper.getTypeFactory())));
        // Don't close the output stream
        mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        // Don't include NULL properties.
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }
    
    @Override
    public void destroy() {}
}
