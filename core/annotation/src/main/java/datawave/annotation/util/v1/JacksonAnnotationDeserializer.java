package datawave.annotation.util.v1;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;

/** Implements a custom Jackson Annotation deserializer that simply uses the native Protobuf JsonFormat.Parser */
public class JacksonAnnotationDeserializer extends JsonDeserializer<Annotation> {
    private static final JsonFormat.Parser protobufJsonParser = AnnotationJsonUtils.getParser();

    private final Annotation defaultInstance;

    public JacksonAnnotationDeserializer() {
        this(Annotation.newBuilder().build());
    }

    public JacksonAnnotationDeserializer(Annotation defaultInstance) {
        this.defaultInstance = defaultInstance;
    }

    @Override
    public Annotation deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        Annotation.Builder builder = defaultInstance.newBuilderForType();
        protobufJsonParser.merge(p.readValueAsTree().toString(), builder);
        return builder.build();
    }
}
