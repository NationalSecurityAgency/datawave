package datawave.annotation.util.v1;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Segment;

/** Implements a custom Jackson Segment deserializer that simply uses the native Protobuf JsonFormat.Parser */
public class JacksonSegmentDeserializer extends JsonDeserializer<Segment> {
    private static final JsonFormat.Parser protobufJsonParser = JsonFormat.parser();

    private final Segment defaultInstance;

    public JacksonSegmentDeserializer() {
        this(Segment.newBuilder().build());
    }

    public JacksonSegmentDeserializer(Segment defaultInstance) {
        this.defaultInstance = defaultInstance;
    }

    @Override
    public Segment deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        Segment.Builder builder = defaultInstance.newBuilderForType();
        protobufJsonParser.merge(p.readValueAsTree().toString(), builder);
        return builder.build();
    }
}
