package datawave.annotation.util.v1;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Segment;

/** Implements a custom Jackson Segment serializer that simply uses the native Protobuf JsonFormat.Printer */
public class JacksonSegmentSerializer extends JsonSerializer<Segment> {
    private static final JsonFormat.Printer protobufJsonPrinter = AnnotationJsonUtils.getPrinter();

    @Override
    public void serialize(Segment segment, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeRawValue(protobufJsonPrinter.print(segment));
    }
}
