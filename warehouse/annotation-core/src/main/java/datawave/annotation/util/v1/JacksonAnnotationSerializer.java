package datawave.annotation.util.v1;

import static datawave.annotation.util.v1.AnnotationUtils.addSegmentBoundaryTypes;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;

/** Implements a custom Jackson Annotation serializer that simply uses the native Protobuf JsonFormat.Printer */
public class JacksonAnnotationSerializer extends JsonSerializer<Annotation> {
    private static final JsonFormat.Printer protobufJsonPrinter = JsonFormat.printer();

    @Override
    public void serialize(Annotation annotation, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeRawValue(protobufJsonPrinter.print(addSegmentBoundaryTypes(annotation)));
    }
}
