package datawave.annotation.util.v1;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;

public class AnnotationUtils {
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().preservingProtoFieldNames();
    private static final JsonFormat.Parser PARSER = JsonFormat.parser().ignoringUnknownFields();

    private AnnotationUtils() {}

    public static Annotation addSegmentBoundaryTypes(Annotation a) {
        Annotation.Builder b = a.toBuilder().clearSegments();
        for (Segment s: a.getSegmentsList()) {
            b.addSegments(SegmentUtils.injectBoundaryType(s));
        }
        return b.build();
    }

    public static String toJsonWithBoundaryTypes(Annotation a) throws InvalidProtocolBufferException {
        return PRINTER.print(addSegmentBoundaryTypes(a));
    }

    public static Annotation fromJson(String json) throws InvalidProtocolBufferException {
        Annotation.Builder b = Annotation.newBuilder();
        PARSER.merge(json, b);
        return b.build();
    }
}
