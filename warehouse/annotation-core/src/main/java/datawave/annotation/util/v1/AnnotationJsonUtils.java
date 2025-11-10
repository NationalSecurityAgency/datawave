package datawave.annotation.util.v1;

import java.util.Set;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Point;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.TextSpanChars;
import datawave.annotation.protobuf.v1.TimeSpanSeconds;

/** Encapsulates and centralizes Protobuf Json-related utilities such as the JsonFormat printer and parser configuration */
public class AnnotationJsonUtils {

    private static final JsonFormat.Printer PRINTER;
    private static final JsonFormat.Parser PARSER;

    static {
        // in some cases, we _always_ want to output certain fields when serializing to JSON. Here are the one we want to output:
        Descriptors.Descriptor timeSpanSecondDescriptor = TimeSpanSeconds.getDescriptor();
        Descriptors.FieldDescriptor startSecondsDescriptor = timeSpanSecondDescriptor.findFieldByName("startSeconds");
        Descriptors.FieldDescriptor endSecondsDescriptor = timeSpanSecondDescriptor.findFieldByName("endSeconds");

        Descriptors.Descriptor textSpanCharsDescriptor = TextSpanChars.getDescriptor();
        Descriptors.FieldDescriptor startCharacterDescriptor = textSpanCharsDescriptor.findFieldByName("startCharacter");
        Descriptors.FieldDescriptor endCharacterDescriptor = textSpanCharsDescriptor.findFieldByName("endCharacter");

        Descriptors.Descriptor pointDescriptor = Point.getDescriptor();
        Descriptors.FieldDescriptor xDescriptor = pointDescriptor.findFieldByName("x");
        Descriptors.FieldDescriptor yDescriptor = pointDescriptor.findFieldByName("y");

        Set<Descriptors.FieldDescriptor> printDefaultFieldDescriptors = Set.of(startSecondsDescriptor, endSecondsDescriptor, startCharacterDescriptor,
                        endCharacterDescriptor, xDescriptor, yDescriptor);
        PRINTER = JsonFormat.printer().preservingProtoFieldNames().includingDefaultValueFields(printDefaultFieldDescriptors);
        PARSER = JsonFormat.parser();
    }

    public static JsonFormat.Printer getPrinter() {
        return PRINTER;
    }

    public static JsonFormat.Parser getParser() {
        return PARSER;
    }

    /**
     * Convert the annotation to json and inject the boundary type
     *
     * @param a
     *            the segment to convert
     * @return json representing the segment.
     * @throws InvalidProtocolBufferException
     *             if there's a problem with serialization.
     */
    public static String annotationToJsonWithBoundaryTypesAndIds(Annotation a) throws InvalidProtocolBufferException {
        return PRINTER.print(AnnotationUtils.injectAnnotationAndSegmentIds(AnnotationUtils.addSegmentBoundaryTypes(a)));
    }

    /**
     * Convert json to an annotation. The conversion depends on having a proper boundary case set
     *
     * @param json
     *            the json to convert.
     * @return an annotation.
     * @throws InvalidProtocolBufferException
     *             if there's a problem with serialization.
     */
    public static Annotation annotationFromJson(String json) throws InvalidProtocolBufferException {
        Annotation.Builder b = Annotation.newBuilder();
        PARSER.merge(json, b);
        return b.build();
    }

    /**
     * Convert the segment to json and inject the boundary type
     *
     * @param s
     *            the segment to convert
     * @return json representing the segment.
     * @throws InvalidProtocolBufferException
     *             if there's a problem with serialization.
     */
    public static String segmentToJsonWithBoundaryType(Segment s) throws InvalidProtocolBufferException {
        return PRINTER.print(AnnotationUtils.injectBoundaryType(s));
    }

    /**
     * Convert json to a segment. The conversion depends on having a proper boundary case set
     *
     * @param json
     *            the json to convert.
     * @return a segment.
     * @throws InvalidProtocolBufferException
     *             if there's a problem with serialization.
     */
    public static Segment segmentFromJson(String json) throws InvalidProtocolBufferException {
        Segment.Builder b = Segment.newBuilder();
        PARSER.merge(json, b);
        return b.build();
    }
}
