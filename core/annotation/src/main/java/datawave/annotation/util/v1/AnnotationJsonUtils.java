package datawave.annotation.util.v1;

import java.util.Set;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Point;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;

/** Encapsulates and centralizes Protobuf Json-related utilities such as the JsonFormat printer and parser configuration */
public class AnnotationJsonUtils {

    private static final JsonFormat.Printer PRINTER;
    private static final JsonFormat.Parser PARSER;

    static {
        // in some cases, we _always_ want to output certain fields when serializing to JSON. Here are the one we want to output:
        Descriptors.Descriptor boundaryDescriptor = SegmentBoundary.getDescriptor();
        Descriptors.FieldDescriptor startDescriptor = boundaryDescriptor.findFieldByName("start");
        Descriptors.FieldDescriptor endDescriptor = boundaryDescriptor.findFieldByName("end");

        Descriptors.Descriptor pointDescriptor = Point.getDescriptor();
        Descriptors.FieldDescriptor xDescriptor = pointDescriptor.findFieldByName("x");
        Descriptors.FieldDescriptor yDescriptor = pointDescriptor.findFieldByName("y");

        Set<Descriptors.FieldDescriptor> printDefaultFieldDescriptors = Set.of(startDescriptor, endDescriptor, xDescriptor, yDescriptor);
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
     * Convert the annotation to json.
     *
     * @param a
     *            the segment to convert
     * @return json representing the segment.
     * @throws InvalidProtocolBufferException
     *             if there's a problem with serialization.
     */
    public static String annotationToJsonWithIds(Annotation a) throws InvalidProtocolBufferException {
        return PRINTER.print(AnnotationUtils.injectAllHashes(a));
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
     * Convert the segment to json.
     *
     * @param s
     *            the segment to convert
     * @return json representing the segment.
     * @throws InvalidProtocolBufferException
     *             if there's a problem with serialization.
     */
    public static String segmentToJson(Segment s) throws InvalidProtocolBufferException {
        return PRINTER.print(s);
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
