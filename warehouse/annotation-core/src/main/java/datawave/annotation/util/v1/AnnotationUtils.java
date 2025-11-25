package datawave.annotation.util.v1;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentValue;

public class AnnotationUtils {
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().preservingProtoFieldNames();
    private static final JsonFormat.Parser PARSER = JsonFormat.parser();

    public static Annotation addSegmentBoundaryTypes(Annotation a) {
        Annotation.Builder b = a.toBuilder().clearSegments();
        for (Segment s : a.getSegmentsList()) {
            b.addSegments(AnnotationUtils.injectBoundaryType(s));
        }
        return b.build();
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
    public static String annotationToJsonWithBoundaryTypes(Annotation a) throws InvalidProtocolBufferException {
        return PRINTER.print(addSegmentBoundaryTypes(a));
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
     * Given a segment boundary case, return the string describing that boundary
     *
     * @param boundaryCase
     *            the boundary case
     * @return a string describing the boundary case.
     */
    public static String getSegmentBoundaryCaseString(Segment.BoundaryCase boundaryCase) {
        switch (boundaryCase) {
            case ALL:
                return "ENTIRE";
            case POINTLIST:
                return "POINTLIST";
            case TIME:
                return "TIME";
            case CHARACTERS:
                return "CHARACTERS";
            case BOUNDARY_NOT_SET:
            default:
                return "";
        }
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
        return PRINTER.print(injectBoundaryType(s));
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

    /**
     * Assign identifiers to the annotation and its segments.
     *
     * @param annotation
     *            the annotation to assign identifiers to.
     * @return the modified annotation with identifiers injected.
     */
    public static Annotation injectAnnotationAndSegmentIds(Annotation annotation) {
        // first assign segment ids and collect the updated segments
        final List<Segment> updatedSegments = new ArrayList<>();
        for (Segment segment : annotation.getSegmentsList()) {
            Segment identifiedSegment = AnnotationUtils.injectSegmentId(segment);
            updatedSegments.add(identifiedSegment);
        }
        // next, add the updated segments to a new annotation
        final Annotation updatedAnnotation = annotation.toBuilder().clearSegments().addAllSegments(updatedSegments).build();

        // finally, generate the annotation id for the updated annotation
        return AnnotationUtils.injectAnnotationId(updatedAnnotation);
    }

    /**
     * Utility method to inject the boundary type field into the segment so that it will be included in the serialized result and support deserialization. This
     * should generally be called by and data access object just prior to writing the segment, but before injecting an identifier.
     *
     * @param segment
     *            the segment to inject.
     * @return the segment with boundary type injected.
     */
    public static Segment injectBoundaryType(Segment segment) {
        String type = getSegmentBoundaryCaseString(segment.getBoundaryCase());
        return segment.toBuilder().setBoundaryType(type).build();
    }

    /**
     * Utility method to generate and inject the annotation identifier into the annotation. included in the serialized result and support deserialization. The
     * identifier is a hash of certain values in the annotation. This should generally be called by and data access object just prior to writing the annotation.
     *
     * @param annotation
     *            the annotation to inject.
     * @return the annotation with boundary type injected.
     */
    public static Annotation injectAnnotationId(Annotation annotation) {
        final String hash = calculateAnnotationHash(annotation);
        return annotation.toBuilder().setAnnotationId(hash).build();
    }

    /**
     * Utility method to generate and inject the segment identifier into the segment. included in the serialized result and support deserialization. The
     * identifier is a hash of certain values in the segment. This should generally be called by and data access object just prior to writing the annotation.
     *
     * @param segment
     *            the segment to inject.
     * @return the segment with boundary type injected.
     */
    public static Segment injectSegmentId(Segment segment) {
        final String hash = calculateSegmentHash(segment);
        return segment.toBuilder().setSegmentId(hash).build();
    }

    /**
     * Calculate the 32-bit murmur3 hash used to identify an annotation, this includes the following attributes:
     * <ul>
     * <li>the annotation type</li>
     * <li>the hash for each segment</li>
     * <li>each key and value in the metadata</li>
     * </ul>
     *
     * @param annotation
     *            the annotation to hash.
     * @return the calculated hash. TODO: validate that this is the right algorithm for hashing the segment.
     */
    @SuppressWarnings("UnstableApiUsage")
    public static String calculateAnnotationHash(Annotation annotation) {
        Hasher hasher = Hashing.murmur3_32_fixed().newHasher();
        hasher.putString(annotation.getAnnotationType(), StandardCharsets.UTF_8);
        for (Segment s : annotation.getSegmentsList()) {
            hasher.putString(calculateSegmentHash(s), StandardCharsets.UTF_8);
        }
        for (Map.Entry<String,String> e : annotation.getMetadataMap().entrySet()) {
            hasher.putString(e.getKey(), StandardCharsets.UTF_8);
            hasher.putString(e.getValue(), StandardCharsets.UTF_8);
        }
        return hasher.hash().toString();
    }

    /**
     * Calculate the 32-bit murmur3 hash used to identify a segment, this includes the following attributes:
     * <ul>
     * <li>each of the segment values in string form (via {@code toString()})</li>
     * <li>the string form of the boundary (via {@code toString()})</li>
     * </ul>
     * TODO: validate that this is the right algorithm for hashing the segment.
     *
     * @param segment
     *            the segment to hash.
     * @return the calculated hash.
     */
    @SuppressWarnings("UnstableApiUsage")
    public static String calculateSegmentHash(Segment segment) {
        Hasher hasher = Hashing.murmur3_32_fixed().newHasher();
        for (SegmentValue v : segment.getSegmentValueList()) {
            hasher.putString(v.toString(), StandardCharsets.UTF_8);
        }
        switch (segment.getBoundaryCase()) {
            case ALL:
                hasher.putString(segment.getAll().toString(), StandardCharsets.UTF_8);
                break;
            case POINTLIST:
                hasher.putString(segment.getPointList().toString(), StandardCharsets.UTF_8);
                break;
            case TIME:
                hasher.putString(segment.getTime().toString(), StandardCharsets.UTF_8);
                break;
            case CHARACTERS:
                hasher.putString(segment.getCharacters().toString(), StandardCharsets.UTF_8);
                break;
        }
        return hasher.hash().toString();
    }
}
