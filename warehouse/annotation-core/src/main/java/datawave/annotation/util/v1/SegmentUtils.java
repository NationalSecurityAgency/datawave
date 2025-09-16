package datawave.annotation.util.v1;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentValue;

public class SegmentUtils {
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().preservingProtoFieldNames();
    private static final JsonFormat.Parser PARSER = JsonFormat.parser().ignoringUnknownFields();

    public static String getBoundaryCaseString(Segment.BoundaryCase boundaryCase) {
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

    public static String toJsonWithBoundaryType(Segment s) throws InvalidProtocolBufferException {
        return PRINTER.print(injectBoundaryType(s));
    }

    public static Segment fromJson(String json) throws InvalidProtocolBufferException {
        Segment.Builder b = Segment.newBuilder();
        PARSER.merge(json, b);
        return b.build();
    }

    public static Segment injectBoundaryType(Segment segment) {
        String type = getBoundaryCaseString(segment.getBoundaryCase());
        return segment.toBuilder().setBoundaryType(type).build();
    }

    public static Annotation injectAnnotationId(Annotation annotation) {
        final String hash = calculateAnnotationHash(annotation);
        return annotation.toBuilder().setAnnotationId(hash).build();
    }

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

    /** Funny pattern to assign segment id */
    public static Segment injectSegmentHash(Segment segment) {
        final String hash = calculateSegmentHash(segment);
        return segment.toBuilder().setSegmentId(hash).build();
    }

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
