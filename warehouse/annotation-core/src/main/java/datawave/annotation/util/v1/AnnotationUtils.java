package datawave.annotation.util.v1;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;

public class AnnotationUtils {

    /** Enum for the SegmentBoundary types */
    public enum BoundaryCase {
        TIME_SPAN, CHARACTER_SPAN, POINTS, ALL, BOUNDARY_NOT_SET
    }

    /**
     * Decode the SegmentBoundary type into an enum value for streamlined handling later
     *
     * @param boundary
     *            the boundary to inspect
     * @return the BoundaryCase for this boundary.
     */
    public static BoundaryCase getBoundaryCase(SegmentBoundary boundary) {
        if (boundary.hasAll()) {
            return BoundaryCase.ALL;
        } else if (boundary.hasTimeSpan()) {
            return BoundaryCase.TIME_SPAN;
        } else if (boundary.hasCharacterSpan()) {
            return BoundaryCase.CHARACTER_SPAN;
        } else if (!boundary.getPointsList().isEmpty()) {
            return BoundaryCase.POINTS;
        } else {
            return BoundaryCase.BOUNDARY_NOT_SET;
        }
    }

    /**
     * Add the segment boundary types to the specified annotation
     *
     * @param a
     *            the annotation to enrich
     * @return the enriched version of the annotation, or the same annotation of the segment list is empty.
     */
    public static Annotation addSegmentBoundaryTypes(Annotation a) {
        if (!a.getSegmentsList().isEmpty()) {
            return a;
        }
        Annotation.Builder b = a.toBuilder().clearSegments();
        for (Segment s : a.getSegmentsList()) {
            b.addSegments(injectBoundaryType(s));
        }
        return b.build();
    }

    /**
     * Given a segment boundary case, return the string describing that boundary
     *
     * @param boundary
     *            the boundary
     * @return a string describing the boundary case.
     */
    public static String getBoundaryTypeString(SegmentBoundary boundary) {
        BoundaryCase boundaryCase = getBoundaryCase(boundary);
        switch (boundaryCase) {
            case ALL:
                return "ENTIRE";
            case POINTS:
                return "POINTS";
            case TIME_SPAN:
                return "TIME_SPAN";
            case CHARACTER_SPAN:
                return "CHARACTER_SPAN";
            case BOUNDARY_NOT_SET:
            default:
                return "";
        }
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
     * @return the segment with the text boundary type injected.
     */
    public static Segment injectBoundaryType(Segment segment) {
        if (segment.hasBoundary()) {
            SegmentBoundary boundary = injectBoundaryType(segment.getBoundary());
            return segment.toBuilder().setBoundary(boundary).build();
        } else {
            // technically invalid, but validation logic belongs elsewhere.
            return segment;
        }
    }

    /**
     * Utility method to inject the boundary type field into the boundary so that it will be included in the serialized result and support deserialization. This
     * should generally be called by and data access object just prior to writing the segment, but before injecting an identifier.
     *
     * @param boundary
     *            the boundary to inject.
     * @return the boundary with the text boundary type injected.
     */
    public static SegmentBoundary injectBoundaryType(SegmentBoundary boundary) {
        String type = getBoundaryTypeString(boundary);
        return boundary.toBuilder().setBoundaryType(type).build();
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
        for (SegmentValue v : segment.getValuesList()) {
            hasher.putString(v.toString(), StandardCharsets.UTF_8);
        }
        final SegmentBoundary boundary = segment.getBoundary();
        final BoundaryCase boundaryCase = getBoundaryCase(boundary);
        switch (boundaryCase) {
            case ALL:
                hasher.putString(boundary.getAll().toString(), StandardCharsets.UTF_8);
                break;
            case POINTS:
                hasher.putString(boundary.getPointsList().toString(), StandardCharsets.UTF_8);
                break;
            case TIME_SPAN:
                hasher.putString(boundary.getTimeSpan().toString(), StandardCharsets.UTF_8);
                break;
            case CHARACTER_SPAN:
                hasher.putString(boundary.getCharacterSpan().toString(), StandardCharsets.UTF_8);
                break;
        }
        return hasher.hash().toString();
    }
}
