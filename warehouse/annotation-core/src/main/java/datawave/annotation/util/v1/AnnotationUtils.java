package datawave.annotation.util.v1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Point;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;

public class AnnotationUtils {
    protected static final Logger log = LoggerFactory.getLogger(AnnotationUtils.class);

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
     * Given a segment boundary case, return the string describing that boundary. TODO: implement this using toString on the enum?
     *
     * @param boundary
     *            the boundary
     * @return a string describing the boundary case.
     */
    public static String getBoundaryTypeString(SegmentBoundary boundary) {
        BoundaryCase boundaryCase = getBoundaryCase(boundary);
        switch (boundaryCase) {
            case ALL:
                return "ALL";
            case TIME_SPAN:
                return "TIME_SPAN";
            case CHARACTER_SPAN:
                return "CHARACTER_SPAN";
            case POINTS:
                return "POINTS";
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
     * Add the segment boundary types to the specified annotation
     *
     * @param a
     *            the annotation to enrich
     * @return the enriched version of the annotation, or the same annotation of the segment list is empty.
     */
    public static Annotation injectSegmentBoundaryTypes(Annotation a) {
        if (a.getSegmentsList().isEmpty()) {
            return a;
        }
        Annotation.Builder b = a.toBuilder().clearSegments();
        for (Segment s : a.getSegmentsList()) {
            b.addSegments(injectBoundaryType(s));
        }
        return b.build();
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
     * Utility method to generate and inject the annotation source identifier into the annotation source. included in the serialized result and supports
     * deserialization. The identifier is a hash of certain values in the annotation source. This should generally be called by and data access object just
     * prior to writing the annotation.
     *
     * @param annotationSource
     *            the annotation to inject.
     * @return the annotation with boundary type injected.
     */
    public static AnnotationSource injectAnnotationSourceId(AnnotationSource annotationSource) {
        final String hash = calculateAnnotationSourceHash(annotationSource);
        return annotationSource.toBuilder().setAnalyticHash(hash).build();
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
     * Calculate the 32-bit murmur3 hash used to identify an annotation source, this includes the following attributes:
     * <ul>
     * <li>the annotation source engine</li>
     * <li>the annotation source model</li>
     * <li>the annotation source label</li>
     * <li>the annotation source configuration</li>
     * </ul>
     *
     * @param annotationSource
     *            the annotation to hash.
     * @return the calculated hash. TODO: validate that this is the right algorithm for hashing the segment.
     */
    @SuppressWarnings("UnstableApiUsage")
    public static String calculateAnnotationSourceHash(AnnotationSource annotationSource) {
        Hasher hasher = Hashing.murmur3_32_fixed().newHasher();
        hasher.putUnencodedChars(annotationSource.getEngine());
        hasher.putUnencodedChars(annotationSource.getModel());
        hasher.putUnencodedChars(annotationSource.getSourceLabel());
        // maps must be hashed in a consistent order (by key)
        // TODO: extract to a subroutine?
        final Map<String,String> configMap = annotationSource.getConfigurationMap();
        final SortedSet<String> sortedKeySet = new TreeSet<>(configMap.keySet());
        for (String key : sortedKeySet) {
            hasher.putUnencodedChars(key);
            hasher.putUnencodedChars(configMap.get(key));
        }
        return hasher.hash().toString().toUpperCase();
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
        final Hasher hasher = Hashing.murmur3_32_fixed().newHasher();
        hasher.putUnencodedChars(annotation.getAnnotationType());
        for (Segment s : annotation.getSegmentsList()) {
            hasher.putUnencodedChars(calculateSegmentHash(s));
        }
        // maps must be hashed in a consistent order (by key)
        final Map<String,String> metadataMap = annotation.getMetadataMap();
        final SortedSet<String> sortedKeySet = new TreeSet<>(metadataMap.keySet());
        for (String key : sortedKeySet) {
            hasher.putUnencodedChars(key);
            hasher.putUnencodedChars(metadataMap.get(key));
        }
        return hasher.hash().toString().toUpperCase();
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
        final Hasher hasher = Hashing.murmur3_32_fixed().newHasher();
        for (SegmentValue v : segment.getValuesList()) {
            hasher.putUnencodedChars(v.getValue());
            hasher.putDouble(v.getScore());
            if (!v.getExtensionMap().isEmpty()) {
                // maps must be hashed in a consistent order (by key)
                final Map<String,String> extensionMap = v.getExtensionMap();
                final SortedSet<String> sortedKeySet = new TreeSet<>(extensionMap.keySet());
                for (String key : sortedKeySet) {
                    hasher.putUnencodedChars(key);
                    hasher.putUnencodedChars(extensionMap.get(key));
                }
            }
        }

        final SegmentBoundary boundary = segment.getBoundary();
        hasher.putInt(calculateBoundaryHash(boundary));

        // maps must be hashed in a consistent order (by key)
        final Map<String,String> metadataMap = segment.getMetadataMap();
        final SortedSet<String> sortedKeySet = new TreeSet<>(metadataMap.keySet());
        for (String key : sortedKeySet) {
            hasher.putUnencodedChars(key);
            hasher.putUnencodedChars(metadataMap.get(key));
        }
        return hasher.hash().toString();
    }

    @SuppressWarnings("UnstableApiUsage")
    public static int calculateBoundaryHash(SegmentBoundary boundary) {
        final BoundaryCase boundaryCase = getBoundaryCase(boundary);
        //@formatter:off
        switch (boundaryCase) {
            case ALL:
                return Hashing.murmur3_32_fixed().newHasher()
                        .putUnencodedChars("ALL")
                        .hash().asInt();
            case POINTS:
                return Hashing.murmur3_32_fixed().newHasher()
                        .putUnencodedChars("POINT_LIST")
                        .putObject(boundary.getPointsList(), boundaryPointsToHasher)
                        .hash().asInt();
            case TIME_SPAN:
                return Objects.hash("OFFSET_RANGE",
                        boundary.getTimeSpan().getStartSeconds(),
                        boundary.getTimeSpan().getEndSeconds()
                );
            case CHARACTER_SPAN:
                return Objects.hash("OFFSET_RANGE",
                        boundary.getCharacterSpan().getStartCharacter(),
                        boundary.getCharacterSpan().getEndCharacter()
                );
            default:
                return 0;
        }
        //@formatter:on
    }

    //@formatter:off
    @SuppressWarnings("UnstableApiUsage") // from Hasher
    private static final Funnel<List<Point>> boundaryPointsToHasher =
            (from, into) -> from.stream().map(AnnotationUtils::calculatePointHash).forEach(into::putInt);
    //@formatter:on

    @SuppressWarnings("UnstableApiUsage")
    public static int calculatePointHash(Point p) {
        //@formatter:off
        return Hashing.murmur3_32_fixed().newHasher()
                .putInt(p.getX())
                .putInt(p.getY())
                .putUnencodedChars(StringUtils.defaultString(p.getLabel()))
                .hash().asInt();
        //@formatter:on
    }
}
