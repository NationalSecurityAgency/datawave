package datawave.annotation.util.v1;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
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

    public static Annotation injectAnnotationSource(Annotation a, AnnotationSource as) {
        return a.toBuilder().clearSource().setSource(as).clearAnalyticSourceHash().setAnalyticSourceHash(as.getAnalyticSourceHash()).build();
    }

    /**
     * Calculate and assign all necessary hashes to annotations, segments and segment values.
     *
     * @param annotation
     *            the annotation to assign identifiers to.
     * @return the modified annotation with identifiers injected.
     */
    public static Annotation injectAllHashes(Annotation annotation) {
        // first assign segment ids and collect the updated segments
        final List<Segment> updatedSegments = new ArrayList<>();
        for (Segment segment : annotation.getSegmentsList()) {
            final List<SegmentValue> updatedSegmentValues = new ArrayList<>();
            for (SegmentValue value : segment.getValuesList()) {
                SegmentValue hashedValue = injectSegmentValueHash(value);
                updatedSegmentValues.add(hashedValue);
            }
            Segment segmentHashedValues = segment.toBuilder().clearValues().addAllValues(updatedSegmentValues).build();
            Segment hashedSegment = AnnotationUtils.injectSegmentHash(segmentHashedValues);
            updatedSegments.add(hashedSegment);
        }
        // next, add the updated segments to a new annotation
        final Annotation updatedAnnotation = annotation.toBuilder().clearSegments().addAllSegments(updatedSegments).build();

        // finally, generate the annotation id for the updated annotation
        return AnnotationUtils.injectAnnotationHash(updatedAnnotation);
    }

    /**
     * Utility method to generate and inject the annotation source hashes into the annotation source. It generates both the long analytic source hash used for
     * identification and the short analytic hash used for grouping.
     *
     * @param annotationSource
     *            the annotation to inject.
     * @return the annotation with boundary type injected.
     */
    public static AnnotationSource injectAnnotationSourceHashes(AnnotationSource annotationSource) {
        final String analyticSourceHash = calculateSourceAnalyticSourceHash(annotationSource);
        final String analyticHash = calculateSourceAnalyticHash(annotationSource);
        return annotationSource.toBuilder().setAnalyticSourceHash(analyticSourceHash).setAnalyticHash(analyticHash).build();
    }

    /**
     * Utility method to generate and inject the annotation hash into the annotation.
     *
     * @param annotation
     *            the annotation to inject.
     * @return the annotation with boundary type injected.
     */
    public static Annotation injectAnnotationHash(Annotation annotation) {
        final String hash = calculateAnnotationHash(annotation);
        return annotation.toBuilder().setAnnotationId(hash).build();
    }

    /**
     * Utility method to generate and inject the segment hash into the segment.
     *
     * @param segment
     *            the segment to inject.
     * @return the segment with boundary type injected.
     */
    public static Segment injectSegmentHash(Segment segment) {
        final String hash = calculateSegmentHash(segment);
        return segment.toBuilder().setSegmentHash(hash).build();
    }

    /**
     * Utility method to generate and inject the segment value hash into the segment value. This should generally be called by the data access object just prior
     * to writing the annotation.
     *
     * @param segmentValue
     *            the segment to inject.
     * @return the segment with boundary type injected.
     */
    public static SegmentValue injectSegmentValueHash(SegmentValue segmentValue) {
        final String hash = calculateSegmentValueHash(segmentValue);
        return segmentValue.toBuilder().setValueHash(hash).build();
    }

    /**
     * Calculate the 32-bit murmur3 hash used to group by annotation source, see {@link #calculateSourceHash(HashFunction, AnnotationSource)} for the fields
     * used in the hash.
     *
     * @param annotationSource
     *            the annotation to hash.
     * @return the calculated hash.
     */
    public static String calculateSourceAnalyticHash(AnnotationSource annotationSource) {
        return calculateSourceHash(Hashing.murmur3_32_fixed(), annotationSource);
    }

    /**
     * Calculate the 128-bit murmur3 hash used to index an annotation source, see {@link #calculateSourceHash(HashFunction, AnnotationSource)} for the fields
     * used in the hash.
     *
     * @param annotationSource
     *            the annotation to hash.
     * @return the calculated hash.
     */
    public static String calculateSourceAnalyticSourceHash(AnnotationSource annotationSource) {
        return calculateSourceHash(Hashing.murmur3_128(), annotationSource);
    }

    /**
     * Calculate a hash on an annotation source using the provided hash function. This method includes the following information in the hash:
     * <ul>
     * <li>the annotation source engine</li>
     * <li>the annotation source model</li>
     * <li>the annotation source configuration</li>
     * </ul>
     *
     * @param hashFunction
     *            the hash function to use in hash calculation
     * @param annotationSource
     *            the annotation source we're hashing
     * @return the hash of the annotation source.
     */
    @SuppressWarnings("UnstableApiUsage")
    public static String calculateSourceHash(HashFunction hashFunction, AnnotationSource annotationSource) {
        //@formatter:off
        return hashFunction.newHasher()
                .putUnencodedChars(annotationSource.getEngine())
                .putUnencodedChars(annotationSource.getModel())
                .putObject(annotationSource.getConfigurationMap(), stringMapFunnel)
                .hash()
                .toString()
                .toUpperCase();
        //@formatter:on
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
     * @return the calculated hash.
     */
    @SuppressWarnings("UnstableApiUsage")
    public static String calculateAnnotationHash(Annotation annotation) {
        Hasher hasher = Hashing.murmur3_32_fixed().newHasher();
        hasher.putString(annotation.getAnnotationType(), StandardCharsets.UTF_8);
        for (Segment s : annotation.getSegmentsList()) {
            hasher.putString(calculateSegmentHash(s), StandardCharsets.UTF_8);
        }
        // maps must be hashed in a consistent order (by key)
        final Map<String,String> metadataMap = annotation.getMetadataMap();
        final SortedSet<String> sortedKeySet = new TreeSet<>(metadataMap.keySet());
        for (String key : sortedKeySet) {
            hasher.putString(key, StandardCharsets.UTF_8);
            hasher.putString(metadataMap.get(key), StandardCharsets.UTF_8);
        }
        return hasher.hash().toString().toUpperCase();
    }

    /**
     * Calculate the 32-bit murmur3 hash used to identify a segment, this includes the following attributes:
     * <ul>
     * <li>each of the segment values in string form (via {@code toString()})</li>
     * <li>the string form of the boundary (via {@code toString()})</li>
     * </ul>
     *
     * @param segment
     *            the segment to hash.
     * @return the calculated hash.
     */
    @SuppressWarnings("UnstableApiUsage")
    public static String calculateSegmentHash(Segment segment) {
        Hasher hasher = Hashing.murmur3_32_fixed().newHasher();
        final SegmentBoundary boundary = segment.getBoundary();
        switch (boundary.getBoundaryType()) {
            case ALL:
                hasher.putUnencodedChars("ALL");
                break;
            case POINTS:
                hasher.putUnencodedChars("POINTS").putObject(boundary.getPointsList(), pointListFunnel);
                break;
            case TIME_MILLI:
                hasher.putUnencodedChars("TIME_MILLI").putInt(boundary.getStart()).putInt(boundary.getEnd());
                break;
            case TEXT_CHAR:
                hasher.putUnencodedChars("TEXT_CHAR").putInt(boundary.getStart()).putInt(boundary.getEnd());
                break;
        }
        return hasher.hash().toString().toUpperCase();
    }

    @SuppressWarnings("UnstableApiUsage")
    public static String calculateSegmentValueHash(SegmentValue v) {
        return Hashing.murmur3_32_fixed().newHasher().putUnencodedChars(v.getValue()).hash().toString().toUpperCase();
    }

    /**
     * Used to generate hashes for maps of strings via the {@code putObject(object, Funnel)} method.
     *
     */
    @SuppressWarnings("UnstableApiUsage")
    private static final Funnel<Map<String,String>> stringMapFunnel = (map, sink) -> {
        if (map != null) {
            map.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                sink.putUnencodedChars(entry.getKey());
                sink.putUnencodedChars(entry.getValue());
            });
        }
    };

    /**
     * Used to generate hashes for lists of points via the {@code putObject(object, Funnel)} method. This sends Point fields into a hasher in the following
     * order:
     * <ul>
     * <li>x</li>
     * <li>y</li>
     * <li>label</li>
     * </ul>
     * If the label is null, an empty string is hashed.
     */
    @SuppressWarnings("UnstableApiUsage")
    private static final Funnel<List<Point>> pointListFunnel = (pointsList, sink) -> {
        if (pointsList != null) {
            pointsList.forEach(point -> {
                sink.putInt(point.getX());
                sink.putInt(point.getY());
                sink.putUnencodedChars(StringUtils.defaultString(point.getLabel()));
            });
        }
    };
}
