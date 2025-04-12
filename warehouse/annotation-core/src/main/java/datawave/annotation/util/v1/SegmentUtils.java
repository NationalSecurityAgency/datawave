package datawave.annotation.util.v1;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentValue;

public class SegmentUtils {
    public static Annotation injectAnnotationId(Annotation.Builder builder) {
        final Annotation tempAnnotation = builder.build();
        final String hash = calculateAnnotationHash(tempAnnotation);
        builder.clear(); // TODO: might not need this
        return builder.mergeFrom(tempAnnotation).setAnnotationId(hash).build();
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
    public static Segment injectSegmentHash(Segment.Builder builder) {
        final Segment tempSegment = builder.build();
        final String hash = calculateSegmentHash(tempSegment);
        builder.clear(); // TODO: might not need this
        return builder.mergeFrom(tempSegment).setSegmentId(hash).build();
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
