package datawave.query.transformer.annotation;

import java.util.List;

import datawave.annotation.protobuf.v1.SegmentBoundary;

/** Internal common contract for standalone and phrase annotation hits. */
public interface AnnotationHit {
    SegmentBoundary getContextStart();

    SegmentBoundary getContextEnd();

    SegmentBoundary getHitStart();

    SegmentBoundary getHitEnd();

    List<SegmentHit> getConstituentHits();

    /** Alias retained for internal callers that use the shorter vocabulary. */
    default List<SegmentHit> getHits() {
        return getConstituentHits();
    }

    /** The boundaries covered by the hit, excluding context. */
    default SegmentBoundary getHitBoundary() {
        return getHitStart();
    }

    default SegmentBoundary getFirstHitBoundary() {
        return getHitStart();
    }

    default SegmentBoundary getLastHitBoundary() {
        return getHitEnd();
    }
}
