package datawave.query.transformer.annotation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;

import datawave.annotation.protobuf.v1.SegmentBoundary;

/** One phrase occurrence and its context window. This is an internal, non-JSON type. */
public final class PhraseHit implements AnnotationHit {
    private final List<SegmentHit> constituents;
    private final SegmentBoundary hitStart;
    private final SegmentBoundary hitEnd;
    private final SegmentBoundary contextStart;
    private final SegmentBoundary contextEnd;
    private final boolean ordered;
    private final int distance;

    /** Creates a phrase whose context bounds have already been calculated. */
    public PhraseHit(List<SegmentHit> constituents, SegmentBoundary contextStart, SegmentBoundary contextEnd) {
        this(constituents, contextStart, contextEnd, true, -1);
    }

    public PhraseHit(List<SegmentHit> constituents, SegmentBoundary contextStart, SegmentBoundary contextEnd, boolean ordered, int distance) {
        if (constituents == null || constituents.isEmpty()) {
            throw new IllegalArgumentException("constituents must not be empty");
        }
        this.constituents = Collections.unmodifiableList(new ArrayList<>(constituents));
        this.hitStart = earliest(constituents);
        this.hitEnd = latest(constituents);
        this.contextStart = Objects.requireNonNull(contextStart, "contextStart");
        this.contextEnd = Objects.requireNonNull(contextEnd, "contextEnd");
        this.ordered = ordered;
        this.distance = distance;
    }

    /** Calculates context around the complete phrase, rather than around each component. */
    public PhraseHit(List<SegmentHit> constituents, SortedMap<SegmentBoundary,?> sortedSegments, int contextSize) {
        this(constituents, contextBounds(constituents, sortedSegments, contextSize)[0], contextBounds(constituents, sortedSegments, contextSize)[1]);
    }

    public static PhraseHit fromConstituents(List<SegmentHit> constituents, SortedMap<SegmentBoundary,?> sortedSegments, int contextSize) {
        List<SegmentBoundary> boundaries = new ArrayList<>(sortedSegments.keySet());
        if (boundaries.isEmpty()) {
            throw new IllegalArgumentException("sortedSegments must not be empty");
        }
        SegmentBoundary first = earliest(constituents);
        SegmentBoundary last = latest(constituents);
        int firstIndex = indexOf(boundaries, first);
        int lastIndex = indexOf(boundaries, last);
        if (firstIndex < 0 || lastIndex < 0) {
            throw new IllegalArgumentException("phrase constituent is not in sortedSegments");
        }
        int start = Math.max(0, firstIndex - Math.max(0, contextSize));
        int end = Math.min(boundaries.size() - 1, lastIndex + Math.max(0, contextSize));
        return new PhraseHit(constituents, boundaries.get(start), boundaries.get(end));
    }

    private static SegmentBoundary[] contextBounds(List<SegmentHit> hits, SortedMap<SegmentBoundary,?> segments, int size) {
        PhraseHit hit = fromConstituents(hits, segments, size);
        return new SegmentBoundary[] {hit.contextStart, hit.contextEnd};
    }

    private static int indexOf(List<SegmentBoundary> boundaries, SegmentBoundary target) {
        for (int i = 0; i < boundaries.size(); i++) {
            if (new BoundaryComparator().compare(boundaries.get(i), target) == 0)
                return i;
        }
        return -1;
    }

    private static SegmentBoundary earliest(List<SegmentHit> hits) {
        SegmentBoundary result = hits.get(0).getHitBoundary();
        BoundaryComparator c = new BoundaryComparator();
        for (SegmentHit hit : hits)
            if (c.compare(hit.getHitBoundary(), result) < 0)
                result = hit.getHitBoundary();
        return result;
    }

    private static SegmentBoundary latest(List<SegmentHit> hits) {
        SegmentBoundary result = hits.get(0).getHitBoundary();
        BoundaryComparator c = new BoundaryComparator();
        for (SegmentHit hit : hits)
            if (c.compare(hit.getHitBoundary(), result) > 0)
                result = hit.getHitBoundary();
        return result;
    }

    @Override
    public List<SegmentHit> getConstituentHits() {
        return constituents;
    }

    @Override
    public SegmentBoundary getContextStart() {
        return contextStart;
    }

    @Override
    public SegmentBoundary getContextEnd() {
        return contextEnd;
    }

    @Override
    public SegmentBoundary getHitStart() {
        return hitStart;
    }

    @Override
    public SegmentBoundary getHitEnd() {
        return hitEnd;
    }

    public boolean isOrdered() {
        return ordered;
    }

    public int getDistance() {
        return distance;
    }
}
