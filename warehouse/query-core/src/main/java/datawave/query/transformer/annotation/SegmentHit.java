package datawave.query.transformer.annotation;

import java.util.List;
import java.util.Objects;

import datawave.annotation.protobuf.v1.SegmentBoundary;

/** A standalone annotation value hit and its context bounds. */
public class SegmentHit implements AnnotationHit {
    private final SegmentBoundary contextStart;
    private final SegmentBoundary hitBoundary;
    private final int valueHitIndex;
    private SegmentBoundary contextEnd;

    /**
     * @param contextStart
     *            beginning of the hit context
     * @param hitBoundary
     *            boundary containing the hit
     * @param valueHitIndex
     *            index of the hit in the boundary's score-sorted values
     */
    public SegmentHit(SegmentBoundary contextStart, SegmentBoundary hitBoundary, int valueHitIndex) {
        this.contextStart = contextStart;
        this.hitBoundary = hitBoundary;
        this.valueHitIndex = valueHitIndex;
    }

    @Override
    public SegmentBoundary getContextStart() {
        return contextStart;
    }

    @Override
    public SegmentBoundary getHitBoundary() {
        return hitBoundary;
    }

    @Override
    public SegmentBoundary getHitStart() {
        return hitBoundary;
    }

    @Override
    public SegmentBoundary getHitEnd() {
        return hitBoundary;
    }

    @Override
    public List<SegmentHit> getConstituentHits() {
        return List.of(this);
    }

    public int getValueHitIndex() {
        return valueHitIndex;
    }

    public void setContextEnd(SegmentBoundary contextEnd) {
        this.contextEnd = contextEnd;
    }

    @Override
    public SegmentBoundary getContextEnd() {
        return contextEnd;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SegmentHit)) {
            return false;
        }
        SegmentHit otherHit = (SegmentHit) other;
        // @formatter:off
        return Objects.equals(contextStart.getBoundaryType(), otherHit.contextStart.getBoundaryType()) &&
                Objects.equals(contextStart.getStart(), otherHit.contextStart.getStart()) &&
                Objects.equals(contextStart.getEnd(), otherHit.contextStart.getEnd()) &&
                Objects.equals(contextEnd.getBoundaryType(), otherHit.contextEnd.getBoundaryType()) &&
                Objects.equals(contextEnd.getStart(), otherHit.contextEnd.getStart()) &&
                Objects.equals(contextEnd.getEnd(), otherHit.contextEnd.getEnd()) &&
                Objects.equals(hitBoundary.getBoundaryType(), otherHit.hitBoundary.getBoundaryType()) &&
                Objects.equals(hitBoundary.getStart(), otherHit.hitBoundary.getStart()) &&
                Objects.equals(hitBoundary.getEnd(), otherHit.hitBoundary.getEnd()) &&
                Objects.equals(valueHitIndex, otherHit.valueHitIndex);
        // @formatter:on
    }

    @Override
    public int hashCode() {
        // @formatter:off
        return Objects.hash(contextStart.getBoundaryTypeValue(), contextStart.getStart(), contextStart.getEnd(), hitBoundary.getBoundaryTypeValue(),
                        hitBoundary.getStart(), hitBoundary.getEnd(), contextEnd.getBoundaryTypeValue(), contextEnd.getStart(), contextEnd.getEnd(),
                        valueHitIndex);
        // @formatter:on
    }
}
