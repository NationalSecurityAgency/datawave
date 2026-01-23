package datawave.query.transformer.annotation;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.query.transformer.annotation.model.AllHit;
import datawave.query.transformer.annotation.model.AllHits;
import datawave.query.transformer.annotation.model.Term;
import datawave.query.transformer.annotation.model.TermHit;

/**
 * Factory for transforming an order set of SegmentHit into structured AllHits. Aggregate individual hits, associating them with surrounding context. Groups
 * hits occurring within the same SegmentBoundary to generate a single one-best and rolled up confidence score. Time will be converted to reflect the given
 * TimeUnit
 */
public class AllHitsFactory {
    /**
     * Convenience method for MILLIS
     *
     * @param annotationId
     *            the annotation id to attribute the hits to
     * @param orderedHits
     *            hits ordered by the SegmentBoundary they occur in
     * @param sortedSegments
     *            a non-null sorted map of segments to produce hits with context from. sortedSegments must be sorted by SegmentBoundary. The values must be
     *            sorted in ascending order as well
     * @return
     * @throws AllHitsException
     */
    public AllHits create(String annotationId, List<AnnotationHitsTransformer.SegmentHit> orderedHits,
                    TreeMap<SegmentBoundary,List<SegmentValue>> sortedSegments) throws AllHitsException {
        return create(annotationId, orderedHits, sortedSegments, TimeUnit.MILLISECONDS);
    }

    /**
     *
     * @param annotationId
     *            the annotation id to attribute the hits to
     * @param orderedHits
     *            hits ordered by the SegmentBoundary they occur in
     * @param sortedSegments
     *            a non-null sorted map of segments to produce hits with context from. sortedSegments must be sorted by SegmentBoundary. The values must be
     *            sorted in ascending order as well
     * @param timeUnit
     *            the time unit to use for all times
     * @return
     * @throws AllHitsException
     */
    public AllHits create(String annotationId, List<AnnotationHitsTransformer.SegmentHit> orderedHits,
                    TreeMap<SegmentBoundary,List<SegmentValue>> sortedSegments, TimeUnit timeUnit) throws AllHitsException {
        if (orderedHits.isEmpty()) {
            return null;
        }

        // contains all hits across all boundaries
        AllHits allHits = new AllHits();
        allHits.setAnnotationId(annotationId);

        // contains all hits across a single boundary
        AllHit allHit = new AllHit();

        // extract hits and convert to pojo
        for (AnnotationHitsTransformer.SegmentHit hit : orderedHits) {
            if (allHit.getHitBoundary() != null && !allHit.getHitBoundary().equals(hit.getHitBoundary())) {
                // create a new allHit
                addAllHit(allHits, allHit);
                allHit = new AllHit();
            }

            // track the current boundary this hit will cover, this is not output, but keeps things organized
            allHit.setHitBoundary(hit.getHitBoundary());

            if (hit.getContextEnd() == null) {
                throw new AllHitsException("hit end context not set");
            }

            // both the start and end are inclusive
            SortedMap<SegmentBoundary,List<SegmentValue>> contextView = sortedSegments.subMap(hit.getContextStart(), true, hit.getContextEnd(), true);
            if (contextView.isEmpty()) {
                // no context means the hit missed the available data
                return null;
            }

            if (allHit.getOneBestContext().isEmpty()) {
                // convert to an iterator to build the best context window
                Iterator<Map.Entry<SegmentBoundary,List<SegmentValue>>> itr = contextView.entrySet().iterator();
                applyContextAndHit(allHit, hit, itr, timeUnit);
            } else {
                // just write the new TermHit
                SegmentBoundary hitBoundary = hit.getHitBoundary();
                SegmentValue hitValue = sortedSegments.get(hitBoundary).get(hit.getValueHitIndex());
                applyHit(allHit, hit, hitValue, timeUnit);
            }
        }

        // add last allHit to allHits
        addAllHit(allHits, allHit);

        return allHits;
    }

    /**
     * convert time from MILLIS to some other time unit
     *
     * @param time
     *            the time to convert
     * @param to
     *            the target unit
     * @return a float representation of the time in the new time unit
     */
    private float convertTime(long time, TimeUnit to) {
        if (to == TimeUnit.MILLISECONDS) {
            return time;
        }
        long timeInNanos = TimeUnit.MILLISECONDS.toNanos(time);
        // convert target to millis and then divide
        long targetInNanos = to.toNanos(1);
        return (float) timeInNanos / (float) targetInNanos;
    }

    private void applyHit(AllHit allHit, AnnotationHitsTransformer.SegmentHit segmentHit, SegmentValue hitValue, TimeUnit timeUnit) {
        TermHit th = new TermHit();
        th.setTermLabel(hitValue.getValue());
        th.setConfidence(hitValue.getScore());
        th.getTimeRange().setStartTime(convertTime(segmentHit.getHitBoundary().getStart(), timeUnit));
        th.getTimeRange().setEndTime(convertTime(segmentHit.getHitBoundary().getEnd(), timeUnit));
        allHit.getTermHits().add(th);

        // rollup confidence
        if (allHit.getConfidence() < th.getConfidence()) {
            allHit.setConfidence(th.getConfidence());
        }
    }

    /**
     * Build the allHit oneBestContext while also applying the hit to the allHit in a single pass
     *
     * @param allHit
     *            the to be updated
     * @param segmentHit
     * @param contextIterator
     * @param timeUnit
     *            the time unit to use for time ranges
     */
    private void applyContextAndHit(AllHit allHit, AnnotationHitsTransformer.SegmentHit segmentHit,
                    Iterator<Map.Entry<SegmentBoundary,List<SegmentValue>>> contextIterator, TimeUnit timeUnit) throws AllHitsException {
        while (contextIterator.hasNext()) {
            Map.Entry<SegmentBoundary,List<SegmentValue>> contextEntry = contextIterator.next();
            SegmentBoundary boundary = contextEntry.getKey();

            if (contextEntry.getValue().isEmpty()) {
                throw new AllHitsException("cannot have a segment with no values");
            }

            // highest score will be last
            SegmentValue firstValue = contextEntry.getValue().get(contextEntry.getValue().size() - 1);
            Term t = new Term();
            t.setLabel(firstValue.getValue());
            t.setConfidence(firstValue.getScore());
            t.getTimeRange().setStartTime(convertTime(boundary.getStart(), timeUnit));
            t.getTimeRange().setEndTime(convertTime(boundary.getEnd(), timeUnit));
            allHit.getOneBestContext().add(t);

            // now check if this segment also contains the hit
            if (segmentHit.getHitBoundary() == boundary) {
                if (contextEntry.getValue().size() <= segmentHit.getValueHitIndex()) {
                    throw new AllHitsException("hit index outside of available values for segment. SegmentValues:" + contextEntry.getValue().size() + " index:"
                                    + segmentHit.getValueHitIndex());
                }
                applyHit(allHit, segmentHit, contextEntry.getValue().get(segmentHit.getValueHitIndex()), timeUnit);
            }
        }
    }

    private void addAllHit(AllHits allHits, AllHit allHit) {
        allHits.getKeywordResultList().add(allHit);
        if (allHits.getMaxTermHitConfidence() < allHit.getConfidence()) {
            allHits.setMaxTermHitConfidence(allHit.getConfidence());
        }
    }
}
