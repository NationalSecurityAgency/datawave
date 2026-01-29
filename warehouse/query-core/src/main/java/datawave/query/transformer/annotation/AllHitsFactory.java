package datawave.query.transformer.annotation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.query.transformer.annotation.AnnotationHitsTransformer.SegmentHit;
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
    public AllHits create(String annotationId, List<SegmentHit> orderedHits, TreeMap<SegmentBoundary,List<SegmentValue>> sortedSegments)
                    throws AllHitsException {
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
    public AllHits create(String annotationId, List<SegmentHit> orderedHits, TreeMap<SegmentBoundary,List<SegmentValue>> sortedSegments, TimeUnit timeUnit)
                    throws AllHitsException {
        if (orderedHits.isEmpty()) {
            return null;
        }

        // contains all hits across all boundaries
        AllHits allHits = new AllHits();
        allHits.setAnnotationId(annotationId);

        // merge segment hits into overlapping contexts to be processed together
        List<MergedHit> mergedHits = mergeOverlapping(orderedHits);

        // extract hits and convert to pojo
        for (MergedHit hit : mergedHits) {
            // contains all hits sharing a context
            AllHit allHit = new AllHit();

            // both the start and end are inclusive
            SortedMap<SegmentBoundary,List<SegmentValue>> contextView = sortedSegments.subMap(hit.getStart(), true, hit.getEnd(), true);
            if (contextView.isEmpty()) {
                // no context means the hit missed the available data
                return null;
            }

            // convert to an iterator to build the best context window
            Iterator<Map.Entry<SegmentBoundary,List<SegmentValue>>> itr = contextView.entrySet().iterator();
            applyContextAndHit(allHit, hit, itr, timeUnit);

            addAllHit(allHits, allHit);
        }

        return allHits;
    }

    /**
     * Convert SegmentHits into MergedHits. If a SegmentHit appears within the context of the previous SegmentHit it will be merged into that hit. The MergedHit
     * will have its context extended to include all SegmentHits that have been merged. This will repeat until a SegmentHit occurs outside the context of the
     * MergedHit. At most one MergedHit will be created for each SegmentHit.
     *
     * @param orderedHits
     *            non-null ordered list of SegmentHit
     * @return
     * @throws AllHitsException
     */
    private List<MergedHit> mergeOverlapping(List<SegmentHit> orderedHits) throws AllHitsException {
        List<MergedHit> merged = new ArrayList<>();

        for (SegmentHit hit : orderedHits) {
            MergedHit merge = null;
            for (MergedHit mergedHit : merged) {
                if (mergedHit.contains(hit.getHitBoundary())) {
                    merge = mergedHit;
                    break;
                }
            }

            if (merge == null) {
                merge = new MergedHit();
                merged.add(merge);
            }

            merge.add(hit);
        }

        return merged;
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

    private void applyHit(AllHit allHit, SegmentBoundary segmentBoundary, SegmentValue hitValue, TimeUnit timeUnit) {
        TermHit th = new TermHit();
        th.setTermLabel(hitValue.getValue());
        th.setConfidence(hitValue.getScore());
        th.getTimeRange().setStartTime(convertTime(segmentBoundary.getStart(), timeUnit));
        th.getTimeRange().setEndTime(convertTime(segmentBoundary.getEnd(), timeUnit));
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
     * @param mergedHit
     * @param contextIterator
     * @param timeUnit
     *            the time unit to use for time ranges
     */
    private void applyContextAndHit(AllHit allHit, MergedHit mergedHit, Iterator<Map.Entry<SegmentBoundary,List<SegmentValue>>> contextIterator,
                    TimeUnit timeUnit) throws AllHitsException {
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
            List<Integer> hitIndexes = mergedHit.getHitIndexes(boundary);
            if (hitIndexes != null && !hitIndexes.isEmpty()) {
                for (Integer hitIndex : hitIndexes) {
                    if (contextEntry.getValue().size() <= hitIndex) {
                        throw new AllHitsException("hit index outside of available values for segment. SegmentValues:" + contextEntry.getValue().size()
                                        + " index:" + hitIndex);
                    }
                    SegmentValue value = contextEntry.getValue().get(hitIndex);
                    applyHit(allHit, boundary, value, timeUnit);
                }
            }
        }
    }

    private void addAllHit(AllHits allHits, AllHit allHit) {
        allHits.getKeywordResultList().add(allHit);
        if (allHits.getMaxTermHitConfidence() < allHit.getConfidence()) {
            allHits.setMaxTermHitConfidence(allHit.getConfidence());
        }
    }

    private static class MergedHit {
        private final Comparator<SegmentBoundary> comparator;

        private SegmentBoundary start;
        private SegmentBoundary end;
        private Map<SegmentBoundary,List<Integer>> hits = new HashMap<>();

        private MergedHit() {
            this(new BoundaryComparator());
        }

        private MergedHit(Comparator<SegmentBoundary> comparator) {
            this.comparator = comparator;
        }

        public void add(SegmentHit hit) throws AllHitsException {
            List<Integer> segmentHits = hits.computeIfAbsent(hit.getHitBoundary(), x -> new ArrayList<>());
            segmentHits.add(hit.getValueHitIndex());

            if (start == null || comparator.compare(start, hit.getContextStart()) > 0) {
                start = hit.getContextStart();
            }

            if (hit.getContextEnd() == null) {
                throw new AllHitsException("hit end context not set");
            }

            if (end == null || comparator.compare(end, hit.getContextEnd()) < 0) {
                end = hit.getContextEnd();
            }
        }

        public boolean contains(SegmentBoundary b) {
            if (b == null || start == null || end == null) {
                return false;
            }
            return comparator.compare(start, b) <= 0 && comparator.compare(b, end) <= 0;
        }

        public List<Integer> getHitIndexes(SegmentBoundary segment) {
            return hits.get(segment);
        }

        public SegmentBoundary getStart() {
            return start;
        }

        public SegmentBoundary getEnd() {
            return end;
        }
    }
}
