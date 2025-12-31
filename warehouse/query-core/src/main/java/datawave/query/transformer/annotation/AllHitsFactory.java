package datawave.query.transformer.annotation;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.query.transformer.annotation.model.AllHit;
import datawave.query.transformer.annotation.model.AllHits;
import datawave.query.transformer.annotation.model.Term;
import datawave.query.transformer.annotation.model.TermHit;

public class AllHitsFactory {
    private static final AllHits EMPTY_ALL_HITS = new AllHits();

    public AllHits create(String annotationId, List<AnnotationHitsTransformer.SegmentHit> hits, TreeMap<SegmentBoundary,List<SegmentValue>> sortedSegments)
                    throws AllHitsException {
        if (hits.isEmpty()) {
            return EMPTY_ALL_HITS;
        }

        // contains all hits across all boundaries
        AllHits allHits = new AllHits();
        allHits.setAnnotationId(annotationId);

        // contains all hits across a single boundary
        AllHit allHit = new AllHit();

        // extract hits and convert to pojo
        for (AnnotationHitsTransformer.SegmentHit hit : hits) {
            if (allHit.getHitBoundary() != null && !allHit.getHitBoundary().equals(hit.getHitBoundary())) {
                // create a new allHit
                addAllHit(allHits, allHit);
                allHit = new AllHit();
            }

            // track the current boundary this hit will cover, this is not output, but keeps things organized
            allHit.setHitBoundary(hit.getHitBoundary());

            // both the start and end are inclusive
            SortedMap<SegmentBoundary,List<SegmentValue>> contextView = sortedSegments.subMap(hit.getContextStart(), true, hit.getContextEnd(), true);
            if (contextView.isEmpty()) {
                // no context means the hit missed the available data
                return EMPTY_ALL_HITS;
            }

            if (allHit.getOneBestContext().isEmpty()) {
                // convert to an iterator to build the best context window
                Iterator<Map.Entry<SegmentBoundary,List<SegmentValue>>> itr = contextView.entrySet().iterator();
                applyContextAndHit(allHit, hit, itr);
            } else {
                // just write the new TermHit
                SegmentBoundary hitBoundary = hit.getHitBoundary();
                SegmentValue hitValue = sortedSegments.get(hitBoundary).get(hit.getValueHitIndex());
                applyHit(allHit, hit, hitValue);
            }
        }

        // add last allHit to allHits
        addAllHit(allHits, allHit);

        return allHits;
    }

    private void applyHit(AllHit allHit, AnnotationHitsTransformer.SegmentHit segmentHit, SegmentValue hitValue) {
        TermHit th = new TermHit();
        th.setTermLabel(hitValue.getValue());
        th.setConfidence(hitValue.getScore());
        th.getTimeRange().setStartTime(segmentHit.getHitBoundary().getStart());
        th.getTimeRange().setEndTime(segmentHit.getHitBoundary().getEnd());
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
     */
    private void applyContextAndHit(AllHit allHit, AnnotationHitsTransformer.SegmentHit segmentHit,
                    Iterator<Map.Entry<SegmentBoundary,List<SegmentValue>>> contextIterator) throws AllHitsException {
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
            t.getTimeRange().setStartTime(boundary.getStart());
            t.getTimeRange().setEndTime(boundary.getEnd());
            allHit.getOneBestContext().add(t);

            // now check if this segment also contains the hit
            if (segmentHit.getHitBoundary() == boundary) {
                if (contextEntry.getValue().size() <= segmentHit.getValueHitIndex()) {
                    throw new AllHitsException("hit index outside of available values for segment. SegmentValues:" + contextEntry.getValue().size() + " index:"
                                    + segmentHit.getValueHitIndex());
                }
                applyHit(allHit, segmentHit, contextEntry.getValue().get(segmentHit.getValueHitIndex()));
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
