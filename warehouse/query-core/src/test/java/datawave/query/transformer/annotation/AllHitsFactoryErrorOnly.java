package datawave.query.transformer.annotation;

import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.query.transformer.annotation.model.AllHits;

/**
 * AllHitsFactory that always throws an exception for testing purposes
 */
public class AllHitsFactoryErrorOnly extends AllHitsFactory {

    @Override
    public AllHits create(String annotationId, List<AnnotationHitsTransformer.SegmentHit> orderedHits,
                    TreeMap<SegmentBoundary,List<SegmentValue>> sortedSegments, TimeUnit timeUNit) throws AllHitsException {
        throw new AllHitsException("test failure");
    }

    @Override
    public AllHits create(String annotationId, List<AnnotationHitsTransformer.SegmentHit> orderedHits,
                    TreeMap<SegmentBoundary,List<SegmentValue>> sortedSegments) throws AllHitsException {
        throw new AllHitsException("test failure");
    }
}
