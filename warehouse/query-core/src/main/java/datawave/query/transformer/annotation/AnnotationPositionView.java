package datawave.query.transformer.annotation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.data.normalizer.Normalizer;

/**
 * Immutable, normalized view of an annotation. Boundary ordinals are semantic positions; value indexes remain indexes into the score-sorted lists supplied to
 * {@link AllHitsFactory}.
 */
public final class AnnotationPositionView {
    private final List<SegmentBoundary> boundaries;
    private final List<List<ValuePosition>> valuesByBoundary;

    private AnnotationPositionView(List<SegmentBoundary> boundaries, List<List<ValuePosition>> valuesByBoundary) {
        this.boundaries = Collections.unmodifiableList(boundaries);
        this.valuesByBoundary = Collections.unmodifiableList(valuesByBoundary);
    }

    public static AnnotationPositionView of(TreeMap<SegmentBoundary,List<SegmentValue>> sortedSegments, Normalizer<String> normalizer) {
        List<SegmentBoundary> boundaries = new ArrayList<>(sortedSegments.keySet());
        List<List<ValuePosition>> positions = new ArrayList<>();
        for (int boundaryIndex = 0; boundaryIndex < boundaries.size(); boundaryIndex++) {
            SegmentBoundary boundary = boundaries.get(boundaryIndex);
            List<SegmentValue> values = sortedSegments.get(boundary);
            List<ValuePosition> boundaryPositions = new ArrayList<>();
            for (int valueIndex = 0; valueIndex < values.size(); valueIndex++) {
                SegmentValue value = values.get(valueIndex);
                boundaryPositions.add(new ValuePosition(boundaryIndex, valueIndex, boundary, value, normalizer.normalize(value.getValue())));
            }
            positions.add(Collections.unmodifiableList(boundaryPositions));
        }
        return new AnnotationPositionView(boundaries, positions);
    }

    public List<SegmentBoundary> getBoundaries() {
        return boundaries;
    }

    public List<List<ValuePosition>> getValuesByBoundary() {
        return valuesByBoundary;
    }

    public List<ValuePosition> getValues(int boundaryIndex) {
        return valuesByBoundary.get(boundaryIndex);
    }
}
