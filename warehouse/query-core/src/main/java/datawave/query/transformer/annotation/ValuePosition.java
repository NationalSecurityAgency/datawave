package datawave.query.transformer.annotation;

import java.util.Objects;

import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;

/** The stable position of one value in an annotation search view. */
public final class ValuePosition {
    private final int boundaryIndex;
    private final int valueIndex;
    private final SegmentBoundary boundary;
    private final SegmentValue value;
    private final String normalizedValue;

    public ValuePosition(int boundaryIndex, int valueIndex, SegmentBoundary boundary, SegmentValue value, String normalizedValue) {
        this.boundaryIndex = boundaryIndex;
        this.valueIndex = valueIndex;
        this.boundary = Objects.requireNonNull(boundary, "boundary");
        this.value = Objects.requireNonNull(value, "value");
        this.normalizedValue = Objects.requireNonNull(normalizedValue, "normalizedValue");
    }

    public int getBoundaryIndex() {
        return boundaryIndex;
    }

    public int getValueIndex() {
        return valueIndex;
    }

    public SegmentBoundary getBoundary() {
        return boundary;
    }

    public SegmentValue getValue() {
        return value;
    }

    public String getNormalizedValue() {
        return normalizedValue;
    }
}
