package datawave.query.transformer.annotation;

import java.util.Comparator;

import datawave.annotation.protobuf.v1.SegmentBoundary;

/**
 * Primarily sorts ascending by boundary type value, with KNOWN boundary types to the front, ascending by value. UNKNOWN boundary type will sort after all KNOWN
 * boundary types, followed by all UNRECOGNIZED types. Secondary sorts ascending by start. Tertiary sorts ascending by end.
 *
 * @see datawave.annotation.protobuf.v1.BoundaryType
 */
public class BoundaryComparator implements Comparator<SegmentBoundary> {
    @Override
    public int compare(SegmentBoundary o1, SegmentBoundary o2) {
        // sort null values to the end
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 == null) {
            return 1;
        } else if (o2 == null) {
            return -1;
        }

        // primary sort on boundary type
        if (o1.getBoundaryTypeValue() != o2.getBoundaryTypeValue()) {
            // excluding UNKNOWN and UNRECOGNIZED
            if (o1.getBoundaryTypeValue() > 0 && o2.getBoundaryTypeValue() > 0) {
                // lower number sorts first
                return o1.getBoundaryTypeValue() - o2.getBoundaryTypeValue();
            } else if (o1.getBoundaryTypeValue() < 1 && o2.getBoundaryTypeValue() < 1) {
                // UNKNOWN comes before UNRECOGNIZED
                return o2.getBoundaryTypeValue() - o1.getBoundaryTypeValue();
            } else if (o1.getBoundaryTypeValue() < 1) {
                // First value is UNKNOWN/UNRECOGNIZED
                return 1;
            } else {
                // Second value is UNKNOWN/UNRECOGNIZED
                return -1;
            }
        }

        if (o1.getStart() != o2.getStart()) {
            // segment that starts sooner is less
            return o1.getStart() - o2.getStart();
        } else {
            // segment that ends sooner, is less
            return o1.getEnd() - o2.getEnd();
        }
    }
}
