package datawave.query.transformer.annotation;

import java.util.Comparator;

import datawave.annotation.protobuf.v1.SegmentBoundary;

/**
 * Sort by ascending boundary type value > 1 first Sort by descending boundary type < 1 always after all boundary type greater than 0 Secondary sort for equal
 * boundary types, sort by ascending start Tertiary sort for equal start, sort by ascending end
 */
public class BoundaryComparator implements Comparator<SegmentBoundary> {
    @Override
    public int compare(SegmentBoundary o1, SegmentBoundary o2) {
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o1 == null) {
            return 1;
        } else if (o2 == null) {
            return -1;
        }

        if (o1.getBoundaryTypeValue() != o2.getBoundaryTypeValue()) {
            if (o1.getBoundaryTypeValue() > 0 && o2.getBoundaryTypeValue() > 0) {
                // lower number sorts first
                return o1.getBoundaryTypeValue() - o2.getBoundaryTypeValue();
            } else if (o1.getBoundaryTypeValue() < 1 && o2.getBoundaryTypeValue() < 1) {
                // 0 comes before -1
                return o2.getBoundaryTypeValue() - o1.getBoundaryTypeValue();
            } else if (o1.getBoundaryTypeValue() < 1) {
                // anything is lower than -1/0
                return 1;
            } else {
                // anything is before -1/0
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
