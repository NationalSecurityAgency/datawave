package datawave.query.transformer.annotation;

import java.util.Comparator;

import datawave.annotation.protobuf.v1.SegmentValue;

/**
 * Sort ascending by score, then by value
 */
public class SegmentValueByScoreComparator implements Comparator<SegmentValue> {
    @Override
    public int compare(SegmentValue o1, SegmentValue o2) {
        float scoreDiff = o1.getScore() - o2.getScore();
        if (scoreDiff == 0) {
            // equal scores sort by value
            return o1.getValue().compareTo(o2.getValue());
        } else if (scoreDiff < 0) {
            return -1;
        } else {
            return 1;
        }
    }
}
