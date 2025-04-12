package datawave.annotation.util.v0;

import datawave.annotation.model.TimeBounds;
import datawave.annotation.protobuf.v0.SegmentBoundary;
import datawave.annotation.protobuf.v0.SegmentBoundaryType;

public class TimeBoundaryParser {
    public static TimeBounds parse(SegmentBoundary source) {
        if (source.getType() != SegmentBoundaryType.TIME) {
            throw new IllegalArgumentException("cannot parse TimeBounds from source type: " + source.getType().name());
        }

        float start = Float.parseFloat(source.getStart());
        float end = Float.parseFloat(source.getEnd());

        return new TimeBounds(start, end);
    }

    public static SegmentBoundary encode(TimeBounds source) {
        return encode(SegmentBoundary.newBuilder(), source);
    }

    public static SegmentBoundary encode(SegmentBoundary.Builder builder, TimeBounds source) {
        builder.clear();

        builder.setType(SegmentBoundaryType.TIME);
        builder.setStart(Float.toString(source.getStart()));
        builder.setEnd(Float.toString(source.getEnd()));

        return builder.build();
    }
}
