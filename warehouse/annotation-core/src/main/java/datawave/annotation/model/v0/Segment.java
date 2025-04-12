package datawave.annotation.model.v0;

import datawave.annotation.protobuf.v0.SegmentData;
import datawave.data.hash.HashUID;

public class Segment {
    private String segmentId;
    private final SegmentData segmentData;

    protected Segment(String segmentId, SegmentData segmentData) {
        this.segmentId = segmentId;
        this.segmentData = segmentData;
    }

    protected void generateUID() {
        if (segmentData == null) {
            throw new IllegalStateException("Can't generate uid because the segment data was null");
        }
        this.segmentId = HashUID.builder().newId(segmentData.toByteArray()).toString();
    }

    public String getSegmentId() {
        if (segmentId == null) {
            throw new IllegalStateException("No UID has been generated, call generateUID() first");
        }
        return segmentId;
    }

    public SegmentData getSegmentData() {
        return segmentData;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        String segmentId;
        SegmentData segmentData;

        protected Builder() {

        }

        public Builder setSegmentId(String segmentId) {
            this.segmentId = segmentId;
            return this;
        }

        public Builder setSegmentData(SegmentData segmentData) {
            this.segmentData = segmentData;
            return this;
        }

        public Segment build() {
            Segment s = new Segment(segmentId, segmentData);
            //@formatter:off
            /* TODO: this is the place where we need to perform validation of the following:
               - segmentData
                 - ensure that there is a segment boundary and at least one valid segment value.
                 - segmentBoundary
                   - ensure the start and end values are appropriate for the type of boundary (TIME/POSITION) (e.g., x,y for point vs. int for token/character vs float for time.
                 - segmentValue
                   - ensure that the segmentValue has a value is non null or empty and has a score and that if the extension is not null is it not empty.
            */
            //@formatter:on
            if (segmentId == null) {
                s.generateUID();
            }
            return s;
        }
    }
}
