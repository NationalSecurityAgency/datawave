package datawave.annotation.protobuf.v0;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.Test;

public class SegmentTest {
    @Test
    public void SimpleParseTest() {
        SegmentBoundary.Builder segmentBoundaryBuilder = SegmentBoundary.newBuilder();
        segmentBoundaryBuilder.setType(SegmentBoundaryType.TIME);
        segmentBoundaryBuilder.setStart("0.154");
        segmentBoundaryBuilder.setEnd("0.52");

        SegmentData.Builder segmentDataBuilder = SegmentData.newBuilder();
        SegmentValue.Builder segmentValueBuilder = SegmentValue.newBuilder();

        segmentDataBuilder.setBoundary(segmentBoundaryBuilder.build());
        segmentValueBuilder.setValue("cow");
        segmentValueBuilder.setScore(.235f);
        segmentDataBuilder.addValue(segmentValueBuilder.build());
        segmentValueBuilder.clear();
        segmentValueBuilder.setValue("horse");
        segmentValueBuilder.setScore(.21f);
        segmentValueBuilder.setExtension("animal");
        segmentDataBuilder.addValue(segmentValueBuilder.build());
        segmentDataBuilder.build();
    }

    @Test
    public void builderPatternSerDeTest() throws Exception {
        SegmentBoundary boundary = SegmentBoundary.newBuilder().setType(SegmentBoundaryType.TIME).setStart("0.154").setEnd("0.52").build();

        SegmentValue segmentValueOne = SegmentValue.newBuilder().setValue("cow").setScore(.235f).build();

        SegmentValue segmentValueTwo = SegmentValue.newBuilder().setValue("horse").setScore(.21f).setExtension("animal").build();

        SegmentData segmentData = SegmentData.newBuilder().addValue(segmentValueOne).addValue(segmentValueTwo).setBoundary(boundary).build();

        byte[] serialized = segmentData.toByteArray();

        SegmentData deserializedData = SegmentData.parseFrom(serialized);
        assertEquals(2, deserializedData.getValueCount());

        SegmentValue deserializedValueOne = deserializedData.getValue(0);
        assertEquals("cow", deserializedValueOne.getValue());
        assertEquals(.235f, deserializedValueOne.getScore());
        assertEquals("", deserializedValueOne.getExtension());

        SegmentValue deserializedValueTwo = deserializedData.getValue(1);
        assertEquals("horse", deserializedValueTwo.getValue());
        assertEquals(.21f, deserializedValueTwo.getScore());
        assertEquals("animal", deserializedValueTwo.getExtension());
    }
}
