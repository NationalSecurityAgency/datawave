package datawave.edge.util;

import com.google.protobuf.InvalidProtocolBufferException;
import datawave.edge.protobuf.EdgeData;
import datawave.edge.util.EdgeKey.EDGE_FORMAT;
import datawave.edge.util.EdgeKey.EdgeKeyBuilder;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.accumulo.core.iterators.user.SummingArrayCombiner;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EdgeValueHelperTest {
    
    private Key standardKey;
    private Value standardValue;
    private Value activityValue;
    private Value durationValue;
    
    private List<Long> activity = new ArrayList<>();
    private List<Long> duration = new ArrayList<>();
    
    @Before
    public void setUp() throws Exception {
        // create 3 reference keys
        EdgeKeyBuilder builder = EdgeKey.newBuilder(EDGE_FORMAT.STANDARD);
        builder.setSourceData("SOURCE").setSinkData("SINK").setType("TYPE").setSourceRelationship("SOURCEREL").setSinkRelationship("SINKREL")
                        .setYyyymmdd("19750814").setSourceAttribute1("SOURCECOLLECT").setSinkAttribute1("SINKCOLLECT").setAttribute2("ATTRIBUTE2")
                        .setAttribute3("ATTRIBUTE3").setColvis(new Text("ALL")).setTimestamp(1234l).setDeleted(false);
        standardKey = builder.build().encode();
        // create 3 reference Values
        EdgeValue.EdgeValueBuilder valueBuilder = EdgeValue.newBuilder();
        valueBuilder.setCount(814l);
        EdgeValue edgeValue = valueBuilder.build();
        standardValue = edgeValue.encode();
        
        activity.add(1l);
        activity.add(2l);
        activity.add(3l);
        activity.add(4l);
        activity.add(5l);
        activity.add(6l);
        activity.add(7l);
        activity.add(8l);
        activity.add(9l);
        activity.add(10l);
        activity.add(11l);
        activity.add(12l);
        activity.add(13l);
        activity.add(14l);
        activity.add(15l);
        activity.add(16l);
        activity.add(17l);
        activity.add(18l);
        activity.add(19l);
        activity.add(20l);
        activity.add(21l);
        activity.add(22l);
        activity.add(23l);
        activity.add(24l);
        activityValue = EdgeValueHelper.encodeActivityHistogram(activity);
        duration.add(1l);
        duration.add(2l);
        duration.add(3l);
        duration.add(4l);
        duration.add(5l);
        duration.add(6l);
        duration.add(7l);
        durationValue = EdgeValueHelper.encodeDurationHistogram(duration);
    }
    
    @After
    public void tearDown() throws Exception {
        activity.clear();
        duration.clear();
    }
    
    @Test
    public void testDecodeValueKeyValue() {
        Long count = null;
        try {
            count = EdgeValue.decode(standardValue).getCount();
        } catch (InvalidProtocolBufferException e) {
            fail("Deserialization to EdgeValue failed");
        }
        
        EdgeKey edgeKey = EdgeKey.decode(standardKey);
        
        List<Long> activityList = EdgeValueHelper.decodeActivityHistogram(activityValue);
        List<Long> durationList = EdgeValueHelper.decodeDurationHistogram(durationValue);
        
        assertEquals(814l, (long) count);
        
        assertEquals("SOURCEREL", edgeKey.getSourceRelationship());
        assertEquals("SINKREL", edgeKey.getSinkRelationship());
        assertEquals("TYPE", edgeKey.getType());
        
        assertEquals(24, activityList.size());
        for (int ii = 0; ii < activityList.size(); ii++) {
            assertEquals((long) activityList.get(ii), (ii + 1));
        }
        
        assertEquals(7, durationList.size());
        for (int ii = 0; ii < durationList.size(); ii++) {
            assertEquals((long) durationList.get(ii), (ii + 1));
        }
    }
    
    @Test
    public void testActivityHistogram() {
        
        List<Long> hourList = EdgeValueHelper.getLongListForHour(1, false);
        hourList = EdgeValueHelper.decodeActivityHistogram(EdgeValueHelper.encodeActivityHistogram(hourList));
        assertTrue("Hour List doesn't have correct bit set", (hourList.get(1) == 1l));
        assertTrue("Hour List has incorrect bit set", allButOneSet(hourList, 1));
        assertEquals("Doesn't exceed expected size", hourList.size(), EdgeValueHelper.ACTIVITY_HISTOGRAM_LENGTH);
        
        hourList = EdgeValueHelper.getLongListForHour(0, false);
        hourList = EdgeValueHelper.decodeActivityHistogram(EdgeValueHelper.encodeActivityHistogram(hourList));
        assertTrue("Hour List doesn't have bit set", (hourList.get(0) == 1l));
        assertTrue("Hour List has incorrect bit set", allButOneSet(hourList, 0));
        
        hourList = EdgeValueHelper.getLongListForHour(23, false);
        hourList = EdgeValueHelper.decodeActivityHistogram(EdgeValueHelper.encodeActivityHistogram(hourList));
        assertTrue("Hour List doesn't have bit set", (hourList.get(23) == 1l));
        assertTrue("Hour List has incorrect bit set", allButOneSet(hourList, 23));
        
        hourList = EdgeValueHelper.getLongListForHour(14, false);
        hourList = EdgeValueHelper.decodeActivityHistogram(EdgeValueHelper.encodeActivityHistogram(hourList));
        assertTrue("Hour List doesn't have bit set", (hourList.get(14) == 1l));
        assertTrue("Hour List has incorrect bit set", allButOneSet(hourList, 14));
        
        hourList = EdgeValueHelper.getLongListForHour(8, true);
        hourList = EdgeValueHelper.decodeActivityHistogram(EdgeValueHelper.encodeActivityHistogram(hourList));
        assertTrue("Hour List doesn't have bit set", (hourList.get(8) == -1l));
        
    }
    
    @Test
    public void testFillHistogramToExactSize() {
        // only set 3 durations instead of the expected 7
        List<Long> incompleteList = createIncompleteHistogram();
        
        assertEquals(3, incompleteList.size());
        
        EdgeValueHelper.fillHistogramGaps(incompleteList, EdgeValueHelper.DURATION_HISTOGRAM_LENGTH);
        assertEquals(EdgeValueHelper.DURATION_HISTOGRAM_LENGTH, incompleteList.size());
        
        EdgeValueHelper.fillHistogramGaps(incompleteList, EdgeValueHelper.ACTIVITY_HISTOGRAM_LENGTH);
        assertEquals(EdgeValueHelper.ACTIVITY_HISTOGRAM_LENGTH, incompleteList.size());
    }
    
    @Test
    public void testDecodeActivityHistogramWithProtoValue() {
        // only set 3 durations instead of the expected 7
        List<Long> incompleteList = createIncompleteHistogram();
        
        EdgeData.EdgeValue.Builder builder = EdgeData.EdgeValue.newBuilder();
        builder.addAllHours(incompleteList);
        Value valueWithMissingDurations = new Value(builder.build().toByteArray());
        
        List<Long> activityByHour = EdgeValueHelper.decodeActivityHistogram(valueWithMissingDurations);
        verifyValuesInIncompleteHistogram(activityByHour, EdgeValueHelper.ACTIVITY_HISTOGRAM_LENGTH);
    }
    
    @Test
    public void testDecodeActivityHistogramWithNonProtoValue() {
        // only set 3 durations instead of the expected 7
        List<Long> incompleteList = createIncompleteHistogram();
        
        Value nonProtoValue = new Value(new SummingArrayCombiner.VarLongArrayEncoder().encode(incompleteList));
        
        List<Long> activityByHour = EdgeValueHelper.decodeActivityHistogram(nonProtoValue);
        verifyValuesInIncompleteHistogram(activityByHour, EdgeValueHelper.ACTIVITY_HISTOGRAM_LENGTH);
    }
    
    @Test
    public void testDecodeActivityHistogramWithList() {
        // only set 3 durations instead of the expected 7
        List<Long> incompleteList = createIncompleteHistogram();
        
        List<Long> activityByHour = EdgeValueHelper.decodeActivityHistogram(incompleteList);
        verifyValuesInIncompleteHistogram(activityByHour, EdgeValueHelper.ACTIVITY_HISTOGRAM_LENGTH);
    }
    
    @Test
    public void testDecodeDurationHistogramWithProtoValue() {
        // only set 3 durations instead of the expected 7
        List<Long> incompleteList = createIncompleteHistogram();
        
        EdgeData.EdgeValue.Builder builder = EdgeData.EdgeValue.newBuilder();
        builder.addAllDuration(incompleteList);
        Value valueWithMissingDurations = new Value(builder.build().toByteArray());
        
        List<Long> durations = EdgeValueHelper.decodeDurationHistogram(valueWithMissingDurations);
        verifyValuesInIncompleteHistogram(durations, EdgeValueHelper.DURATION_HISTOGRAM_LENGTH);
    }
    
    @Test
    public void testDecodeDurationHistogramWithNonProtoValue() {
        // only set 3 durations instead of the expected 7
        List<Long> incompleteList = createIncompleteHistogram();
        
        Value nonProtoValue = new Value(new SummingArrayCombiner.VarLongArrayEncoder().encode(incompleteList));
        
        List<Long> durations = EdgeValueHelper.decodeDurationHistogram(nonProtoValue);
        verifyValuesInIncompleteHistogram(durations, EdgeValueHelper.DURATION_HISTOGRAM_LENGTH);
    }
    
    @Test
    public void testDecodeDurationHistogramWithList() {
        // only set 3 durations instead of the expected 7
        List<Long> incompleteList = createIncompleteHistogram();
        
        List<Long> durations = EdgeValueHelper.decodeDurationHistogram(incompleteList);
        verifyValuesInIncompleteHistogram(durations, EdgeValueHelper.DURATION_HISTOGRAM_LENGTH);
    }
    
    private List<Long> createIncompleteHistogram() {
        List<Long> incompleteList = new ArrayList<>();
        incompleteList.add((long) 0);
        incompleteList.add((long) 1);
        incompleteList.add((long) 2);
        return incompleteList;
    }
    
    private void verifyValuesInIncompleteHistogram(List<Long> histogram, int expectedLength) {
        assertEquals("Doesn't exceed expected size", expectedLength, histogram.size());
        assertEquals("Hour List doesn't have correct bit set " + histogram.get(0), 0L, (long) histogram.get(0));
        assertEquals("Hour List doesn't have correct bit set " + histogram.get(1), 1L, (long) histogram.get(1));
        assertEquals("Hour List doesn't have correct bit set " + histogram.get(2), 2L, (long) histogram.get(2));
        for (int i = 3; i < expectedLength; i++) {
            assertEquals("Hour List's missing hours weren't correctly filled with zeros " + i + " " + expectedLength, 0, (long) histogram.get(i));
        }
    }
    
    @Test
    public void testReadOldActivity() {
        List<Long> longList = EdgeValueHelper.initUnitList(24, 6, false);
        byte[] oldBytes = null;
        try {
            oldBytes = SummingArrayCombiner.VAR_LONG_ARRAY_ENCODER.encode(longList);
        } catch (ValueFormatException e) {
            fail("Failed to encode variable length long array: " + e.getMessage());
        }
        List<Long> oldAsList = EdgeValueHelper.decodeActivityHistogram(new Value(oldBytes));
        assertEquals(new Long(1l), oldAsList.get(6));
    }
    
    private static final Value NULL_VALUE = new Value(new byte[0]);
    
    @Test
    public void testReadNullActivity() {
        List<Long> oldAsList = EdgeValueHelper.decodeActivityHistogram(NULL_VALUE);
        assertEquals(EdgeValueHelper.ACTIVITY_HISTOGRAM_LENGTH, oldAsList.size());
    }
    
    @Test
    public void testReadNullDuration() {
        List<Long> oldAsList = EdgeValueHelper.decodeDurationHistogram(NULL_VALUE);
        assertEquals(EdgeValueHelper.DURATION_HISTOGRAM_LENGTH, oldAsList.size());
    }
    
    private boolean allButOneSet(List<Long> longs, int index) {
        for (int ii = 0; ii < longs.size(); ii++) {
            if (ii != index) {
                if (longs.get(ii) != 0) {
                    return false;
                }
            } else {
                if (longs.get(ii) != 1) {
                    return false;
                }
            }
        }
        return true;
    }
    
}
