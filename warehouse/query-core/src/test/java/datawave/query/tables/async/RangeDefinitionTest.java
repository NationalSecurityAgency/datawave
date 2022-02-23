package datawave.query.tables.async;

import datawave.query.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RangeDefinitionTest {
    
    @Test
    public void docSpecificRangeTest() {
        Range docSpecificRange = new Range(new Key("20190101_0", "dataType\u0000some-doc-id"), false, new Key("20190101_0", "dataType\u0000some-doc-id\u0000"),
                        false);
        assertTrue(RangeDefinition.isDocSpecific(docSpecificRange));
    }
    
    @Test
    public void shardRangeTest() {
        Range shardRange = new Range(new Key("20190101_0"), false, new Key("20190101_0\u0000"), false);
        assertFalse(RangeDefinition.isDocSpecific(shardRange));
    }
    
    @Test
    public void dayRangeTest() {
        Range dayRange = new Range(new Key("20190101_0"), false, new Key("20190101" + Constants.MAX_UNICODE_STRING), false);
        assertFalse(RangeDefinition.isDocSpecific(dayRange));
    }
    
    @Test
    public void allDocSpecificRangeTest() {
        List<Range> ranges = new ArrayList<>();
        ranges.add(new Range(new Key("20190101_0", "dataType\u0000some-doc-id"), false, new Key("20190101_0", "dataType\u0000some-doc-id\u0000"), false));
        ranges.add(new Range(new Key("20190101_1", "dataType\u0000some-doc-id1"), false, new Key("20190101_1", "dataType\u0000some-doc-id1\u0000"), false));
        ranges.add(new Range(new Key("20190101_2", "dataType\u0000some-doc-id2"), false, new Key("20190101_2", "dataType\u0000some-doc-id2\u0000"), false));
        ranges.add(new Range(new Key("20190101_3", "dataType\u0000some-doc-id3"), false, new Key("20190101_3", "dataType\u0000some-doc-id3\u0000"), false));
        ranges.add(new Range(new Key("20190101_4", "dataType\u0000some-doc-id4"), false, new Key("20190101_4", "dataType\u0000some-doc-id4\u0000"), false));
        assertTrue(RangeDefinition.allDocSpecific(ranges));
    }
    
    @Test
    public void allDocSpecificWithDayRangeTest() {
        List<Range> ranges = new ArrayList<>();
        ranges.add(new Range(new Key("20190101_0", "dataType\u0000some-doc-id"), false, new Key("20190101_0", "dataType\u0000some-doc-id\u0000"), false));
        ranges.add(new Range(new Key("20190101_1", "dataType\u0000some-doc-id1"), false, new Key("20190101_1", "dataType\u0000some-doc-id1\u0000"), false));
        ranges.add(new Range(new Key("20190101_2", "dataType\u0000some-doc-id2"), false, new Key("20190101_2", "dataType\u0000some-doc-id2\u0000"), false));
        ranges.add(new Range(new Key("20190101_3", "dataType\u0000some-doc-id3"), false, new Key("20190101_3", "dataType\u0000some-doc-id3\u0000"), false));
        ranges.add(new Range(new Key("20190101_4", "dataType\u0000some-doc-id4"), false, new Key("20190101_4", "dataType\u0000some-doc-id4\u0000"), false));
        ranges.add(new Range(new Key("20190101_0"), false, new Key("20190101" + Constants.MAX_UNICODE_STRING), false));
        assertFalse(RangeDefinition.allDocSpecific(ranges));
    }
    
    @Test
    public void allDocSpecificWithShardRangeTest() {
        List<Range> ranges = new ArrayList<>();
        ranges.add(new Range(new Key("20190101_0", "dataType\u0000some-doc-id"), false, new Key("20190101_0", "dataType\u0000some-doc-id\u0000"), false));
        ranges.add(new Range(new Key("20190101_1", "dataType\u0000some-doc-id1"), false, new Key("20190101_1", "dataType\u0000some-doc-id1\u0000"), false));
        ranges.add(new Range(new Key("20190101_2", "dataType\u0000some-doc-id2"), false, new Key("20190101_2", "dataType\u0000some-doc-id2\u0000"), false));
        ranges.add(new Range(new Key("20190101_3", "dataType\u0000some-doc-id3"), false, new Key("20190101_3", "dataType\u0000some-doc-id3\u0000"), false));
        ranges.add(new Range(new Key("20190101_4", "dataType\u0000some-doc-id4"), false, new Key("20190101_4", "dataType\u0000some-doc-id4\u0000"), false));
        ranges.add(new Range(new Key("20190101_0"), false, new Key("20190101_0\u0000"), false));
        assertFalse(RangeDefinition.allDocSpecific(ranges));
    }
}
