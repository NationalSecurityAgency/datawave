package datawave.core.iterators;

import datawave.query.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class DatawaveFieldIndexIteratorJexlTest {
    @Test
    public void buildBoundingFiRange_notUpperInclusive_singleChar_test() {
        DatawaveFieldIndexFilterIteratorJexl iteratorJexl = DatawaveFieldIndexFilterIteratorJexl.builder().upperInclusive(false).lowerInclusive(true)
                        .withMaxRangeSplit(1).withFieldName("FIELD").withFieldValue("a").withUpperBound("b").build();
        
        Text row = new Text("row");
        Text fiName = new Text("fi" + Constants.NULL + "FIELD");
        Text fieldValue = new Text("a");
        Text fieldValueNullAppended = new Text("a" + Constants.NULL);
        
        List<Range> ranges = iteratorJexl.buildBoundingFiRanges(row, fiName, fieldValue);
        
        Assert.assertNotEquals(null, ranges);
        Assert.assertEquals(1, ranges.size());
        Range r = ranges.get(0);
        
        // note that the end key is expected to be inclusive even though upperInclusive is set to false because the value has been decremented by one
        Assert.assertTrue(r.isStartKeyInclusive());
        Assert.assertTrue(r.isEndKeyInclusive());
        Assert.assertEquals(new Key(row, fiName, fieldValueNullAppended), r.getStartKey());
        Assert.assertEquals(new Key(row, fiName, new Text("a" + Constants.MAX_UNICODE_STRING)), r.getEndKey());
    }
    
    @Test
    public void buildBoundingFiRange_notUpperInclusive_singleCharAZ_test() {
        DatawaveFieldIndexFilterIteratorJexl iteratorJexl = DatawaveFieldIndexFilterIteratorJexl.builder().upperInclusive(false).lowerInclusive(true)
                        .withMaxRangeSplit(1).withFieldName("FIELD").withFieldValue("a").withUpperBound("z").build();
        
        Text row = new Text("row");
        Text fiName = new Text("fi" + Constants.NULL + "FIELD");
        Text fieldValue = new Text("a");
        Text fieldValueNullAppended = new Text("a" + Constants.NULL);
        
        List<Range> ranges = iteratorJexl.buildBoundingFiRanges(row, fiName, fieldValue);
        
        Assert.assertNotEquals(null, ranges);
        Assert.assertEquals(1, ranges.size());
        Range r = ranges.get(0);
        
        // note that the end key is expected to be inclusive even though upperInclusive is set to false because the value has been decremented by one
        Assert.assertTrue(r.isStartKeyInclusive());
        Assert.assertTrue(r.isEndKeyInclusive());
        Assert.assertEquals(new Key(row, fiName, fieldValueNullAppended), r.getStartKey());
        Assert.assertEquals(new Key(row, fiName, new Text("y" + Constants.MAX_UNICODE_STRING)), r.getEndKey());
    }
    
    @Test
    public void buildBoundingFiRange_notUpperInclusive_multiChar_test() {
        DatawaveFieldIndexFilterIteratorJexl iteratorJexl = DatawaveFieldIndexFilterIteratorJexl.builder().upperInclusive(false).lowerInclusive(true)
                        .withMaxRangeSplit(1).withFieldName("FIELD").withFieldValue("a").withUpperBound("az").build();
        
        Text row = new Text("row");
        Text fiName = new Text("fi" + Constants.NULL + "FIELD");
        Text fieldValue = new Text("aa");
        Text fieldValueNullAppended = new Text("aa" + Constants.NULL);
        
        List<Range> ranges = iteratorJexl.buildBoundingFiRanges(row, fiName, fieldValue);
        
        Assert.assertNotEquals(null, ranges);
        Assert.assertEquals(1, ranges.size());
        Range r = ranges.get(0);
        
        // note that the end key is expected to be inclusive even though upperInclusive is set to false because the value has been decremented by one
        Assert.assertTrue(r.isStartKeyInclusive());
        Assert.assertTrue(r.isEndKeyInclusive());
        Assert.assertEquals(new Key(row, fiName, fieldValueNullAppended), r.getStartKey());
        Assert.assertEquals(new Key(row, fiName, new Text("ay" + Constants.MAX_UNICODE_STRING)), r.getEndKey());
    }
    
    @Test
    public void buildBoundingFiRange_notUpperInclusive_multiCharStartSingleEnd_test() {
        DatawaveFieldIndexFilterIteratorJexl iteratorJexl = DatawaveFieldIndexFilterIteratorJexl.builder().upperInclusive(false).lowerInclusive(true)
                        .withMaxRangeSplit(1).withFieldName("FIELD").withFieldValue("a").withUpperBound("z").build();
        
        Text row = new Text("row");
        Text fiName = new Text("fi" + Constants.NULL + "FIELD");
        Text fieldValue = new Text("aa");
        Text fieldValueNullAppended = new Text("aa" + Constants.NULL);
        
        List<Range> ranges = iteratorJexl.buildBoundingFiRanges(row, fiName, fieldValue);
        
        Assert.assertNotEquals(null, ranges);
        Assert.assertEquals(1, ranges.size());
        Range r = ranges.get(0);
        
        // note that the end key is expected to be inclusive even though upperInclusive is set to false because the value has been decremented by one
        Assert.assertTrue(r.isStartKeyInclusive());
        Assert.assertTrue(r.isEndKeyInclusive());
        Assert.assertEquals(new Key(row, fiName, fieldValueNullAppended), r.getStartKey());
        Assert.assertEquals(new Key(row, fiName, new Text("y" + Constants.MAX_UNICODE_STRING)), r.getEndKey());
    }
}
