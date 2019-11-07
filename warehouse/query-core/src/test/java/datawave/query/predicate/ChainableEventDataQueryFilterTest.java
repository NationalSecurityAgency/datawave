package datawave.query.predicate;

import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Map;

public class ChainableEventDataQueryFilterTest {
    private ChainableEventDataQueryFilter filter;
    
    @Before
    public void setup() {
        filter = new ChainableEventDataQueryFilter();
    }
    
    @Test
    public void startNewDocumentTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        
        mockFilter1.startNewDocument(key);
        mockFilter2.startNewDocument(key);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        filter.startNewDocument(key);
        
        EasyMock.verify(mockFilter1, mockFilter2);
    }
    
    @Test
    public void apply_successTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Map.Entry<Key,String> entry = new AbstractMap.SimpleEntry<>(key, "");
        
        EasyMock.expect(mockFilter1.apply(entry)).andReturn(true);
        EasyMock.expect(mockFilter2.apply(entry)).andReturn(true);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        boolean result = filter.apply(entry);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertTrue(result);
    }
    
    @Test
    public void apply_failFilter2Test() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Map.Entry<Key,String> entry = new AbstractMap.SimpleEntry<>(key, "");
        
        EasyMock.expect(mockFilter1.apply(entry)).andReturn(true);
        EasyMock.expect(mockFilter2.apply(entry)).andReturn(false);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        boolean result = filter.apply(entry);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result);
    }
    
    @Test
    public void apply_failFilter1Test() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Map.Entry<Key,String> entry = new AbstractMap.SimpleEntry<>(key, "");
        
        EasyMock.expect(mockFilter1.apply(entry)).andReturn(false);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        boolean result = filter.apply(entry);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result);
    }
    
    @Test
    public void keep_successTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        
        EasyMock.expect(mockFilter1.keep(key)).andReturn(true);
        EasyMock.expect(mockFilter2.keep(key)).andReturn(true);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        boolean result = filter.keep(key);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertTrue(result);
    }
    
    @Test
    public void keep_failFilter2Test() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        
        EasyMock.expect(mockFilter1.keep(key)).andReturn(true);
        EasyMock.expect(mockFilter2.keep(key)).andReturn(false);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        boolean result = filter.keep(key);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result);
    }
    
    @Test
    public void keep_failFilter1Test() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        
        EasyMock.expect(mockFilter1.keep(key)).andReturn(false);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        boolean result = filter.keep(key);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result);
    }
    
    @Test
    public void getStartKey_filter1MaxTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Key filter1Result = new Key("234");
        Key filter2Result = new Key("123");
        
        EasyMock.expect(mockFilter1.getStartKey(key)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getStartKey(key)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Key result = filter.getStartKey(key);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result == filter1Result);
    }
    
    @Test
    public void getStartKey_filter2MaxTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Key filter1Result = new Key("123");
        Key filter2Result = new Key("234");
        
        EasyMock.expect(mockFilter1.getStartKey(key)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getStartKey(key)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Key result = filter.getStartKey(key);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result == filter2Result);
    }
    
    @Test
    public void getStopKey_filter1MinTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Key filter1Result = new Key("123");
        Key filter2Result = new Key("234");
        
        EasyMock.expect(mockFilter1.getStopKey(key)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getStopKey(key)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Key result = filter.getStopKey(key);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result == filter1Result);
    }
    
    @Test
    public void getStartKey_filter2MinTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Key filter1Result = new Key("234");
        Key filter2Result = new Key("123");
        
        EasyMock.expect(mockFilter1.getStopKey(key)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getStopKey(key)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Key result = filter.getStopKey(key);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result == filter2Result);
    }
    
    @Test
    public void getKeyRange_filter1InclusiveTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Document document = new Document();
        Map.Entry<Key,Document> keyDocumentEntry = new AbstractMap.SimpleEntry<>(key, document);
        Range filter1Result = new Range(new Key("234"), true, new Key("999"), true);
        Range filter2Result = new Range(new Key("2"), true, new Key("9999"), true);
        
        EasyMock.expect(mockFilter1.getKeyRange(keyDocumentEntry)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getKeyRange(keyDocumentEntry)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Range result = filter.getKeyRange(keyDocumentEntry);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result.equals(filter1Result));
    }
    
    @Test
    public void getKeyRange_filter1ExclusiveTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Document document = new Document();
        Map.Entry<Key,Document> keyDocumentEntry = new AbstractMap.SimpleEntry<>(key, document);
        Range filter1Result = new Range(new Key("234"), false, new Key("999"), false);
        Range filter2Result = new Range(new Key("2"), true, new Key("9999"), true);
        
        EasyMock.expect(mockFilter1.getKeyRange(keyDocumentEntry)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getKeyRange(keyDocumentEntry)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Range result = filter.getKeyRange(keyDocumentEntry);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result.equals(filter1Result));
    }
    
    @Test
    public void getKeyRange_filter2InclusiveTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Document document = new Document();
        Map.Entry<Key,Document> keyDocumentEntry = new AbstractMap.SimpleEntry<>(key, document);
        Range filter1Result = new Range(new Key("2"), true, new Key("9999"), true);
        Range filter2Result = new Range(new Key("234"), true, new Key("999"), true);
        
        EasyMock.expect(mockFilter1.getKeyRange(keyDocumentEntry)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getKeyRange(keyDocumentEntry)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Range result = filter.getKeyRange(keyDocumentEntry);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result.equals(filter2Result));
    }
    
    @Test
    public void getKeyRange_filter2ExclusiveTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Document document = new Document();
        Map.Entry<Key,Document> keyDocumentEntry = new AbstractMap.SimpleEntry<>(key, document);
        Range filter1Result = new Range(new Key("2"), true, new Key("9999"), true);
        Range filter2Result = new Range(new Key("234"), false, new Key("999"), false);
        
        EasyMock.expect(mockFilter1.getKeyRange(keyDocumentEntry)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getKeyRange(keyDocumentEntry)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Range result = filter.getKeyRange(keyDocumentEntry);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result.equals(filter2Result));
    }
    
    @Test
    public void getKeyRange_mixedTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Document document = new Document();
        Map.Entry<Key,Document> keyDocumentEntry = new AbstractMap.SimpleEntry<>(key, document);
        Range filter1Result = new Range(new Key("1"), false, new Key("9999"), true);
        Range filter2Result = new Range(new Key("1"), true, new Key("9999"), false);
        
        EasyMock.expect(mockFilter1.getKeyRange(keyDocumentEntry)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getKeyRange(keyDocumentEntry)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Range result = filter.getKeyRange(keyDocumentEntry);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result.getStartKey().getRow().toString().equals("1"));
        Assert.assertFalse(result.isStartKeyInclusive());
        Assert.assertTrue(result.getEndKey().getRow().toString().equals("9999"));
        Assert.assertFalse(result.isEndKeyInclusive());
    }
    
    @Test
    public void getSeekRange_filter1InclusiveTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key current = new Key();
        Key end = new Key();
        
        Range filter1Result = new Range(new Key("234"), true, new Key("999"), true);
        Range filter2Result = new Range(new Key("2"), true, new Key("9999"), true);
        
        EasyMock.expect(mockFilter1.getSeekRange(current, end, true)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getSeekRange(current, end, true)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Range result = filter.getSeekRange(current, end, true);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result.equals(filter1Result));
    }
    
    @Test
    public void getSeekRange_filter1ExclusiveTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key current = new Key();
        Key end = new Key();
        
        Range filter1Result = new Range(new Key("234"), false, new Key("999"), false);
        Range filter2Result = new Range(new Key("2"), true, new Key("9999"), true);
        
        EasyMock.expect(mockFilter1.getSeekRange(current, end, true)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getSeekRange(current, end, true)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Range result = filter.getSeekRange(current, end, true);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result.equals(filter1Result));
    }
    
    @Test
    public void getSeekRange_filter2InclusiveTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key current = new Key();
        Key end = new Key();
        
        Range filter1Result = new Range(new Key("2"), true, new Key("9999"), true);
        Range filter2Result = new Range(new Key("234"), true, new Key("999"), true);
        
        EasyMock.expect(mockFilter1.getSeekRange(current, end, true)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getSeekRange(current, end, true)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Range result = filter.getSeekRange(current, end, true);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result.equals(filter2Result));
    }
    
    @Test
    public void getSeekRange_filter2ExclusiveTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key current = new Key();
        Key end = new Key();
        
        Range filter1Result = new Range(new Key("2"), true, new Key("9999"), true);
        Range filter2Result = new Range(new Key("234"), false, new Key("999"), false);
        
        EasyMock.expect(mockFilter1.getSeekRange(current, end, true)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getSeekRange(current, end, true)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Range result = filter.getSeekRange(current, end, true);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result.equals(filter2Result));
    }
    
    @Test
    public void getSeekRange_mixedTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key current = new Key();
        Key end = new Key();
        
        Range filter1Result = new Range(new Key("1"), false, new Key("9999"), true);
        Range filter2Result = new Range(new Key("1"), true, new Key("9999"), false);
        
        EasyMock.expect(mockFilter1.getSeekRange(current, end, true)).andReturn(filter1Result);
        EasyMock.expect(mockFilter2.getSeekRange(current, end, true)).andReturn(filter2Result);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Range result = filter.getSeekRange(current, end, true);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertFalse(result == null);
        Assert.assertTrue(result.getStartKey().getRow().toString().equals("1"));
        Assert.assertFalse(result.isStartKeyInclusive());
        Assert.assertTrue(result.getEndKey().getRow().toString().equals("9999"));
        Assert.assertFalse(result.isEndKeyInclusive());
    }
    
    @Test
    public void getMaxNextCount_noneTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        EasyMock.expect(mockFilter1.getMaxNextCount()).andReturn(-1);
        EasyMock.expect(mockFilter2.getMaxNextCount()).andReturn(-1);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        int result = filter.getMaxNextCount();
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertTrue(result == -1);
    }
    
    @Test
    public void getMaxNextCount_filter1Test() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        EasyMock.expect(mockFilter1.getMaxNextCount()).andReturn(10);
        EasyMock.expect(mockFilter2.getMaxNextCount()).andReturn(-1);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        int result = filter.getMaxNextCount();
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertTrue(result == 10);
    }
    
    @Test
    public void getMaxNextCount_filter2Test() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        EasyMock.expect(mockFilter1.getMaxNextCount()).andReturn(-1);
        EasyMock.expect(mockFilter2.getMaxNextCount()).andReturn(10);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        int result = filter.getMaxNextCount();
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertTrue(result == 10);
    }
    
    @Test
    public void getMaxNextCount_mixedTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        EasyMock.expect(mockFilter1.getMaxNextCount()).andReturn(15);
        EasyMock.expect(mockFilter2.getMaxNextCount()).andReturn(8);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        int result = filter.getMaxNextCount();
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertTrue(result == 8);
    }
    
    @Test
    public void getMaxNextCount_mixedReversedTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        EasyMock.expect(mockFilter1.getMaxNextCount()).andReturn(8);
        EasyMock.expect(mockFilter2.getMaxNextCount()).andReturn(15);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        int result = filter.getMaxNextCount();
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertTrue(result == 8);
    }
    
    @Test
    public void transform_keepTrueTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        
        EasyMock.expect(mockFilter1.keep(key)).andReturn(true);
        EasyMock.expect(mockFilter2.keep(key)).andReturn(true);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Key result = filter.transform(key);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertTrue(result == null);
    }
    
    @Test
    public void transform_keepFalseNoTransformTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        
        EasyMock.expect(mockFilter1.keep(key)).andReturn(false);
        EasyMock.expect(mockFilter2.keep(key)).andReturn(false);
        EasyMock.expect(mockFilter1.transform(key)).andReturn(null);
        EasyMock.expect(mockFilter2.transform(key)).andReturn(null);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Key result = filter.transform(key);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertTrue(result == null);
    }
    
    @Test
    public void transform_keepFalseNoTransformFilter1Test() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Key key2 = new Key("123");
        
        EasyMock.expect(mockFilter1.keep(key)).andReturn(false);
        EasyMock.expect(mockFilter2.keep(key)).andReturn(false);
        EasyMock.expect(mockFilter1.transform(key)).andReturn(null);
        EasyMock.expect(mockFilter2.transform(key)).andReturn(key2);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Key result = filter.transform(key);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertTrue(result == key2);
    }
    
    @Test
    public void transform_keepFalseShortCircuitTest() {
        EventDataQueryFilter mockFilter1 = EasyMock.createMock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = EasyMock.createMock(EventDataQueryFilter.class);
        
        Key key = new Key();
        Key key2 = new Key("123");
        
        EasyMock.expect(mockFilter1.keep(key)).andReturn(false);
        EasyMock.expect(mockFilter1.transform(key)).andReturn(key2);
        
        EasyMock.replay(mockFilter1, mockFilter2);
        
        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);
        
        Key result = filter.transform(key);
        
        EasyMock.verify(mockFilter1, mockFilter2);
        
        Assert.assertTrue(result == key2);
    }
}
