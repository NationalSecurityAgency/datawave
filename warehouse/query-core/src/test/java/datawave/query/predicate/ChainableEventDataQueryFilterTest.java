package datawave.query.predicate;

import java.util.AbstractMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ChainableEventDataQueryFilterTest {
    private ChainableEventDataQueryFilter filter;

    @Before
    public void setup() {
        filter = new ChainableEventDataQueryFilter();
    }

    @Test
    public void startNewDocumentTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key key = new Key();

        mockFilter1.startNewDocument(key);
        mockFilter2.startNewDocument(key);

        Mockito.verify(mockFilter1).startNewDocument(key);
        Mockito.verify(mockFilter2).startNewDocument(key);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        filter.startNewDocument(key);

    }

    @Test
    public void apply_successTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key key = new Key();
        Map.Entry<Key,String> entry = new AbstractMap.SimpleEntry<>(key, "");

        Mockito.doReturn(true).when(mockFilter1).apply(entry);
        Mockito.doReturn(true).when(mockFilter2).apply(entry);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        boolean result = filter.apply(entry);

        Mockito.verify(mockFilter1).apply(entry);
        Mockito.verify(mockFilter2).apply(entry);

        Assert.assertTrue(result);
    }

    @Test
    public void apply_failFilter2Test() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key key = new Key();
        Map.Entry<Key,String> entry = new AbstractMap.SimpleEntry<>(key, "");

        Mockito.doReturn(true).when(mockFilter1).apply(entry);
        Mockito.doReturn(false).when(mockFilter2).apply(entry);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        boolean result = filter.apply(entry);

        Mockito.verify(mockFilter1).apply(entry);
        Mockito.verify(mockFilter2).apply(entry);

        Assert.assertFalse(result);
    }

    @Test
    public void apply_failFilter1Test() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key key = new Key();
        Map.Entry<Key,String> entry = new AbstractMap.SimpleEntry<>(key, "");

        Mockito.doReturn(false).when(mockFilter1).apply(entry);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        boolean result = filter.apply(entry);

        Assert.assertFalse(result);
    }

    @Test
    public void keep_successTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key key = new Key();

        Mockito.doReturn(true).when(mockFilter1).keep(key);
        Mockito.doReturn(true).when(mockFilter2).keep(key);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        boolean result = filter.keep(key);

        Mockito.verify(mockFilter1).keep(key);
        Mockito.verify(mockFilter2).keep(key);

        Assert.assertTrue(result);
    }

    @Test
    public void keep_failFilter2Test() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key key = new Key();

        Mockito.doReturn(true).when(mockFilter1).keep(key);
        Mockito.doReturn(false).when(mockFilter2).keep(key);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        boolean result = filter.keep(key);

        Mockito.verify(mockFilter1).keep(key);
        Mockito.verify(mockFilter2).keep(key);

        Assert.assertFalse(result);
    }

    @Test
    public void keep_failFilter1Test() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key key = new Key();

        Mockito.doReturn(false).when(mockFilter1).keep(key);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        boolean result = filter.keep(key);

        Mockito.verify(mockFilter1).keep(key);

        Assert.assertFalse(result);
    }

    @Test
    public void getSeekRange_filter1InclusiveTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key current = new Key();
        Key end = new Key();

        Range filter1Result = new Range(new Key("234"), true, new Key("999"), true);
        Range filter2Result = new Range(new Key("2"), true, new Key("9999"), true);

        Mockito.doReturn(filter1Result).when(mockFilter1).getSeekRange(current, end, true);
        Mockito.doReturn(filter2Result).when(mockFilter2).getSeekRange(current, end, true);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        Range result = filter.getSeekRange(current, end, true);

        Mockito.verify(mockFilter1).getSeekRange(current, end, true);
        Mockito.verify(mockFilter2).getSeekRange(current, end, true);

        Assert.assertFalse(result == null);
        Assert.assertTrue(result.equals(filter1Result));
    }

    @Test
    public void getSeekRange_filter1ExclusiveTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key current = new Key();
        Key end = new Key();

        Range filter1Result = new Range(new Key("234"), false, new Key("999"), false);
        Range filter2Result = new Range(new Key("2"), true, new Key("9999"), true);

        Mockito.doReturn(filter1Result).when(mockFilter1).getSeekRange(current, end, true);
        Mockito.doReturn(filter2Result).when(mockFilter2).getSeekRange(current, end, true);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        Range result = filter.getSeekRange(current, end, true);

        Mockito.verify(mockFilter1).getSeekRange(current, end, true);
        Mockito.verify(mockFilter2).getSeekRange(current, end, true);

        Assert.assertFalse(result == null);
        Assert.assertTrue(result.equals(filter1Result));
    }

    @Test
    public void getSeekRange_filter2InclusiveTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key current = new Key();
        Key end = new Key();

        Range filter1Result = new Range(new Key("2"), true, new Key("9999"), true);
        Range filter2Result = new Range(new Key("234"), true, new Key("999"), true);

        Mockito.doReturn(filter1Result).when(mockFilter1).getSeekRange(current, end, true);
        Mockito.doReturn(filter2Result).when(mockFilter2).getSeekRange(current, end, true);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        Range result = filter.getSeekRange(current, end, true);

        Mockito.verify(mockFilter1).getSeekRange(current, end, true);
        Mockito.verify(mockFilter2).getSeekRange(current, end, true);

        Assert.assertFalse(result == null);
        Assert.assertTrue(result.equals(filter2Result));
    }

    @Test
    public void getSeekRange_filter2ExclusiveTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key current = new Key();
        Key end = new Key();

        Range filter1Result = new Range(new Key("2"), true, new Key("9999"), true);
        Range filter2Result = new Range(new Key("234"), false, new Key("999"), false);

        Mockito.doReturn(filter1Result).when(mockFilter1).getSeekRange(current, end, true);
        Mockito.doReturn(filter2Result).when(mockFilter2).getSeekRange(current, end, true);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        Range result = filter.getSeekRange(current, end, true);

        Mockito.verify(mockFilter1).getSeekRange(current, end, true);
        Mockito.verify(mockFilter2).getSeekRange(current, end, true);

        Assert.assertFalse(result == null);
        Assert.assertTrue(result.equals(filter2Result));
    }

    @Test
    public void getSeekRange_mixedTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key current = new Key();
        Key end = new Key();

        Range filter1Result = new Range(new Key("1"), false, new Key("9999"), true);
        Range filter2Result = new Range(new Key("1"), true, new Key("9999"), false);

        Mockito.doReturn(filter1Result).when(mockFilter1).getSeekRange(current, end, true);
        Mockito.doReturn(filter2Result).when(mockFilter2).getSeekRange(current, end, true);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        Range result = filter.getSeekRange(current, end, true);

        Mockito.verify(mockFilter1).getSeekRange(current, end, true);
        Mockito.verify(mockFilter2).getSeekRange(current, end, true);

        Assert.assertFalse(result == null);
        Assert.assertTrue(result.getStartKey().getRow().toString().equals("1"));
        Assert.assertFalse(result.isStartKeyInclusive());
        Assert.assertTrue(result.getEndKey().getRow().toString().equals("9999"));
        Assert.assertFalse(result.isEndKeyInclusive());
    }

    @Test
    public void getMaxNextCount_noneTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Mockito.doReturn(-1).when(mockFilter1).getMaxNextCount();
        Mockito.doReturn(-1).when(mockFilter2).getMaxNextCount();

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        int result = filter.getMaxNextCount();

        Mockito.verify(mockFilter1).getMaxNextCount();
        Mockito.verify(mockFilter2).getMaxNextCount();

        Assert.assertTrue(result == -1);
    }

    @Test
    public void getMaxNextCount_filter1Test() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Mockito.doReturn(10).when(mockFilter1).getMaxNextCount();
        Mockito.doReturn(-1).when(mockFilter2).getMaxNextCount();

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        int result = filter.getMaxNextCount();

        Mockito.verify(mockFilter1).getMaxNextCount();
        Mockito.verify(mockFilter2).getMaxNextCount();

        Assert.assertTrue(result == 10);
    }

    @Test
    public void getMaxNextCount_filter2Test() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Mockito.doReturn(-1).when(mockFilter1).getMaxNextCount();
        Mockito.doReturn(10).when(mockFilter2).getMaxNextCount();

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        int result = filter.getMaxNextCount();

        Mockito.verify(mockFilter1).getMaxNextCount();
        Mockito.verify(mockFilter2).getMaxNextCount();

        Assert.assertTrue(result == 10);
    }

    @Test
    public void getMaxNextCount_mixedTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Mockito.doReturn(15).when(mockFilter1).getMaxNextCount();
        Mockito.doReturn(8).when(mockFilter2).getMaxNextCount();

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        int result = filter.getMaxNextCount();

        Mockito.verify(mockFilter1).getMaxNextCount();
        Mockito.verify(mockFilter2).getMaxNextCount();

        Assert.assertTrue(result == 8);
    }

    @Test
    public void getMaxNextCount_mixedReversedTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Mockito.doReturn(8).when(mockFilter1).getMaxNextCount();
        Mockito.doReturn(15).when(mockFilter2).getMaxNextCount();

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        int result = filter.getMaxNextCount();

        Mockito.verify(mockFilter1).getMaxNextCount();
        Mockito.verify(mockFilter2).getMaxNextCount();

        Assert.assertTrue(result == 8);
    }

    @Test
    public void transform_keepTrueTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key key = new Key();

        Mockito.doReturn(true).when(mockFilter1).keep(key);
        Mockito.doReturn(true).when(mockFilter2).keep(key);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        Key result = filter.transform(key);

        Mockito.verify(mockFilter1).keep(key);
        Mockito.verify(mockFilter2).keep(key);

        Assert.assertTrue(result == null);
    }

    @Test
    public void transform_keepFalseNoTransformTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key key = new Key();

        Mockito.doReturn(false).when(mockFilter1).keep(key);
        Mockito.doReturn(false).when(mockFilter2).keep(key);
        Mockito.doReturn(null).when(mockFilter1).transform(key);
        Mockito.doReturn(null).when(mockFilter2).transform(key);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        Key result = filter.transform(key);

        Mockito.verify(mockFilter1).keep(key);
        Mockito.verify(mockFilter2).keep(key);
        Mockito.verify(mockFilter1).transform(key);
        Mockito.verify(mockFilter2).transform(key);

        Assert.assertTrue(result == null);
    }

    @Test
    public void transform_keepFalseNoTransformFilter1Test() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key key = new Key();
        Key key2 = new Key("123");

        Mockito.doReturn(false).when(mockFilter1).keep(key);
        Mockito.doReturn(false).when(mockFilter2).keep(key);
        Mockito.doReturn(null).when(mockFilter1).transform(key);
        Mockito.doReturn(key2).when(mockFilter2).transform(key);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        Key result = filter.transform(key);

        Mockito.verify(mockFilter1).keep(key);
        Mockito.verify(mockFilter2).keep(key);
        Mockito.verify(mockFilter1).transform(key);
        Mockito.verify(mockFilter2).transform(key);

        Assert.assertTrue(result == key2);
    }

    @Test
    public void transform_keepFalseShortCircuitTest() {
        EventDataQueryFilter mockFilter1 = Mockito.mock(EventDataQueryFilter.class);
        EventDataQueryFilter mockFilter2 = Mockito.mock(EventDataQueryFilter.class);

        Key key = new Key();
        Key key2 = new Key("123");

        Mockito.doReturn(false).when(mockFilter1).keep(key);
        Mockito.doReturn(key2).when(mockFilter2).transform(key);

        filter.addFilter(mockFilter1);
        filter.addFilter(mockFilter2);

        Key result = filter.transform(key);

        Mockito.verify(mockFilter1).keep(key);
        Mockito.verify(mockFilter2).transform(key);

        Assert.assertTrue(result == key2);
    }
}
