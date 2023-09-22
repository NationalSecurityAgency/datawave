package datawave.query.iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Maps;

import datawave.query.function.DocumentRangeProvider;
import datawave.query.function.Equality;
import datawave.query.function.PrefixEquality;
import datawave.query.function.RangeProvider;

/**
 * Unit test for {@link QueryIterator}.
 * <p>
 * Currently only covers some helper methods.
 */
public class QueryIteratorTest {
    public static final String DEFAULT_CQ = "\uffff";
    private static final SimpleDateFormat shardFormatter = new SimpleDateFormat("yyyyMMdd HHmmss");
    private static long ts = -1;

    public static SortedMap<Key,Value> createTestData() throws ParseException {
        return createTestData("");
    }

    public static SortedMap<Key,Value> createTestData(String preId) throws ParseException {
        shardFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
        ts = shardFormatter.parse("20121126 123023").getTime();
        long ts2 = ts + 10000;
        long ts3 = ts + 200123;

        TreeMap<Key,Value> map = Maps.newTreeMap();

        map.put(new Key("20121126_0", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 1, ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 2, ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 3, ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 1, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 1, "BAR\0foo", ts2), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 2, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 2, "BAR\0foo", ts2), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 3, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 3, "BAR\0foo", ts2), new Value(new byte[0]));

        map.put(new Key("20121126_0", "foobar\0" + preId + 23, "FOO\0bar1", ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 23, "BAR\0foo1", ts2), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 24, "FOO\0bar2", ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 24, "BAR\0foo2", ts2), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 25, "FOO\0bar3", ts), new Value(new byte[0]));
        map.put(new Key("20121126_0", "foobar\0" + preId + 25, "BAR\0foo3", ts2), new Value(new byte[0]));

        map.put(new Key("20121126_1", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 4, ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 5, ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 6, ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "foobar\0" + preId + 4, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "foobar\0" + preId + 5, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_1", "foobar\0" + preId + 5, "BAR\0foo", ts2), new Value(new byte[0]));
        map.put(new Key("20121126_1", "foobar\0" + preId + 6, "FOO\0bar", ts), new Value(new byte[0]));

        map.put(new Key("20121126_2", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 7, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 8, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "fi\0" + "FOO", "bar\0" + "foobar\0" + preId + 9, ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + preId + 7, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + preId + 7, "BAR\0foo", ts3), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + preId + 8, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + preId + 8, "BAR\0foo", ts3), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + preId + 9, "FOO\0bar", ts), new Value(new byte[0]));
        map.put(new Key("20121126_2", "foobar\0" + preId + 9, "BAR\0foo", ts3), new Value(new byte[0]));
        map.put(new Key("20121126_3", "fi\0" + "FOOSICKLES", "bar\0" + "foobar\0" + 33, ts), new Value(new byte[0]));

        return map;
    }

    public static long getTimeStamp() {
        return ts;
    }

    @Test
    public void testIsDocumentSpecificRange_withInfiniteKeys() {
        // Test the case of an infinite start key
        Key end = new Key("20190314_0", "dataType\0doc0\0");
        Range infiniteStartRange = new Range(null, end);
        assertFalse(QueryIterator.isDocumentSpecificRange(infiniteStartRange));

        // Test the case of an infinite end key
        Key start = new Key("20190314_0", "dataType\0doc0");
        Range infiniteEndRange = new Range(start, null);
        assertFalse(QueryIterator.isDocumentSpecificRange(infiniteEndRange));
    }

    @Test
    public void testIsDocumentSpecificRange_spansMultipleRows() {
        Key start = new Key("20190314_0", "dataType\0doc0");
        Key end = new Key("20190314_9", "dataType\0doc0\0");
        Range multipleRowRange = new Range(start, end);
        assertFalse(QueryIterator.isDocumentSpecificRange(multipleRowRange));
    }

    @Test
    public void testIsDocumentSpecificRange_withDocRange() {
        Key start = new Key("20190314_0", "dataType\0doc0");
        Key end = new Key("20190314_0", "dataType\0doc0\0");
        Range docRange = new Range(start, end);
        assertTrue(QueryIterator.isDocumentSpecificRange(docRange));
    }

    @Test
    public void testIsDocumentSpecificRange_withShardRange() {
        Key start = new Key("20190314_0");
        Key end = new Key("20190314_0");
        Range shardRange = new Range(start, end);
        assertFalse(QueryIterator.isDocumentSpecificRange(shardRange));
    }

    @Test
    public void testIsDocumentSpecificRange_withRebuiltShardRange() {
        Key start = new Key("20190314_0", "dataType\0doc0");
        Key end = new Key("20190314_0\u0000");
        Range range = new Range(start, false, end, false);
        assertFalse(QueryIterator.isDocumentSpecificRange(range));
    }

    /**
     * <pre>
     * Shard key format
     * Key.row = shard
     * Key.columnFamily = datatype\0docId
     * Key.columnQualifier = field\0value
     * </pre>
     */
    @Test
    public void testRowColfamToString() {
        String expected = "20190314_0 test%00;doc0:FOO%00;bar";

        Text row = new Text("20190314_0");
        Text cf = new Text("test\0doc0");
        Text cq = new Text("FOO\0bar");
        Key key = new Key(row, cf, cq);

        String parsed = QueryIterator.rowColFamToString(key);
        assertEquals(expected, parsed);

        // Test the null case as well
        assertEquals("null", QueryIterator.rowColFamToString(null));
    }

    @Test
    public void testGetRangeProvider() {
        QueryIterator iterator = new QueryIterator();
        RangeProvider provider = iterator.getRangeProvider();
        assertEquals(DocumentRangeProvider.class.getSimpleName(), provider.getClass().getSimpleName());
    }

    @Test
    public void testGetEquality() {
        QueryIterator iterator = new QueryIterator();
        Equality equality = iterator.getEquality();
        assertEquals(PrefixEquality.class.getSimpleName(), equality.getClass().getSimpleName());
    }
}
