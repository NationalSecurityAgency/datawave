package datawave.query.index.lookup;

import com.google.common.collect.Lists;
import nsa.datawave.query.index.lookup.IndexInfo;
import nsa.datawave.query.index.lookup.IndexMatch;
import nsa.datawave.query.index.lookup.TupleToRange;
import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.query.rewrite.planner.QueryPlan;
import nsa.datawave.query.util.Tuple2;
import nsa.datawave.query.util.Tuples;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TupleToRangeTest {
    
    protected JexlNode queryNode = null;
    
    @Before
    public void setup() throws ParseException {
        queryNode = JexlASTHelper.parseJexlQuery("true==true");
    }
    
    @Test
    public void test() {
        IndexInfo info = new IndexInfo(Lists.newArrayList(new IndexMatch("a"), new IndexMatch("b"), new IndexMatch("c"), new IndexMatch("d")));
        info.applyNode(queryNode);
        Tuple2<String,IndexInfo> tuple = Tuples.tuple("s", info);
        Iterator<QueryPlan> ranges = new TupleToRange(queryNode, new RefactoredShardQueryConfiguration()).apply(tuple);
        assertTrue(ranges.hasNext());
        assertEquals(makeTestRange("s", "a"), ranges.next().getRanges().iterator().next());
        assertTrue(ranges.hasNext());
        assertEquals(makeTestRange("s", "b"), ranges.next().getRanges().iterator().next());
        assertTrue(ranges.hasNext());
        assertEquals(makeTestRange("s", "c"), ranges.next().getRanges().iterator().next());
        assertTrue(ranges.hasNext());
        assertEquals(makeTestRange("s", "d"), ranges.next().getRanges().iterator().next());
        assertFalse(ranges.hasNext());
    }
    
    @Test
    public void testTld() {
        IndexInfo info = new IndexInfo(Lists.newArrayList(new IndexMatch("a"), new IndexMatch("b"), new IndexMatch("c"), new IndexMatch("d")));
        info.applyNode(queryNode);
        Tuple2<String,IndexInfo> tuple = Tuples.tuple("s", info);
        RefactoredShardQueryConfiguration config = new RefactoredShardQueryConfiguration();
        config.setTldQuery(true);
        Iterator<QueryPlan> ranges = new TupleToRange(queryNode, config).apply(tuple);
        assertTrue(ranges.hasNext());
        assertEquals(makeTldTestRange("s", "a"), ranges.next().getRanges().iterator().next());
        assertTrue(ranges.hasNext());
        assertEquals(makeTldTestRange("s", "b"), ranges.next().getRanges().iterator().next());
        assertTrue(ranges.hasNext());
        assertEquals(makeTldTestRange("s", "c"), ranges.next().getRanges().iterator().next());
        assertTrue(ranges.hasNext());
        assertEquals(makeTldTestRange("s", "d"), ranges.next().getRanges().iterator().next());
        assertFalse(ranges.hasNext());
    }
    
    @Test
    public void testShards() {
        IndexInfo info = new IndexInfo();
        info.applyNode(queryNode);
        Tuple2<String,IndexInfo> tuple = Tuples.tuple("20130101_0", info);
        Iterator<QueryPlan> ranges = new TupleToRange(queryNode, new RefactoredShardQueryConfiguration()).apply(tuple);
        assertTrue(ranges.hasNext());
        assertEquals(makeShardedRange("20130101_0"), ranges.next().getRanges().iterator().next());
        assertFalse(ranges.hasNext());
    }
    
    @Test
    public void testDays() {
        IndexInfo info = new IndexInfo();
        info.applyNode(queryNode);
        Tuple2<String,IndexInfo> tuple = Tuples.tuple("20130101", info);
        Iterator<QueryPlan> ranges = new TupleToRange(queryNode, new RefactoredShardQueryConfiguration()).apply(tuple);
        assertTrue(ranges.hasNext());
        assertEquals(makeDayRange("20130101"), ranges.next().getRanges().iterator().next());
        assertFalse(ranges.hasNext());
    }
    
    public static Range makeTestRange(String r, String c) {
        Key s = new Key(r, c), e = s.followingKey(PartialKey.ROW_COLFAM);
        return new Range(s, true, e, false);
    }
    
    public static Range makeTldTestRange(String r, String c) {
        Key s = new Key(r, c), e = new Key(r, c + TupleToRange.MAX_UNICODE_STRING);
        return new Range(s, true, e, false);
    }
    
    public static Range makeShardedRange(String r) {
        Key s = new Key(r), e = new Key(r + TupleToRange.NULL_BYTE_STRING);
        return new Range(s, true, e, false);
    }
    
    public static Range makeDayRange(String r) {
        Key s = new Key(r + "_0"), e = new Key(r + TupleToRange.MAX_UNICODE_STRING);
        return new Range(s, true, e, false);
    }
}
