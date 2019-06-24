package datawave.query.index.lookup;

import com.google.common.collect.Sets;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.planner.QueryPlan;
import datawave.query.util.Tuple2;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static datawave.common.test.utils.query.RangeFactoryForTests.makeDayRange;
import static datawave.common.test.utils.query.RangeFactoryForTests.makeShardedRange;
import static datawave.common.test.utils.query.RangeFactoryForTests.makeTestRange;
import static datawave.common.test.utils.query.RangeFactoryForTests.makeTldTestRange;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TupleToRangeTest {
    
    private JexlNode queryNode = null;
    private ShardQueryConfiguration config;
    
    @Before
    public void setup() throws ParseException {
        queryNode = JexlASTHelper.parseJexlQuery("true==true");
        config = new ShardQueryConfiguration();
    }
    
    @Test
    public void testIsDocumentRange() {
        Set<String> docIds = Sets.newHashSet("docId0", "docId1", "docId2");
        IndexInfo indexInfo = new IndexInfo(docIds);
        assertTrue(TupleToRange.isDocumentRange(indexInfo));
        
        IndexInfo otherInfo = new IndexInfo(3L);
        assertFalse(TupleToRange.isDocumentRange(otherInfo));
    }
    
    @Test
    public void testIsShardRange() {
        String shardRange = "20190314_0";
        String dayRange = "20190314";
        
        assertTrue(TupleToRange.isShardRange(shardRange));
        assertFalse(TupleToRange.isShardRange(dayRange));
    }
    
    @Test
    public void testGenerateDocumentRanges() {
        String shard = "20190314_0";
        Set<String> docIds = Sets.newHashSet("docId0", "docId1", "docId2");
        IndexInfo indexInfo = new IndexInfo(docIds);
        indexInfo.applyNode(queryNode);
        
        // Build expected shard ranges
        List<Range> expectedRanges = new ArrayList<>(3);
        expectedRanges.add(makeTestRange(shard, "docId0"));
        expectedRanges.add(makeTestRange(shard, "docId1"));
        expectedRanges.add(makeTestRange(shard, "docId2"));
        
        // Create the ranges
        Iterator<QueryPlan> ranges = TupleToRange.createDocumentRanges(queryNode, shard, indexInfo, config.isTldQuery());
        
        // Assert ranges against expected ranges
        eval(expectedRanges, ranges);
    }
    
    @Test
    public void testGenerateTldDocumentRanges() {
        String shard = "20190314_0";
        Set<String> docIds = Sets.newHashSet("docId0", "docId1", "docId2");
        IndexInfo indexInfo = new IndexInfo(docIds);
        indexInfo.applyNode(queryNode);
        
        // Build expected shard ranges
        List<Range> expectedRanges = new ArrayList<>(3);
        expectedRanges.add(makeTldTestRange(shard, "docId0"));
        expectedRanges.add(makeTldTestRange(shard, "docId1"));
        expectedRanges.add(makeTldTestRange(shard, "docId2"));
        
        // Create the ranges
        config.setTldQuery(true);
        Iterator<QueryPlan> ranges = TupleToRange.createDocumentRanges(queryNode, shard, indexInfo, config.isTldQuery());
        
        // Assert ranges against expected ranges
        eval(expectedRanges, ranges);
    }
    
    @Test
    public void testGenerateShardRange() {
        String shard = "20190314_0";
        IndexInfo indexInfo = new IndexInfo(-1);
        indexInfo.applyNode(queryNode);
        
        // Build expected shard ranges
        List<Range> expectedRanges = new ArrayList<>(1);
        expectedRanges.add(makeShardedRange(shard));
        
        // Create the ranges
        Iterator<QueryPlan> ranges = TupleToRange.createShardRange(queryNode, shard, indexInfo);
        
        // Assert ranges against expected ranges
        eval(expectedRanges, ranges);
    }
    
    @Test
    public void testGenerateDayRange() {
        String shard = "20190314";
        IndexInfo indexInfo = new IndexInfo(-1);
        indexInfo.applyNode(queryNode);
        
        // Build expected shard ranges
        List<Range> expectedRanges = new ArrayList<>(1);
        expectedRanges.add(makeDayRange(shard));
        
        // Create the ranges
        Iterator<QueryPlan> ranges = TupleToRange.createDayRange(queryNode, shard, indexInfo);
        
        // Assert ranges against expected ranges
        eval(expectedRanges, ranges);
    }
    
    @Test
    public void testApplyWithDocumentRange() {
        String shard = "20190314_0";
        Set<String> docIds = Sets.newHashSet("docId0", "docId1", "docId2");
        IndexInfo indexInfo = new IndexInfo(docIds);
        indexInfo.applyNode(queryNode);
        Tuple2<String,IndexInfo> tuple = new Tuple2<>(shard, indexInfo);
        
        // Build expected shard ranges
        List<Range> expectedRanges = new ArrayList<>(3);
        expectedRanges.add(makeTestRange(shard, "docId0"));
        expectedRanges.add(makeTestRange(shard, "docId1"));
        expectedRanges.add(makeTestRange(shard, "docId2"));
        
        // Create the ranges
        TupleToRange tupleToRange = new TupleToRange(queryNode, config);
        Iterator<QueryPlan> ranges = tupleToRange.apply(tuple);
        
        // Assert ranges against expected ranges
        eval(expectedRanges, ranges);
    }
    
    @Test
    public void testApplyWithTldDocumentRange() {
        String shard = "20190314_0";
        Set<String> docIds = Sets.newHashSet("docId0", "docId1", "docId2");
        IndexInfo indexInfo = new IndexInfo(docIds);
        indexInfo.applyNode(queryNode);
        Tuple2<String,IndexInfo> tuple = new Tuple2<>(shard, indexInfo);
        
        // Build expected shard ranges
        List<Range> expectedRanges = new ArrayList<>(3);
        expectedRanges.add(makeTldTestRange(shard, "docId0"));
        expectedRanges.add(makeTldTestRange(shard, "docId1"));
        expectedRanges.add(makeTldTestRange(shard, "docId2"));
        
        // Create the ranges
        config.setTldQuery(true);
        TupleToRange tupleToRange = new TupleToRange(queryNode, config);
        Iterator<QueryPlan> ranges = tupleToRange.apply(tuple);
        
        // Assert ranges against expected ranges
        eval(expectedRanges, ranges);
    }
    
    @Test
    public void testApplyWithShardRange() {
        String shard = "20190314_0";
        IndexInfo indexInfo = new IndexInfo(-1);
        indexInfo.applyNode(queryNode);
        Tuple2<String,IndexInfo> tuple = new Tuple2<>(shard, indexInfo);
        
        // Build expected shard ranges
        List<Range> expectedRanges = new ArrayList<>(1);
        expectedRanges.add(makeShardedRange(shard));
        
        // Create the ranges
        TupleToRange tupleToRange = new TupleToRange(queryNode, config);
        Iterator<QueryPlan> ranges = tupleToRange.apply(tuple);
        
        // Assert ranges against expected ranges
        eval(expectedRanges, ranges);
    }
    
    @Test
    public void testApplyWithDayRange() {
        String shard = "20190314";
        IndexInfo indexInfo = new IndexInfo(-1);
        indexInfo.applyNode(queryNode);
        Tuple2<String,IndexInfo> tuple = new Tuple2<>(shard, indexInfo);
        
        // Build expected shard ranges
        List<Range> expectedRanges = new ArrayList<>(1);
        expectedRanges.add(makeDayRange(shard));
        
        // Create the ranges
        TupleToRange tupleToRange = new TupleToRange(queryNode, config);
        Iterator<QueryPlan> ranges = tupleToRange.apply(tuple);
        
        // Assert ranges against expected ranges
        eval(expectedRanges, ranges);
    }
    
    private void eval(List<Range> expectedRanges, Iterator<QueryPlan> ranges) {
        Iterator<Range> expectedIter = expectedRanges.iterator();
        while (expectedIter.hasNext()) {
            
            Range expectedRange = expectedIter.next();
            
            Iterator<Range> generatedIter = ranges.next().getRanges().iterator();
            Range generatedRange = generatedIter.next();
            assertEquals(expectedRange, generatedRange);
            
            assertFalse("Query plan generated unexpected ranges", generatedIter.hasNext());
        }
        
        assertFalse(expectedIter.hasNext());
        assertFalse(ranges.hasNext());
    }
}
