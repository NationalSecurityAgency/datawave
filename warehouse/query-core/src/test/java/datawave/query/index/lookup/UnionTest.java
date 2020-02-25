package datawave.query.index.lookup;

import com.google.common.collect.Lists;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.util.Tuple2;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Some basic tests of the {@link Union} class.
 */
public class UnionTest {
    
    @Test
    public void testUnion_AllUid() throws ParseException {
        // A || B || C
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' || B == '2' || C == '3'");
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c"));
        
        // B - uids
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", Arrays.asList("a.b.c", "a.b.c.d"));
        
        // C - uids
        ScannerStream s3 = buildScannerStream("20090101_1", "C", "3", Arrays.asList("a.b.c", "az"));
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2, s3);
        
        Union union = new Union(toMerge);
        assertTrue(union.hasNext());
        
        Tuple2<String,IndexInfo> tuple = union.next();
        assertEquals(tuple.first(), ("20090101_1"));
        
        IndexInfo info = tuple.second();
        assertEquals(info.count, 3);
        assertEquals(info.uids().size(), 3);
        
        Iterator<IndexMatch> matchIter = info.uids().iterator();
        
        IndexMatch m1 = matchIter.next();
        assertEquals("a.b.c", m1.uid);
        assertEquals(IndexMatchType.OR, m1.type);
        
        IndexMatch m2 = matchIter.next();
        assertEquals("a.b.c.d", m2.uid);
        assertEquals(IndexMatchType.OR, m2.type);
        
        IndexMatch m3 = matchIter.next();
        assertEquals("az", m3.uid);
        assertEquals(IndexMatchType.OR, m3.type);
        
        // Assert all the match nodes in one place.
        ASTJexlScript m2script = JexlASTHelper.parseJexlQuery("B == '2'");
        ASTJexlScript m3script = JexlASTHelper.parseJexlQuery("C == '3'");
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m1.getNode()), new TreeEqualityVisitor.Reason()));
        assertTrue(TreeEqualityVisitor.isEqual(m2script, JexlNodeFactory.createScript(m2.getNode()), new TreeEqualityVisitor.Reason()));
        assertTrue(TreeEqualityVisitor.isEqual(m3script, JexlNodeFactory.createScript(m3.getNode()), new TreeEqualityVisitor.Reason()));
        
        assertFalse(matchIter.hasNext()); // Done with matches.
        
        assertFalse(union.hasNext()); // Done with union.
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
    }
    
    @Test
    public void testUnion_EmptyUidsOrInfinite() throws ParseException {
        // A || B
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' || B == '2'");
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList());
        
        // B - infinite
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", null);
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2);
        
        Union union = new Union(toMerge);
        
        Tuple2<String,IndexInfo> tuple = union.next();
        assertEquals(tuple.first(), ("20090101_1"));
        
        IndexInfo info = tuple.second();
        assertEquals(info.count, -1);
        assertEquals(info.uids().size(), 0);
        
        assertFalse(union.hasNext());
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
    }
    
    @Test
    public void testUnion_UidsAndInfinite() throws ParseException {
        // A || B || C
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' || B == '2' || C == '3'");
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c", "a.b.z"));
        
        // B - infinite
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", null);
        
        // C - infinite
        ScannerStream s3 = buildScannerStream("20090101_1", "C", "3", null);
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2, s3);
        
        Union union = new Union(toMerge);
        
        Tuple2<String,IndexInfo> tuple = union.next();
        assertEquals(tuple.first(), ("20090101_1"));
        
        IndexInfo info = tuple.second();
        assertEquals(info.count, -1);
        assertEquals(info.uids().size(), 0);
        
        assertFalse(union.hasNext());
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
    }
    
    @Test
    public void testUnion_AllInfinite() throws ParseException {
        // A || B || C
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' || B == '2' || C == '3'");
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", null);
        
        // B - infinite
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", null);
        
        // C - infinite
        ScannerStream s3 = buildScannerStream("20090101_1", "C", "3", null);
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2, s3);
        
        Union union = new Union(toMerge);
        assertTrue(union.hasNext());
        
        Tuple2<String,IndexInfo> tuple = union.next();
        assertEquals(tuple.first(), ("20090101_1"));
        
        IndexInfo info = tuple.second();
        assertEquals(info.count, -1);
        assertEquals(info.uids().size(), 0);
        
        assertFalse(union.hasNext());
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
    }
    
    @Test
    public void testUnion_AllDelayed() {
        // A || B when both terms are delayed.
        ScannerStream s1 = ScannerStream.delayedExpression(ASTDelayedPredicate.create(JexlNodeFactory.buildEQNode("A", "1")));
        
        ScannerStream s2 = ScannerStream.delayedExpression(ASTDelayedPredicate.create(JexlNodeFactory.buildEQNode("B", "2")));
        
        Union union = new Union(Arrays.asList(s1, s2));
        
        // Cannot process only delayed index streams.
        assertFalse(union.hasNext());
        
        // Assert that the Union still produces the logical union of JexlNodes.
        JexlNode delay1 = ASTDelayedPredicate.create(JexlNodeFactory.buildEQNode("A", "1"));
        JexlNode delay2 = ASTDelayedPredicate.create(JexlNodeFactory.buildEQNode("B", "2"));
        JexlNode orNode = TreeFlatteningRebuildingVisitor.flatten(JexlNodeFactory.createUnwrappedOrNode(Arrays.asList(delay1, delay2)));
        ASTJexlScript script = JexlNodeFactory.createScript(orNode);
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
    }
    
    @Test
    public void testUnion_EmptyUidsWithCounts() throws ParseException {
        // ( A or B ) when uids are collapsed.
        // A - uids, collapsed
        ScannerStream s1 = buildCollapsedScannerStream("20090101_1", "A", "1", 10);
        
        // B - uids, collapsed
        ScannerStream s2 = buildCollapsedScannerStream("20090101_1", "B", "2", 32);
        
        Union union = new Union(Arrays.asList(s1, s2));
        
        Tuple2<String,IndexInfo> tuple = union.next();
        assertEquals(tuple.first(), ("20090101_1"));
        
        IndexInfo info = tuple.second();
        assertEquals(info.count, 42);
        assertEquals(info.uids().size(), 0);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' || B == '2'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(info.getNode()), new TreeEqualityVisitor.Reason()));
        
        // Finally assert the union's JexlNode
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
        
        assertFalse(union.hasNext());
    }
    
    @Test
    public void testUnion_AllUids_WithEmptyUid() throws ParseException {
        // ( A or B or C) when A and B have uids, C is collapsed (empty uid list, positive count)
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c"));
        
        // B - uids
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", Arrays.asList("a.b.c", "b.c.d"));
        
        // C - uids, collapsed
        ScannerStream s3 = buildCollapsedScannerStream("20090101_1", "C", "3", 10);
        
        Union union = new Union(Arrays.asList(s1, s2, s3));
        
        Tuple2<String,IndexInfo> tuple = union.next();
        assertEquals(tuple.first(), ("20090101_1"));
        
        IndexInfo info = tuple.second();
        assertEquals(info.count, 13); // Counting is best-guess.
        assertEquals(info.uids().size(), 2);
        
        Iterator<IndexMatch> matchIter = info.uids.iterator();
        
        // Match 1
        IndexMatch match = matchIter.next();
        assertEquals("a.b.c", match.uid);
        assertEquals(IndexMatchType.OR, match.type);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' || B == '2'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(match.getNode()), new TreeEqualityVisitor.Reason()));
        
        // Match 2
        match = matchIter.next();
        assertEquals("b.c.d", match.uid);
        assertEquals(IndexMatchType.OR, match.type);
        
        script = JexlASTHelper.parseJexlQuery("B == '2'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(match.getNode()), new TreeEqualityVisitor.Reason()));
        
        // MatchIter should be exhausted.
        assertFalse(matchIter.hasNext());
        
        // Done with matches, assert IndexInfo's JexlNode.
        script = JexlASTHelper.parseJexlQuery("A == '1' || B == '2' || C == '3'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(info.getNode()), new TreeEqualityVisitor.Reason()));
        
        // Finally assert the union's JexlNode
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
        
        assertFalse(union.hasNext());
    }
    
    @Test
    public void testUnion_AllInfinite_WithEmptyUid() throws ParseException {
        // ( A or B or C) when A and B are infinite, C is collapsed (empty uid list, positive count).
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", null);
        
        // B - uids
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", null);
        
        // C - uids, collapsed
        ScannerStream s3 = buildCollapsedScannerStream("20090101_1", "C", "3", 10);
        
        Union union = new Union(Arrays.asList(s1, s2, s3));
        
        Tuple2<String,IndexInfo> tuple = union.next();
        assertEquals(tuple.first(), ("20090101_1"));
        
        IndexInfo info = tuple.second();
        assertEquals(info.count, -1);
        assertEquals(info.uids().size(), 0);
        
        // Done with matches, assert IndexInfo's JexlNode.
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' || B == '2' || C == '3'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(info.getNode()), new TreeEqualityVisitor.Reason()));
        
        // Finally assert the union's JexlNode
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
        
        assertFalse(union.hasNext());
    }
    
    @Test
    public void testUnion_SingleEmptyUid() throws ParseException {
        // ( A or B ) when A has no data and B has uids, collapsed.
        // A -- no data
        ScannerStream s1 = ScannerStream.noData(JexlNodeFactory.buildEQNode("A", "1"));
        
        // B -- uids, collapsed.
        ScannerStream s2 = buildCollapsedScannerStream("20090101_1", "B", "2", 5);
        
        Union union = new Union(Arrays.asList(s1, s2));
        
        Tuple2<String,IndexInfo> tuple = union.next();
        assertEquals(tuple.first(), ("20090101_1"));
        
        IndexInfo info = tuple.second();
        assertEquals(5, info.count);
        assertEquals(0, info.uids.size());
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("B == '2'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(info.getNode()), new TreeEqualityVisitor.Reason()));
        
        // Assert Union's JexlNode.
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
        
        assertFalse(union.hasNext());
    }
    
    @Test
    public void testUnion_SingleUid() throws ParseException {
        // ( A or B ) when A has no data and B has a uid hit.
        // A -- no data
        ScannerStream s1 = ScannerStream.noData(JexlNodeFactory.buildEQNode("A", "1"));
        
        // B -- uids
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", Arrays.asList("a.b.c"));
        
        Union union = new Union(Arrays.asList(s1, s2));
        
        Tuple2<String,IndexInfo> tuple = union.next();
        assertEquals(tuple.first(), ("20090101_1"));
        
        IndexInfo info = tuple.second();
        assertEquals(1, info.count);
        assertEquals(1, info.uids.size());
        
        Iterator<IndexMatch> matchIter = info.uids.iterator();
        
        IndexMatch match = matchIter.next();
        assertEquals("a.b.c", match.uid);
        assertEquals(IndexMatchType.OR, match.type);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("B == '2'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(match.getNode())));
        
        // No more matches.
        assertFalse(matchIter.hasNext());
        
        // IndexInfo's JexlNode.
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(info.getNode())));
        
        // Union's JexlNode.
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode())));
        
        assertFalse(union.hasNext());
    }
    
    @Test
    public void testUnion_SingleInfinite() throws ParseException {
        // ( A or B ) when A has no data and B is infinite
        // A -- no data
        ScannerStream s1 = ScannerStream.noData(JexlNodeFactory.buildEQNode("A", "1"));
        
        // B -- infinite
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", null);
        
        Union union = new Union(Arrays.asList(s1, s2));
        
        Tuple2<String,IndexInfo> tuple = union.next();
        assertEquals(tuple.first(), ("20090101_1"));
        
        IndexInfo info = tuple.second();
        assertEquals(-1, info.count);
        assertEquals(0, info.uids.size());
        
        // IndexInfo's JexlNode.
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("B == '2'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(info.getNode())));
        
        // Union's JexlNode.
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode())));
        
        assertFalse(union.hasNext());
    }
    
    @Test
    public void testUnion_SingleDelayed() {
        // ( A or B ) when A has no data and B is delayed
        // A -- no data
        ScannerStream s1 = ScannerStream.noData(JexlNodeFactory.buildEQNode("A", "1"));
        
        // B -- delayed
        ScannerStream s2 = ScannerStream.delayedExpression(ASTDelayedPredicate.create(JexlNodeFactory.buildEQNode("B", "2")));
        
        Union union = new Union(Arrays.asList(s1, s2));
        
        // Union of No Data and a Delayed term will produce nothing.
        assertFalse(union.hasNext());
        
        // Union's JexlNode.
        ASTJexlScript script = JexlNodeFactory.createScript(ASTDelayedPredicate.create(JexlNodeFactory.buildEQNode("B", "2")));
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode())));
    }
    
    @Test
    public void testUnion_DifferentShardStreams() throws ParseException {
        // A || B || C
        ASTJexlScript script;
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", null);
        
        // B - infinite
        ScannerStream s2 = buildScannerStream("20090101_10", "B", "2", null);
        
        // C - infinite
        ScannerStream s3 = buildScannerStream("20090101_5", "C", "3", null);
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2, s3);
        
        Union union = new Union(toMerge);
        
        // Prior to advancing the queue the union is pointed at the first match. Assert currentNode
        // before calling next().
        script = JexlASTHelper.parseJexlQuery("A == '1'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
        
        // First Union
        Tuple2<String,IndexInfo> tuple2_01 = union.next();
        assertEquals(tuple2_01.first(), ("20090101_1"));
        
        IndexInfo info = tuple2_01.second();
        assertEquals(info.count, -1);
        assertEquals(info.uids().size(), 0);
        
        script = JexlASTHelper.parseJexlQuery("B == '2'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
        
        // Second union
        Tuple2<String,IndexInfo> tuple2_02 = union.next();
        assertEquals(tuple2_02.first(), ("20090101_10"));
        
        info = tuple2_02.second();
        assertEquals(info.count, -1);
        assertEquals(info.uids().size(), 0);
        
        script = JexlASTHelper.parseJexlQuery("C == '3'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
        
        // Third union
        Tuple2<String,IndexInfo> tuple2_03 = union.next();
        assertEquals(tuple2_03.first(), ("20090101_5"));
        
        info = tuple2_03.second();
        assertEquals(info.count, -1);
        assertEquals(info.uids().size(), 0);
        
        // Union's currentNode has not changed, even though we are at the end of the stream.
        script = JexlASTHelper.parseJexlQuery("C == '3'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
        
        assertFalse(union.hasNext());
    }
    
    @Test
    public void testUnion_DifferentDayStreams() throws ParseException {
        // A || B || C
        ASTJexlScript script;
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", null);
        
        // B - infinite
        ScannerStream s2 = buildScannerStream("20090102_1", "B", "2", null);
        
        // C - infinite
        ScannerStream s3 = buildScannerStream("20090103_1", "C", "3", null);
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2, s3);
        
        Union union = new Union(toMerge);
        
        // Prior to advancing the queue the union is pointed at the first match. Assert currentNode
        // before calling next().
        script = JexlASTHelper.parseJexlQuery("A == '1'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
        
        // First Union
        Tuple2<String,IndexInfo> tuple2_01 = union.next();
        assertEquals(tuple2_01.first(), ("20090101_1"));
        
        IndexInfo info = tuple2_01.second();
        assertEquals(info.count, -1);
        assertEquals(info.uids().size(), 0);
        
        script = JexlASTHelper.parseJexlQuery("B == '2'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
        
        // Second union
        Tuple2<String,IndexInfo> tuple2_02 = union.next();
        assertEquals(tuple2_02.first(), ("20090102_1"));
        
        info = tuple2_02.second();
        assertEquals(info.count, -1);
        assertEquals(info.uids().size(), 0);
        
        script = JexlASTHelper.parseJexlQuery("C == '3'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
        
        // Third union
        Tuple2<String,IndexInfo> tuple2_03 = union.next();
        assertEquals(tuple2_03.first(), ("20090103_1"));
        
        info = tuple2_03.second();
        assertEquals(info.count, -1);
        assertEquals(info.uids().size(), 0);
        
        // Union's currentNode has not changed, even though we are at the end of the stream.
        script = JexlASTHelper.parseJexlQuery("C == '3'");
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
        
        assertFalse(union.hasNext());
    }
    
    @Test
    public void testUnion_UidOrUnindexedOrDelayed() {
        // (A or B or C), where C is a delayed term.
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c", "a.b.z"));
        
        // B - unindexed
        ScannerStream s2 = ScannerStream.unindexed(JexlNodeFactory.buildEQNode("B", "2"));
        
        // C - delayed
        ScannerStream s3 = ScannerStream.delayedExpression(ASTDelayedPredicate.create(JexlNodeFactory.buildEQNode("C", "3")));
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2, s3);
        
        Union union = new Union(toMerge);
        assertTrue(union.hasNext());
        
        Tuple2<String,IndexInfo> tuple = union.next();
        assertEquals(tuple.first(), ("20090101_1"));
        
        IndexInfo info = tuple.second();
        
        // Assert info
        assertEquals(info.count, 2);
        assertEquals(info.uids().size(), 2);
        
        // Assert matches
        Iterator<IndexMatch> matchIter = info.uids.iterator();
        
        assertTrue(matchIter.hasNext());
        IndexMatch match1 = matchIter.next();
        assertEquals(match1.uid, "a.b.c");
        assertEquals(match1.type, IndexMatchType.OR);
        
        // Assert proper JexlNode for Match 1. Constructed by hand due to weirdness in how DelayedPredicates are parsed from a string.
        // A == '1' || B == '2' || ((ASTDelayedPredicate = true) && (C == '3'))
        JexlNode node1 = JexlNodeFactory.buildEQNode("A", "1");
        JexlNode node2 = JexlNodeFactory.buildEQNode("B", "2");
        JexlNode delayed = ASTDelayedPredicate.create(JexlNodeFactory.buildEQNode("C", "3"));
        JexlNode orNode = JexlNodeFactory.createUnwrappedOrNode(Lists.newArrayList(node1, node2, delayed));
        orNode = TreeFlatteningRebuildingVisitor.flatten(orNode);
        ASTJexlScript matchScript1 = JexlNodeFactory.createScript(orNode);
        
        assertTrue(TreeEqualityVisitor.isEqual(matchScript1, JexlNodeFactory.createScript(match1.getNode()), new TreeEqualityVisitor.Reason()));
        
        assertTrue(matchIter.hasNext());
        IndexMatch match2 = matchIter.next();
        assertEquals(match2.uid, "a.b.z");
        assertEquals(match2.type, IndexMatchType.OR);
        
        // Assert proper JexlNode for Match 2
        assertTrue(TreeEqualityVisitor.isEqual(matchScript1, JexlNodeFactory.createScript(match2.getNode()), new TreeEqualityVisitor.Reason()));
        
        // Done with matches.
        assertFalse(matchIter.hasNext());
        
        // Finally, assert info's JexlNode
        assertTrue(TreeEqualityVisitor.isEqual(matchScript1, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
    }
    
    @Test
    public void testUnion_UidOrUnindexedOrDelayedOrInfinite() {
        // (A || B || C || D)
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c", "a.b.z"));
        
        // B - unindexed
        ScannerStream s2 = ScannerStream.unindexed(JexlNodeFactory.buildEQNode("B", "2"));
        
        // C - delayed
        ScannerStream s3 = ScannerStream.delayedExpression(ASTDelayedPredicate.create(JexlNodeFactory.buildEQNode("C", "3")));
        
        // D - infinite
        ScannerStream s4 = buildScannerStream("20090101_1", "D", "4", null);
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2, s3, s4);
        
        Union union = new Union(toMerge);
        assertTrue(union.hasNext());
        Tuple2<String,IndexInfo> ii = union.next();
        assertEquals(ii.first(), ("20090101_1"));
        assertEquals(ii.second().count, -1);
        assertEquals(ii.second().uids().size(), 0);
        
        // Assert proper JexlNode for union. Constructed by hand due to weirdness in how DelayedPredicates are parsed from a string.
        // A == '1' || B == '2' || ((ASTDelayedPredicate = true) && (C == '3')) || D == '4'
        JexlNode node1 = JexlNodeFactory.buildEQNode("A", "1");
        JexlNode node2 = JexlNodeFactory.buildEQNode("B", "2");
        JexlNode delayed = ASTDelayedPredicate.create(JexlNodeFactory.buildEQNode("C", "3"));
        JexlNode node4 = JexlNodeFactory.buildEQNode("D", "4");
        JexlNode orNode = JexlNodeFactory.createUnwrappedOrNode(Lists.newArrayList(node1, node2, delayed, node4));
        orNode = TreeFlatteningRebuildingVisitor.flatten(orNode);
        ASTJexlScript script = JexlNodeFactory.createScript(orNode);
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(union.currentNode()), new TreeEqualityVisitor.Reason()));
    }
    
    private ScannerStream buildScannerStream(String shard, String field, String value, List<String> uids) {
        IndexInfo info;
        if (uids == null) {
            // Build infinite range if no uids.
            info = new IndexInfo(-1);
        } else {
            info = new IndexInfo(uids);
        }
        
        JexlNode node = JexlNodeFactory.buildEQNode(field, value);
        info.applyNode(node);
        
        Iterator<Tuple2<String,IndexInfo>> iter = Collections.singletonList(new Tuple2<>(shard, info)).iterator();
        return ScannerStream.variable(iter, node);
    }
    
    /**
     * Builds a ScannerStream as if the collapseUids option were set to true. This preserves the count of uids coming off the global index without persisting
     * the actual uids.
     */
    private ScannerStream buildCollapsedScannerStream(String shard, String field, String value, int uidCount) {
        IndexInfo info = new IndexInfo(uidCount);
        
        JexlNode node = JexlNodeFactory.buildEQNode(field, value);
        info.applyNode(node);
        
        Iterator<Tuple2<String,IndexInfo>> iter = Collections.singletonList(new Tuple2<>(shard, info)).iterator();
        return ScannerStream.variable(iter, node);
    }
}
