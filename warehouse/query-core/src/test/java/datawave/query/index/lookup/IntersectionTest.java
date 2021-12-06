package datawave.query.index.lookup;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuples;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IntersectionTest {
    
    // Helper method to generate index matches (document id - field, value)
    private List<IndexMatch> buildIndexMatches(String field, String value, String... docIds) {
        List<IndexMatch> matches = new ArrayList<>(docIds.length);
        for (String docId : docIds) {
            matches.add(buildIndexMatch(field, value, docId));
        }
        return matches;
    }
    
    // Helper method to generate index matches (document id - field, value)
    private IndexMatch buildIndexMatch(String field, String value, String docId) {
        JexlNode eqNode = JexlNodeFactory.buildEQNode(field, value);
        return new IndexMatch(docId, eqNode);
    }
    
    // Helper method to generate expected index matches (document id only)
    private Set<IndexMatch> buildExpectedIndexMatches(String... docIds) {
        Set<IndexMatch> expected = new HashSet<>(docIds.length);
        for (String docId : docIds) {
            expected.add(new IndexMatch(docId));
        }
        return expected;
    }
    
    /**
     * Intersection of two terms hitting on the same shard.
     */
    @Test
    public void testIntersection_SameShardStreams() {
        // Build a peeking iterator for a left side term
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);
        left.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> leftTuple = Tuples.tuple("20190314_0", left);
        PeekingIterator<Tuple2<String,IndexInfo>> leftIter = Iterators.peekingIterator(Collections.singleton(leftTuple).iterator());
        
        // Build a peeking iterator for a right side term.
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        right.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> rightTuple = Tuples.tuple("20190314_0", right);
        PeekingIterator<Tuple2<String,IndexInfo>> rightIter = Iterators.peekingIterator(Collections.singleton(rightTuple).iterator());
        
        // Build the Intersection.
        IndexStream leftStream = ScannerStream.withData(leftIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        IndexStream rightStream = ScannerStream.withData(rightIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        List<IndexStream> indexStreams = Lists.newArrayList(leftStream, rightStream);
        
        Intersection intersection = new Intersection(indexStreams, new IndexInfo());
        assertTrue(intersection.hasNext());
        
        // Assert the Intersection
        Tuple2<String,IndexInfo> peekedTuple = intersection.peek();
        Tuple2<String,IndexInfo> nextedTuple = intersection.next();
        assertEquals(peekedTuple, nextedTuple);
        assertEquals("20190314_0", nextedTuple.first());
        
        // Assert expected index info
        Set<IndexMatch> expectedDocs = buildExpectedIndexMatches("doc2", "doc3");
        IndexInfo expectedIndexInfo = new IndexInfo(expectedDocs);
        
        expectedIndexInfo.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        assertEquals(expectedIndexInfo, nextedTuple.second());
        assertFalse(intersection.hasNext());
    }
    
    /**
     * Intersection with one term being a high cardinality hit.
     */
    @Test
    public void testIntersection_OneTermIsHighCardinality() {
        // Build a peeking iterator for a left side term. High cardinality means only counts, no document ids.
        IndexInfo left = new IndexInfo(100L);
        left.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> leftTuple = Tuples.tuple("20190314_0", left);
        PeekingIterator<Tuple2<String,IndexInfo>> leftIter = Iterators.peekingIterator(Collections.singleton(leftTuple).iterator());
        
        // Build a peeking iterator for a right side term.
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        right.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> rightTuple = Tuples.tuple("20190314_0", right);
        PeekingIterator<Tuple2<String,IndexInfo>> rightIter = Iterators.peekingIterator(Collections.singleton(rightTuple).iterator());
        
        // Build the Intersection.
        IndexStream leftStream = ScannerStream.withData(leftIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        IndexStream rightStream = ScannerStream.withData(rightIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        List<IndexStream> indexStreams = Lists.newArrayList(leftStream, rightStream);
        
        Intersection intersection = new Intersection(indexStreams, new IndexInfo());
        assertTrue(intersection.hasNext());
        
        // Assert the Intersection
        Tuple2<String,IndexInfo> peekedTuple = intersection.peek();
        Tuple2<String,IndexInfo> nextedTuple = intersection.next();
        assertEquals(peekedTuple, nextedTuple);
        assertEquals("20190314_0", nextedTuple.first());
        
        // Assert expected index info
        Set<IndexMatch> expectedDocs = buildExpectedIndexMatches("doc2", "doc3", "doc4");
        IndexInfo expectedIndexInfo = new IndexInfo(expectedDocs);
        
        expectedIndexInfo.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        assertEquals(expectedIndexInfo, nextedTuple.second());
        assertFalse(intersection.hasNext());
    }
    
    /**
     * Cannot intersect two disjoint shards.
     */
    @Test
    public void testIntersection_DifferentShardStreams() {
        // Build a peeking iterator for a left side term
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);
        left.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> leftTuple = Tuples.tuple("20190314_0", left);
        PeekingIterator<Tuple2<String,IndexInfo>> leftIter = Iterators.peekingIterator(Collections.singleton(leftTuple).iterator());
        
        // Build a peeking iterator for a right side term.
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        right.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> rightTuple = Tuples.tuple("20190314_1", right);
        PeekingIterator<Tuple2<String,IndexInfo>> rightIter = Iterators.peekingIterator(Collections.singleton(rightTuple).iterator());
        
        // Build the Intersection.
        IndexStream leftStream = ScannerStream.withData(leftIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        IndexStream rightStream = ScannerStream.withData(rightIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        List<IndexStream> indexStreams = Lists.newArrayList(leftStream, rightStream);
        
        Intersection intersection = new Intersection(indexStreams, new IndexInfo());
        assertFalse(intersection.hasNext());
    }
    
    /**
     * Following a seek of a day_shard against day only tuples calling next would result in the last returned day being returned again rather than the next one
     */
    @Test
    public void testIntersectionAfterSeek() {
        // Build a peeking iterator for a left side term
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);
        left.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        
        List<Tuple2<String,IndexInfo>> tupleList = new ArrayList();
        tupleList.add(Tuples.tuple("20190314", left));
        tupleList.add(Tuples.tuple("20190315", left));
        tupleList.add(Tuples.tuple("20190316", left));
        
        PeekingIterator<Tuple2<String,IndexInfo>> leftIter = Iterators.peekingIterator(tupleList.iterator());
        
        // Build a peeking iterator for a right side term.
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        right.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        
        tupleList = new ArrayList();
        tupleList.add(Tuples.tuple("20190314", right));
        tupleList.add(Tuples.tuple("20190315", right));
        tupleList.add(Tuples.tuple("20190316", right));
        
        PeekingIterator<Tuple2<String,IndexInfo>> rightIter = Iterators.peekingIterator(tupleList.iterator());
        
        // Build the Intersection.
        IndexStream leftStream = ScannerStream.withData(leftIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        IndexStream rightStream = ScannerStream.withData(rightIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        List<IndexStream> indexStreams = Lists.newArrayList(leftStream, rightStream);
        
        Intersection intersection = new Intersection(indexStreams, new IndexInfo());
        assertTrue(intersection.hasNext());
        assertEquals("20190314", intersection.peek().first());
        assertEquals("20190315_11", intersection.seek("20190315_11"));
        assertTrue(intersection.hasNext());
        intersection.next();
        assertEquals("20190316", intersection.peek().first());
    }
    
    /**
     * Intersection of day range with a shard range within the day range is possible.
     */
    @Test
    public void testIntersection_ShardAndDayStreams() {
        // Build a peeking iterator for a left side term
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);
        left.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> leftTuple = Tuples.tuple("20190314", left);
        PeekingIterator<Tuple2<String,IndexInfo>> leftIter = Iterators.peekingIterator(Collections.singleton(leftTuple).iterator());
        
        // Build a peeking iterator for a right side term.
        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        right.setNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        Tuple2<String,IndexInfo> rightTuple = Tuples.tuple("20190314_0", right);
        PeekingIterator<Tuple2<String,IndexInfo>> rightIter = Iterators.peekingIterator(Collections.singleton(rightTuple).iterator());
        
        // Build the Intersection.
        IndexStream leftStream = ScannerStream.withData(leftIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        IndexStream rightStream = ScannerStream.withData(rightIter, JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        List<IndexStream> indexStreams = Lists.newArrayList(leftStream, rightStream);
        
        Intersection intersection = new Intersection(indexStreams, new IndexInfo());
        assertTrue(intersection.hasNext());
        
        Tuple2<String,IndexInfo> peekedTuple = intersection.peek();
        Tuple2<String,IndexInfo> nextedTuple = intersection.next();
        assertEquals(peekedTuple, nextedTuple);
        assertEquals("20190314_0", nextedTuple.first());
        
        // Assert expected index info
        Set<IndexMatch> expectedDocs = buildExpectedIndexMatches("doc2", "doc3");
        IndexInfo expectedIndexInfo = new IndexInfo(expectedDocs);
        
        expectedIndexInfo.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        assertEquals(expectedIndexInfo, nextedTuple.second());
        assertFalse(intersection.hasNext());
    }
    
    @Test
    public void testIntersection_EmptyExceededValueThreshold() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("THIS_FIELD == 20");
        ScannerStream scannerStream = ScannerStream.exceededValueThreshold(Collections.emptyIterator(), script);
        List<? extends IndexStream> iterable = Collections.singletonList(scannerStream);
        
        Intersection intersection = new Intersection(iterable, null);
        assertEquals(IndexStream.StreamContext.ABSENT, intersection.context());
    }
    
    private ScannerStream buildScannerStream(String shard, String field, String value, List<String> uids) {
        IndexInfo ii1;
        if (uids != null) {
            ii1 = new IndexInfo(uids);
        } else {
            ii1 = new IndexInfo(-1);
        }
        JexlNode n1 = JexlNodeFactory.buildEQNode(field, value);
        ii1.applyNode(n1);
        Iterator<Tuple2<String,IndexInfo>> i1 = Arrays.asList(new Tuple2<>(shard, ii1)).iterator();
        return ScannerStream.variable(i1, n1);
    }
    
    @Test
    public void testIntersection_allUid() throws ParseException {
        // A && B && C
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' && B == '2' && C == '3'");
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c"));
        
        // B - uids
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", Arrays.asList("a.b.c", "a.b.c.d"));
        
        // C - uids
        ScannerStream s3 = buildScannerStream("20090101_1", "C", "3", Arrays.asList("a.b.c", "az"));
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2, s3);
        
        Intersection i = new Intersection(toMerge, new IndexInfo());
        assertTrue(i.hasNext());
        Tuple2<String,IndexInfo> ii = i.next();
        assertEquals(ii.first(), ("20090101_1"));
        assertEquals(ii.second().count, 1);
        assertEquals(ii.second().uids().size(), 1);
        assertEquals(ii.second().uids.iterator().next().uid, "a.b.c");
        assertEquals(ii.second().uids.iterator().next().type, IndexMatchType.AND);
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(ii.second().uids.iterator().next().getNode())));
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode())));
    }
    
    @Test
    public void testIntersection_noMatchesTest() throws ParseException {
        // A && B
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' && B == '2'");
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList());
        
        // B - uids
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", null);
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2);
        
        Intersection i = new Intersection(toMerge, new IndexInfo());
        assertFalse(i.hasNext());
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode())));
    }
    
    @Test
    public void testIntersection_uidAndInfinite() throws ParseException {
        // A && B && C
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' && B == '2' && C == '3'");
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c", "a.b.z"));
        
        // B - infinite
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", null);
        
        // C - infinite
        ScannerStream s3 = buildScannerStream("20090101_1", "C", "3", null);
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2, s3);
        
        Intersection i = new Intersection(toMerge, new IndexInfo());
        assertTrue(i.hasNext());
        Tuple2<String,IndexInfo> ii = i.next();
        assertEquals(ii.first(), ("20090101_1"));
        assertEquals(ii.second().count, 2);
        assertEquals(ii.second().uids().size(), 2);
        Iterator<IndexMatch> uidsIterator = ii.second().uids.iterator();
        
        // can't guarantee order but need to for validation
        List<IndexMatch> all = new ArrayList<>();
        assertTrue(uidsIterator.hasNext());
        IndexMatch m = uidsIterator.next();
        all.add(m);
        assertTrue(uidsIterator.hasNext());
        m = uidsIterator.next();
        all.add(m);
        
        assertFalse(uidsIterator.hasNext());
        
        Collections.sort(all, (m1, m2) -> m1.getUid().compareTo(m2.getUid()));
        
        m = all.get(0);
        assertEquals(m.uid, "a.b.c");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode())));
        m = all.get(1);
        assertEquals(m.uid, "a.b.z");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode())));
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode())));
    }
    
    @Test
    public void testIntersection_uidAndUnindexedAndDelayed() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' && B == '2' && C == '3'");
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c", "a.b.z"));
        
        // B - unindexed
        ScannerStream s2 = ScannerStream.unindexed(JexlNodeFactory.buildEQNode("B", "2"));
        
        // C - delayed
        ScannerStream s3 = ScannerStream.delayedExpression(JexlNodeFactory.buildEQNode("C", "3"));
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2, s3);
        
        Intersection i = new Intersection(toMerge, new IndexInfo());
        assertTrue(i.hasNext());
        Tuple2<String,IndexInfo> ii = i.next();
        assertEquals(ii.first(), ("20090101_1"));
        assertEquals(ii.second().count, 2);
        assertEquals(ii.second().uids().size(), 2);
        Iterator<IndexMatch> uidsIterator = ii.second().uids.iterator();
        
        // can't guarantee order but need to for validation
        List<IndexMatch> all = new ArrayList<>();
        assertTrue(uidsIterator.hasNext());
        IndexMatch m = uidsIterator.next();
        all.add(m);
        assertTrue(uidsIterator.hasNext());
        m = uidsIterator.next();
        all.add(m);
        
        assertFalse(uidsIterator.hasNext());
        
        Collections.sort(all, (m1, m2) -> m1.getUid().compareTo(m2.getUid()));
        
        m = all.get(0);
        assertEquals(m.uid, "a.b.c");
        assertEquals(m.type, IndexMatchType.OR);
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode())));
        m = all.get(1);
        assertEquals(m.uid, "a.b.z");
        assertEquals(m.type, IndexMatchType.OR);
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode())));
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode())));
    }
    
    @Test
    public void testIntersection_uidAndUnindexedAndDelayedAndInfinite() throws ParseException {
        // (A && B && C && D)
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' && B == '2' && C == '3' && D == '4'");
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c", "a.b.z"));
        
        // B - unindexed
        ScannerStream s2 = ScannerStream.unindexed(JexlNodeFactory.buildEQNode("B", "2"));
        
        // C - delayed
        ScannerStream s3 = ScannerStream.delayedExpression(JexlNodeFactory.buildEQNode("C", "3"));
        
        // D - infinite
        ScannerStream s4 = buildScannerStream("20090101_1", "D", "4", null);
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2, s3, s4);
        
        Intersection i = new Intersection(toMerge, new IndexInfo());
        assertTrue(i.hasNext());
        Tuple2<String,IndexInfo> ii = i.next();
        assertEquals(ii.first(), ("20090101_1"));
        assertEquals(ii.second().count, 2);
        assertEquals(ii.second().uids().size(), 2);
        Iterator<IndexMatch> uidsIterator = ii.second().uids.iterator();
        
        // can't guarantee order but need to for validation
        List<IndexMatch> all = new ArrayList<>();
        assertTrue(uidsIterator.hasNext());
        IndexMatch m = uidsIterator.next();
        all.add(m);
        assertTrue(uidsIterator.hasNext());
        m = uidsIterator.next();
        all.add(m);
        
        assertFalse(uidsIterator.hasNext());
        
        Collections.sort(all, (m1, m2) -> m1.getUid().compareTo(m2.getUid()));
        
        m = all.get(0);
        assertEquals(m.uid, "a.b.c");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode())));
        m = all.get(1);
        assertEquals(m.uid, "a.b.z");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode())));
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode())));
    }
    
    @Test
    public void testIntersection_nestedOrReduction() throws ParseException {
        // (A || B) && C
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(A == '1' || B == '2') && C == '3'");
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c", "a.b.z"));
        
        // B - uids
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", Arrays.asList("x.y.z", "x.y.z.1"));
        
        // C - uids
        ScannerStream s3 = buildScannerStream("20090101_1", "C", "3", null);
        
        List<? extends IndexStream> toUnion = Arrays.asList(s1, s2);
        Union union = new Union(toUnion);
        List<? extends IndexStream> toMerge = Arrays.asList(union, s3);
        
        Intersection i = new Intersection(toMerge, new IndexInfo());
        assertTrue(i.hasNext());
        Tuple2<String,IndexInfo> ii = i.next();
        assertEquals(ii.first(), ("20090101_1"));
        assertEquals(ii.second().count, 4);
        assertEquals(ii.second().uids().size(), 4);
        Iterator<IndexMatch> uidsIterator = ii.second().uids.iterator();
        
        // can't guarantee order but need to for validation
        List<IndexMatch> all = new ArrayList<>();
        assertTrue(uidsIterator.hasNext());
        IndexMatch m = uidsIterator.next();
        all.add(m);
        assertTrue(uidsIterator.hasNext());
        m = uidsIterator.next();
        all.add(m);
        assertTrue(uidsIterator.hasNext());
        m = uidsIterator.next();
        all.add(m);
        assertTrue(uidsIterator.hasNext());
        m = uidsIterator.next();
        all.add(m);
        
        assertFalse(uidsIterator.hasNext());
        
        Collections.sort(all, (m1, m2) -> m1.getUid().compareTo(m2.getUid()));
        
        m = all.get(0);
        assertEquals(m.uid, "a.b.c");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("A == '1' && C == '3'"), JexlNodeFactory.createScript(m.getNode())));
        m = all.get(1);
        assertEquals(m.uid, "a.b.z");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("A == '1' && C == '3'"), JexlNodeFactory.createScript(m.getNode())));
        m = all.get(2);
        assertEquals(m.uid, "x.y.z");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("B == '2' && C == '3'"), JexlNodeFactory.createScript(m.getNode())));
        m = all.get(3);
        assertEquals(m.uid, "x.y.z.1");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("B == '2' && C == '3'"), JexlNodeFactory.createScript(m.getNode())));
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode())));
    }
    
    @Test
    public void testIntersection_nestedAndReduction() throws ParseException {
        // (A AND B) AND (C AND D) AND E
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("(A == '1' && B == '2') && (C == '3' && D == '4') && E =='5'");
        
        // A uid
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c", "a.b.z"));
        
        // B infinite
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", null);
        
        // C uid
        ScannerStream s3 = buildScannerStream("20090101_1", "C", "3", Arrays.asList("a.b.c", "x.y.z.1"));
        
        // D infinite
        ScannerStream s4 = buildScannerStream("20090101_1", "D", "4", null);
        
        // E infinite
        ScannerStream s5 = buildScannerStream("20090101_1", "E", "5", null);
        
        List<? extends IndexStream> toIntersection1 = Arrays.asList(s1, s2);
        Intersection intersection1 = new Intersection(toIntersection1, new IndexInfo());
        List<? extends IndexStream> toIntersection2 = Arrays.asList(s3, s4);
        Intersection intersection2 = new Intersection(toIntersection2, new IndexInfo());
        List<? extends IndexStream> toMerge = Arrays.asList(intersection1, intersection2, s5);
        
        Intersection i = new Intersection(toMerge, new IndexInfo());
        assertTrue(i.hasNext());
        Tuple2<String,IndexInfo> ii = i.next();
        assertEquals(ii.first(), ("20090101_1"));
        assertEquals(ii.second().count, 1);
        assertEquals(ii.second().uids().size(), 1);
        Iterator<IndexMatch> uidsIterator = ii.second().uids.iterator();
        
        // can't guarantee order but need to for validation
        List<IndexMatch> all = new ArrayList<>();
        assertTrue(uidsIterator.hasNext());
        IndexMatch m = uidsIterator.next();
        all.add(m);
        
        assertFalse(uidsIterator.hasNext());
        
        Collections.sort(all, (m1, m2) -> m1.getUid().compareTo(m2.getUid()));
        
        m = all.get(0);
        assertEquals(m.uid, "a.b.c");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode())));
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode())));
    }
    
    @Test
    public void testIntersection_doubleNestedOrReduction() throws ParseException {
        // (((A OR B) AND C) OR ((D OR E) AND F)) AND G
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c", "a.b.z"));
        
        // B - uids
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", Arrays.asList("x.y.z", "x.y.z.1"));
        
        // C - infinite
        ScannerStream s3 = buildScannerStream("20090101_1", "C", "3", null);
        
        // D - uids
        ScannerStream s4 = buildScannerStream("20090101_1", "D", "4", Arrays.asList("a.a.a", "b.b.b"));
        
        // E - empty
        ScannerStream s5 = ScannerStream.noData(JexlNodeFactory.buildEQNode("E", "5"));
        
        // F - uid
        ScannerStream s6 = buildScannerStream("20090101_1", "F", "6", Arrays.asList("b.b.b"));
        
        // G - single uid
        ScannerStream s7 = buildScannerStream("20090101_1", "G", "7", Arrays.asList("a.b.c"));
        
        List<? extends IndexStream> toUnion1 = Arrays.asList(s1, s2);
        Union union1 = new Union(toUnion1);
        List<? extends IndexStream> toMerge1 = Arrays.asList(union1, s3);
        Intersection intersection1 = new Intersection(toMerge1, new IndexInfo());
        
        List<? extends IndexStream> toUnion2 = Arrays.asList(s4, s5);
        Union union2 = new Union(toUnion2);
        List<? extends IndexStream> toMerge2 = Arrays.asList(union2, s6);
        Intersection intersection2 = new Intersection(toMerge2, new IndexInfo());
        
        Union union3 = new Union(Arrays.asList(intersection1, intersection2));
        Intersection i = new Intersection(Arrays.asList(union3, s7), new IndexInfo());
        
        assertTrue(i.hasNext());
        Tuple2<String,IndexInfo> ii = i.next();
        assertEquals(ii.first(), ("20090101_1"));
        assertEquals(ii.second().count, 1);
        assertEquals(ii.second().uids().size(), 1);
        Iterator<IndexMatch> uidsIterator = ii.second().uids.iterator();
        
        // can't guarantee order but need to for validation
        List<IndexMatch> all = new ArrayList<>();
        assertTrue(uidsIterator.hasNext());
        IndexMatch m = uidsIterator.next();
        all.add(m);
        
        assertFalse(uidsIterator.hasNext());
        
        Collections.sort(all, (m1, m2) -> m1.getUid().compareTo(m2.getUid()));
        
        m = all.get(0);
        assertEquals(m.uid, "a.b.c");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("A == '1' && C == '3' && G == '7'"), JexlNodeFactory.createScript(m.getNode())));
        
        assertTrue(TreeEqualityVisitor.isEqual(
                        JexlASTHelper.parseJexlQuery("G =='7' && ((A == '1' && C == '3') || (B == '2' && C == '3') || (D == '4' && F == '6'))"),
                        JexlNodeFactory.createScript(i.currentNode())));
    }
    
    @Test
    public void testIntersection_doubleNestedOrReductionMultipleBackPropagation() throws ParseException {
        // (((A OR B) AND C) OR ((D OR E) AND F)) AND G
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c", "a.b.z"));
        
        // B - uids
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", Arrays.asList("x.y.z", "x.y.z.1"));
        
        // C - infinite
        ScannerStream s3 = buildScannerStream("20090101_1", "C", "3", null);
        
        // D - uids
        ScannerStream s4 = buildScannerStream("20090101_1", "D", "4", Arrays.asList("a.a.a", "b.b.b"));
        
        // E - empty
        ScannerStream s5 = ScannerStream.noData(JexlNodeFactory.buildEQNode("E", "5"));
        
        // F - uid
        ScannerStream s6 = buildScannerStream("20090101_1", "F", "6", Arrays.asList("b.b.b"));
        
        // G - single uid
        ScannerStream s7 = buildScannerStream("20090101_1", "G", "7", Arrays.asList("a.b.c", "x.y.z.1"));
        
        List<? extends IndexStream> toUnion1 = Arrays.asList(s1, s2);
        Union union1 = new Union(toUnion1);
        List<? extends IndexStream> toMerge1 = Arrays.asList(union1, s3);
        Intersection intersection1 = new Intersection(toMerge1, new IndexInfo());
        
        List<? extends IndexStream> toUnion2 = Arrays.asList(s4, s5);
        Union union2 = new Union(toUnion2);
        List<? extends IndexStream> toMerge2 = Arrays.asList(union2, s6);
        Intersection intersection2 = new Intersection(toMerge2, new IndexInfo());
        
        Union union3 = new Union(Arrays.asList(intersection1, intersection2));
        Intersection i = new Intersection(Arrays.asList(union3, s7), new IndexInfo());
        
        assertTrue(i.hasNext());
        Tuple2<String,IndexInfo> ii = i.next();
        assertEquals(ii.first(), ("20090101_1"));
        assertEquals(ii.second().count, 2);
        assertEquals(ii.second().uids().size(), 2);
        Iterator<IndexMatch> uidsIterator = ii.second().uids.iterator();
        
        // can't guarantee order but need to for validation
        List<IndexMatch> all = new ArrayList<>();
        assertTrue(uidsIterator.hasNext());
        IndexMatch m = uidsIterator.next();
        all.add(m);
        assertTrue(uidsIterator.hasNext());
        m = uidsIterator.next();
        all.add(m);
        
        assertFalse(uidsIterator.hasNext());
        
        Collections.sort(all, (m1, m2) -> m1.getUid().compareTo(m2.getUid()));
        
        m = all.get(0);
        assertEquals(m.uid, "a.b.c");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("A == '1' && C == '3' && G == '7'"), JexlNodeFactory.createScript(m.getNode())));
        m = all.get(1);
        assertEquals(m.uid, "x.y.z.1");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("B == '2' && C == '3' && G == '7'"), JexlNodeFactory.createScript(m.getNode())));
        
        assertTrue(TreeEqualityVisitor.isEqual(
                        JexlASTHelper.parseJexlQuery("G =='7' && ((A == '1' && C == '3') || (B == '2' && C == '3') || (D == '4' && F == '6'))"),
                        JexlNodeFactory.createScript(i.currentNode())));
    }
    
    @Test
    public void testSeekBeyondEndOfIntersection() throws ParseException {
        // A && B && C
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == '1' && B == '2' && C == '3'");
        
        // A - uids
        ScannerStream s1 = buildScannerStream("20090101_1", "A", "1", Arrays.asList("a.b.c"));
        
        // B - uids
        ScannerStream s2 = buildScannerStream("20090101_1", "B", "2", Arrays.asList("a.b.c", "a.b.c.d"));
        
        // C - uids
        ScannerStream s3 = buildScannerStream("20090101_1", "C", "3", Arrays.asList("a.b.c", "az"));
        
        List<? extends IndexStream> toMerge = Arrays.asList(s1, s2, s3);
        
        Intersection i = new Intersection(toMerge, new IndexInfo());
        assertTrue(i.hasNext());
        
        assertNull(i.seek("20091231_1"));
        assertFalse(i.hasNext());
        assertNull(i.next());
    }
    
    // A && B
    @Test
    public void testIntersectionOfShards_seekBeforeStreamStart() {
        // Seek to a day range before the IndexStream
        Intersection intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190301_0", intersection.seek("20190202"));
        assertEquals("20190301_0", intersection.next().first());
        
        // Seek to a day-underscore range before the IndexStream
        intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190301_0", intersection.seek("20190202_"));
        assertEquals("20190301_0", intersection.next().first());
        
        // Seek to a shard range before the IndexStream
        intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190301_0", intersection.seek("20190202_0"));
        assertEquals("20190301_0", intersection.next().first());
    }
    
    // A && B
    @Test
    public void testIntersectionOfShards_seekToStreamStart() {
        // Seek to a day range at the start of the IndexStream
        Intersection intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190301_0", intersection.seek("20190301"));
        assertEquals("20190301_0", intersection.next().first());
        
        // Seek to a day-underscore range at the start of the IndexStream
        intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190301_0", intersection.seek("20190301_"));
        assertEquals("20190301_0", intersection.next().first());
        
        // Seek to a shard range before at the start of the IndexStream
        intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190301_0", intersection.seek("20190301_0"));
        assertEquals("20190301_0", intersection.next().first());
    }
    
    // A && B
    @Test
    public void testIntersectionOfShards_seekToMiddleOfStream() {
        // Seek to a day range in the middle of the IndexStream
        Intersection intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190303_0", intersection.seek("20190303"));
        assertEquals("20190303_0", intersection.next().first());
        
        // Seek to a day-underscore range in the middle of the IndexStream
        intersection = buildIntersectionOfShards();
        assertEquals("20190303_0", intersection.seek("20190303_"));
        assertEquals("20190303_0", intersection.next().first());
        
        // Seek to a shard range in the middle of the IndexStream
        intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190303_3", intersection.seek("20190303_3"));
        assertEquals("20190303_3", intersection.next().first());
    }
    
    // A && B
    @Test
    public void testIntersectionOfShards_seekToNonExistentMiddleOfStream() {
        // Seek to a non-existent day range in the middle of the IndexStream
        Intersection intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190307_0", intersection.seek("20190305"));
        assertEquals("20190307_0", intersection.next().first());
        
        // Seek to a non-existent day-underscore range to the middle of the IndexStream
        intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190307_0", intersection.seek("20190305_"));
        assertEquals("20190307_0", intersection.next().first());
        
        // Seek to a non-existent shard range to the middle of of the IndexStream
        intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190307_0", intersection.seek("20190305_0"));
        assertEquals("20190307_0", intersection.next().first());
    }
    
    // A && B
    @Test
    public void testIntersectionOfShards_seekToEndOfStream() {
        // Seek to the last day range in this IndexStream
        Intersection intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190309_0", intersection.seek("20190309"));
        assertEquals("20190309_0", intersection.next().first());
        
        // Seek to the last day-underscore range in this IndexStream
        intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190309_0", intersection.seek("20190309_"));
        assertEquals("20190309_0", intersection.next().first());
        
        // Seek to the last shard range in this IndexStream
        intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertEquals("20190309_9", intersection.seek("20190309_9"));
        assertEquals("20190309_9", intersection.next().first());
    }
    
    // A && B
    @Test
    public void testIntersectionOfShards_seekBeyondEndOfStream() {
        // Seek to a day range beyond the end of this IndexStream
        Intersection intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertNull(intersection.seek("20190310"));
        assertFalse(intersection.hasNext());
        assertNull(intersection.next());
        
        // Seek to a day-underscore range beyond the end of this IndexStream
        intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertNull(intersection.seek("20190310_"));
        assertFalse(intersection.hasNext());
        assertNull(intersection.next());
        
        // Seek to a shard range beyond the end of this IndexStream
        intersection = buildIntersectionOfShards();
        assertTrue(intersection.hasNext());
        assertNull(intersection.seek("20190309_99"));
        assertFalse(intersection.hasNext());
        assertNull(intersection.next());
    }
    
    // A && B
    @Test
    public void testIntersectionOfDays_seekBeforeStreamStart() {
        // Seek to a day range before the IndexStream
        Intersection intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190301", intersection.seek("20190202"));
        assertEquals("20190301", intersection.next().first());
        
        // Seek to a day-underscore range before the IndexStream
        intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190301", intersection.seek("20190202_"));
        assertEquals("20190301", intersection.next().first());
        
        // Seek to a shard range before the IndexStream
        intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190301", intersection.seek("20190202_0"));
        assertEquals("20190301", intersection.next().first());
    }
    
    // A && B
    @Test
    public void testIntersectionOfDays_seekToStreamStart() {
        // Seek to a day range at the start of the IndexStream
        Intersection intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190301", intersection.seek("20190301"));
        assertEquals("20190301", intersection.next().first());
        
        // Seek to a day-underscore range at the start of the IndexStream
        intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190301", intersection.seek("20190301_"));
        assertEquals("20190301", intersection.next().first());
        
        // Seek to a shard range before at the start of the IndexStream
        intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190301_0", intersection.seek("20190301_0"));
        assertEquals("20190301_0", intersection.next().first());
    }
    
    // A && B
    @Test
    public void testIntersectionOfDays_seekToMiddleOfStream() {
        // Seek to a day range in the middle of the IndexStream
        Intersection intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190303", intersection.seek("20190303"));
        assertEquals("20190303", intersection.next().first());
        
        // Seek to a day-underscore range in the middle of the IndexStream
        intersection = buildIntersectionOfDays();
        assertEquals("20190303", intersection.seek("20190303_"));
        assertEquals("20190303", intersection.next().first());
        
        // Seek to a shard range in the middle of the IndexStream
        intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190303_3", intersection.seek("20190303_3"));
        assertEquals("20190303_3", intersection.next().first());
    }
    
    // A && B
    @Test
    public void testIntersectionOfDays_seekToNonExistentMiddleOfStream() {
        // Seek to a non-existent day range in the middle of the IndexStream
        Intersection intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190307", intersection.seek("20190305"));
        assertEquals("20190307", intersection.next().first());
        
        // Seek to a non-existent day-underscore range to the middle of the IndexStream
        intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190307", intersection.seek("20190305_"));
        assertEquals("20190307", intersection.next().first());
        
        // Seek to a non-existent shard range to the middle of of the IndexStream
        intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190307", intersection.seek("20190305_0"));
        assertEquals("20190307", intersection.next().first());
    }
    
    // A && B
    @Test
    public void testIntersectionOfDays_seekToEndOfStream() {
        // Seek to the last day range in this IndexStream
        Intersection intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190309", intersection.seek("20190309"));
        assertEquals("20190309", intersection.next().first());
        
        // Seek to the last day-underscore range in this IndexStream
        intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190309", intersection.seek("20190309_"));
        assertEquals("20190309", intersection.next().first());
        
        // Seek to the last shard range in this IndexStream
        intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertEquals("20190309_9", intersection.seek("20190309_9"));
        assertEquals("20190309_9", intersection.next().first());
    }
    
    // A && B
    @Test
    public void testIntersectionOfDays_seekBeyondEndOfStream() {
        // Seek to a day range beyond the end of this IndexStream
        Intersection intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertNull(intersection.seek("20190310"));
        assertFalse(intersection.hasNext());
        assertNull(intersection.next());
        
        // Seek to a day-underscore range beyond the end of this IndexStream
        intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertNull(intersection.seek("20190310_"));
        assertFalse(intersection.hasNext());
        assertNull(intersection.next());
        
        // Seek to a shard range beyond the end of this IndexStream
        intersection = buildIntersectionOfDays();
        assertTrue(intersection.hasNext());
        assertNull(intersection.seek("20190310_0"));
        assertFalse(intersection.hasNext());
        assertNull(intersection.next());
    }
    
    @Test
    public void testTopElementMatch_topIsShard() {
        Intersection intersection = buildIntersectionOfShards();
        
        // Match on same shard
        assertEquals("20190301_0", intersection.isTopElementAMatch("20190301_0"));
        
        // Match on same day
        assertEquals("20190301_0", intersection.isTopElementAMatch("20190301"));
        
        // No match on same day, different shard
        assertNull(intersection.isTopElementAMatch("20190301_9"));
        
        // No match on different day
        assertNull(intersection.isTopElementAMatch("20190302"));
    }
    
    @Test
    public void testTopElementMatch_topIsDay() {
        Intersection intersection = buildIntersectionOfDays();
        
        // Match on shard within the day
        assertEquals("20190301_0", intersection.isTopElementAMatch("20190301_0"));
        
        // Match on same day
        assertEquals("20190301", intersection.isTopElementAMatch("20190301"));
        
        // Match on different shard within day
        assertEquals("20190301_9", intersection.isTopElementAMatch("20190301_9"));
        
        // No match on different day
        assertNull(intersection.isTopElementAMatch("20190302"));
    }
    
    private Intersection buildIntersectionOfShards() {
        List<String> ignoredDays = Lists.newArrayList("20190304", "20190305", "20190306");
        SortedSet<String> shards = buildShards(ignoredDays);
        
        ScannerStream s1 = buildFullScannerStream(shards, "A", "1");
        ScannerStream s2 = buildFullScannerStream(shards, "B", "2");
        
        return new Intersection(Arrays.asList(s1, s2), new IndexInfo());
    }
    
    private Intersection buildIntersectionOfDays() {
        List<String> ignoredDays = Lists.newArrayList("20190304", "20190305", "20190306");
        SortedSet<String> shards = buildDays(ignoredDays);
        
        ScannerStream s1 = buildFullScannerStream(shards, "A", "1");
        ScannerStream s2 = buildFullScannerStream(shards, "B", "2");
        
        return new Intersection(Arrays.asList(s1, s2), new IndexInfo());
    }
    
    // Build a set of shards for the first 9 days in march, with the option to ignore days.
    private SortedSet<String> buildShards(List<String> ignoredDays) {
        SortedSet<String> shards = new TreeSet<>();
        for (int ii = 1; ii < 10; ii++) {
            String day = "2019030" + ii;
            if (ignoredDays.contains(day)) {
                continue;
            }
            for (int jj = 0; jj < 20; jj++) {
                shards.add(day + "_" + jj);
            }
        }
        return shards;
    }
    
    // Build a set of day shards for the first 9 days in march, with the option to ignore days.
    private SortedSet<String> buildDays(List<String> ignoredDays) {
        SortedSet<String> shards = new TreeSet<>();
        for (int ii = 1; ii < 10; ii++) {
            String day = "2019030" + ii;
            if (ignoredDays.contains(day)) {
                continue;
            }
            shards.add(day);
        }
        return shards;
    }
    
    // Build a ScannerStream specifically for testing the ability to seek through the stream.
    private ScannerStream buildFullScannerStream(SortedSet<String> shards, String field, String value) {
        JexlNode node = JexlNodeFactory.buildEQNode(field, value);
        
        List<Tuple2<String,IndexInfo>> elements = new ArrayList<>();
        for (String shard : shards) {
            IndexInfo info = new IndexInfo(-1);
            info.applyNode(node);
            elements.add(new Tuple2<>(shard, info));
        }
        
        return ScannerStream.variable(elements.iterator(), node);
    }
}
