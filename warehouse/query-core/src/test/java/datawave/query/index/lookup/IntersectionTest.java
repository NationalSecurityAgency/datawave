package datawave.query.index.lookup;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuples;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(ii.second().uids.iterator().next().getNode()),
                        new TreeEqualityVisitor.Reason()));
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode()), new TreeEqualityVisitor.Reason()));
    }
    
    @Test
    public void testIntersection_uidAndinfinite() throws ParseException {
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
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode()), new TreeEqualityVisitor.Reason()));
        m = all.get(1);
        assertEquals(m.uid, "a.b.z");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode()), new TreeEqualityVisitor.Reason()));
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode()), new TreeEqualityVisitor.Reason()));
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
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode()), new TreeEqualityVisitor.Reason()));
        m = all.get(1);
        assertEquals(m.uid, "a.b.z");
        assertEquals(m.type, IndexMatchType.OR);
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode()), new TreeEqualityVisitor.Reason()));
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode()), new TreeEqualityVisitor.Reason()));
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
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode()), new TreeEqualityVisitor.Reason()));
        m = all.get(1);
        assertEquals(m.uid, "a.b.z");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode()), new TreeEqualityVisitor.Reason()));
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode()), new TreeEqualityVisitor.Reason()));
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
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("A == '1' && C == '3'"), JexlNodeFactory.createScript(m.getNode()),
                        new TreeEqualityVisitor.Reason()));
        m = all.get(1);
        assertEquals(m.uid, "a.b.z");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("A == '1' && C == '3'"), JexlNodeFactory.createScript(m.getNode()),
                        new TreeEqualityVisitor.Reason()));
        m = all.get(2);
        assertEquals(m.uid, "x.y.z");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("B == '2' && C == '3'"), JexlNodeFactory.createScript(m.getNode()),
                        new TreeEqualityVisitor.Reason()));
        m = all.get(3);
        assertEquals(m.uid, "x.y.z.1");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("B == '2' && C == '3'"), JexlNodeFactory.createScript(m.getNode()),
                        new TreeEqualityVisitor.Reason()));
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode()), new TreeEqualityVisitor.Reason()));
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
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(m.getNode()), new TreeEqualityVisitor.Reason()));
        
        assertTrue(TreeEqualityVisitor.isEqual(script, JexlNodeFactory.createScript(i.currentNode()), new TreeEqualityVisitor.Reason()));
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
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("A == '1' && C == '3' && G == '7'"), JexlNodeFactory.createScript(m.getNode()),
                        new TreeEqualityVisitor.Reason()));
        
        assertTrue(TreeEqualityVisitor.isEqual(
                        JexlASTHelper.parseJexlQuery("G =='7' && ((A == '1' && C == '3') || (B == '2' && C == '3') || (D == '4' && F == '6'))"),
                        JexlNodeFactory.createScript(i.currentNode()), new TreeEqualityVisitor.Reason()));
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
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("A == '1' && C == '3' && G == '7'"), JexlNodeFactory.createScript(m.getNode()),
                        new TreeEqualityVisitor.Reason()));
        m = all.get(1);
        assertEquals(m.uid, "x.y.z.1");
        assertEquals(m.type, IndexMatchType.AND);
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery("B == '2' && C == '3' && G == '7'"), JexlNodeFactory.createScript(m.getNode()),
                        new TreeEqualityVisitor.Reason()));
        
        assertTrue(TreeEqualityVisitor.isEqual(
                        JexlASTHelper.parseJexlQuery("G =='7' && ((A == '1' && C == '3') || (B == '2' && C == '3') || (D == '4' && F == '6'))"),
                        JexlNodeFactory.createScript(i.currentNode()), new TreeEqualityVisitor.Reason()));
    }
}
