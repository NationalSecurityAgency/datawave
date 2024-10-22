package datawave.query.index.lookup;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Test;

import com.google.common.collect.ImmutableSortedSet;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.util.count.CountMap;

/**
 * Test basic functionality of the {@link IndexInfo} class.
 */
public class IndexInfoTest {

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
    private Set<IndexMatch> buildExpectedIndexMatches(String field, String value, String... docIds) {
        JexlNode node = JexlNodeFactory.buildEQNode(field, value);
        Set<IndexMatch> expected = new HashSet<>(docIds.length);
        for (String docId : docIds) {
            expected.add(new IndexMatch(docId, node));
        }
        return expected;
    }

    /**
     * Intersection of query terms when both terms have document ids.
     */
    @Test
    public void testIntersection_BothTermsHaveDocIds() {
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);

        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);

        IndexInfo merged = left.intersect(right);

        // The intersection of left and right should be a set of two document ids
        Set<IndexMatch> expectedDocs = buildExpectedIndexMatches("FIELD", "VALUE", "doc2", "doc3");

        assertEquals(2, merged.uids().size());
        assertEquals(expectedDocs, merged.uids());
    }

    /**
     * Intersection of query terms when only one term has document ids.
     */
    @Test
    public void testIntersection_OnlyOneTermHasDocIds() {
        IndexInfo left = new IndexInfo(20L);
        left.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);
        right.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        IndexInfo merged = left.intersect(right);

        // intersection of uids and a countable shard range is a shard range
        assertTrue(merged.uids().isEmpty());
    }

    @Test
    public void testIntersection_NestedOrTermHasDocIdsInfiniteToSmall() {
        IndexInfo left = new IndexInfo(-1);
        List<JexlNode> children = new ArrayList<>();
        children.add(JexlNodeFactory.buildEQNode("FIELD1", "VALUE1"));
        children.add(JexlNodeFactory.buildEQNode("FIELD2", "VALUE2"));
        children.add(JexlNodeFactory.buildEQNode("FIELD3", "VALUE3"));

        left.applyNode(JexlNodeFactory.createOrNode(children));

        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc1");
        IndexInfo right = new IndexInfo(rightMatches);
        right.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        // build the query string before
        List<JexlNode> andChildren = new ArrayList<>();
        andChildren.add(left.getNode());
        andChildren.add(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        JexlNode origQueryTree = JexlNodeFactory.createAndNode(andChildren);

        IndexInfo merged = left.intersect(right);

        // The intersection of left and right should be a set of 1 document ids

        // the intersection of uids and an infinite shard range is a shard range
        assertTrue(merged.uids().isEmpty());
        assertTrue(TreeEqualityVisitor.isEqual(JexlNodeFactory.createScript(origQueryTree), JexlNodeFactory.createScript(merged.getNode())));
    }

    @Test
    public void testIntersection_NestedOrTermHasDocIdsSmallToInfinite() {
        IndexInfo left = new IndexInfo(-1);
        List<JexlNode> children = new ArrayList<>();
        children.add(JexlNodeFactory.buildEQNode("FIELD1", "VALUE1"));
        children.add(JexlNodeFactory.buildEQNode("FIELD2", "VALUE2"));
        children.add(JexlNodeFactory.buildEQNode("FIELD3", "VALUE3"));

        left.applyNode(JexlNodeFactory.createOrNode(children));

        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc1");
        IndexInfo right = new IndexInfo(rightMatches);
        right.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        // build the query string before
        List<JexlNode> andChildren = new ArrayList<>();
        andChildren.add(left.getNode());
        andChildren.add(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        JexlNode origQueryTree = JexlNodeFactory.createAndNode(andChildren);

        IndexInfo merged = right.intersect(left);

        // intersection of uid and infinite shard range is a shard range
        assertTrue(merged.uids().isEmpty());
        assertTrue(TreeEqualityVisitor.isEqual(JexlNodeFactory.createScript(origQueryTree), JexlNodeFactory.createScript(merged.getNode())));
    }

    @Test
    public void testIntersection_NestedOrTermHasDocIdsInfiniteToSmallWithDelayed() {
        IndexInfo left = new IndexInfo(-1);
        List<JexlNode> children = new ArrayList<>();
        children.add(JexlNodeFactory.buildEQNode("FIELD1", "VALUE1"));
        children.add(JexlNodeFactory.buildEQNode("FIELD2", "VALUE2"));
        children.add(JexlNodeFactory.buildEQNode("FIELD3", "VALUE3"));

        left.applyNode(JexlNodeFactory.createOrNode(children));

        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc1");
        IndexInfo right = new IndexInfo(rightMatches);
        right.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        JexlNode delayed = JexlNodeFactory.buildEQNode("DELAYED_FIELD", "DELAYED_VALUE");

        // build the query string before
        List<JexlNode> andChildren = new ArrayList<>();
        andChildren.add(left.getNode());
        andChildren.add(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        andChildren.add(delayed);

        JexlNode origQueryTree = JexlNodeFactory.createAndNode(andChildren);

        IndexInfo merged = left.intersect(right, Arrays.asList(delayed), left);

        // The intersection of left and right should be a set of 1 document ids
        List<IndexMatch> expectedDocs = buildIndexMatches("FIELD", "VALUE", "doc1");
        ImmutableSortedSet<IndexMatch> expectedSorted = ImmutableSortedSet.copyOf(expectedDocs);

        assertTrue(merged.uids().isEmpty());
        assertTrue(TreeEqualityVisitor.isEqual(JexlNodeFactory.createScript(origQueryTree), JexlNodeFactory.createScript(merged.getNode())));
    }

    @Test
    public void testIntersection_NestedOrTermHasDocIdsSmallToInfiniteWithDelay() {
        IndexInfo left = new IndexInfo(-1);
        List<JexlNode> children = new ArrayList<>();
        children.add(JexlNodeFactory.buildEQNode("FIELD1", "VALUE1"));
        children.add(JexlNodeFactory.buildEQNode("FIELD2", "VALUE2"));
        children.add(JexlNodeFactory.buildEQNode("FIELD3", "VALUE3"));

        left.applyNode(JexlNodeFactory.createOrNode(children));

        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc1");
        IndexInfo right = new IndexInfo(rightMatches);
        right.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        JexlNode delayed = JexlNodeFactory.buildEQNode("DELAYED_FIELD", "DELAYED_VALUE");

        // build the query string before
        List<JexlNode> andChildren = new ArrayList<>();
        andChildren.add(left.getNode());
        andChildren.add(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));
        andChildren.add(delayed);

        JexlNode origQueryTree = JexlNodeFactory.createAndNode(andChildren);

        IndexInfo merged = right.intersect(left, Arrays.asList(delayed), right);

        // intersection of uid and infinite shard range is a shard range
        assertTrue(merged.uids().isEmpty());
        assertTrue(TreeEqualityVisitor.isEqual(JexlNodeFactory.createScript(origQueryTree), JexlNodeFactory.createScript(merged.getNode())));
    }

    /**
     * Intersection of query terms when neither term has document ids. The intersection of {50,90} is {50}
     */
    @Test
    public void testIntersection_NoTermHasDocIds() {
        IndexInfo left = new IndexInfo(50L);
        left.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        IndexInfo right = new IndexInfo(90L);
        right.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        IndexInfo merged = left.intersect(right);
        assertEquals(50L, merged.count());
        assertTrue(merged.uids().isEmpty());
    }

    /**
     * Intersection of query terms when one term is infinite. The intersection of {inf, 3} is {3}
     */
    @Test
    public void testIntersection_InfiniteTerm() {
        IndexInfo left = new IndexInfo(-1L);
        left.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo right = new IndexInfo(rightMatches);
        right.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        IndexInfo expectedMerged = new IndexInfo(-1);
        expectedMerged.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        assertEquals(expectedMerged, left.intersect(right));
        assertEquals(expectedMerged, right.intersect(left));
    }

    /**
     * Union of query terms when both terms have document ids.
     */
    @Test
    public void testUnion_BothTermsHaveDocIds() {
        List<IndexMatch> leftMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo left = new IndexInfo(leftMatches);

        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc2", "doc3", "doc4");
        IndexInfo right = new IndexInfo(rightMatches);

        IndexInfo merged = left.union(right);

        // The union of left and right should be a set of four document ids
        Set<IndexMatch> expectedDocs = buildExpectedIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3", "doc4");

        assertEquals(4, merged.uids().size());
        assertEquals(expectedDocs, merged.uids());
    }

    /**
     * Union of query terms when one term is a delayed predicate
     */
    @Test
    public void testUnion_OneTermIsDelayedPredicate() {
        JexlNode delayedPredicate = QueryPropertyMarker.create(JexlNodeFactory.buildEQNode("FIELD", "VALUE"), DELAYED);

        IndexInfo left = new IndexInfo(50);
        left.applyNode(delayedPredicate);

        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo right = new IndexInfo(rightMatches);
        right.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        IndexInfo merged = right.union(left);
        assertEquals(0, merged.uids().size());
        assertEquals("Count should be 53 but is " + merged.count(), 53, merged.count());
        String expectedQuery = "((_Delayed_ = true) && (FIELD == 'VALUE'))";
        String actualQuery = JexlStringBuildingVisitor.buildQuery(merged.getNode());
        assertEquals(expectedQuery, actualQuery);
    }

    /**
     * Union of query terms when neither term has document ids. The union of {50, 90} is {140}.
     */
    @Test
    public void testUnion_NoTermHasDocIds() {
        IndexInfo left = new IndexInfo(50L);
        left.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        IndexInfo right = new IndexInfo(90L);
        right.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        IndexInfo merged = left.union(right);
        assertEquals(140L, merged.count());
        assertTrue(merged.uids().isEmpty());
    }

    /**
     * Union of query terms when one term is infinite. The union of {inf, 3} is {inf}
     */
    @Test
    public void testUnion_InfiniteTerm() {
        IndexInfo left = new IndexInfo(-1L);
        left.applyNode(JexlNodeFactory.buildEQNode("FIELD", "VALUE"));

        List<IndexMatch> rightMatches = buildIndexMatches("FIELD", "VALUE", "doc1", "doc2", "doc3");
        IndexInfo right = new IndexInfo(rightMatches);

        IndexInfo expectedMerged = new IndexInfo(-1L);
        assertEquals(expectedMerged, left.union(right));
        assertEquals(expectedMerged, right.union(left));
    }

    @Test
    public void testIntersectCaseEF() throws ParseException {
        List<IndexMatch> leftMatches = buildIndexMatches("F", "v", "uid5", "uid6");
        IndexInfo left = new IndexInfo(leftMatches);
        left.applyNode(JexlNodeFactory.buildEQNode("F", "v"));

        IndexInfo right = new IndexInfo(345);
        right.applyNode(JexlNodeFactory.buildEQNode("F2", "v2"));

        JexlNode expected = JexlASTHelper.parseJexlQuery("F == 'v' && F2 == 'v2'").jjtGetChild(0);

        IndexInfo merged = left.intersect(right);
        assertTrue(merged.uids().isEmpty());
        assertTrue(TreeEqualityVisitor.isEqual(expected, merged.getNode()));

        // and the inverse
        merged = right.intersect(left);
        assertTrue(merged.uids().isEmpty());
        assertTrue(TreeEqualityVisitor.isEqual(expected, merged.getNode()));
    }

    @Test
    public void testFieldCountSerialization() throws IOException {

        CountMap counts = new CountMap();
        counts.put("FIELD_A", 23L);
        counts.put("FIELD_B", 2077L);

        IndexInfo indexInfo = new IndexInfo();
        indexInfo.setFieldCounts(counts);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        indexInfo.write(out);
        out.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);

        IndexInfo other = new IndexInfo();
        other.readFields(in);
        bais.close();

        assertEquals(counts, other.getFieldCounts());
    }

    @Test
    public void testMergeFieldCounts() {
        CountMap firstCounts = new CountMap();
        firstCounts.put("FOO", 17L);

        CountMap secondCounts = new CountMap();
        secondCounts.put("FOO", 23L);

        IndexInfo first = new IndexInfo();
        IndexInfo second = new IndexInfo();

        first.setFieldCounts(firstCounts);
        second.setFieldCounts(secondCounts);

        IndexInfo merged = first.union(second);
        assertEquals(40L, merged.getFieldCounts().get("FOO").longValue());
    }
}
