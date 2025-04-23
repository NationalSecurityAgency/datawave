package datawave.query.lucene.visitors;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.AnyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.BoostQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.DeletedQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchAllDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchNoDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.NoTokenFoundQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.NotBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OpaqueQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.PathQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.PhraseSlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ProximityQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.queryparser.flexible.standard.nodes.BooleanModifierNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.MultiPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PrefixWildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.SynonymQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import datawave.query.language.parser.lucene.EscapeQuerySyntaxImpl;

public class LuceneQueryStringBuildingVisitorTest {

    private static final EscapeQuerySyntax escapeQuerySyntax = new EscapeQuerySyntaxImpl();
    private static final SyntaxParser parser = new AccumuloSyntaxParser();

    private QueryNode queryNode;
    private String expectedQuery;

    @AfterEach
    void tearDown() {
        queryNode = null;
        expectedQuery = null;
    }

    /**
     * Test a {@link FieldQueryNode} with a non-empty field.
     */
    @Test
    void testFieldQueryNodeWithField() throws QueryNodeParseException {
        FieldQueryNode node = new FieldQueryNode("FIELD", "abc", 0, 3);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link FieldQueryNode} with an empty field.
     */
    @Test
    void testFieldQueryNodeWithoutField() throws QueryNodeParseException {
        FieldQueryNode node = new FieldQueryNode("", "abc", 0, 3);
        givenQueryNode(node);

        expect("abc");

        assertResult();
    }

    /**
     * Test a {@link FieldQueryNode} with a non-empty field.
     */
    @Test
    void testQuotedFieldQueryNodeWithField() throws QueryNodeParseException {
        FieldQueryNode node = new QuotedFieldQueryNode("FIELD", "abc", 0, 3);
        givenQueryNode(node);

        expect("FIELD:\"abc\"");

        assertResult();
    }

    /**
     * Test a {@link FieldQueryNode} with an empty field.
     */
    @Test
    void testQuotedFieldQueryNodeWithoutField() throws QueryNodeParseException {
        FieldQueryNode node = new QuotedFieldQueryNode("", "abc", 0, 3);
        givenQueryNode(node);

        expect("\"abc\"");

        assertResult();
    }

    /**
     * Test a {@link AndQueryNode} with no parent.
     */
    @Test
    void testAndQueryNodeWithoutParent() throws QueryNodeParseException {
        ArrayList<QueryNode> clauses = new ArrayList<>();
        clauses.add(new FieldQueryNode("FOO", "abc", 0, 3));
        clauses.add(new FieldQueryNode("BAR", "def", 4, 7));
        AndQueryNode node = new AndQueryNode(clauses);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link AndQueryNode} with a group parent.
     */
    @Test
    void testAndQueryNodeWithGroupParent() throws QueryNodeParseException {
        ArrayList<QueryNode> clauses = new ArrayList<>();
        clauses.add(new FieldQueryNode("FOO", "abc", 0, 3));
        clauses.add(new FieldQueryNode("BAR", "def", 4, 7));
        AndQueryNode node = new AndQueryNode(clauses);
        // Ensure the node has a group parent.
        new GroupQueryNode(node);

        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link AndQueryNode} with a parent that is not a {@link GroupQueryNode}.
     */
    @Test
    void testAndQueryNodeWithNonGroupParent() throws QueryNodeParseException {
        ArrayList<QueryNode> clauses = new ArrayList<>();
        clauses.add(new FieldQueryNode("FOO", "abc", 0, 3));
        clauses.add(new FieldQueryNode("BAR", "def", 4, 7));
        AndQueryNode node = new AndQueryNode(clauses);
        // Ensure the node has an OR parent.
        new OrQueryNode(List.of(node));

        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link OrQueryNode} with no parent.
     */
    @Test
    void testOrQueryNodeWithoutParent() throws QueryNodeParseException {
        ArrayList<QueryNode> clauses = new ArrayList<>();
        clauses.add(new FieldQueryNode("FOO", "abc", 0, 3));
        clauses.add(new FieldQueryNode("BAR", "def", 4, 7));
        OrQueryNode node = new OrQueryNode(clauses);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link OrQueryNode} with a group parent.
     */
    @Test
    void testOrQueryNodeWithGroupParent() throws QueryNodeParseException {
        ArrayList<QueryNode> clauses = new ArrayList<>();
        clauses.add(new FieldQueryNode("FOO", "abc", 0, 3));
        clauses.add(new FieldQueryNode("BAR", "def", 4, 7));
        OrQueryNode node = new OrQueryNode(clauses);
        // Ensure the node has a group node parent.
        new GroupQueryNode(node);

        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link OrQueryNode} with a parent that is not a {@link GroupQueryNode}.
     */
    @Test
    void testOrQueryNodeWithNonGroupParent() throws QueryNodeParseException {
        ArrayList<QueryNode> clauses = new ArrayList<>();
        clauses.add(new FieldQueryNode("FOO", "abc", 0, 3));
        clauses.add(new FieldQueryNode("BAR", "def", 4, 7));
        OrQueryNode node = new OrQueryNode(clauses);
        // Ensure the node has an AND parent.
        new AndQueryNode(List.of(node));

        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link GroupQueryNode}.
     */
    @Test
    void testGroupQueryNode() throws QueryNodeParseException {
        GroupQueryNode node = new GroupQueryNode(parse("FIELD1:abc"));
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link BoostQueryNode}.
     */
    @Test
    void testBoostQueryNode() throws QueryNodeParseException {
        BoostQueryNode node = new BoostQueryNode(new FieldQueryNode("FOO", "abc", 0, 3), 2F);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link DeletedQueryNode}.
     */
    @Test
    void testDeletedQueryNode() throws QueryNodeParseException {
        DeletedQueryNode node = new DeletedQueryNode();
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link DeletedQueryNode}.
     */
    @Test
    void testFuzzyQueryNodeWithField() throws QueryNodeParseException {
        FuzzyQueryNode node = new FuzzyQueryNode("FOO", "abc", 2, 0, 3);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link DeletedQueryNode}.
     */
    @Test
    void testFuzzyQueryNodeWithoutField() throws QueryNodeParseException {
        FuzzyQueryNode node = new FuzzyQueryNode("", "abc", 2, 0, 3);
        givenQueryNode(node);

        expect("abc~2.0");

        assertResult();
    }

    /**
     * Test a {@link MatchAllDocsQueryNode}.
     */
    @Test
    void testMatchAllDocsQueryNode() throws QueryNodeParseException {
        MatchAllDocsQueryNode node = new MatchAllDocsQueryNode();
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link MatchNoDocsQueryNode}.
     */
    @Test
    void testMatchNoDocsQueryNode() throws QueryNodeParseException {
        MatchNoDocsQueryNode node = new MatchNoDocsQueryNode();
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link NoTokenFoundQueryNode}.
     */
    @Test
    void testNoTokenFoundQueryNode() throws QueryNodeParseException {
        NoTokenFoundQueryNode node = new NoTokenFoundQueryNode();
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test an {@link OpaqueQueryNode}.
     */
    @Test
    void testOpaqueQueryNode() throws QueryNodeParseException {
        OpaqueQueryNode node = new OpaqueQueryNode("wiki", "abc");
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test an {@link PathQueryNode}.
     */
    @Test
    void testPathQueryNode() throws QueryNodeParseException {
        List<PathQueryNode.QueryText> elements = new ArrayList<>();
        elements.add(new PathQueryNode.QueryText("etc", 0, 3));
        elements.add(new PathQueryNode.QueryText("udev", 0, 3));
        elements.add(new PathQueryNode.QueryText("dev.conf", 0, 3));
        PathQueryNode node = new PathQueryNode(elements);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test an {@link PhraseSlopQueryNode}.
     */
    @Test
    void testPhraseSlopQueryNode() throws QueryNodeParseException {
        PhraseSlopQueryNode node = new PhraseSlopQueryNode(new FieldQueryNode("FOO", "abc", 0, 3), 2);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test an {@link SlopQueryNode}.
     */
    @Test
    void testSlopQueryNode() throws QueryNodeParseException {
        SlopQueryNode node = new SlopQueryNode(new FieldQueryNode("FOO", "abc", 0, 3), 2);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test an {@link TokenizedPhraseQueryNode}.
     */
    @Test
    void testTokenizedPhraseQueryNode() throws QueryNodeParseException {
        TokenizedPhraseQueryNode node = new TokenizedPhraseQueryNode();
        node.add(new FieldQueryNode("FOO", "abc", 0, 3));
        node.add(new FieldQueryNode("BAR", "def", 0, 3));
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test an {@link MultiPhraseQueryNode}.
     */
    @Test
    void testMultiPhraseQueryNode() throws QueryNodeParseException {
        MultiPhraseQueryNode node = new MultiPhraseQueryNode();
        node.add(new FieldQueryNode("FOO", "abc", 0, 3));
        node.add(new FieldQueryNode("BAR", "def", 0, 3));
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test an {@link PointQueryNode} with a field.
     */
    @Test
    void testPointQueryNodeWithField() throws QueryNodeParseException {
        PointQueryNode node = new PointQueryNode("FOO", 23, NumberFormat.getIntegerInstance());
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test an {@link PointQueryNode} with a blank field.
     */
    @Test
    void testPointQueryNodeWithoutField() throws QueryNodeParseException {
        String value = NumberFormat.getCurrencyInstance(Locale.US).format(23);
        PointQueryNode node = new PointQueryNode("", 23, NumberFormat.getCurrencyInstance(Locale.US));
        givenQueryNode(node);

        expect(value);

        assertResult();
    }

    /**
     * Test a {@link PointRangeQueryNode} where both bounds are inclusive.
     */
    @Test
    void testPointRangeQueryNodeBothBoundsInclusive() throws QueryNodeException {
        PointsConfig config = new PointsConfig(NumberFormat.getIntegerInstance(), Integer.class);

        PointQueryNode lowerBound = new PointQueryNode("FOO", 1, NumberFormat.getIntegerInstance());
        PointQueryNode upperBound = new PointQueryNode("FOO", 5, NumberFormat.getIntegerInstance());

        PointRangeQueryNode rangeNode = new PointRangeQueryNode(lowerBound, upperBound, true, true, config);
        givenQueryNode(rangeNode);

        expectExactMatchToQueryString(rangeNode);

        assertResult();
    }

    /**
     * Test a {@link PointRangeQueryNode} where both bounds are exclusive.
     */
    @Test
    void testPointRangeQueryNodeBothBoundExclusive() throws QueryNodeException {
        PointsConfig config = new PointsConfig(NumberFormat.getIntegerInstance(), Integer.class);

        PointQueryNode lowerBound = new PointQueryNode("FOO", 1, NumberFormat.getIntegerInstance());
        PointQueryNode upperBound = new PointQueryNode("FOO", 5, NumberFormat.getIntegerInstance());

        PointRangeQueryNode rangeNode = new PointRangeQueryNode(lowerBound, upperBound, false, false, config);
        givenQueryNode(rangeNode);

        expectExactMatchToQueryString(rangeNode);

        assertResult();
    }

    /**
     * Test a {@link PointRangeQueryNode} where one bound is exclusive.
     */
    @Test
    void testPointRangeQueryNodeOneBoundExclusive() throws QueryNodeException {
        PointsConfig config = new PointsConfig(NumberFormat.getIntegerInstance(), Integer.class);

        PointQueryNode lowerBound = new PointQueryNode("FOO", 1, NumberFormat.getIntegerInstance());
        PointQueryNode upperBound = new PointQueryNode("FOO", 5, NumberFormat.getIntegerInstance());

        PointRangeQueryNode rangeNode = new PointRangeQueryNode(lowerBound, upperBound, false, true, config);
        givenQueryNode(rangeNode);

        expectExactMatchToQueryString(rangeNode);

        assertResult();
    }

    /**
     * Test a {@link PrefixWildcardQueryNode} with a non-blank field.
     */
    @Test
    void testPrefixWildcardNodeWithField() throws QueryNodeParseException {
        PrefixWildcardQueryNode node = new PrefixWildcardQueryNode("FOO", "ab*", 0, 3);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link PrefixWildcardQueryNode} with a blank field.
     */
    @Test
    void testPrefixWildcardNodeWithoutField() throws QueryNodeParseException {
        PrefixWildcardQueryNode node = new PrefixWildcardQueryNode("", "ab*", 0, 3);
        givenQueryNode(node);

        expect("ab*");

        assertResult();
    }

    /**
     * Test a {@link WildcardQueryNode} with a non-blank field.
     */
    @Test
    void testWildcardQueryNodeWithField() throws QueryNodeParseException {
        WildcardQueryNode node = new WildcardQueryNode("FOO", "ab*", 0, 3);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link WildcardQueryNode} with a blank field.
     */
    @Test
    void testWildcardQueryNodeWithoutField() throws QueryNodeParseException {
        WildcardQueryNode node = new WildcardQueryNode("", "ab*", 0, 3);
        givenQueryNode(node);

        expect("ab*");

        assertResult();
    }

    /**
     * Test a {@link RegexpQueryNode} with a non-blank field.
     */
    @Test
    void testRegexpQueryNodeWithField() throws QueryNodeParseException {
        RegexpQueryNode node = new RegexpQueryNode("FOO", "ab*", 0, 3);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link RegexpQueryNode} with a blank field.
     */
    @Test
    void testRegexpQueryNodeWithoutField() throws QueryNodeParseException {
        RegexpQueryNode node = new RegexpQueryNode("", "ab*", 0, 3);
        givenQueryNode(node);

        expect("/ab*/");

        assertResult();
    }

    /**
     * Test a {@link FunctionQueryNode}.
     */
    @Test
    void testFunctionQueryNode() throws QueryNodeParseException {
        FunctionQueryNode node = (FunctionQueryNode) parse("#INCLUDE(FIELD, reg\\,ex)");
        givenQueryNode(node);

        expect("#INCLUDE(FIELD, reg,ex)");

        assertResult();
    }

    /**
     * Test a {@link NotBooleanQueryNode} that has a single clause.
     */
    @Test
    void testNotBooleanQueryNode() throws QueryNodeParseException {
        QueryNode node = parse("FOO:abc NOT BAR:def");
        givenQueryNode(node);

        expect("FOO:abc NOT BAR:def");

        assertResult();
    }

    /**
     * Test a {@link NotBooleanQueryNode} that has multiple clauses.
     */
    @Test
    void testNotBooleanQueryNodeWithMultipleClauses() throws QueryNodeParseException {
        QueryNode node = parse("FOO:abc BAR:abc NOT HAT:bbb HEY:whomai");
        givenQueryNode(node);

        expectExactMatchToQueryString(queryNode);

        assertResult();
    }

    /**
     * Test an {@link AnyQueryNode} with a field.
     */
    @Test
    void testAnyQueryNodeWithField() throws QueryNodeParseException {
        List<QueryNode> clauses = new ArrayList<>();
        clauses.add(new FieldQueryNode("FOO", "abc", 0, 3));
        clauses.add(new FieldQueryNode("FOO", "def", 0, 3));
        clauses.add(new FieldQueryNode("FOO", "efg", 0, 3));
        AnyQueryNode node = new AnyQueryNode(clauses, "FOO", 2);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test an {@link AnyQueryNode} without a field.
     */
    @Test
    void testAnyQueryNodeWithoutField() throws QueryNodeParseException {
        List<QueryNode> clauses = new ArrayList<>();
        clauses.add(new FieldQueryNode("FOO", "abc", 0, 3));
        clauses.add(new FieldQueryNode("FOO", "def", 0, 3));
        clauses.add(new FieldQueryNode("FOO", "efg", 0, 3));
        AnyQueryNode node = new AnyQueryNode(clauses, "", 2);
        givenQueryNode(node);

        expect("( abc def efg ) ANY 2");

        assertResult();
    }

    /**
     * Test a {@link ProximityQueryNode} with a field.
     */
    @Test
    void testProximityQueryNodeWithField() throws QueryNodeParseException {
        List<QueryNode> clauses = new ArrayList<>();
        clauses.add(new FieldQueryNode("FOO", "1", 0, 3));
        clauses.add(new FieldQueryNode("FOO", "2", 0, 3));
        clauses.add(new FieldQueryNode("FOO", "3", 0, 3));
        ProximityQueryNode node = new ProximityQueryNode(clauses, "FOO", ProximityQueryNode.Type.NUMBER, 2, true);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link ProximityQueryNode} without a field.
     */
    @Test
    void testProximityQueryNodeWithoutField() throws QueryNodeParseException {
        List<QueryNode> clauses = new ArrayList<>();
        clauses.add(new FieldQueryNode("FOO", "1", 0, 3));
        clauses.add(new FieldQueryNode("FOO", "2", 0, 3));
        clauses.add(new FieldQueryNode("FOO", "3", 0, 3));
        ProximityQueryNode node = new ProximityQueryNode(clauses, "", ProximityQueryNode.Type.NUMBER, 2, true);
        givenQueryNode(node);

        expect("( 1 2 3 ) WITHIN 2 INORDER");

        assertResult();
    }

    /**
     * Test a {@link TermRangeQueryNode} where both bounds are inclusive.
     */
    @Test
    void testTermRangeQueryNodeBothBoundsInclusive() throws QueryNodeException {
        FieldQueryNode lowerBound = new FieldQueryNode("FOO", "aaa", 0, 3);
        FieldQueryNode upperBound = new FieldQueryNode("FOO", "zzz", 0, 3);

        TermRangeQueryNode rangeNode = new TermRangeQueryNode(lowerBound, upperBound, true, true);
        givenQueryNode(rangeNode);

        expectExactMatchToQueryString(rangeNode);

        assertResult();
    }

    /**
     * Test a {@link TermRangeQueryNode} where both bounds are exclusive.
     */
    @Test
    void testTermRangeQueryNodeBothBoundExclusive() throws QueryNodeException {
        FieldQueryNode lowerBound = new FieldQueryNode("FOO", "aaa", 0, 3);
        FieldQueryNode upperBound = new FieldQueryNode("FOO", "zzz", 0, 3);

        TermRangeQueryNode rangeNode = new TermRangeQueryNode(lowerBound, upperBound, false, false);
        givenQueryNode(rangeNode);

        expectExactMatchToQueryString(rangeNode);

        assertResult();
    }

    /**
     * Test a {@link TermRangeQueryNode} where one bound is exclusive.
     */
    @Test
    void testTermRangeQueryNodeOneBoundExclusive() throws QueryNodeException {
        FieldQueryNode lowerBound = new FieldQueryNode("FOO", "aaa", 0, 3);
        FieldQueryNode upperBound = new FieldQueryNode("FOO", "zzz", 0, 3);

        TermRangeQueryNode rangeNode = new TermRangeQueryNode(lowerBound, upperBound, false, true);
        givenQueryNode(rangeNode);

        expectExactMatchToQueryString(rangeNode);

        assertResult();
    }

    /**
     * Test a {@link SynonymQueryNode}.
     */
    @Test
    void testSynonymQueryNode() throws QueryNodeParseException {
        List<QueryNode> clauses = new ArrayList<>();
        clauses.add(new FieldQueryNode("FOO", "1", 0, 3));
        clauses.add(new FieldQueryNode("FOO", "2", 0, 3));
        clauses.add(new FieldQueryNode("FOO", "3", 0, 3));
        SynonymQueryNode node = new SynonymQueryNode(clauses);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    /**
     * Test a {@link BooleanModifierNode}.
     */
    @Test
    void testBooleanModifierNode() throws QueryNodeParseException {
        BooleanModifierNode node = new BooleanModifierNode(new FieldQueryNode("FOO", "abc", 0, 3), ModifierQueryNode.Modifier.MOD_REQ);
        givenQueryNode(node);

        expectExactMatchToQueryString(node);

        assertResult();
    }

    private void givenQueryNode(QueryNode node) {
        this.queryNode = node;
    }

    private void expect(String query) {
        this.expectedQuery = query;
    }

    private void expectExactMatchToQueryString(QueryNode queryNode) throws QueryNodeParseException {
        expect(queryNode.toQueryString(escapeQuerySyntax).toString());
    }

    private void assertResult() throws QueryNodeParseException {
        String actual = LuceneQueryStringBuildingVisitor.build(queryNode);
        Assert.assertEquals(expectedQuery, actual);
    }

    private QueryNode parse(String query) throws QueryNodeParseException {
        return parser.parse(query, "");
    }
}
