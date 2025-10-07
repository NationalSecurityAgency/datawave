package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.SyntaxParser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;

public class InvalidSlopProximityVisitorTest {

    private static final SyntaxParser parser = new AccumuloSyntaxParser();

    private QueryNode query;

    private final List<InvalidSlopProximityVisitor.InvalidSlop> expected = new ArrayList<>() {};

    @After
    public void tearDown() throws Exception {
        query = null;
        clearExpected();
    }

    /**
     * Test a query without a slop value.
     */
    @Test
    public void testSlopAbsent() throws QueryNodeParseException {
        givenQuery(parse("FIELD:abc"));
        assertResults();
    }

    /**
     * Test a query with a slop operator but no numeric value.
     */
    @Test
    public void testSlopNumberAbsent() throws QueryNodeParseException {
        givenQuery(parse("FIELD:\"term1 term2 term3\"~"));
        assertResults();
    }

    /**
     * Test a proximity query with a single term and a slop value less than the minimum allowed.
     */
    @Test
    public void testSingleTermProximityLessThan() throws Exception {
        QueryNode node = parse("FIELD:\"term1\"~0");
        givenQuery(node);
        expect(new InvalidSlopProximityVisitor.InvalidSlop((SlopQueryNode) node, 1));
        assertResults();
    }

    /**
     * Test a proximity query with multiple terms and a slop value less than the minimum allowed.
     */
    @Test
    public void testMultipleTermProximityLessThan() throws Exception {
        QueryNode node = parse("FIELD:\"term1 term2\"~1");
        givenQuery(node);
        expect(new InvalidSlopProximityVisitor.InvalidSlop((SlopQueryNode) node, 2));
        assertResults();
    }

    /**
     * Test a proximity query with a single term and a slop value equal to the minimum allowed.
     */
    @Test
    public void testSingleTermProximityEqualTo() throws Exception {
        givenQuery(parse("FIELD:\"term1\"~1"));
        assertResults();
    }

    /**
     * Test a proximity query with multiple terms and a slop value equal to the minimum allowed.
     */
    @Test
    public void testMultipleTermProximityEqualTo() throws Exception {
        givenQuery(parse("FIELD:\"term1 term2\"~2"));
        assertResults();
    }

    /**
     * Test a proximity query with a single term and a slop value greater than the minimum allowed.
     */
    @Test
    public void testSingleTermProximityGreaterThan() throws Exception {
        givenQuery(parse("FIELD:\"term1\"~2"));
        assertResults();
    }

    /**
     * Test a proximity query with multiple terms and a slop value greater than the minimum allowed.
     */
    @Test
    public void testMultipleTermProximityGreaterThan() throws Exception {
        givenQuery(parse("FIELD:\"term1 term2\"~3"));
        assertResults();
    }

    /**
     * Test a proximity query with padded white space on the left.
     */
    @Test
    public void testValidWithPaddedWhiteSpaceLeft() throws Exception {
        givenQuery(parse("FIELD:\"                  term1 term2 term3\"~3"));
        assertResults();
    }

    /**
     * Test a proximity query with padded white space on the right.
     */
    @Test
    public void testValidWithPaddedWhiteSpaceRight() throws Exception {
        givenQuery(parse("FIELD:\"term1 term2 term3                  \"~3"));
        assertResults();
    }

    /**
     * Test a proximity query with padded white space between terms.
     */
    @Test
    public void testValidWithPaddedWhiteSpaceBetween() throws Exception {
        givenQuery(parse("FIELD:\"term1             term2       term3\"~3"));
        assertResults();
    }

    /**
     * Test a proximity query with padded white space on both left and right sides.
     */
    @Test
    public void testValidWithPaddedWhiteSpaceLeftRight() throws Exception {
        givenQuery(parse("FIELD:\"            term1 term2 term3      \"~3"));
        assertResults();
    }

    /**
     * Test an invalid proximity query with padded white space on the left and insufficient slop value.
     */
    @Test
    public void testInvalidWithPaddedWhiteSpaceLeft() throws Exception {
        QueryNode node = parse("FIELD:\"                  term1 term2 term3\"~2");
        givenQuery(node);
        expect(new InvalidSlopProximityVisitor.InvalidSlop((SlopQueryNode) node, 3));
        assertResults();
    }

    /**
     * Test an invalid proximity query with padded white space on the right and insufficient slop value.
     */
    @Test
    public void testInvalidWithPaddedWhiteSpaceRight() throws Exception {
        QueryNode node = parse("FIELD:\"term1 term2 term3                  \"~2");
        givenQuery(node);
        expect(new InvalidSlopProximityVisitor.InvalidSlop((SlopQueryNode) node, 3));
        assertResults();
    }

    /**
     * Test an invalid proximity query with padded white space between terms and insufficient slop value.
     */
    @Test
    public void testInvalidWithPaddedWhiteSpaceBetween() throws Exception {
        QueryNode node = parse("FIELD:\"term1             term2       term3\"~2");
        givenQuery(node);
        expect(new InvalidSlopProximityVisitor.InvalidSlop((SlopQueryNode) node, 3));
        assertResults();
    }

    /**
     * Test an invalid proximity query with padded white space on both sides and insufficient slop value.
     */
    @Test
    public void testInvalidWithPaddedWhiteSpaceLeftRight() throws Exception {
        QueryNode node = parse("FIELD:\"            term1 term2 term3      \"~2");
        givenQuery(node);
        expect(new InvalidSlopProximityVisitor.InvalidSlop((SlopQueryNode) node, 3));
        assertResults();
    }

    private QueryNode parse(String query) throws QueryNodeParseException {
        return parser.parse(query, "");
    }

    private void givenQuery(QueryNode query) {
        this.query = query;
    }

    private void expect(InvalidSlopProximityVisitor.InvalidSlop invalidSlop) {
        this.expected.add(invalidSlop);
    }

    private void clearExpected() {
        this.expected.clear();
    }

    private void assertResults() {
        List<InvalidSlopProximityVisitor.InvalidSlop> actual = InvalidSlopProximityVisitor.check(query);
        List<String> actualStrings = actual.stream().map(Object::toString).collect(Collectors.toList());
        List<String> expectedStrings = expected.stream().map(Object::toString).collect(Collectors.toList());
        Assert.assertEquals(expectedStrings, actualStrings);
        clearExpected();
    }
}
