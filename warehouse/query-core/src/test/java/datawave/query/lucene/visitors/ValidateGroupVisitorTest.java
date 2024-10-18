package datawave.query.lucene.visitors;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValidateGroupVisitorTest {
    
    private boolean logQueryTrees = true;

    @Test
    public void testSingleTermQueries() throws Exception {
        assertValid("FIELD1:abc");                        // Simple term
        assertValid("FIELD1:ab*");                        // Wildcard term
        assertValid("FIELD1:a?c");                        // Single character wildcard
        assertValid("FIELD1:*lector");                    // Leading wildcard (as per syntax)
        assertValid("abc*");                              // Wildcard without field
        assertValid("\"quick brown dog\"");               // Phrase query
        assertValid("\"quick brown dog\"~20");            // Proximity search
        assertValid("#FUNCTION(ARG1, ARG2)");             // Function query
    }

    @Test
    public void testRangeQueries() throws Exception {
        assertValid("FIELD1:[begin TO end]");             // Inclusive range
        assertValid("FIELD1:{begin TO end}");             // Exclusive range
    }

    @Test
    public void testImplicitAndOperator() throws Exception {
        assertValid("FIELD1:abc FIELD2:def");             // Implicit AND between terms
        assertValid("FIELD1:abc FIELD2:def FIELD3:ghi");  // Multiple terms
    }

    @Test
    public void testExplicitAndOrOperators() throws Exception {
        assertValid("FIELD1:abc AND FIELD2:def");         // Explicit AND
        assertValid("FIELD1:abc AND FIELD2:def AND FIELD3:ghi");
        assertValid("FIELD1:abc OR FIELD2:def");          // Simple OR
        assertValid("FIELD1:abc OR FIELD2:def OR FIELD3:ghi");
        assertValid("FIELD1:abc AND FIELD2:def OR FIELD3:ghi");  // Combining AND and OR
    }

    @Test
    public void testGroupedTerms() throws Exception {
        assertValid("FIELD1:(abc def)");            // Multiple terms in field value
        assertValid("FIELD1:(abc AND def)");        // Multiple terms in field value
        assertValid("FIELD1:(abc OR def)");         // Multiple terms in field value
        assertInvalid("FIELD1:abc def");            // Ambiguous without grouping
        assertInvalid("(FIELD1:abc def)");          // Ambiguous without grouping
        assertInvalid("FIELD1:abc AND def");        // Ambiguous without grouping
        assertInvalid("FIELD1:abc OR def");         // Ambiguous without grouping
    }

    @Test
    public void testNotOperatorBetweenTerms() throws Exception {
        assertValid("FIELD1:abc NOT FIELD2:def");                        // NOT between two terms
        assertInvalid("FIELD1:abc NOT FIELD2:def NOT FIELD3:ghi");       // Multiple NOTs between terms
        assertInvalid("FIELD1:abc NOT FIELD2:def FIELD3:ghi");           // Ambiguous NOT placement
    }

    @Test
    public void testParenthesesUsageWithNot() throws Exception {
        assertValid("(FIELD1:abc NOT FIELD2:def) AND FIELD3:ghi");       // NOT within parentheses
        assertValid("FIELD1:abc AND (FIELD2:def NOT FIELD3:ghi)");       // NOT within parentheses
        assertInvalid("(FIELD1:abc NOT FIELD2:def FIELD3:ghi)");         // Ambiguous within parentheses
    }

    @Test
    public void testComplexExpressions() throws Exception {
        assertValid("FIELD1:abc NOT (FIELD2:def AND FIELD3:ghi)");       // NOT before group
        assertValid("(FIELD1:abc AND FIELD2:def) NOT (FIELD3:ghi OR FIELD4:jkl)"); // NOT between groups
    }

    @Test
    public void testNestedParenthesesWithNot() throws Exception {
        assertValid("((FIELD1:abc NOT FIELD2:def))");                    // Nested parentheses with NOT
        assertValid("(FIELD1:abc NOT (FIELD2:def AND FIELD3:ghi))");     // NOT with nested group
    }

    @Test
    public void testFunctionsAndPhrasesInComplexExpressions() throws Exception {
        assertValid("#FUNCTION(ARG1, ARG2) NOT (\"quick brown dog\"~20 AND FIELD1:abc)");
        assertInvalid("#FUNCTION(ARG1, ARG2) NOT \"quick brown dog\"~20 FIELD1:abc"); // Ambiguous without parentheses
    }

    @Test
    public void testWildcardsAndRangeQueriesWithNot() throws Exception {
        assertValid("FIELD1:selec* NOT FIELD2:selec?or");                // Wildcards with NOT
        assertValid("FIELD1:[begin TO end] NOT FIELD2:{begin TO end}");  // Range queries with NOT
        assertInvalid("FIELD1:[begin TO end] NOT FIELD2:{begin TO end} FIELD3:ghi"); // Ambiguous without operator
    }

    @Test
    public void testEscapingSpecialCharacters() throws Exception {
        assertValid("FIELD1:foo\\-bar NOT FIELD2:abc");                  // Escaped hyphen with NOT
        assertValid("FIELD1:foo\\+bar AND FIELD2:abc");                  // Escaped plus with AND
        assertValid("FIELD1:foo\\@bar OR FIELD2:abc");                   // Escaped at symbol with OR
    }

    @Test
    public void testQueriesWithProximitySearchesAndFunctionsCombined() throws Exception {
        assertValid("\"quick brown dog\"~5 NOT (\"lazy fox\"~10 AND FIELD1:abc)");
        assertValid("#FUNCTION(ARG1) AND (#FUNCTION(ARG1, ARG2) NOT FIELD2:def)");
    }

    @Test
    public void testQueriesWithSpecialTermsAndModifiersAndNot() throws Exception {
        assertValid("FIELD1:\"complex term\" NOT FIELD2:[begin TO end]");
        assertInvalid("FIELD1:{* TO end} NOT FIELD2:selec* NOT FIELD3:selec?or"); // Ambiguous without parentheses
        assertValid("FIELD1:{* TO end} NOT (FIELD2:selec* OR FIELD3:selec?or)");  // Parentheses resolve ambiguity
    }

    @Test
    public void testTestingPrecedenceAndGrouping() throws Exception {
        assertValid("FIELD1:abc AND ((FIELD2:def OR FIELD3:ghi) NOT FIELD4:jkl)");
        assertInvalid("FIELD1:abc AND FIELD2:def OR FIELD3:ghi NOT FIELD4:jkl FIELD5:mno"); // Ambiguous without parentheses
    }

    /**
     * Generates a LUCENE {@link QueryNode} tree from the given query string.
     * @param query The query string to parse.
     * @return The LUCENE {@link QueryNode} tree generated from the query string.
     * @throws QueryNodeParseException If the query cannot be parsed.
     */
    private QueryNode parseQuery(String query) throws QueryNodeParseException {
        AccumuloSyntaxParser parser = new AccumuloSyntaxParser();
        return parser.parse(query, "");
    }

    /**
     * Asserts that the given query is valid.
     * Useful when expecting no exception to be thrown.
     * @param query The query to validate.
     * @throws Exception If an error occurs while validating the query.
     */
    private void assertValid(String query) throws Exception {
        printQueryStructure(query);
        ValidateGroupVisitor.validate(parseQuery(query));
    }

    /**
     * Asserts that the given query is invalid.
     * Useful when expecting an exception to be thrown.
     * @param query The query to validate.
     * @throws Exception If an error occurs while validating the query.
     */
    private void assertInvalid(String query) throws Exception {
        printQueryStructure(query);
        assertThrows(IllegalArgumentException.class,
                () -> ValidateGroupVisitor.validate(parseQuery(query)),
                "Query did not throw an exception: " + query);
    }

    /**
     * Prints the structure of the query tree to the console if logQueryTrees is true.
     * Useful for debugging.
     * @param query The query to print the structure of.
     * @throws QueryNodeParseException If the query cannot be parsed.
     */
    private void printQueryStructure(String query) throws QueryNodeParseException {

        if(!logQueryTrees) return;

        System.out.println(
                "*************************************\n" +
                        "Query: " + query + "\n");
        AccumuloSyntaxParser parser = new AccumuloSyntaxParser();
        QueryNode node = parser.parse(query, "");
        PrintingVisitor.printToStdOut(node);
        System.out.println();
    }
}