package datawave.query.lucene.visitors;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValidateNotVisitorTest {

    private boolean logQueryTrees = true;

    /**
     * Validates the simplest case without a NOT.
     * @throws Exception If an error occurs while validating the query.
     */
    @Test
    public void testNotAbsent() throws Exception {
        assertValid("FIELD1:def");
    }

    /**
     * Validates that when a NOT is present in a query
     * and no junction is present, no exception is thrown.
     * @throws Exception If an error occurs while validating the query.
     */
    @Test
    public void testNotWithoutJunction() throws Exception {
        assertValid("FIELD2:def NOT FIELD3:123");
    }

    /**
     * Validates that when a NOT is present in a query
     * and a AND is present without parentheses, an exception is thrown.
     * @throws Exception If an error occurs while validating the query.
     */
    @Test
    public void testNotWithJunctionWithoutParens() throws Exception {
        assertInvalid("FIELD1:abc OR FIELD2:def NOT FIELD3:123");
        assertInvalid("FIELD1:abc AND FIELD2:def NOT FIELD3:123");
        assertInvalid("FIELD1:123 NOT FIELD2:456 NOT FIELD3:abc");
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
        ValidateNotVisitor.validate(parseQuery(query));
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
                () -> ValidateNotVisitor.validate(parseQuery(query)),
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

        System.out.println("Query: " + query);
        AccumuloSyntaxParser parser = new AccumuloSyntaxParser();
        QueryNode node = parser.parse(query, "");
        PrintingVisitor.printToStdOut(node);
    }
}