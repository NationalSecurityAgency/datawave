package datawave.query.lucene.visitors;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValidateNotVisitorTest {

    @Test
    public void baseTest() throws QueryNodeParseException {

        printQueryStructure(("FIELD1:123 NOT FIELD2:456 OR FIELD3:abc")); //ask ivan if invalid or not
        printQueryStructure("FIELD1:abc OR FIELD2:def NOT FIELD3:123");
        printQueryStructure("FIELD2:def NOT FIELD3:123");
        printQueryStructure("FIELD2:def NOT (FIELD3:123 FIELD4:456)");
        printQueryStructure("(FIELD1:abc or FIELD2:def) NOT FIELD3:123");
        printQueryStructure("FIELD2:def NOT (FIELD3:123 NOT FIELD4:ghi)");
        System.out.println("--------------------------------------------------");
        printQueryStructure("FIELD1:abc or FIELD2:def NOT FIELD3:123");
        printQueryStructure("FIELD2:def NOT FIELD3:123 or FIELD4:ghi");
        printQueryStructure("FIELD2:def NOT FIELD3:123 and FIELD4:ghi");
        printQueryStructure("FIELD1:def FIELD2:123 FIELD4:ghi NOT FIELD5:jkl");
    }

    /**
     * Validates that when a NOT is not present in a query, no exception is thrown.
     */
    @Test
    public void testNotAbsent() throws Exception {
        assertValid("FIELD1:def");
    }

    /**
     * Validates that when a NOT is present in a query
     * and no junction is present, no exception is thrown.
     */
    @Test
    public void testNotWithoutJunction() throws Exception {
        assertValid("FIELD2:def NOT FIELD3:123");
    }

    /**
     * Validates that when a NOT is present in a query
     * and a AND is present without parentheses, an exception is thrown.
     */
    @Test
    public void testNotWithJunctionWithoutParens() throws Exception {
        assertInvalid("FIELD1:abc OR FIELD2:def NOT FIELD3:123");
        assertInvalid("FIELD1:abc AND FIELD2:def NOT FIELD3:123");
        assertInvalid("FIELD1:123 NOT FIELD2:456 NOT FIELD3:abc");
    }

    private QueryNode parseQuery(String query) throws QueryNodeParseException {
        AccumuloSyntaxParser parser = new AccumuloSyntaxParser();
        return parser.parse(query, "");
    }

    private void assertValid(String query) throws Exception {
        ValidateNotVisitor.validate(parseQuery(query));
    }

    private void assertInvalid(String query) throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> ValidateNotVisitor.validate(parseQuery(query)),
        "Query did not throw an exception: " + query);
    }

    private void printQueryStructure(String query) throws QueryNodeParseException {
        System.out.println("Query: " + query);
        AccumuloSyntaxParser parser = new AccumuloSyntaxParser();
        QueryNode node = parser.parse(query, "");
        PrintingVisitor.printToStdOut(node);
    }
}