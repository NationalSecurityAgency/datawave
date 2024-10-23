package datawave.query.lucene.visitors;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValidateNotVisitorTest {

    private final boolean logQueryStructure = true;

    /**
     * Validates that when a NOT is not present in a query, no exception is thrown.
     */
    @Test
    public void testNotAbsent() throws Exception {
        System.out.println("\n---------- testNotAbsent ----------\n");
        assertValid("FIELD1:def");
    }

    /**
     * Validates that when a NOT is present in a query
     * and no junction is present, no exception is thrown.
     */
    @Test
    public void testNotWithoutJunction() throws Exception {
        System.out.println("\n---------- testNotWithoutJunction ----------\n");
        assertValid("FIELD2:def NOT FIELD3:123");
        assertValid("(FIELD2:def NOT FIELD3:123)");
        assertValid("FIELD2:def NOT (FIELD3:123)");
        assertValid("(FIELD2:def) NOT FIELD3:123");
        assertValid("(FIELD2:def) NOT (FIELD3:123)");
    }

    /**
     * Validates that when a NOT is present in a query
     * and a AND is present without parentheses, an exception is thrown.
     */
    @Test
    public void testNotBeforeJunctionWithoutParens() throws Exception {
        System.out.println("\n---------- testNotBeforeJunctionWithoutParens ----------\n");
        //Ambiguous Cases
        assertInvalid("FIELD1:abc NOT FIELD2:def AND FIELD3:123");
        assertInvalid("FIELD1:abc NOT FIELD2:def OR FIELD3:123");
        assertInvalid("(FIELD1:abc) NOT FIELD2:def AND FIELD3:123");
        assertInvalid("FIELD1:abc NOT (FIELD2:def) OR FIELD3:123");
        assertInvalid("FIELD1:abc NOT FIELD2:def OR (FIELD3:123)");
        //Unambiguous Cases
        assertValid("(FIELD1:abc NOT FIELD2:def) AND FIELD3:123");
        assertValid("(FIELD1:abc NOT FIELD2:def) OR FIELD3:123");
        assertValid("FIELD1:abc NOT (FIELD2:def AND FIELD3:123)");
        assertValid("FIELD1:abc NOT (FIELD2:def OR FIELD3:123)");
    }

    @Test
    public void testNotAfterJunctionWithoutParens() throws Exception {
        System.out.println("\n---------- testNotAfterJunctionWithoutParens ----------\n");
        assertInvalid("FIELD1:abc OR FIELD2:def NOT FIELD3:123");
        assertInvalid("FIELD1:abc AND FIELD2:def NOT FIELD3:123");
        assertInvalid("FIELD1:123 NOT FIELD2:456 NOT FIELD3:abc");
    }

    private QueryNode parseQuery(String query) throws QueryNodeParseException {
        AccumuloSyntaxParser parser = new AccumuloSyntaxParser();
        return parser.parse(query, "");
    }

    private void assertValid(String query) throws Exception {
        if(logQueryStructure) {
            System.out.print("*** VALID ");
            printQueryStructure(query);
        }
        ValidateNotVisitor.validate(parseQuery(query));
    }

    private void assertInvalid(String query) throws Exception {
        if(logQueryStructure) {
            System.out.print("*** INVALID ");
            printQueryStructure(query);
        }
        assertThrows(IllegalArgumentException.class,
                () -> ValidateNotVisitor.validate(parseQuery(query)),
        "Query did not throw an exception: " + query);
    }

    private void printQueryStructure(String query) throws QueryNodeParseException {
        System.out.println("Query: " + query + " ***");
        AccumuloSyntaxParser parser = new AccumuloSyntaxParser();
        QueryNode node = parser.parse(query, "");
        PrintingVisitor.printToStdOut(node);
        System.out.println();
    }
}