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
        if(logQueryStructure) {
            printQueryStructure(query);
        }
        ValidateNotVisitor.validate(parseQuery(query));
    }

    private void assertInvalid(String query) throws Exception {
        if(logQueryStructure) {
            printQueryStructure(query);
        }
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