package datawave.query.lucene.visitors;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValidateFieldTermsVisitorTest {
    private final boolean logQueryStructure = true;

    @Test
    public void testShorthandQueries() throws Exception {
        System.out.println("\n---------- testShorthandQueriesz ----------\n");
        assertValid("abc");
        assertValid("FIELD:abc");
        assertValid("abc 123");
        assertValid("abc FIELD:123");
        assertValid("1234 OR 9101");
        assertValid("1234 AND 9101");
        assertValid("1234 OR 9101 OR 1121");
        assertValid("1234 AND 9101 AND 1121");
    }

    @Test
    public void testMixedFieldQueries() throws Exception {
        System.out.println("\n---------- testMixedFieldQueries ----------\n");
        assertInvalid("abc def FIELD:123 345");
        assertValid("abc def FIELD:(123 345)");
    }

    @Test
    public void testBooleanQueries() throws Exception {
        System.out.println("\n---------- testBooleanQueries ----------\n");
        assertInvalid("FIELD:1234 5678");
        assertInvalid("FIELD:1234 OR 5678");
        assertInvalid("FIELD:1234 AND 5678");
        assertInvalid("(FIELD:1234 OR 5678)");
        assertInvalid("(FIELD:1234 AND 5678)");

        assertValid("FIELD:1234 AND (5678)");
        assertValid("(FIELD:1234) OR 5678");
        assertValid("FIELD:(1234) OR 5678");
        assertValid("FIELD:(1234 5678)");
        assertValid("FIELD:(1234 OR 5678)");
        assertValid("FIELD:(1234 AND 5678)");
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
        ValidateFieldTermsVisitor.validate(parseQuery(query));
    }

    private void assertInvalid(String query) throws Exception {
        if(logQueryStructure) {
            System.out.print("*** INVALID ");
            printQueryStructure(query);
        }
        assertThrows(IllegalArgumentException.class,
                () -> ValidateFieldTermsVisitor.validate(parseQuery(query)),
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