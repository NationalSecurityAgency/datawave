package datawave.query.lucene.visitors;

import datawave.query.language.parser.lucene.AccumuloSyntaxParser;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CoreVisitorTests {

    /**
     * Permutations to Test
     *
     * <p>List of test cases using {@code assertValid} and {@code assertInvalid} statements that cover
     * all possible forms of queries according to the modified Lucene syntax:</p>
     *
     * <p>(Only includes cases that are readable queries given the provided syntax guidelines)</p>
     *
     * <pre>{@code
     * // Single term queries
     * assertValid("FIELD1:abc");                        // Simple term
     * assertValid("FIELD1:ab*");                        // Wildcard term
     * assertValid("FIELD1:a?c");                        // Single character wildcard
     * assertValid("FIELD1:*lector");                    // Leading wildcard (as per syntax)
     * assertValid("abc*");                              // Wildcard without field
     * assertValid("\"quick brown dog\"");               // Phrase query (as per syntax)
     * assertValid("\"quick brown dog\"~20");            // Proximity search (as per syntax)
     * assertValid("#FUNCTION(ARG1, ARG2)");             // Function query
     *
     * // Range queries
     * assertValid("FIELD1:[begin TO end]");             // Inclusive range (as per syntax)
     * assertValid("FIELD1:{begin TO end}");             // Exclusive range (as per syntax)
     *
     * // Multiple terms with implicit AND (default operator)
     * assertValid("FIELD1:abc FIELD2:def");             // Implicit AND between terms
     * assertValid("FIELD1:abc FIELD2:def FIELD3:ghi");  // Multiple terms
     *
     * // Explicit AND operator
     * assertValid("FIELD1:abc AND FIELD2:def");         // Explicit AND
     * assertValid("FIELD1:abc AND FIELD2:def AND FIELD3:ghi");
     *
     * // Using OR operator
     * assertValid("FIELD1:abc OR FIELD2:def");          // Simple OR
     * assertValid("FIELD1:abc OR FIELD2:def OR FIELD3:ghi");
     * assertValid("FIELD1:abc AND FIELD2:def OR FIELD3:ghi");  // Combining AND and OR
     *
     * // Using NOT operator between terms
     * assertValid("FIELD1:abc NOT FIELD2:def");                         // NOT between terms
     * assertValid("FIELD1:abc NOT FIELD2:def NOT FIELD3:ghi");          // Multiple NOTs between terms
     * assertInvalid("FIELD1:abc NOT FIELD2:def FIELD3:ghi");            // Ambiguous NOT placement
     *
     * // Parentheses usage with NOT between terms
     * assertValid("(FIELD1:abc NOT FIELD2:def) AND FIELD3:ghi");        // NOT within parentheses
     * assertValid("FIELD1:abc AND (FIELD2:def NOT FIELD3:ghi)");        // NOT within parentheses
     * assertInvalid("(FIELD1:abc NOT FIELD2:def FIELD3:ghi)");          // Ambiguous within parentheses
     *
     * // Complex expressions without unary NOT
     * assertValid("FIELD1:abc NOT (FIELD2:def AND FIELD3:ghi)");        // NOT before group
     * assertValid("(FIELD1:abc AND FIELD2:def) NOT (FIELD3:ghi OR FIELD4:jkl)"); // NOT between groups
     *
     * // Nested parentheses without unary NOT
     * assertValid("((FIELD1:abc NOT FIELD2:def))");                     // Nested parentheses with NOT
     * assertValid("(FIELD1:abc NOT (FIELD2:def AND FIELD3:ghi))");      // NOT with nested group
     *
     * // Functions and phrases within complex expressions
     * assertValid("#FUNCTION(ARG1, ARG2) NOT (\"quick brown dog\"~20 AND FIELD1:abc)");
     * assertInvalid("#FUNCTION(ARG1, ARG2) NOT \"quick brown dog\"~20 FIELD1:abc"); // Ambiguous without parentheses
     *
     * // Wildcards and range queries with NOT between terms
     * assertValid("FIELD1:selec* NOT FIELD2:selec?or");                 // Wildcards with NOT
     * assertValid("FIELD1:[begin TO end] NOT FIELD2:{begin TO end}");    // Range queries with NOT
     * assertInvalid("FIELD1:[begin TO end] NOT FIELD2:{begin TO end} FIELD3:ghi"); // Ambiguous without operator
     *
     * // Escaping special characters in terms
     * assertValid("FIELD1:foo\\-bar NOT FIELD2:abc");                   // Escaped hyphen with NOT
     * assertValid("FIELD1:foo\\+bar AND FIELD2:abc");                   // Escaped plus with AND
     * assertValid("FIELD1:foo\\@bar OR FIELD2:abc");                    // Escaped at symbol with OR
     *
     * // Queries with proximity searches and functions combined
     * assertValid("\"quick brown dog\"~5 NOT (\"lazy fox\"~10 AND FIELD1:abc)");
     * assertValid("#FUNCTION(ARG1) AND (#FUNCTION(ARG1, ARG2) NOT FIELD2:def)");
     *
     * // Queries with special terms and modifiers
     * assertValid("FIELD1:\"complex term\" NOT FIELD2:[begin TO end]");
     * assertInvalid("FIELD1:{* TO end} NOT FIELD2:selec* NOT FIELD3:selec?or"); // Ambiguous without parentheses
     * // Corrected unambiguous version
     * assertValid("FIELD1:{* TO end} NOT (FIELD2:selec* OR FIELD3:selec?or)");  // Parentheses resolve ambiguity
     *
     * // Testing precedence and grouping without unary NOT
     * assertValid("FIELD1:abc AND ((FIELD2:def OR FIELD3:ghi) NOT FIELD4:jkl)");
     * assertInvalid("FIELD1:abc AND FIELD2:def OR FIELD3:ghi NOT FIELD4:jkl FIELD5:mno"); // Ambiguous without parentheses
     *
     * }</pre>
     *
     * <strong>Explanation:</strong>
     * <ul>
     *   <li><strong>Single Term Queries:</strong> Tests basic terms, wildcards, phrases, proximity searches, and functions as per the provided syntax.</li>
     *   <li><strong>Range Queries:</strong> Covers inclusive and exclusive ranges according to the syntax.</li>
     *   <li><strong>Implicit AND Operator:</strong> Checks multiple terms without explicit operators.</li>
     *   <li><strong>Explicit AND/OR Operators:</strong> Validates queries using {@code AND} and {@code OR}.</li>
     *   <li><strong>NOT Operator Between Terms:</strong> Tests placement of {@code NOT} between terms and identifies ambiguous cases.</li>
     *   <li><strong>Parentheses Usage with NOT:</strong> Ensures parentheses group expressions correctly when using {@code NOT}.</li>
     *   <li><strong>Complex Expressions:</strong> Combines multiple operators and parentheses without unary {@code NOT}.</li>
     *   <li><strong>Functions and Phrases:</strong> Tests advanced terms in complex queries.</li>
     *   <li><strong>Wildcards and Range Queries with NOT:</strong> Validates {@code NOT} with wildcards and ranges between terms.</li>
     *   <li><strong>Precedence and Grouping:</strong> Checks operator precedence and grouping without unary {@code NOT}.</li>
     *   <li><strong>Escaping Special Characters:</strong> Ensures special characters are properly escaped in various contexts.</li>
     *   <li><strong>Edge Cases:</strong> Covers ambiguous cases and ensures they are properly flagged as invalid. For example, multiple {@code NOT} operators without parentheses can lead to ambiguity.</li>
     *   <li><strong>Reserved Words as Field Names or Terms:</strong> Confirms that reserved words can be used as field names or terms where appropriate.</li>
     * </ul>
     *
     * <p>These test cases cover a wide range of possible query forms, ensuring that both valid and invalid (ambiguous) queries are properly identified according to the provided syntax guidelines.</p>
     */

//    private boolean logQueryTrees = true;
//
//    /*Specific test cases*/
//
//    /**
//     * Ivan's ask #1
//     */
//    @Test
//    public void testAmbiguousJunctions() throws Exception {
//        assertInvalid("FIELD1:1234 5678", new Exception());            // Ambiguous without grouping
//        assertInvalid("(FIELD1:1234 5678)", new Exception());          // Ambiguous without grouping
//        assertInvalid("FIELD1:1234 AND 5678", new Exception());        // Ambiguous without grouping
//        assertInvalid("FIELD1:1234 OR 5678", new Exception());         // Ambiguous without grouping
//        assertValid("FIELD1:(1234 AND 5678)");                         // Grouped terms
//        assertValid("FIELD1:(1234 OR 5678)");                          // Grouped terms
//        assertValid("FIELD1:(1234 5678)");                             // Grouped terms
//    }
//
//    /**
//     * Ivan's ask #2
//     */
//    @Test
//    public void testUnquotedPhrases() throws Exception {
//        assertInvalid("FIELD:term1 term2", new Exception());        // Ambiguous without quotes
//        assertInvalid("FIELD:term1 AND term2", new Exception());    // Ambiguous without quotes
//        assertInvalid("FIELD:term1 OR term2", new Exception());     // Ambiguous without quotes
//        assertInvalid("\"FIELD:term1 term2\"", new Exception());    //Wrong quote placement
//        assertInvalid("\"FIELD:term1 AND term2\"", new Exception());//Wrong quote placement
//        assertInvalid("\"FIELD:term1 OR term2\"", new Exception()); //Wrong quote placement
//        assertValid("FIELD:\"term1 term2\"");
//        assertValid("FIELD:\"term1 AND term2\"");
//        assertValid("FIELD:\"term1 OR term2\"");
//    }
//
//    /*General test cases*/
//
//    @Test
//    public void testSingleTermQueries() throws Exception {
//        assertValid("FIELD1:abc");                        // Simple term
//        assertValid("FIELD1:ab*");                        // Wildcard term
//        assertValid("FIELD1:a?c");                        // Single character wildcard
//        assertValid("FIELD1:*lector");                    // Leading wildcard (as per syntax)
//        assertValid("abc*");                              // Wildcard without field
//        assertValid("\"quick brown dog\"");               // Phrase query
//        assertValid("\"quick brown dog\"~20");            // Proximity search
//        assertValid("#FUNCTION(ARG1, ARG2)");             // Function query
//    }
//
//    @Test
//    public void testRangeQueries() throws Exception {
//        assertValid("FIELD1:[begin TO end]");             // Inclusive range
//        assertValid("FIELD1:{begin TO end}");             // Exclusive range
//    }
//
//    @Test
//    public void testImplicitAndOperator() throws Exception {
//        assertValid("FIELD1:abc FIELD2:def");             // Implicit AND between terms
//        assertValid("FIELD1:abc FIELD2:def FIELD3:ghi");  // Multiple terms
//    }
//
//    @Test
//    public void testExplicitAndOrOperators() throws Exception {
//        assertValid("FIELD1:abc AND FIELD2:def");         // Explicit AND
//        assertValid("FIELD1:abc AND FIELD2:def AND FIELD3:ghi");
//        assertValid("FIELD1:abc OR FIELD2:def");          // Simple OR
//        assertValid("FIELD1:abc OR FIELD2:def OR FIELD3:ghi");
//        assertInvalid("FIELD1:abc AND FIELD2:def OR FIELD3:ghi", new Exception());  // Combining AND and OR
//        assertInvalid("FIELD1:abc OR FIELD2:def AND FIELD3:ghi", new Exception());  // Also ambiguous
//    }
//
//    @Test
//    public void testGroupedTerms() throws Exception {
//        assertValid("FIELD1:(abc def)");            // Multiple terms in field value
//        assertValid("FIELD1:(abc AND def)");        // Multiple terms in field value
//        assertValid("FIELD1:(abc OR def)");         // Multiple terms in field value
//        assertInvalid("FIELD1:abc def", new Exception());            // Ambiguous without grouping
//        assertInvalid("(FIELD1:abc def)", new Exception());          // Ambiguous without grouping
//        assertInvalid("FIELD1:abc AND def", new Exception());        // Ambiguous without grouping
//        assertInvalid("FIELD1:abc OR def", new Exception());         // Ambiguous without grouping
//    }
//
//
//    @Test
//    public void testNotOperatorBetweenTerms() throws Exception {
//        assertValid("FIELD1:abc NOT FIELD2:def");                        // NOT between two terms
//        assertInvalid("FIELD1:abc NOT FIELD2:def NOT FIELD3:ghi", new Exception());       // Multiple NOTs between terms
//        assertInvalid("FIELD1:abc NOT FIELD2:def FIELD3:ghi", new Exception());           // Ambiguous NOT placement
//    }
//
//    @Test
//    public void testParenthesesUsageWithNot() throws Exception {
//        assertValid("(FIELD1:abc NOT FIELD2:def) AND FIELD3:ghi");       // NOT within parentheses
//        assertValid("FIELD1:abc AND (FIELD2:def NOT FIELD3:ghi)");       // NOT within parentheses
//        assertInvalid("(FIELD1:abc NOT FIELD2:def FIELD3:ghi)", new Exception());         // Ambiguous within parentheses
//    }
//
//    @Test
//    public void testComplexExpressions() throws Exception {
//        assertValid("FIELD1:abc NOT (FIELD2:def AND FIELD3:ghi)");       // NOT before group
//        assertValid("(FIELD1:abc AND FIELD2:def) NOT (FIELD3:ghi OR FIELD4:jkl)"); // NOT between groups
//    }
//
//    @Test
//    public void testNestedParenthesesWithNot() throws Exception {
//        assertValid("((FIELD1:abc NOT FIELD2:def))");                    // Nested parentheses with NOT
//        assertValid("(FIELD1:abc NOT (FIELD2:def AND FIELD3:ghi))");     // NOT with nested group
//    }
//
//
//    @Test
//    public void testFunctionsAndPhrasesInComplexExpressions() throws Exception {
//        assertValid("#FUNCTION(ARG1, ARG2) NOT (\"quick brown dog\"~20 AND FIELD1:abc)");
//        assertInvalid("#FUNCTION(ARG1, ARG2) NOT \"quick brown dog\"~20 FIELD1:abc", new Exception()); // Ambiguous without parentheses
//    }
//
//
//    @Test
//    public void testWildcardsAndRangeQueriesWithNot() throws Exception {
//        assertValid("FIELD1:selec* NOT FIELD2:selec?or");                // Wildcards with NOT
//        assertValid("FIELD1:[begin TO end] NOT FIELD2:{begin TO end}");  // Range queries with NOT
//        assertInvalid("FIELD1:[begin TO end] NOT FIELD2:{begin TO end} FIELD3:ghi", new Exception()); // Ambiguous without operator
//    }
//
//
//    @Test
//    public void testEscapingSpecialCharacters() throws Exception {
//        assertValid("FIELD1:foo\\-bar NOT FIELD2:abc");                  // Escaped hyphen with NOT
//        assertValid("FIELD1:foo\\+bar AND FIELD2:abc");                  // Escaped plus with AND
//        assertValid("FIELD1:foo\\@bar OR FIELD2:abc");                   // Escaped at symbol with OR
//    }
//
//
//    @Test
//    public void testQueriesWithProximitySearchesAndFunctionsCombined() throws Exception {
//        assertValid("\"quick brown dog\"~5 NOT (\"lazy fox\"~10 AND FIELD1:abc)");
//        assertValid("#FUNCTION(ARG1) AND (#FUNCTION(ARG1, ARG2) NOT FIELD2:def)");
//    }
//
//
//    @Test
//    public void testQueriesWithSpecialTermsAndModifiersAndNot() throws Exception {
//        assertValid("FIELD1:\"complex term\" NOT FIELD2:[begin TO end]");
//        assertInvalid("FIELD1:{* TO end} NOT FIELD2:selec* NOT FIELD3:selec?or", new Exception()); // Ambiguous without parentheses
//        assertValid("FIELD1:{* TO end} NOT (FIELD2:selec* OR FIELD3:selec?or)");  // Parentheses resolve ambiguity
//    }
//
//
//    @Test
//    public void testTestingPrecedenceAndGrouping() throws Exception {
//        assertValid("FIELD1:abc AND ((FIELD2:def OR FIELD3:ghi) NOT FIELD4:jkl)");
//        assertInvalid("FIELD1:abc AND FIELD2:def OR FIELD3:ghi NOT FIELD4:jkl FIELD5:mno", new Exception()); // Ambiguous without parentheses
//    }
}