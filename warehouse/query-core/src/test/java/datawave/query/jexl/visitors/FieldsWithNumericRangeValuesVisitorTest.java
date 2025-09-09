package datawave.query.jexl.visitors;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.tree.QueryNode;

class FieldsWithNumericRangeValuesVisitorTest {

    // Toggle printing the parsed AST via PrintingVisitor.
    // Set to true to see parsed JEXL and AST output during tests.
    private static final boolean PRINT_VISITOR = false;

    private String query;
    private String luceneQuery;
    private final Set<String> expectedFields = new LinkedHashSet<>();

    private static final LuceneToJexlQueryParser luceneParser = new LuceneToJexlQueryParser();

    @AfterEach
    void tearDown() {
        query = null;
        luceneQuery = null;
        expectedFields.clear();
    }

    @Test
    void testLuceneSinglePointRange() throws ParseException {
        givenLuceneQuery("FOO:[10 TO 10]");
        expectFields("FOO");
        assertLuceneResult();
    }

    @Test
    void testLuceneClosedClosedRange() throws ParseException {
        givenLuceneQuery("FOO:[10 TO 20]");
        expectFields("FOO");
        assertLuceneResult();
    }

    @Test
    void testLuceneClosedOpenRange() throws ParseException {
        givenLuceneQuery("FOO:[10 TO 20}");
        expectFields("FOO");
        assertLuceneResult();
    }

    @Test
    void testLuceneOpenOpenRange() throws ParseException {
        givenLuceneQuery("FOO:{10 TO 20}");
        expectFields("FOO");
        assertLuceneResult();
    }

    @Test
    void testLuceneLowerBoundOnly() throws ParseException {
        givenLuceneQuery("FOO:[10 TO *]");
        expectFields("FOO");
        assertLuceneResult();
    }

    @Test
    void testLuceneUpperBoundOnly() throws ParseException {
        givenLuceneQuery("FOO:[* TO 20]");
        expectFields("FOO");
        assertLuceneResult();
    }

    @Test
    void testLuceneMixedWithNonNumeric() throws ParseException {
        givenLuceneQuery("FOO:[10 TO 20] AND BAR:'abc' OR BAZ:true");
        expectFields("FOO");
        assertLuceneResult();
    }

    @ParameterizedTest
    @ValueSource(strings = {"FOO:[-1 TO 0]", "FOO:{-1.5 TO 10.25}", "FOO:[-1.5 TO *]", "FOO:[* TO 10.25}", "FOO:{-100 TO -1}", "FOO:[0 TO 0]"})
    void testLuceneVariousRanges(String lucene) throws ParseException {
        givenLuceneQuery(lucene);
        expectFields("FOO");
        assertLuceneResult();
    }

    @Test
    void testLuceneMultipleFields() throws ParseException {
        givenLuceneQuery("A:[1 TO 2] OR B:[* TO 10} AND C:'xyz' OR D:[5 TO 5]");
        expectFields("A", "B", "D");
        assertLuceneResult();
    }

    @Test
    void testLuceneEqualsShouldNotMatch() throws ParseException {
        // Equality is not a range comparison.
        givenLuceneQuery("A:1");
        // Expect no fields.
        assertLuceneResult();
    }

    @Test
    void testLuceneNotEqualsShouldNotMatch() throws ParseException {
        // A positive equality and a negated equality are not ranges.
        givenLuceneQuery("A:1 NOT B:4");
        // Expect no fields.
        assertLuceneResult();
    }

    @Test
    void testJexlExplicitBoundedRangeShouldMatch() throws ParseException {
        givenJexlQuery("((_Bounded_ = true) && (FOO >= '1' && FOO <= '4'))");
        expectFields("FOO");
        assertJexlResult();
    }

    @Test
    void testJexlEqualsShouldNotMatch() throws ParseException {
        givenJexlQuery("FOO == '1'");
        // Expect no fields.
        assertJexlResult();
    }

    @Test
    void testJexlNotEqualsShouldNotMatch() throws ParseException {
        givenJexlQuery("FOO != '1'");
        // Expect no fields.
        assertJexlResult();
    }

    @Test
    void testJexlGreaterThanShouldMatch() throws ParseException {
        givenJexlQuery("FOO > '1'");
        expectFields("FOO");
        assertJexlResult();
    }

    @Test
    void testJexlGreaterThanOrEqualShouldMatch() throws ParseException {
        givenJexlQuery("FOO >= '1'");
        expectFields("FOO");
        assertJexlResult();
    }

    @Test
    void testJexlLessThanShouldMatch() throws ParseException {
        givenJexlQuery("FOO < '1'");
        expectFields("FOO");
        assertJexlResult();
    }

    @Test
    void testJexlLessThanOrEqualShouldMatch() throws ParseException {
        givenJexlQuery("FOO <= '1'");
        expectFields("FOO");
        assertJexlResult();
    }

    // Non-numeric variants: should not match

    @Test
    void testJexlExplicitBoundedRangeWithTextShouldNotMatch() throws ParseException {
        givenJexlQuery("((_Bounded_ = true) && (FOO >= 'abc' && FOO <= 'def'))");
        // Expect no fields when range values are non-numeric.
        assertJexlResult();
    }

    @Test
    void testJexlGreaterThanWithTextShouldNotMatch() throws ParseException {
        givenJexlQuery("FOO > 'abc'");
        // Expect no fields when value is non-numeric.
        assertJexlResult();
    }

    @Test
    void testJexlGreaterThanOrEqualWithTextShouldNotMatch() throws ParseException {
        givenJexlQuery("FOO >= 'abc'");
        // Expect no fields when value is non-numeric.
        assertJexlResult();
    }

    @Test
    void testJexlLessThanWithTextShouldNotMatch() throws ParseException {
        givenJexlQuery("FOO < 'abc'");
        // Expect no fields when value is non-numeric.
        assertJexlResult();
    }

    @Test
    void testJexlLessThanOrEqualWithTextShouldNotMatch() throws ParseException {
        givenJexlQuery("FOO <= 'abc'");
        // Expect no fields when value is non-numeric.
        assertJexlResult();
    }

    private void givenJexlQuery(String query) {
        this.query = query;
    }

    private void givenLuceneQuery(String lucene) {
        this.luceneQuery = lucene;
    }

    private void expectFields(String... fields) {
        this.expectedFields.addAll(List.of(fields));
    }

    private void assertJexlResult() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        if (PRINT_VISITOR) {
            System.out.println("JEXL input: " + query);
            PrintingVisitor.printQuery(script);
        }
        Set<String> actual = FieldsWithNumericRangeValuesVisitor.getFields(script);
        Assertions.assertEquals(expectedFields, actual);
    }

    private void assertLuceneResult() throws ParseException {
        try {
            // Lucene -> QueryNode (whose toString is a JEXL string)
            QueryNode node = luceneParser.parse(luceneQuery);
            String jexl = node.toString();

            // JEXL string -> AST
            ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(jexl);

            if (PRINT_VISITOR) {
                System.out.println("Lucene input: " + luceneQuery);
                System.out.println("Converted JEXL: " + jexl);
                PrintingVisitor.printQuery(jexlScript);
            }

            // Visitor under test
            Set<String> actual = FieldsWithNumericRangeValuesVisitor.getFields(jexlScript);
            Assertions.assertEquals(expectedFields, actual, "Lucene: " + luceneQuery + "  JEXL: " + jexl);
        } catch (datawave.query.language.parser.ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
