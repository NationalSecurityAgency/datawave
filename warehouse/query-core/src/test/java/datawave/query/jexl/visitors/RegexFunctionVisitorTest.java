package datawave.query.jexl.visitors;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;
import datawave.test.JexlNodeAssert;

public class RegexFunctionVisitorTest {

    private final ASTValidator validator = new ASTValidator();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    // expected base cases -- single fielded

    @Test
    public void testRewriteSingleFieldedIncludeRegex() throws ParseException {
        Set<String> indexOnlyFields = Collections.singleton("FIELDA");
        String query = "FOO == 'bar' && filter:includeRegex(FIELDA, 'ba.*')";
        String expected = "FOO == 'bar' && FIELDA =~ 'ba.*'";
        assertVisitorResult(query, expected, indexOnlyFields);

        query = "FOO == 'bar' || filter:includeRegex(FIELDA, 'ba.*')";
        expected = "FOO == 'bar' || FIELDA =~ 'ba.*'";
        assertVisitorResult(query, expected, indexOnlyFields);
    }

    @Test
    public void testRewriteSingleFieldedExcludeRegex() throws ParseException {
        Set<String> indexOnlyFields = Collections.singleton("FIELDA");
        String query = "FOO == 'bar' && filter:excludeRegex(FIELDA, 'ba.*')";
        String expected = "FOO == 'bar' && FIELDA !~ 'ba.*'";
        assertVisitorResult(query, expected, indexOnlyFields);

        query = "FOO == 'bar' || filter:excludeRegex(FIELDA, 'ba.*')";
        expected = "FOO == 'bar' || FIELDA !~ 'ba.*'";
        assertVisitorResult(query, expected, indexOnlyFields);
    }

    // expected base cases -- multi fielded

    @Test
    public void testRewriteMultiFieldedIncludeRegex() throws ParseException {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELDA", "FIELDB");
        String query = "FOO == 'bar' && filter:includeRegex(FIELDA||FIELDB, 'ba.*')";
        String expected = "FOO == 'bar' && (FIELDA =~ 'ba.*' || FIELDB =~ 'ba.*')";
        assertVisitorResult(query, expected, indexOnlyFields);

        query = "FOO == 'bar' || filter:includeRegex(FIELDA||FIELDB, 'ba.*')";
        expected = "FOO == 'bar' || (FIELDA =~ 'ba.*' || FIELDB =~ 'ba.*')";
        assertVisitorResult(query, expected, indexOnlyFields);
    }

    @Test
    public void testRewriteMultiHeteroIndexOnlyFieldsIncludeRegex() throws ParseException {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELDB");
        String query = "FOO == 'bar' && filter:includeRegex(FIELDA||FIELDB||FIELDC, 'ba.*')";
        String expected = "FOO == 'bar' && (FIELDB =~ 'ba.*' || filter:includeRegex(FIELDA || FIELDC, 'ba.*'))";
        assertVisitorResult(query, expected, indexOnlyFields);

        query = "FOO == 'bar' || filter:includeRegex(FIELDA||FIELDB||FIELDC, 'ba.*')";
        expected = "FOO == 'bar' || (FIELDB =~ 'ba.*' || filter:includeRegex(FIELDA || FIELDC, 'ba.*'))";
        assertVisitorResult(query, expected, indexOnlyFields);
    }

    @Test
    public void testRewriteMultiHeteroIndexOnlyFieldsIncludeRegex2() throws ParseException {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELDB", "FIELDC");
        String query = "FOO == 'bar' && filter:includeRegex(FIELDA||FIELDB||FIELDC, 'ba.*')";
        String expected = "FOO == 'bar' && (FIELDB =~ 'ba.*' || FIELDC =~ 'ba.*' || filter:includeRegex(FIELDA, 'ba.*'))";
        assertVisitorResult(query, expected, indexOnlyFields);

        query = "FOO == 'bar' || filter:includeRegex(FIELDA||FIELDB||FIELDC, 'ba.*')";
        expected = "FOO == 'bar' || (FIELDB =~ 'ba.*' || FIELDC =~ 'ba.*' || filter:includeRegex(FIELDA, 'ba.*'))";
        assertVisitorResult(query, expected, indexOnlyFields);
    }

    @Test
    public void testRewriteHeteroIndexOnlyFieldsExcludeRegex() throws ParseException {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELDA", "FIELDB");
        String query = "FOO == 'bar' && filter:excludeRegex(FIELDA||FIELDB||FIELDC, 'ba.*')";
        String expected = "FOO == 'bar' && FIELDA !~ 'ba.*' && FIELDB !~ 'ba.*' && filter:excludeRegex(FIELDC, 'ba.*')";
        assertVisitorResult(query, expected, indexOnlyFields);

        query = "FOO == 'bar' || filter:excludeRegex(FIELDA||FIELDB||FIELDC, 'ba.*')";
        expected = "FOO == 'bar' || (FIELDA !~ 'ba.*' && FIELDB !~ 'ba.*' && filter:excludeRegex(FIELDC, 'ba.*'))";
        assertVisitorResult(query, expected, indexOnlyFields);
    }

    @Test
    public void testANDIncludeRegex() throws ParseException {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELDA", "FIELDB");
        String query = "FOO == 'bar' && filter:excludeRegex(FIELDA&&FIELDB&&FIELDC, 'ba.*')";
        assertVisitorResult(query, query, indexOnlyFields);

    }

    @Test
    public void testRewriteMultiFieldedExcludeRegex() throws ParseException {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELDA", "FIELDB");
        String query = "FOO == 'bar' && filter:excludeRegex(FIELDA||FIELDB, 'ba.*')";
        String expected = "FOO == 'bar' && (FIELDA !~ 'ba.*' && FIELDB !~ 'ba.*')";
        assertVisitorResult(query, expected, indexOnlyFields);

        query = "FOO == 'bar' || filter:excludeRegex(FIELDA||FIELDB, 'ba.*')";
        expected = "FOO == 'bar' || (FIELDA !~ 'ba.*' && FIELDB !~ 'ba.*')";
        assertVisitorResult(query, expected, indexOnlyFields);
    }

    // non index-only fields should not be expanded

    @Test
    public void testEndWildCardNotIndexOnly() throws ParseException {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELDA");
        String query = "FOO == 'bar' && filter:includeRegex(FIELDB, 'all_.*?')";
        assertVisitorResult(query, query, indexOnlyFields);

        query = "FOO == 'bar' || filter:includeRegex(FIELDB, 'all_.*?')";
        assertVisitorResult(query, query, indexOnlyFields);
    }

    @Test
    public void testMixedEventNonEvent() throws ParseException {
        Set<String> indexOnlyFields = Sets.newHashSet("NON_EVENT_FIELD");
        String query = "filter:includeRegex(EVENT_FIELD || NON_EVENT_FIELD,'all_.*?')";
        String expected = " NON_EVENT_FIELD =~ 'all_.*?' || filter:includeRegex(EVENT_FIELD, 'all_.*?')";
        assertVisitorResult(query, expected, indexOnlyFields);
    }

    // bad regex cases

    @Test
    public void testDoubleEndedWildCard() throws ParseException {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELDA");
        String query = "FOO == 'bar' && filter:includeRegex(FIELDA,'.*all_.*')";
        assertVisitorResult(query, query, indexOnlyFields);
    }

    @Test
    public void testDoubleEndedWildCard2() throws ParseException {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELDA");
        String query = "FOO == 'bar' && filter:includeRegex(FIELDA,'?.*all_.*?')";
        assertVisitorResult(query, query, indexOnlyFields);
    }

    @Test
    public void testBadRegex() throws ParseException {
        exception.expect(DatawaveFatalQueryException.class);
        Set<String> indexOnlyFields = Sets.newHashSet("FIELDA");
        String query = "FOO == 'bar' && filter:includeRegex(FIELDA, '(?#icu)Friendly')";
        assertVisitorResult(query, query, indexOnlyFields);
    }

    // legacy tests

    @Test
    public void testEndWildcard() throws ParseException {
        Set<String> indexOnlyFields = Sets.newHashSet("FIELDA");
        String query = "_ANYFIELD_=='email' and ANOTHER_FIELD=='blah' and filter:includeRegex(FIELDA,'all_.*?')";
        String expected = "_ANYFIELD_ == 'email' && ANOTHER_FIELD == 'blah' && FIELDA =~ 'all_.*?'";

        assertVisitorResult(query, expected, indexOnlyFields);
    }

    private void assertVisitorResult(String original, String expected, Set<String> indexOnlyFields) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);

        JexlNode actual = RegexFunctionVisitor.expandRegex(null, null, indexOnlyFields, originalScript);

        // Verify the resulting script is as expected, with a valid lineage.
        JexlNodeAssert.assertThat(actual).isEqualTo(expected).hasValidLineage();

        // Verify the original script was not modified and has a valid lineage.
        JexlNodeAssert.assertThat(originalScript).isEqualTo(original).hasValidLineage();

        // assert that the result script is valid
        try {
            validator.isValid(actual);
        } catch (InvalidQueryTreeException e) {
            Assert.fail("Invalid query tree detected for script: " + JexlStringBuildingVisitor.buildQueryWithoutParse(actual));
        }
    }
}
