package datawave.query.jexl.visitors;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;

public class QueryPatternsVisitorTest {

    private String query;
    private final Set<String> expected = new HashSet<>();

    @After
    public void tearDown() throws Exception {
        query = null;
        expected.clear();
    }

    @Test
    public void testNoPatterns() throws ParseException {
        givenQuery("BAR == '1'");
        assertResults();
    }

    @Test
    public void testER() throws ParseException {
        givenQuery("BAR == '1' && FOO =~ '1234.*\\d'");
        expectPatterns("1234.*\\d");
        assertResults();

    }

    @Test
    public void testNR() throws ParseException {
        givenQuery("BAR == '1' && FOO !~ '1234.*\\d'");
        expectPatterns("1234.*\\d");
        assertResults();
    }

    @Test
    public void testFilterFunctionIncludeRegex() throws ParseException {
        givenQuery("A == '1' && filter:includeRegex(B,'*2*')");
        expectPatterns("*2*");
        assertResults();
    }

    @Test
    public void testFilterFunctionExcludeRegex() throws ParseException {
        givenQuery("A == '1' && filter:excludeRegex(B,'*2*')");
        expectPatterns("*2*");
        assertResults();
    }

    @Test
    public void testFilterFunctionGetAllMatches() throws ParseException {
        givenQuery("A == '1' && filter:getAllMatches(B,'*2*')");
        expectPatterns("*2*");
        assertResults();
    }

    @Test
    public void testDoubleSidedER() throws ParseException {
        givenQuery("A =~ B");
        assertResults();
    }

    @Test
    public void testDoubleSidedNR() throws ParseException {
        givenQuery("A !~ B");
        assertResults();
    }

    @Test
    public void testCombo() {
        givenQuery("BAR == '1' && FOO =~ '1234.*\\d' && FOO !~ '444.*' && filter:includeRegex(B,'*2*')");
        expectPatterns("1234.*\\d", "444.*", "*2*");
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void expectPatterns(String... patterns) {
        expected.addAll(Arrays.asList(patterns));
    }

    private void assertResults() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Set<String> patterns = QueryPatternsVisitor.findPatterns(script);
        Assert.assertEquals(expected, patterns);
    }
}
