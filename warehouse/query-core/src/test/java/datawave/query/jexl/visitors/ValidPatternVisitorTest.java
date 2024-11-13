package datawave.query.jexl.visitors;

import java.util.regex.PatternSyntaxException;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;

public class ValidPatternVisitorTest {

    @Test
    public void testValidER() throws ParseException {
        String queryString = "BAR == '1' && FOO =~ '1234.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidPatternVisitor.check(script);
    }

    @Test
    public void testValidERWithCacheHit() throws ParseException {
        String queryString = "BAR == '1' && FOO =~ '1234.*\\d' && FOO2 =~ '1234.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidPatternVisitor.check(script);
    }

    @Test(expected = PatternSyntaxException.class)
    public void testInvalidER() throws ParseException {
        String queryString = "BAR == '1' && FOO =~ '1234.**'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidPatternVisitor.check(script);
    }

    @Test
    public void testValidNR() throws ParseException {
        String queryString = "BAR == '1' && FOO !~ '1234.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidPatternVisitor.check(script);
    }

    @Test
    public void testValidNRWithCacheHit() throws ParseException {
        String queryString = "BAR == '1' && FOO !~ '1234.*\\d' && FOO2 !~ '1234.*\\d'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidPatternVisitor.check(script);
    }

    @Test(expected = PatternSyntaxException.class)
    public void testInvalidNR() throws ParseException {
        String queryString = "BAR == '1' && FOO !~ '1234.**'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidPatternVisitor.check(script);
    }

    @Test(expected = PatternSyntaxException.class)
    public void testFilterFunctionIncludeRegex() throws ParseException {
        String queryString = "A == '1' && filter:includeRegex(B,'*2*')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidPatternVisitor.check(script);
    }

    @Test(expected = PatternSyntaxException.class)
    public void testFilterFunctionExcludeRegex() throws ParseException {
        String queryString = "A == '1' && filter:excludeRegex(B,'*2*')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidPatternVisitor.check(script);
    }

    @Test(expected = PatternSyntaxException.class)
    public void testFilterFunctionGetAllMatches() throws ParseException {
        String queryString = "A == '1' && filter:getAllMatches(B,'*2*')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidPatternVisitor.check(script);
    }

    @Test
    public void testValidDoubleSidedEr() throws ParseException {
        String queryString = "A =~ B";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidPatternVisitor.check(script);
    }

    @Test
    public void testValidDoubleSidedNr() throws ParseException {
        String queryString = "A !~ B";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        ValidPatternVisitor.check(script);
    }
}
