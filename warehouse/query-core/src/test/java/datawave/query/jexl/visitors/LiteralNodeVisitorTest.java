package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import datawave.query.jexl.JexlASTHelper;

public class LiteralNodeVisitorTest {

    @Test
    public void testSingleLiteral() throws ParseException {
        String queryString = "FOO == '1234'";
        Multimap<String,String> literals = ArrayListMultimap.create();

        literals.put("FOO", "1234");
        test(queryString, literals);
    }

    @Test
    public void testMultipleLiterals() throws ParseException {
        String queryString = "FOO == '1234' || BAR == 'abcd' && BAZ == '1b3d'";
        Multimap<String,String> literals = ArrayListMultimap.create();

        literals.put("FOO", "1234");
        literals.put("BAR", "abcd");
        literals.put("BAZ", "1b3d");
        test(queryString, literals);
    }

    @Test
    public void testSingleLiteralAndRegex() throws ParseException {
        String queryString = "FOO == '1234' || BAR =~ 'abcd.*\\d'";
        Multimap<String,String> literals = ArrayListMultimap.create();

        literals.put("FOO", "1234");
        test(queryString, literals);
    }

    @Test
    public void testMultipleLiteralsAndRegex() throws ParseException {
        String queryString = "FOO == '1234' && BAR == 'abcd' || BAZ =~ 'abcd.*\\d'";
        Multimap<String,String> literals = ArrayListMultimap.create();

        literals.put("FOO", "1234");
        literals.put("BAR", "abcd");
        test(queryString, literals);
    }

    @Test
    public void testExcludeRegex1() throws ParseException {
        String query = "filter:excludeRegex(FOO,'ba.*')";
        Multimap<String,String> literals = ArrayListMultimap.create();

        test(query, literals);
    }

    @Test
    public void testExcludeRegex2() throws ParseException {
        String query = "FOO == 'bar' && filter:excludeRegex(FOO,'ba.*')";
        Multimap<String,String> literals = ArrayListMultimap.create();
        literals.put("FOO", "bar");

        test(query, literals);
    }

    @Test
    public void testExcludeRegex3() throws ParseException {
        String query = "FOO == 'bar' || filter:excludeRegex(FOO,'ba.*')";
        Multimap<String,String> literals = ArrayListMultimap.create();
        literals.put("FOO", "bar");

        test(query, literals);
    }

    @Test
    public void testIncludeRegex1() throws ParseException {
        String query = "filter:includeRegex(FOO,'ba.*')";
        Multimap<String,String> literals = ArrayListMultimap.create();

        test(query, literals);
    }

    @Test
    public void testIncludeRegex2() throws ParseException {
        String query = "FOO == 'bar' && filter:includeRegex(FOO,'ba.*')";
        Multimap<String,String> literals = ArrayListMultimap.create();
        literals.put("FOO", "bar");

        test(query, literals);
    }

    @Test
    public void testIncludeRegex3() throws ParseException {
        String query = "FOO == 'bar' || filter:includeRegex(FOO,'ba.*')";
        Multimap<String,String> literals = ArrayListMultimap.create();
        literals.put("FOO", "bar");

        test(query, literals);
    }

    @Test
    public void testEqNodeInsideQueryPropertyMarker() throws ParseException {
        String query = "((_Delayed_ = true) && FOO == 'bar')";
        Multimap<String,String> literals = ArrayListMultimap.create();
        literals.put("FOO", "bar");

        test(query, literals);
    }

    private void test(String query, Multimap<String,String> expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Set<String> expectedLiterals = new HashSet<>(expected.keySet());
        expectedLiterals.addAll(expected.values());
        Multimap<String,String> actualLiterals = LiteralNodeVisitor.getLiterals(script);

        assertEquals(expected.size(), actualLiterals.size());
        for (Map.Entry<String,String> entry : expected.entries()) {
            assertTrue(actualLiterals.containsEntry(entry.getKey(), entry.getValue()));
        }
    }

}
