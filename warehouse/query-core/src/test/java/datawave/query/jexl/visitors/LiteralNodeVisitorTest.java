package datawave.query.jexl.visitors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LiteralNodeVisitorTest {

    @Test
    public void testSingleLiteral() throws ParseException {
        String queryString = "FOO == '1234'";
        Multimap<String, String> literals = ArrayListMultimap.create();

        literals.put("FOO", "1234");
        test(queryString, literals);
    }

    @Test
    public void testMultipleLiterals() throws ParseException {
        String queryString = "FOO == '1234' || BAR == 'ABCD' && BAZ == '1b3d'";
        Multimap<String, String> literals = ArrayListMultimap.create();

        literals.put("FOO", "1234");
        literals.put("BAR", "abcd");
        literals.put("BAZ", "1b3d");
        test(queryString, literals);
    }

    @Test
    public void testSingleLiteralAndRegex() throws ParseException {
        String queryString = "FOO == '1234' || BAR =~ 'abcd.*\\d'";
        Multimap<String, String> literals = ArrayListMultimap.create();

        literals.put("FOO", "1234");
        test(queryString, literals);
    }

    @Test
    public void testMultipleLiteralsAndRegex() throws ParseException {
        String queryString = "FOO == '1234' && BAR == 'abcd' || BAZ =~ 'abcd.*\\d'";
        Multimap<String, String> literals = ArrayListMultimap.create();

        literals.put("FOO", "1234");
        literals.put("BAR", "abcd");
        test(queryString, literals);
    }

    private void test(String query, Multimap<String, String> expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Set<String> expectedLiterals = new HashSet<>(expected.keySet());
        expectedLiterals.addAll(expected.values());
        Multimap<String, String> actualLiterals = LiteralNodeVisitor.getLiterals(script);

        Assert.assertEquals(expected.size(), actualLiterals.size());
        for (Map.Entry<String, String> entry : expected.entries()) {
            Assert.assertTrue(actualLiterals.containsEntry(entry.getKey(), entry.getValue()));
        }
    }

}
