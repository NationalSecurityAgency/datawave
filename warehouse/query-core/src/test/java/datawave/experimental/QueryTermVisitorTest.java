package datawave.experimental;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.experimental.visitor.QueryTermVisitor;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

public class QueryTermVisitorTest {

    @Test
    public void testEq() {
        String query = "FOO == 'bar'";
        test(query, Collections.singleton(query));
    }

    @Test
    public void testNe() {
        String query = "FOO != 'bar'";
        test(query, Collections.singleton(query));
    }

    @Test
    public void testNot() {
        String query = "!(FOO == 'bar')";
        test(query, Collections.singleton("FOO == 'bar'"));
    }

    @Test
    public void testSimpleConjunction() {
        String query = "FOO == 'bar' && FOO == 'baz'";
        Set<String> expected = Sets.newHashSet("FOO == 'bar'", "FOO == 'baz'");
        test(query, expected);
    }

    @Test
    public void testSimpleDisjunction() {
        String query = "FOO == 'bar' || FOO == 'baz'";
        Set<String> expected = Sets.newHashSet("FOO == 'bar'", "FOO == 'baz'");
        test(query, expected);
    }

    @Test
    public void testLessThan() {
        String query = "FOO < 'bar'";
        test(query, Collections.singleton(query));
    }

    @Test
    public void testGreaterThan() {
        String query = "FOO > 'bar'";
        test(query, Collections.singleton(query));
    }

    @Test
    public void testLessThanOrEqual() {
        String query = "FOO <= 'bar'";
        test(query, Collections.singleton(query));
    }

    @Test
    public void testGreaterThanOrEqual() {
        String query = "FOO >= 'bar'";
        test(query, Collections.singleton(query));
    }

    @Ignore
    @Test
    public void testEvaluationOnlyMarker() {
        String query = "((_Eval_ = true) && (FOO > '2' && FOO < '4'))";
        test(query, Collections.singleton("(_Eval_ = true) && (FOO > '2' && FOO < '4')"));
    }

    @Test
    public void testBoundedRange() {
        String query = "((_Bounded_ = true) && (FOO > '2' && FOO < '4'))";
        test(query, Collections.singleton("(_Bounded_ = true) && (FOO > '2' && FOO < '4')"));
    }

    @Test
    public void testListIvarator() {
        // TODO -- this is technically wrong
        String query = "((_List_ = true) && (FOO == '1' && FOO == '3'))";
        test(query, Sets.newHashSet("FOO == '1'", "FOO == '3'"));
    }

    @Ignore
    @Test
    public void testValueExceeded() {
        String query = "((_Value_ = true) && (FOO =~ 'ba.*'))";
        test(query, Collections.singleton("(_Value_ = true) && (FOO =~ 'ba.*')"));
    }

    @Ignore
    @Test
    public void testTermExceeded() {
        String query = "((_Term_ = true) && (FOO =~ 'ba.*'))";
        test(query, Collections.singleton("(_Term_ = true) && (FOO =~ 'ba.*')"));
    }

    @Ignore
    @Test
    public void testMethodInNestedUnion() {
        String query = "UUID == 'capone' && ( AGE.size() > 1 || AGE == '18')";
        test(query, Sets.newHashSet("UUID == 'capone'", "AGE == '18'"));
    }

    // and the other normal permutations

    // test some functions

    // test some negated things

    @Test
    public void testEqAndNotEq() {
        String query = "FOO == 'bar' && !(FOO == 'baz')";
    }

    // test some malformed queries just to see

    @Test
    public void testNotNe() {
        // this case should never happen in normal query planning. but it's interesting to see what would happen
        String query = "!(FOO != 'bar')";
        test(query, Collections.singleton("FOO != 'bar'"));
    }

    private void test(String query, Set<String> expected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

            Set<JexlNode> nodes = QueryTermVisitor.parse(script);

            Set<String> keys = nodes.stream().map(JexlStringBuildingVisitor::buildQueryWithoutParse).collect(Collectors.toSet());

            assertEquals(expected, keys);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        }
    }

}
