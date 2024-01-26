package datawave.query.predicate;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.query.jexl.JexlASTHelper;

public class NegationPredicateTest {

    @Test
    public void testEquals() throws ParseException {
        String query = "fieldA == 'value'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        NegationPredicate predicate = new NegationPredicate();
        assertFalse(predicate.apply(script));
    }

    @Test
    public void testNotEquals() throws ParseException {
        Set<String> indexOnlyFields = Sets.newHashSet();
        indexOnlyFields.add("fieldA");
        String query = "fieldA != 'value'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        NegationPredicate predicate = new NegationPredicate(indexOnlyFields);
        assertTrue(predicate.apply(script));
    }

    @Test
    public void testFunction() throws ParseException {
        String query = "fieldA == 'value' and filter:isNull(fieldB)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        NegationPredicate predicate = new NegationPredicate();
        assertFalse(predicate.apply(script));
    }

    @Test
    public void testEqualsNot() throws ParseException {
        String query = "!(fieldA == 'value' and fieldC == 'value2') and filter:isNull(fieldB)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        NegationPredicate predicate = new NegationPredicate();
        assertTrue(predicate.apply(script));
    }

    @Test
    public void testEqualsNotNot() throws ParseException {
        String query = "!(!(fieldA == 'value' and fieldC == 'value2')) and filter:isNull(fieldB)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        NegationPredicate predicate = new NegationPredicate();
        assertTrue(predicate.apply(script));
    }

    @Test
    public void testNot() throws ParseException {
        String query = "!(filter:inNull(fieldB) and filter:isNull(fieldC)) and fieldA=='value'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        NegationPredicate predicate = new NegationPredicate();
        assertFalse(predicate.apply(script));
    }

    @Test
    public void testFunctionNot() throws ParseException {
        String query = "fieldA == 'value' and !(filter:inNull(fieldB))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        NegationPredicate predicate = new NegationPredicate();
        assertFalse(predicate.apply(script));
    }
}
