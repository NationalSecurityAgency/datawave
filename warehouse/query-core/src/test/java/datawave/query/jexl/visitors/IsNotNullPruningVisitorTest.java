package datawave.query.jexl.visitors;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IsNotNullPruningVisitorTest {
    
    @Test
    public void testSimpleEqCase() {
        String query = "!(FOO == null) && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        test(query, expected);
    }
    
    @Test
    public void testSimpleErCase() {
        String query = "!(FOO == null) && FOO =~ 'ba.*'";
        String expected = "FOO =~ 'ba.*'";
        test(query, expected);
    }
    
    @Test
    public void testLargerCase() {
        String query = "!(FOO == null) && FOO == 'bar' && (FOO2 == 'bar2' || FOO3 == 'bar3')";
        String expected = "FOO == 'bar' && (FOO2 == 'bar2' || FOO3 == 'bar3')";
        test(query, expected);
    }
    
    @Test
    public void testNoOpCases() {
        String query = "!(FOO == null) || FOO == 'bar'";
        test(query, query);
        
        query = "!(FOO == null)"; // single IsNotNull term
        test(query, query);
        
        query = "!(FOO == null) || !(FOO == null)"; // union of repeated IsNotNull term
        test(query, query);
        
        query = "!(FOO == null) && !(FOO == null)"; // intersection of repeated IsNotNull term
        test(query, query);
        
        query = "!(FOO == null) || !(FOO2 == null)"; // union of different IsNotNull terms
        test(query, query);
        
        query = "!(FOO == null) && !(FOO2 == null)"; // intersection of different IsNotNull terms
        test(query, query);
    }
    
    // code that handles producing a flattened query tree with respect to wrapped single terms
    // should not modify marked nodes
    @Test
    public void testNoOpQueryPropertyMarkers() {
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ '.*r'))";
        test(query, query);
        
        query = "((_List_ = true) && (FOO_USER >= '09021f44' && FOO_USER <= '09021f47'))";
        test(query, query);
        
        query = "((_Term_ = true) && (FOO == 'bar'))";
        test(query, query);
        
        query = "((_Delayed_ = true) && (!(F1 == 'v1') || !((_Term_ = true) && (F2 == 'v2'))))";
        test(query, query);
    }
    
    // there's no realistic code path to delaying a subtree like this, but let's document expected behavior anyway
    @Test
    public void testContrivedEdgeCase() {
        String query = "((_Delayed_ = true) && (!(FOO == null) && FOO == 'bar'))";
        test(query, query);
    }
    
    @Test
    public void testNestedCase() {
        String query = "(!(FOO == null) && FOO == 'bar') || (FOO2 == 'bar2' && FOO3 == 'bar3')";
        String expected = "(FOO == 'bar') || (FOO2 == 'bar2' && FOO3 == 'bar3')";
        test(query, expected);
        
        // flip which branch contains the not null
        query = "(FOO2 == 'bar2' && FOO3 == 'bar3') || (!(FOO == null) && FOO == 'bar')";
        expected = "(FOO2 == 'bar2' && FOO3 == 'bar3') || (FOO == 'bar')";
        test(query, expected);
    }
    
    @Test
    public void testOneNotNullToManyEqs() {
        String query = "!(FOO == null) && FOO == 'bar' && FOO == 'baz'";
        String expected = "FOO == 'bar' && FOO == 'baz'";
        test(query, expected);
        
        query = "!(FOO == null) && FOO =~ 'bar.*' && FOO =~ 'baz.*'";
        expected = "FOO =~ 'bar.*' && FOO =~ 'baz.*'";
        test(query, expected);
    }
    
    @Test
    public void testMultipleNotNullsSameField() {
        String query = "!(FOO == null) && !(FOO == null) && FOO == 'bar'";
        String expected = "FOO == 'bar'";
        test(query, expected);
        
        // unordered
        query = "!(FOO == null) && FOO == 'bar' && !(FOO == null)";
        expected = "FOO == 'bar'";
        test(query, expected);
    }
    
    @Test
    public void testMultipleNotNullsDifferentFields() {
        String query = "!(FOO == null) && FOO == 'bar' && !(FOO2 == null) && FOO2 == 'baz'";
        String expected = "FOO == 'bar' && FOO2 == 'baz'";
        test(query, expected);
    }
    
    @Test
    public void testMultipleNotNullsAndEqsUnordered() {
        String query = "FOO2 == 'baz' && FOO == 'bar' && !(FOO == null) && !(FOO2 == null)";
        String expected = "FOO2 == 'baz' && FOO == 'bar'";
        test(query, expected);
    }
    
    @Test
    public void testMultipleNotNullsWithSingleEquality() {
        String query = "!(FOO == null) && FOO == 'bar' && !(FOO2 == null)";
        String expected = "FOO == 'bar' && !(FOO2 == null)";
        test(query, expected);
    }
    
    // every field of the middle union matches the IsNotNull term's field (FOO). Thus, we can still prune.
    @Test
    public void testSuperFunEdgeCase() {
        String query = "!(FOO == null) && (FOO == 'bar' || FOO == 'baz')";
        String expected = "(FOO == 'bar' || FOO == 'baz')";
        test(query, expected);
        
        // with extras
        query = "!(FOO == null) && (FOO == 'bar' || FOO == 'baz') && (FEE == 'fi' || FO == 'fum')";
        expected = "(FOO == 'bar' || FOO == 'baz') && (FEE == 'fi' || FO == 'fum')";
        test(query, expected);
        
        // union of same field, different node type (EQ and ER)
        query = "!(FOO == null) && (FOO == 'bar' || FOO =~ 'baz.*') && (FEE == 'fi' || FO == 'fum')";
        expected = "(FOO == 'bar' || FOO =~ 'baz.*') && (FEE == 'fi' || FO == 'fum')";
        test(query, expected);
        
        // union of same field, contains invalid node type (NR)
        query = "!(FOO == null) && (FOO == 'bar' || FOO !~ 'baz.*') && (FEE == 'fi' || FO == 'fum')";
        test(query, query);
    }
    
    private void test(String query, String expected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            ASTJexlScript visited = (ASTJexlScript) IsNotNullPruningVisitor.prune(script);
            ASTJexlScript expectedScript = JexlASTHelper.parseAndFlattenJexlQuery(expected);
            
            assertTrue(JexlStringBuildingVisitor.buildQueryWithoutParse(visited), TreeEqualityVisitor.checkEquality(visited, expectedScript).isEqual());
            assertTrue(ASTValidator.isValid(visited));
        } catch (ParseException | InvalidQueryTreeException e) {
            e.printStackTrace();
            fail("Failed to parse or validate query: " + query);
        }
    }
    
}
