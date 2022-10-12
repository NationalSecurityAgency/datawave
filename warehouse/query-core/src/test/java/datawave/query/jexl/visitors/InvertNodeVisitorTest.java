package datawave.query.jexl.visitors;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InvertNodeVisitorTest {
    
    private final ASTValidator validator = new ASTValidator();
    
    @Test
    public void testInvertEq() {
        test("null == FOO", "FOO == null");
        test("'1' == FOO", "FOO == '1'");
        test("true == FOO", "FOO == true");
        test("false == FOO", "FOO == false");
    }
    
    @Test
    public void testInvertNe() {
        test("null != FOO", "FOO != null");
        test("'1' != FOO", "FOO != '1'");
        test("true != FOO", "FOO != true");
        test("false != FOO", "FOO != false");
    }
    
    @Test
    public void testInvertLt() {
        test("null < FOO", "FOO > null");
        test("'1' < FOO", "FOO > '1'");
        test("true < FOO", "FOO > true");
        test("false < FOO", "FOO > false");
    }
    
    @Test
    public void testInvertGt() {
        test("null > FOO", "FOO < null");
        test("'1' > FOO", "FOO < '1'");
        test("true > FOO", "FOO < true");
        test("false > FOO", "FOO < false");
    }
    
    @Test
    public void testInvertLe() {
        test("null <= FOO", "FOO >= null");
        test("'1' <= FOO", "FOO >= '1'");
        test("true <= FOO", "FOO >= true");
        test("false <= FOO", "FOO >= false");
    }
    
    @Test
    public void testInvertGe() {
        test("null > FOO", "FOO < null");
        test("'1' > FOO", "FOO < '1'");
        test("true > FOO", "FOO < true");
        test("false > FOO", "FOO < false");
    }
    
    @Test
    public void testInvertEr() {
        test("null =~ FOO", "FOO =~ null");
        test("'1' =~ FOO", "FOO =~ '1'");
        test("true =~ FOO", "FOO =~ true");
        test("false =~ FOO", "FOO =~ false");
    }
    
    @Test
    public void testInvertNr() {
        test("null !~ FOO", "FOO !~ null");
        test("'1' !~ FOO", "FOO !~ '1'");
        test("true !~ FOO", "FOO !~ true");
        test("false !~ FOO", "FOO !~ false");
    }
    
    private void test(String query, String expected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            ASTJexlScript inverted = InvertNodeVisitor.invertSwappedNodes(script);
            
            // query strings match
            String invertedString = JexlStringBuildingVisitor.buildQueryWithoutParse(inverted);
            assertEquals(expected, invertedString);
            
            // scripts match
            ASTJexlScript expectedScript = JexlASTHelper.parseAndFlattenJexlQuery(expected);
            assertTrue(TreeEqualityVisitor.isEqual(expectedScript, inverted));
            
            // visited script is valid
            assertTrue(validator.isValid(inverted));
            
        } catch (ParseException e) {
            fail("Could not parse query: " + query);
        } catch (InvalidQueryTreeException e) {
            fail("Query failed after visit: " + query);
        }
    }
}
