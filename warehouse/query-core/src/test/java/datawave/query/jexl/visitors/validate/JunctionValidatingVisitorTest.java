package datawave.query.jexl.visitors.validate;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JunctionValidatingVisitorTest {
    
    @Test
    public void testValidDisjunction() throws ParseException {
        String query = "FOO == 'bar' || FOO == 'baz'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertTrue(validate(script));
    }
    
    @Test
    public void testInvalidDisjunctionNoChild() {
        ASTOrNode disjunction = (ASTOrNode) JexlNodeFactory.createOrNode(Collections.emptyList());
        assertFalse(validate(disjunction));
    }
    
    @Test
    public void testInvalidDisjunctionOneChild() {
        ASTEQNode eqNode = (ASTEQNode) JexlNodeFactory.buildEQNode("FOO", "bar");
        ASTOrNode disjunction = (ASTOrNode) JexlNodeFactory.createOrNode(Arrays.asList(eqNode));
        assertFalse(validate(disjunction));
    }
    
    @Test
    public void testValidConjunction() throws ParseException {
        String query = "FOO == 'bar' && FOO == 'baz'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertTrue(validate(script));
    }
    
    @Test
    public void testInvalidConjunctionNoChild() {
        ASTAndNode conjunction = (ASTAndNode) JexlNodeFactory.createAndNode(Collections.emptyList());
        assertFalse(validate(conjunction));
    }
    
    @Test
    public void testInvalidConjunctionOneChild() {
        ASTEQNode eqNode = (ASTEQNode) JexlNodeFactory.buildEQNode("FOO", "bar");
        ASTAndNode conjunction = (ASTAndNode) JexlNodeFactory.createAndNode(Arrays.asList(eqNode));
        assertFalse(validate(conjunction));
    }
    
    // (A || (B && C))
    @Test
    public void testValidDisjunctionConjunction() throws ParseException {
        String query = "(FOO == 'bar' || (FOO2 == 'baz' && FOO3 == 'barzee'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        assertTrue(validate(script));
    }
    
    // (A || (B && null))
    @Test
    public void testValidDisjunctionInvalidConjunction() {
        ASTEQNode eqNode = (ASTEQNode) JexlNodeFactory.buildEQNode("FOO", "baz");
        ASTAndNode invalidConjunction = (ASTAndNode) JexlNodeFactory.createUnwrappedAndNode(Arrays.asList(eqNode));
        
        ASTEQNode termA = (ASTEQNode) JexlNodeFactory.buildEQNode("FOO", "bar");
        ASTOrNode validDisjunction = (ASTOrNode) JexlNodeFactory.createUnwrappedOrNode(Arrays.asList(termA, invalidConjunction));
        
        assertFalse(validate(validDisjunction));
        // nested AndNode prints like (term)
        assertEquals("(FOO == 'bar' || (FOO == 'baz'))", JexlStringBuildingVisitor.buildQueryWithoutParse(validDisjunction));
        assertEquals(ASTOrNode.class.getName(), validDisjunction.getClass().getName());
    }
    
    // (null && (B || C))
    @Test
    public void testInvalidDisjunctionValidConjunction() {
        ASTEQNode left = (ASTEQNode) JexlNodeFactory.buildEQNode("FOO", "bar");
        ASTEQNode right = (ASTEQNode) JexlNodeFactory.buildEQNode("FOO", "baz");
        ASTOrNode validDisjunction = (ASTOrNode) JexlNodeFactory.createUnwrappedOrNode(Arrays.asList(left, right));
        
        ASTAndNode invalidConjunction = (ASTAndNode) JexlNodeFactory.createUnwrappedAndNode(Arrays.asList(validDisjunction));
        
        assertFalse(validate(invalidConjunction));
        // top level AndNode does not print because of single child
        assertEquals("((FOO == 'bar' || FOO == 'baz'))", JexlStringBuildingVisitor.buildQueryWithoutParse(invalidConjunction));
        assertEquals(ASTAndNode.class.getName(), invalidConjunction.getClass().getName());
    }
    
    private boolean validate(JexlNode node) {
        return JunctionValidatingVisitor.validate(node);
    }
}
