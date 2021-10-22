package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static datawave.query.jexl.functions.GeoWaveFunctionsDescriptorTest.convertFunctionToIndexQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class GeoWavePruningVisitorTest {
    
    @Test
    public void testNonIntersectingTermIsPruned() throws ParseException {
        String function = "geowave:intersects(GEO_FIELD, 'POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))')";
        // Get the expanded geowave terms.
        String indexQuery = convertFunctionToIndexQuery(function, new ShardQueryConfiguration());
        
        // Add a term that should be pruned.
        String query = function + " && (GEO_FIELD == '0100' || " + indexQuery + ")";
        String expected = function + " && (" + indexQuery + ")";
        
        Multimap<String,String> expectedPrunedTerms = HashMultimap.create();
        expectedPrunedTerms.put("GEO_FIELD", "0100");
        
        assertResult(query, expected, expectedPrunedTerms);
    }
    
    @Test
    public void testPrunedWrappedTermDoesNotLeaveEmptyWrappedTerm() throws ParseException {
        String function = "geowave:intersects(GEO_FIELD, 'POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))')";
        // Get the expanded geowave terms.
        String indexQuery = convertFunctionToIndexQuery(function, new ShardQueryConfiguration());
        
        // Add a wrapped term that should be pruned.
        String query = function + " && ((GEO_FIELD == '0100') || " + indexQuery + ")";
        String expected = function + " && (" + indexQuery + ")";
        
        Multimap<String,String> expectedPrunedTerms = HashMultimap.create();
        expectedPrunedTerms.put("GEO_FIELD", "0100");
        
        assertResult(query, expected, expectedPrunedTerms);
    }
    
    private void assertResult(String original, String expected, Multimap<String,String> expectedPrunedTerms) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        
        Multimap<String,String> prunedTerms = HashMultimap.create();
        ASTJexlScript actualScript = GeoWavePruningVisitor.pruneTree(originalScript, prunedTerms, null);
        
        // Verify the result is as expected, with a valid lineage.
        JexlNodeAssert.assertThat(actualScript).isEqualTo(expected).hasValidLineage();
        
        // Verify there are no empty wrapped nodes.
        assertNoChildlessReferences(actualScript);
        
        // Verify the correct terms were pruned.
        assertEquals(prunedTerms, expectedPrunedTerms);
        
        // Verify the original script was not modified, and has a valid lineage.
        JexlNodeAssert.assertThat(originalScript).isEqualTo(original).hasValidLineage();
    }
    
    private void assertNoChildlessReferences(JexlNode node) {
        HasChildlessReferenceVisitor visitor = new HasChildlessReferenceVisitor();
        node.jjtAccept(visitor, null);
        assertFalse(visitor.hasChildlessNode);
    }
    
    private static class HasChildlessReferenceVisitor extends BaseVisitor {
        private boolean hasChildlessNode = false;
        
        @Override
        public Object visit(ASTReference node, Object data) {
            if (node.jjtGetNumChildren() == 0)
                hasChildlessNode = true;
            
            return super.visit(node, data);
        }
        
        @Override
        public Object visit(ASTReferenceExpression node, Object data) {
            if (node.jjtGetNumChildren() == 0)
                hasChildlessNode = true;
            
            return super.visit(node, data);
        }
    }
}
