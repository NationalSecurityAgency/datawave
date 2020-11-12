package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static datawave.query.jexl.functions.GeoWaveFunctionsDescriptorTest.convertFunctionToIndexQuery;
import static org.junit.Assert.assertTrue;

public class GeoWavePruningVisitorTest {
    
    @Test
    public void pruningVisitorTest() throws Exception {
        String query = "geowave:intersects(GEO_FIELD, 'POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))')";
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        
        // Get the expanded geowave terms, and add a term which would not normally be included for this polygon
        String expandedQuery = query + " && (GEO_FIELD == '0100' || " + convertFunctionToIndexQuery(query, config) + ")";
        
        // prune the tree, and see that the added term is removed
        Multimap<String,String> prunedTerms = HashMultimap.create();
        JexlNode prunedTree = GeoWavePruningVisitor.pruneTree(JexlASTHelper.parseJexlQuery(expandedQuery), prunedTerms, null);
        
        Assert.assertEquals(1, prunedTerms.entries().size());
        Map.Entry<String,String> term = prunedTerms.entries().iterator().next();
        
        Assert.assertEquals("GEO_FIELD", term.getKey());
        Assert.assertEquals("0100", term.getValue());
        
        Assert.assertEquals(query + " && (" + convertFunctionToIndexQuery(query, config) + ")", JexlStringBuildingVisitor.buildQuery(prunedTree));
        assertTrue(JexlASTHelper.validateLineage(prunedTree, true));
    }
    
    @Test
    public void emptyRefExpTest() throws Exception {
        String query = "geowave:intersects(GEO_FIELD, 'POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))')";
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        
        // Get the expanded geowave terms, and add a term which would not normally be included for this polygon within a reference expression
        String expandedQuery = query + " && ((GEO_FIELD == '0100') || " + convertFunctionToIndexQuery(query, config) + ")";
        
        // prune the tree, and see that the added term is removed
        Multimap<String,String> prunedTerms = HashMultimap.create();
        JexlNode prunedTree = GeoWavePruningVisitor.pruneTree(JexlASTHelper.parseJexlQuery(expandedQuery), prunedTerms, null);
        
        // ensure that there are no childless references or reference expressions in the tree
        RefExpVisitor refExpVisitor = new RefExpVisitor();
        prunedTree.jjtAccept(refExpVisitor, null);
        
        Assert.assertFalse(refExpVisitor.hasChildlessNode);
        assertTrue(JexlASTHelper.validateLineage(prunedTree, true));
    }
    
    private static class RefExpVisitor extends BaseVisitor {
        public boolean hasChildlessNode = false;
        
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
