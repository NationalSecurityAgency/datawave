package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import org.apache.commons.jexl2.parser.ASTCompositePredicate;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class QueryPropertyMarkerVisitorTest {
    
    @Test
    public void instanceOfTest() throws Exception {
        String query = "((ASTDelayedPredicate = true) && (((ASTCompositePredicate = true) && ((GEO >= '0202' && GEO <= '020d') && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8')))))";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        List<JexlNode> sourceNodes = new ArrayList<>();
        
        Assert.assertTrue(QueryPropertyMarkerVisitor.instanceOf(node, ASTDelayedPredicate.class, sourceNodes));
        Assert.assertEquals(1, sourceNodes.size());
        Assert.assertEquals("(ASTCompositePredicate = true) && ((GEO >= '0202' && GEO <= '020d') && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))",
                        JexlStringBuildingVisitor.buildQuery(sourceNodes.get(0)));
        
        sourceNodes.clear();
        Assert.assertFalse(QueryPropertyMarkerVisitor.instanceOf(node, ASTCompositePredicate.class, sourceNodes));
        Assert.assertEquals(0, sourceNodes.size());
    }
}
