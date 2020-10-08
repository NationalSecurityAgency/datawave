package datawave.query.jexl.nodes;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Assert;
import org.junit.Test;

public class QueryPropertyMarkerTest {
    
    @Test
    public void instanceOfTest() throws Exception {
        String baseQuery = "(GEO >= '0202' && GEO <= '020d') && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8')";
        JexlNode baseQueryNode = JexlASTHelper.parseJexlQuery(baseQuery);
        JexlNode delayedNode = ASTDelayedPredicate.create(baseQueryNode);
        Assert.assertTrue(ASTDelayedPredicate.instanceOf(delayedNode));
        
        JexlNode sourceNode = ASTDelayedPredicate.getDelayedPredicateSource(delayedNode);
        Assert.assertEquals(baseQuery, JexlStringBuildingVisitor.buildQuery(sourceNode));
        
        String delayedQueryString = JexlStringBuildingVisitor.buildQuery(delayedNode);
        JexlNode reconstructedDelayedNode = JexlASTHelper.parseJexlQuery(delayedQueryString);
        Assert.assertTrue(ASTDelayedPredicate.instanceOf(reconstructedDelayedNode));
        
        sourceNode = ASTDelayedPredicate.getDelayedPredicateSource(delayedNode);
        Assert.assertEquals(baseQuery, JexlStringBuildingVisitor.buildQuery(sourceNode));
    }
}
