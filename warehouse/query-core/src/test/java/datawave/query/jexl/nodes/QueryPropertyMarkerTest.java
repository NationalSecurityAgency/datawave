package datawave.query.jexl.nodes;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
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
    
    @Test
    public void getPropertyMarker_happy_test() throws ParseException {
        String query = "FIELD == 'value'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        for (JexlNode eqNode : JexlASTHelper.getEQNodes(script)) {
            // create a delay
            JexlNode delayedNode = ASTDelayedPredicate.create(eqNode);
            Assert.assertEquals(delayedNode, QueryPropertyMarker.getQueryPropertyMarker(eqNode, null));
        }
    }
    
    @Test
    public void getPropertyMarker_null_test() {
        Assert.assertEquals(null, QueryPropertyMarker.getQueryPropertyMarker(null, null));
    }
    
    @Test
    public void getPropertyMarker_arbitraryReference_test() throws ParseException {
        String query = "FIELD == 'value'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        for (JexlNode eqNode : JexlASTHelper.getEQNodes(script)) {
            // create a delay
            ASTDelayedPredicate.create(eqNode);
            Assert.assertEquals(null, QueryPropertyMarker.getQueryPropertyMarker(eqNode.jjtGetParent(), null));
        }
    }
    
    @Test
    public void getPropertyMarker_nested_test() throws ParseException {
        String query = "FIELD == 'value'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        for (JexlNode eqNode : JexlASTHelper.getEQNodes(script)) {
            // create a delay that has an ExceededValueThresholdMarkerJexlNode inside
            ExceededValueThresholdMarkerJexlNode valueMarker = new ExceededValueThresholdMarkerJexlNode(eqNode);
            JexlNode delayedMarker = ASTDelayedPredicate.create(valueMarker);
            TreeFlatteningRebuildingVisitor.flatten(delayedMarker);
            Assert.assertEquals(valueMarker, QueryPropertyMarker.getQueryPropertyMarker(eqNode, ExceededValueThresholdMarkerJexlNode.class));
            Assert.assertEquals(delayedMarker, QueryPropertyMarker.getQueryPropertyMarker(eqNode, ASTDelayedPredicate.class));
        }
    }
    
    @Test
    public void getPropertyMarker_notPropertyMarker_test() throws ParseException {
        String query = "FIELD == 'value'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        for (JexlNode eqNode : JexlASTHelper.getEQNodes(script)) {
            Assert.assertEquals(null, QueryPropertyMarker.getQueryPropertyMarker(eqNode, ExceededValueThresholdMarkerJexlNode.class));
        }
        
        query = "FIELD == 'value1' AND FIELD == 'value2'";
        script = JexlASTHelper.parseJexlQuery(query);
        for (JexlNode eqNode : JexlASTHelper.getEQNodes(script)) {
            Assert.assertEquals(null, QueryPropertyMarker.getQueryPropertyMarker(eqNode, ExceededValueThresholdMarkerJexlNode.class));
        }
        
        query = "FIELD == 'value1' OR FIELD == 'value2'";
        script = JexlASTHelper.parseJexlQuery(query);
        for (JexlNode eqNode : JexlASTHelper.getEQNodes(script)) {
            Assert.assertEquals(null, QueryPropertyMarker.getQueryPropertyMarker(eqNode, ExceededValueThresholdMarkerJexlNode.class));
        }
    }
}
