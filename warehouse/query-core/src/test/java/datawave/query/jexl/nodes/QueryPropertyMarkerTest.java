package datawave.query.jexl.nodes;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.QueryPropertyMarkerVisitor;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryPropertyMarkerTest {
    
    @Test
    public void instanceOfTest() throws Exception {
        String baseQuery = "(GEO >= '0202' && GEO <= '020d') && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8')";
        JexlNode baseQueryNode = JexlASTHelper.parseJexlQuery(baseQuery);
        JexlNode delayedNode = ASTDelayedPredicate.create(baseQueryNode);
        assertTrue(ASTDelayedPredicate.instanceOf(delayedNode));
        
        JexlNode sourceNode = ASTDelayedPredicate.getDelayedPredicateSource(delayedNode);
        assertEquals(baseQuery, JexlStringBuildingVisitor.buildQuery(sourceNode));
        
        String delayedQueryString = JexlStringBuildingVisitor.buildQuery(delayedNode);
        JexlNode reconstructedDelayedNode = JexlASTHelper.parseJexlQuery(delayedQueryString);
        assertTrue(ASTDelayedPredicate.instanceOf(reconstructedDelayedNode));
        
        sourceNode = ASTDelayedPredicate.getDelayedPredicateSource(delayedNode);
        assertEquals(baseQuery, JexlStringBuildingVisitor.buildQuery(sourceNode));
    }
    
    @Test
    public void testGetQueryPropertySource() throws ParseException {
        String source = "FOO == 'bar'";
        JexlNode sourceNode = JexlASTHelper.parseJexlQuery(source);
        
        JexlNode delayedNode = ASTDelayedPredicate.create(sourceNode);
        String delayedString = JexlStringBuildingVisitor.buildQueryWithoutParse(delayedNode);
        assertEquals(delayedString, "((_Delayed_ = true) && (FOO == 'bar'))");
        
        JexlNode parsedSource = QueryPropertyMarker.getQueryPropertySource(delayedNode, ASTDelayedPredicate.class);
        assertEquals(sourceNode, parsedSource);
    }
    
    @Test
    public void instanceOfAnyExceptTest() throws Exception {
        String query = "FIELD == 'value'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode evalOnly = ASTEvaluationOnly.create(script);
        Assert.assertFalse(QueryPropertyMarkerVisitor.instanceOfAnyExcept(evalOnly, ASTEvaluationOnly.class));
        Assert.assertTrue(QueryPropertyMarkerVisitor.instanceOfAnyExcept(evalOnly, ASTDelayedPredicate.class));
    }
    
    @Test
    public void verifyEachSubclassHasDistinctLabel() {
        
    }
}
