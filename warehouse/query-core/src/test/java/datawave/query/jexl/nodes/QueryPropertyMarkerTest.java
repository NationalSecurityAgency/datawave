package datawave.query.jexl.nodes;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.QueryPropertyMarkerVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.scripting.JexlScriptEngine;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

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
    public void instanceOfAnyExceptTest() throws Exception {
        String query = "FIELD == 'value'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode evalOnly = ASTEvaluationOnly.create(script);
        Assert.assertFalse(QueryPropertyMarkerVisitor.instanceOfAnyExcept(evalOnly, Arrays.asList(ASTEvaluationOnly.class)));
        Assert.assertTrue(QueryPropertyMarkerVisitor.instanceOfAnyExcept(evalOnly, Arrays.asList(ASTDelayedPredicate.class)));
    }
}
