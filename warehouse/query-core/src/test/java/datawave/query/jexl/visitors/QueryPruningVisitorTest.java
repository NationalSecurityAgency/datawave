package datawave.query.jexl.visitors;

import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.DefaultArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.Tuple3;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class QueryPruningVisitorTest {
    @Test
    public void falseAndTest() throws ParseException {
        String query = "FIELD1 == 'x' && _NOFIELD_ == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void falseAndRewriteTest() throws ParseException {
        String query = "FIELD1 == 'x' && _NOFIELD_ == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        JexlEvaluation jexlEvaluation = new JexlEvaluation(JexlStringBuildingVisitor.buildQuery(reduced), new DefaultArithmetic());
        boolean jexlState = jexlEvaluation.apply(new Tuple3<>(new Key(), new Document(), new DatawaveJexlContext()));
        Assert.assertFalse(jexlState);
        
        Assert.assertEquals("((pruned = '" + query.replaceAll("'", "\\\\'") + "') && false)", JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals("false", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void trueRewriteTest() throws ParseException {
        String query = "true || (FIELD1 == '1' && filter:isNull(FIELD2))";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        JexlEvaluation jexlEvaluation = new JexlEvaluation(JexlStringBuildingVisitor.buildQuery(reduced), new DefaultArithmetic());
        boolean jexlState = jexlEvaluation.apply(new Tuple3<>(new Key(), new Document(), new DatawaveJexlContext()));
        Assert.assertTrue(jexlState);
        
        Assert.assertEquals("((pruned = '" + query.replaceAll("'", "\\\\'") + "') && true)", JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals("true", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void falseDoubleAndTest() throws ParseException {
        String query = "FIELD1 == 'x' && _NOFIELD_ == 'y' && FIELD2 == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void falseDoubleAndRewriteTest() throws ParseException {
        String query = "FIELD1 == 'x' && _NOFIELD_ == 'y' && FIELD2 == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        JexlEvaluation jexlEvaluation = new JexlEvaluation(JexlStringBuildingVisitor.buildQuery(reduced), new DefaultArithmetic());
        boolean jexlState = jexlEvaluation.apply(new Tuple3<>(new Key(), new Document(), new DatawaveJexlContext()));
        Assert.assertFalse(jexlState);
        
        Assert.assertEquals("((pruned = '" + query.replaceAll("'", "\\\\'") + "') && false)", JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals("false", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void unknownOrTest() throws ParseException {
        String query = "FIELD1 == 'x' || _NOFIELD_ == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void trueOrTest() throws ParseException {
        String query = "true || _NOFIELD_ == 'y'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.TRUE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void branchedTest() throws ParseException {
        String query = "FIELD1 == 'x' || (_NOFIELD_ == 'y' && FIELD2 == 'z')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void branchedRewriteTest() throws ParseException {
        String subtree = "_NOFIELD_ == 'y' && FIELD2 == 'z'";
        String query = "FIELD1 == 'x' || (" + subtree + ")";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        Assert.assertEquals("FIELD1 == 'x' || (((pruned = '" + subtree.replaceAll("'", "\\\\'") + "') && false))",
                        JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals("FIELD1 == 'x' || (false)", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void nestedBranchRewriteTest() throws ParseException {
        String query = "((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && _NOFIELD_ == 'z') || FIELD2 == 'z'";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        Assert.assertEquals("(((pruned = '(_NOFIELD_ == \\'x\\' || _NOFIELD_ == \\'y\\') && _NOFIELD_ == \\'z\\'') && false)) || FIELD2 == 'z'",
                        JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals("(false) || FIELD2 == 'z'", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void deeplyNestedBranchRewriteTest() throws ParseException {
        String query = "((_NOFIELD_ == 'x' || _NOFIELD_ == 'y') && (_NOFIELD_ == 'a' || _NOFIELD_ == 'b') && _NOFIELD_ == 'z') || FIELD2 == 'z'";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        Assert.assertEquals(
                        "(((pruned = '(_NOFIELD_ == \\'x\\' || _NOFIELD_ == \\'y\\') && (_NOFIELD_ == \\'a\\' || _NOFIELD_ == \\'b\\') && _NOFIELD_ == \\'z\\'') && false)) || FIELD2 == 'z'",
                        JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals("(false) || FIELD2 == 'z'", JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void unchangedTest() throws ParseException {
        String query = "FIELD1 == 'x' || (FIELD1 == 'y' && FIELD2 == 'z')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void unchangedRewriteTest() throws ParseException {
        String subtree = "FIELD1 == 'y' && FIELD2 == 'z'";
        String query = "FIELD1 == 'x' || (" + subtree + ")";
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode reduced = QueryPruningVisitor.reduce(script, true);
        
        Assert.assertEquals(query, JexlStringBuildingVisitor.buildQuery(reduced));
        Assert.assertEquals(query, JexlStringBuildingVisitor.buildQuery(QueryPruningVisitor.reduce(script, false)));
    }
    
    @Test
    public void combinationTest() throws ParseException {
        String query = "F(_NOFIELD_) == 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void andTrueTest() throws ParseException {
        String query = "true && !(_NOFIELD_ == 'z')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.TRUE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void orFalseTest() throws ParseException {
        String query = "false || _NOFIELD_ == 'z'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void notUnknownTest() throws ParseException {
        String query = "FIELD != 'a'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void notNoFieldTest() throws ParseException {
        String query = "_NOFIELD_ != 'a'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void notFalseTest() throws ParseException {
        String query = "!false";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.TRUE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void notTrueTest() throws ParseException {
        String query = "!true";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void methodTest() throws ParseException {
        String query = "x('a')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void functionTest() throws ParseException {
        String query = "filter:isNull(FIELD)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void GETest() throws ParseException {
        String query = "FIELD >= 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void GTTest() throws ParseException {
        String query = "FIELD > 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void LETest() throws ParseException {
        String query = "FIELD <= 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void LTTest() throws ParseException {
        String query = "FIELD < 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.getState(script), QueryPruningVisitor.TruthState.UNKNOWN);
    }
    
    @Test
    public void RETest() throws ParseException {
        String query = "FIELD =~ 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void NRTest() throws ParseException {
        String query = "FIELD !~ 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.UNKNOWN, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void GENoFieldTest() throws ParseException {
        String query = "_NOFIELD_ >= 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void GTNoFieldTest() throws ParseException {
        String query = "_NOFIELD_ > 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void LENoFieldTest() throws ParseException {
        String query = "_NOFIELD_ <= 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void LTNoFieldTest() throws ParseException {
        String query = "_NOFIELD_ < 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void RENoFieldTest() throws ParseException {
        String query = "_NOFIELD_ =~ 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
    }
    
    @Test
    public void NRNoFieldTest() throws ParseException {
        String query = "_NOFIELD_ !~ 'x'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals(QueryPruningVisitor.TruthState.FALSE, QueryPruningVisitor.getState(script));
    }
}
