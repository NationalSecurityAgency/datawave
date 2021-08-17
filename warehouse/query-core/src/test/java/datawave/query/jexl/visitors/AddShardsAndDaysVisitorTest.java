package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AddShardsAndDaysVisitorTest {
    
    private static final Logger log = Logger.getLogger(AddShardsAndDaysVisitorTest.class);
    
    @Test
    public void testNullNode() {
        assertNull(AddShardsAndDaysVisitor.update(null, "20190314_2"));
    }
    
    @Test
    public void testNullShardsAndDaysHint() throws ParseException {
        String original = "FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(null, original, expected);
    }
    
    @Test
    public void testEmptyShardsAndDaysHint() throws ParseException {
        String original = "FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(" ", original, expected);
    }
    
    @Test
    public void testShardsAndDaysHintsWithOnlyWhitespaceAndCommas() throws ParseException {
        String original = "FOO == 'bar'";
        String expected = "FOO == 'bar'";
        visitAndValidate(" ,  , , ,", original, expected);
    }
    
    /**
     * Verify that duplicate SHARD_AND_DAY hints are not added to an existing SHARD_AND_DAY assignment.
     */
    @Test
    public void testAddingDuplicateValueToAssignment() throws ParseException {
        String original = "(FOO == 'bar') && (SHARDS_AND_DAYS = '20190314_1,20190314_2')";
        String expected = "(FOO == 'bar') && (SHARDS_AND_DAYS = '20190314_1,20190314_2')";
        visitAndValidate("20190314_2", original, expected);
    }
    
    /**
     * Verify that new SHARD_AND_DAY hint are added to an existing SHARD_AND_DAY assignment.
     */
    @Test
    public void testAddingSingleValueToAssignment() throws ParseException {
        String original = "(FOO == 'bar') && (SHARDS_AND_DAYS = '20190314_1')";
        String expected = "(FOO == 'bar') && (SHARDS_AND_DAYS = '20190314_1,20190314_2')";
        visitAndValidate("20190314_2", original, expected);
    }
    
    /**
     * Verify that multiple new/duplicate SHARD_AND_DAY hints are handled correctly for an existing SHARD_AND_DAY assignment.
     */
    @Test
    public void testAddingMultipleValuesToAssignment() throws ParseException {
        String original = "(FOO == 'bar') && (SHARDS_AND_DAYS = '20190314_1')";
        String expected = "(FOO == 'bar') && (SHARDS_AND_DAYS = '20190314_1,20190314_2,20190314_3')";
        visitAndValidate("20190314_2,20190314_1,20190314_3", original, expected);
    }
    
    /**
     * Verify that a new SHARD_AND_DAYS assignment node is added correctly for a simplistic unwrapped query.
     */
    @Test
    public void testAddingNewAssignmentToUnwrappedSimpleQuery() throws ParseException {
        String original = "FOO == 'bar'";
        String expected = "((FOO == 'bar') && (SHARDS_AND_DAYS = '20190314_1,20190314_2'))";
        visitAndValidate("20190314_1,20190314_2", original, expected);
    }
    
    /**
     * Verify that a new SHARD_AND_DAYS assignment node is added correctly for an unwrapped AND node.
     */
    @Test
    public void testAddingNewAssignmentToUnwrappedAndNode() throws ParseException {
        String original = "FOO == 'bar' && FOOLY == 'apple'";
        String expected = "((FOO == 'bar' && FOOLY == 'apple') && (SHARDS_AND_DAYS = '20190314_1,20190314_2'))";
        visitAndValidate("20190314_1,20190314_2", original, expected);
    }
    
    /**
     * Verify that a new SHARD_AND_DAYS assignment node is added correctly for an unwrapped OR node.
     */
    @Test
    public void testAddingNewAssignmentToUnwrappedOrNode() throws ParseException {
        String original = "FOO == 'bar' || FOOLY == 'apple'";
        String expected = "((FOO == 'bar' || FOOLY == 'apple') && (SHARDS_AND_DAYS = '20190314_1,20190314_2'))";
        visitAndValidate("20190314_1,20190314_2", original, expected);
    }
    
    /**
     * Verify that a new SHARD_AND_DAYS assignment node is added correctly for an wrapped node.
     */
    @Test
    public void testAddingNewAssignmentToWrappedNode() throws ParseException {
        String original = "(FOO == 'bar' || FOOLY == 'apple')";
        String expected = "((FOO == 'bar' || FOOLY == 'apple') && (SHARDS_AND_DAYS = '20190314_1,20190314_2'))";
        visitAndValidate("20190314_1,20190314_2", original, expected);
    }
    
    /**
     * Verify that a new SHARDS_AND_DAYS hint will be correctly added to an empty SHARDS_AND_DAYS assignment node.
     */
    @Test
    public void testAddingNewValueToEmptyShardsAndDaysAssignment() throws ParseException {
        String original = "(FOO == 'bar') && (SHARDS_AND_DAYS = '')";
        String expected = "(FOO == 'bar') && (SHARDS_AND_DAYS = '20190314_1')";
        visitAndValidate("20190314_1", original, expected);
    }
    
    private void visitAndValidate(String shardsAndDays, String original, String expected) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        // Add the specified shards and days hint.
        ASTJexlScript visitedScript = AddShardsAndDaysVisitor.update(originalScript, shardsAndDays);
        
        // Verify the script is as expected, and has a valid lineage.
        assertScriptEquality(visitedScript, expected);
        assertLineage(visitedScript);
        
        // Verify the original script was not modified, and still has a valid lineage.
        assertScriptEquality(originalScript, original);
        assertLineage(originalScript);
    }
    
    private void assertScriptEquality(ASTJexlScript actualScript, String expected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedScript, actualScript);
        if (!comparison.isEqual()) {
            log.error("Actual " + PrintingVisitor.formattedQueryString(actualScript));
        }
        assertTrue(comparison.getReason(), comparison.isEqual());
    }
    
    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }
}
