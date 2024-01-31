package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.validate.ASTValidator;
import datawave.query.util.MockMetadataHelper;

public class PullupUnexecutableNodesVisitorTest {

    private Set<String> indexedFields;
    private Set<String> indexOnlyFields;
    private Set<String> nonEventFields;

    private ShardQueryConfiguration config;
    private MockMetadataHelper helper;

    private final ASTValidator validator = new ASTValidator();

    @Before
    public void setup() {
        indexedFields = Sets.newHashSet("FOO");
        indexOnlyFields = Sets.newHashSet("IO_FOO");
        nonEventFields = Sets.newHashSet("NE_FOO");

        config = ShardQueryConfiguration.create();
        config.setIndexedFields(indexedFields);

        helper = new MockMetadataHelper();
        helper.setIndexedFields(indexedFields);
        helper.setIndexOnlyFields(indexOnlyFields);
        helper.setNonEventFields(nonEventFields);
    }

    @Test
    public void testDelayedEq() {
        String query = "((_Delayed_ = true) && (FOO == 'bar'))";
        String expected = "FOO == 'bar'";
        test(query, expected);
    }

    @Test
    public void testDelayedUnion() {
        String query = "((_Delayed_ = true) && (FOO == 'bar' || FOO == 'baz'))";
        String expected = "(FOO == 'bar' || FOO == 'baz')";
        test(query, expected);
    }

    @Test
    public void testDelayedIntersection() {
        String query = "((_Delayed_ = true) && (FOO == 'bar' && FOO == 'baz'))";
        String expected = "(FOO == 'bar' && FOO == 'baz')";
        test(query, expected);
    }

    // and some negations

    @Test
    public void testNegatedEq() {
        String query = "!((_Delayed_ = true) && (FOO == 'bar'))";
        String expected = "!(FOO == 'bar')";
        test(query, expected);
    }

    @Test
    public void testNegatedDelayedUnion() {
        String query = "!((_Delayed_ = true) && (FOO == 'bar' || FOO == 'baz'))";
        String expected = "(!(FOO == 'bar') && !(FOO == 'baz'))"; // negations are pushed down prior to pull up
        test(query, expected);
    }

    @Test
    public void testNegatedDelayedIntersection() {
        String query = "!((_Delayed_ = true) && (FOO == 'bar' && FOO == 'baz'))";
        String expected = "(!(FOO == 'bar') || !(FOO == 'baz'))"; // negations are pushed down prior to pull up
        test(query, expected);
    }

    // junctions where one or more terms is delayed

    @Test
    public void testUnionWithDelayedTerm() {
        String query = "(FOO == 'bar' || ((_Delayed_ = true) && (FOO == 'baz')))";
        String expected = "(FOO == 'bar' || FOO == 'baz')";
        test(query, expected);
    }

    @Test
    public void testIntersectionWithDelayedTerm() {
        String query = "(NE_FOO == 'bar' && ((_Delayed_ = true) && (FOO == 'baz')))";
        String expected = "(NE_FOO == 'bar' && FOO == 'baz')";
        test(query, expected);
    }

    @Test
    public void testUnionWithNegatedDelayedTerm() {
        String query = "(FOO == 'bar' || !((_Delayed_ = true) && (FOO == 'baz')))";
        String expected = "(FOO == 'bar' || !(FOO == 'baz'))";
        test(query, expected);
    }

    @Test
    public void testIntersectionWithNegatedDelayedTerm() {
        String query = "(NE_FOO == 'bar' && !((_Delayed_ = true) && (FOO == 'baz')))";
        String expected = "(NE_FOO == 'bar' && !(FOO == 'baz'))";
        test(query, expected);
    }

    @Test
    public void testNegatedUnionWithDelayedTerm() {
        String query = "!(FOO == 'bar' || ((_Delayed_ = true) && (FOO == 'baz')))";
        String expected = "(!(FOO == 'bar') && !(FOO == 'baz'))"; // negations pushed, delayed marker correctly removed
        test(query, expected);
    }

    @Test
    public void testNegatedIntersectionWithDelayedTerm() {
        String query = "!(FOO == 'bar' && ((_Delayed_ = true) && (FOO == 'baz')))";
        String expected = "(!(FOO == 'bar') || !(FOO == 'baz'))"; // negations pushed, delayed marker correctly removed
        test(query, expected);
    }

    @Test
    public void testNegatedUnionWithNegatedDelayedTerm() {
        String query = "!(FOO == 'bar' || !((_Delayed_ = true) && (FOO == 'baz')))";
        String expected = "(!(FOO == 'bar') && FOO == 'baz')"; // negations pushed, flips negation of delayed term, delayed term correctly removed
        test(query, expected);
    }

    @Test
    public void testNegatedIntersectionWithNegatedDelayedTerm() {
        String query = "!(FOO == 'bar' && !((_Delayed_ = true) && (FOO == 'baz')))";
        String expected = "(!(FOO == 'bar') || FOO == 'baz')"; // negations pushed, flips negation of delayed term, delayed term correctly removed
        test(query, expected);
    }

    private void test(String query, String expected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            ASTJexlScript visitedScript = (ASTJexlScript) PullupUnexecutableNodesVisitor.pullupDelayedPredicates(script, false, config, indexedFields,
                            indexOnlyFields, nonEventFields, helper);

            String visitedString = JexlStringBuildingVisitor.buildQuery(visitedScript);
            ASTJexlScript expectedScript = JexlASTHelper.parseAndFlattenJexlQuery(expected);

            assertTrue("Expected " + expected + " but got " + visitedString, TreeEqualityVisitor.isEqual(expectedScript, visitedScript));
            assertTrue(validator.isValid(visitedScript));
            assertEquals(expected, visitedString);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        } catch (InvalidQueryTreeException e) {
            fail("Failed to validate query: " + query);
        }
    }

}
