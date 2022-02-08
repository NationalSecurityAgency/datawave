package datawave.query.jexl;

import com.google.common.collect.Sets;
import datawave.data.type.LcNoDiacriticsType;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

/**
 * Run all tests in {@link DatawaveInterpreterTest} but with a JexlEngine built with a partial interpreter.
 * <p>
 * Additional tests for events that contain incomplete fields
 */
public class DatawavePartialInterpreterTest extends DatawaveInterpreterTest {
    
    private final String INCOMPLETE_FIELD_A = "FIELD_A";
    private final String INCOMPLETE_FIELD_B = "FIELD_B";
    private final Set<String> incompleteFields = Sets.newHashSet(INCOMPLETE_FIELD_A, INCOMPLETE_FIELD_B);
    
    /**
     * Use a JexlEngine built with a partial interpreter
     *
     * @return a JexlEngine
     */
    @Override
    protected JexlEngine getJexlEngine() {
        return ArithmeticJexlEngines.getEngine(new HitListArithmetic(), true, incompleteFields);
    }
    
    @Override
    protected boolean matchResult(Object o) {
        return ArithmeticJexlEngines.isMatched(o, true);
    }
    
    @Test
    public void testSimpleEqualityAgainstExistingIncompleteField() {
        // field exists, can't be evaluated, defaults to true
        String query = "FIELD_A == 'something'";
        test(query, true);
        
        // can't be evaluated, defaults to true
        query = "FIELD_A != 'something'";
        test(query, true);
    }
    
    @Test
    public void testSimpleEqualityAgainstNonExistentIncompleteField() {
        // field doesn't exist, evaluates to false
        String query = "FIELD_B == 'something'";
        test(query, false);
        
        // field doesn't exist, evaluates to false
        query = "FIELD_B != 'something'";
        test(query, true);
    }
    
    @Test
    public void testUnions() {
        // field with incomplete field (TRUE || UNKNOWN) = TRUE
        String query = "FOO == 'bar' || FIELD_A == 'something'";
        test(query, true);
        
        // (FALSE || UNKNOWN) = UNKNOWN
        query = "FOO == 'fewer' || FIELD_A == 'something'";
        test(query, true);
        
        // (UNKNOWN || FALSE) = UNKNOWN (order of terms shouldn't matter)
        query = "FIELD_A == 'something' || FOO == 'fewer'";
        test(query, true);
        
        // two incomplete fields (UNKNOWN || UNKNOWN) = UNKNOWN
        query = "FIELD_A == 'something' || FIELD_A == 'nothing'";
        test(query, true);
        
        // incomplete field that exists with incomplete field that does not exist
        // two incomplete fields (UNKNOWN || FALSE) = UNKNOWN
        query = "FIELD_A == 'something' || FIELD_B == 'perfection'";
        test(query, true);
    }
    
    @Test
    public void testIntersections() {
        // field with incomplete field (TRUE && UNKNOWN) = TRUE
        String query = "FOO == 'bar' && FIELD_A == 'something'";
        test(query, true);
        
        // (FALSE && UNKNOWN) = FALSE
        query = "FOO == 'fewer' && FIELD_A == 'something'";
        test(query, false);
        
        // (UNKNOWN && FALSE) = FALSE (order of terms shouldn't matter)
        query = "FIELD_A == 'something' && FOO == 'fewer'";
        test(query, false);
        
        // two incomplete fields (UNKNOWN && UNKNOWN) = UNKNOWN
        query = "FIELD_A == 'something' && FIELD_A == 'nothing'";
        test(query, true);
        
        // incomplete field that exists with incomplete field that does not exist
        // two incomplete fields (UNKNOWN && FALSE) = FALSE
        query = "FIELD_A == 'something' && FIELD_B == 'perfection'";
        test(query, false);
    }
    
    // verifies the interpreter fully traverses the intersection
    @Test
    public void testLargeIntersections() {
        String query = "FOO == 'bar' && FOO == 'baz' && FIELD_A == 'barzee' && FIELD_A == 'zeebar'";
        test(query, true);
        
        query = "FIELD_A == 'barzee' && FIELD_A == 'zeebar' && FOO == 'bar' && FOO == 'baz'";
        test(query, true);
    }
    
    // verifies the interpreter fully traverses the union
    @Test
    public void testLargeUnions() {
        String query = "FOO == 'bar' || FOO == 'baz' || FIELD_A == 'barzee' || FIELD_A == 'zeebar'";
        test(query, true);
        
        query = "FIELD_A == 'barzee' || FIELD_A == 'zeebar' || FOO == 'bar' || FOO == 'baz'";
        test(query, true);
        
        query = "FOO == 'zip' || FOO == 'nada' || FOO == 'nope' || FIELD_A == 'maybe'";
        test(query, true);
    }
    
    @Test
    public void testMixOfUnionsAndIntersections() {
        String query = "((FOO == 'bar' && FOO == 'baz') || (FIELD_A == 'barzee' && FIELD_A == 'zeebar'))";
        test(query, true);
        
        query = "((FOO == 'bar' && FIELD_A == 'zeebar') || (FIELD_A == 'barzee' && FOO == 'baz'))";
        test(query, true);
        
        query = "((FOO == 'bar' || FOO == 'baz') && (FIELD_A == 'barzee' || FIELD_A == 'zeebar'))";
        test(query, true);
        
        query = "((FOO == 'bar' || FIELD_A == 'zeebar') && (FIELD_A == 'barzee' || FOO == 'baz'))";
        test(query, true);
    }
    
    // phrase with normal field -> true
    @Test
    public void testPhraseQueryAgainstNormalField() {
        // this phrase hits
        String query = "content:phrase(TEXT, termOffsetMap, 'red', 'dog') && TEXT == 'red' && TEXT == 'dog'";
        test(query, buildTermOffsetIncompleteContext(), true);
        
        // this phrase doesn't
        query = "content:phrase(TEXT, termOffsetMap, 'big', 'dog') && TEXT == 'big' && TEXT == 'dog'";
        test(query, buildTermOffsetIncompleteContext(), false);
    }
    
    // incomplete field cannot be evaluated, document must be returned
    @Test
    public void testPhraseQueryAgainstIncompleteField() {
        String query = "content:phrase(FIELD_A, termOffsetMap, 'red', 'dog') && FIELD_A == 'red' && FIELD_A == 'dog'";
        test(query, buildTermOffsetIncompleteContext(), true);
    }
    
    // normal field could evaluate, but presence of incomplete field means the document is returned
    @Test
    public void testPhraseQueryAgainstIncompleteFieldAndNormalField() {
        String query = "(content:phrase(TEXT, termOffsetMap, 'red', 'dog') && TEXT == 'red' && TEXT == 'dog') || (content:phrase(FIELD_A, termOffsetMap, 'red', 'dog') && FIELD_A == 'red' && FIELD_A == 'dog')";
        test(query, buildTermOffsetIncompleteContext(), true);
        
        query = "(content:phrase(FIELD_A, termOffsetMap, 'red', 'dog') && FIELD_A == 'red' && FIELD_A == 'dog') || (content:phrase(TEXT, termOffsetMap, 'red', 'dog') && TEXT == 'red' && TEXT == 'dog')";
        test(query, buildTermOffsetIncompleteContext(), true);
    }
    
    // normal field is not in the context so evaluates to false, but presence of incomplete field means the document is returned
    @Test
    public void testPhraseQueryAgainstIncompleteFieldAndMissingNormalField() {
        String query = "(content:phrase(ZEE, termOffsetMap, 'red', 'dog') && ZEE == 'red' && ZEE == 'dog') || (content:phrase(FIELD_A, termOffsetMap, 'red', 'dog') && FIELD_A == 'red' && FIELD_A == 'dog')";
        test(query, buildTermOffsetIncompleteContext(), true);
        
        query = "(content:phrase(FIELD_A, termOffsetMap, 'red', 'dog') && FIELD_A == 'red' && FIELD_A == 'dog') || (content:phrase(ZEE, termOffsetMap, 'red', 'dog') && ZEE == 'red' && ZEE == 'dog')";
        test(query, buildTermOffsetIncompleteContext(), true);
    }
    
    // #INCLUDE
    @Test
    public void testIncludeFunctionAgainstIncompleteField() {
        String query = "FOO == 'bar' && filter:regexInclude(FIELD_A,'ba.*')";
        test(query, buildDefaultIncompleteContext(), true);
    }
    
    /**
     * Evaluate a query against a context with an incomplete field
     * 
     * @param query
     *            the query
     * @param expectedResult
     *            the expected evaluation
     */
    protected void test(String query, boolean expectedResult) {
        test(query, buildDefaultIncompleteContext(), expectedResult);
    }
    
    /**
     * Builds a JexlContext with some incomplete fields.
     *
     * @return
     */
    protected JexlContext buildDefaultIncompleteContext() {
        JexlContext context = buildDefaultContext();
        context.set(INCOMPLETE_FIELD_A, "a1b2c3");
        return context;
    }
    
    /**
     * Builds a normal {@link DatawaveInterpreterTest#buildTermOffsetContext()} and adds term offsets for an incomplete field
     *
     * @return a JexlContext with additional term offsets
     */
    protected JexlContext buildTermOffsetIncompleteContext() {
        JexlContext context = buildTermOffsetContext();
        
        // a term offset map already exists for the TEXT field. No values are necessary for the
        // incomplete FIELD_A. The only necessary addition to the context is the kv pairs for
        // FIELD_A.
        
        //@formatter:off
        context.set("FIELD_A", new FunctionalSet(Arrays.asList(
                new ValueTuple("FIELD_A", "big", "big", new TypeAttribute<>(new LcNoDiacriticsType("big"), new Key("dt\0uid"), true)),
                new ValueTuple("FIELD_A", "red", "red", new TypeAttribute<>(new LcNoDiacriticsType("red"), new Key("dt\0uid"), true)),
                new ValueTuple("FIELD_A", "dog", "dog", new TypeAttribute<>(new LcNoDiacriticsType("dog"), new Key("dt\0uid"), true)))));
        //@formatter:on
        return context;
    }
}
