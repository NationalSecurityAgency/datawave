package datawave.query.jexl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.data.type.LcNoDiacriticsType;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;

public class DatawavePartialInterpreterTest extends DatawaveInterpreterTest {

    private final String FIELD_A = "FIELD_A";
    private final String FIELD_B = "FIELD_B";
    private final Set<String> incompleteFields = Sets.newHashSet(FIELD_A, FIELD_B);

    /**
     * Use a JexlEngine built with a partial interpreter
     *
     * @return a JexlEngine
     */
    @Override
    protected JexlEngine getJexlEngine() {
        return ArithmeticJexlEngines.getEngine(new HitListArithmetic(), incompleteFields);
    }

    @Override
    protected boolean matchResult(Object o) {
        return ArithmeticJexlEngines.isMatched(o, true);
    }

    @Test
    public void testEqualitiesWithIncompleteFields() {
        //  @formatter:off
        Object[][] array = {
                {"FIELD_A == 'something'", true, true},     // field exists, can't be evaluated, defaults to true
                {"!(FIELD_A == 'something')", true, true},  // can't be evaluated, defaults to true
                {"FIELD_B == 'something'", false, false},   // field doesn't exist, evaluates to false
                {"!(FIELD_B == 'something')", true, true}, // field doesn't exist, evaluates to true
        };
        //  @formatter:on

        for (int i = 0; i < array.length; i++) {
            test((String) array[i][0], (Boolean) array[i][1], (Boolean) array[i][2]);
        }
    }

    @Test
    public void testEdgeCase() {
        //  @formatter:off
        Object[][] array = {
                {"(!(FOO == null) || !(FOO == null))", true, false},
                {"(!(FOO == null) && !(FOO == null))", true, false},
                {"(!(FOO == null) || !(FOO == null)) && (!(FOO == null) || !(FOO == null))", true, false},
                {"(!(FOO == null) && !(FOO == null)) || (!(FOO == null) && !(FOO == null))", true, false},
        };
        //  @formatter:on

        for (Object[] o : array) {
            test((String) o[0], (Boolean) o[1], (Boolean) o[2]);
        }
    }

    @Test
    public void testUnionsWithIncompleteFields() {
        //  @formatter:off
        Object[][] array = {
                {"FOO == 'bar' || FIELD_A == 'something'", true, false},            // (TRUE || UNKNOWN) = TRUE
                {"FIELD_A == 'something' || FOO == 'bar'", true, false},            // (UNKNOWN || TRUE) = TRUE
                {"FOO == 'fewer' || FIELD_A == 'something'", true, true},           // (FALSE || UNKNOWN) = UNKNOWN
                {"FIELD_A == 'something' || FOO == 'fewer'", true, true},           // (UNKNOWN || FALSE) = UNKNOWN (order of terms shouldn't matter)
                {"FIELD_A == 'something' || FIELD_A == 'nothing'", true, true},     // (UNKNOWN || UNKNOWN) = UNKNOWN
                {"FIELD_A == 'something' || FIELD_B == 'perfection'", true, true},  // (UNKNOWN || FALSE) = UNKNOWN
                // verifies the interpreter fully traverses the union
                {"FOO == 'bar' || FOO == 'baz' || FIELD_A == 'barzee' || FIELD_A == 'zeebar'", true, false},
                {"FIELD_A == 'barzee' || FIELD_A == 'zeebar' || FOO == 'bar' || FOO == 'baz'", true, false},
                {"FOO == 'zip' || FOO == 'nada' || FOO == 'nope' || FIELD_A == 'maybe'", true, true}
        };
        //  @formatter:on

        for (Object[] o : array) {
            test((String) o[0], (Boolean) o[1], (Boolean) o[2]);
        }
    }

    @Test
    public void testIntersectionsWithIncompleteFields() {
        //  @formatter:off
        Object[][] array = {
                {"FOO == 'bar' && FIELD_A == 'something'", true, true},     // (TRUE && UNKNOWN) = TRUE
                {"FOO == 'bar' && FIELD_A == 'something'", true, true},     // (UNKNOWN && TRUE) = TRUE
                {"FOO == 'fewer' && FIELD_A == 'something'", false, false}, // (FALSE && UNKNOWN) = FALSE
                {"FIELD_A == 'something' && FOO == 'fewer'", false, false}, // (UNKNOWN && FALSE) = FALSE (order of terms shouldn't matter)
                {"FIELD_A == 'something' && FIELD_A == 'nothing'", true, true},         // (UNKNOWN && UNKNOWN) = UNKNOWN
                {"FIELD_A == 'something' && FIELD_B == 'perfection'", false, false},    // (UNKNOWN && FALSE) = FALSE
                // verifies the interpreter fully traverses the intersection
                {"FOO == 'bar' && FOO == 'baz' && FIELD_A == 'barzee' && FIELD_A == 'zeebar'", true, true},
                {"FIELD_A == 'barzee' && FIELD_A == 'zeebar' && FOO == 'bar' && FOO == 'baz'", true, true},
        };
        //  @formatter:on

        for (Object[] o : array) {
            test((String) o[0], (Boolean) o[1], (Boolean) o[2]);
        }
    }

    @Test
    public void testMixOfUnionsAndIntersections() {
        //  @formatter:off
        Object[][] array = {
                {"((FOO == 'bar' && FOO == 'baz') || (FIELD_A == 'barzee' && FIELD_A == 'zeebar'))", true, false},
                {"((FOO == 'bar' && FIELD_A == 'zeebar') || (FIELD_A == 'barzee' && FOO == 'baz'))", true, true},
                {"((FOO == 'bar' || FOO == 'baz') && (FIELD_A == 'barzee' || FIELD_A == 'zeebar'))", true, true},
                {"((FOO == 'bar' || FIELD_A == 'zeebar') && (FIELD_A == 'barzee' || FOO == 'baz'))", true, false}
        };
        //  @formatter:on

        for (Object[] o : array) {
            test((String) o[0], (Boolean) o[1], (Boolean) o[2]);
        }
    }

    // phrase with normal field -> true
    @Test
    public void testPhraseQueryAgainstNormalField() {
        // this phrase hits
        String query = "content:phrase(TEXT, termOffsetMap, 'red', 'dog') && TEXT == 'red' && TEXT == 'dog'";
        test(buildTermOffsetIncompleteContext(), query, true, false);

        // this phrase doesn't
        query = "content:phrase(TEXT, termOffsetMap, 'big', 'dog') && TEXT == 'big' && TEXT == 'dog'";
        test(buildTermOffsetIncompleteContext(), query, false, false);
    }

    // incomplete field cannot be evaluated, document must be returned
    @Test
    public void testPhraseQueryAgainstIncompleteField() {
        String query = "content:phrase(FIELD_A, termOffsetMap, 'red', 'dog') && FIELD_A == 'red' && FIELD_A == 'dog'";
        test(buildTermOffsetIncompleteContext(), query, true, true);
    }

    // normal field could evaluate, but presence of incomplete field means the document is returned
    @Test
    public void testPhraseQueryAgainstIncompleteFieldAndNormalField() {
        String query = "(content:phrase(TEXT, termOffsetMap, 'red', 'dog') && TEXT == 'red' && TEXT == 'dog') || (content:phrase(FIELD_A, termOffsetMap, 'red', 'dog') && FIELD_A == 'red' && FIELD_A == 'dog')";
        testInputWithContext(buildTermOffsetIncompleteContext(), query, true);

        query = "(content:phrase(FIELD_A, termOffsetMap, 'red', 'dog') && FIELD_A == 'red' && FIELD_A == 'dog') || (content:phrase(TEXT, termOffsetMap, 'red', 'dog') && TEXT == 'red' && TEXT == 'dog')";
        testInputWithContext(buildTermOffsetIncompleteContext(), query, true);
    }

    // normal field is not in the context so evaluates to false, but presence of incomplete field means the document is returned
    @Test
    public void testPhraseQueryAgainstIncompleteFieldAndMissingNormalField() {
        String query = "(content:phrase(ZEE, termOffsetMap, 'red', 'dog') && ZEE == 'red' && ZEE == 'dog') || (content:phrase(FIELD_A, termOffsetMap, 'red', 'dog') && FIELD_A == 'red' && FIELD_A == 'dog')";
        test(buildTermOffsetIncompleteContext(), query, true, true);

        query = "(content:phrase(FIELD_A, termOffsetMap, 'red', 'dog') && FIELD_A == 'red' && FIELD_A == 'dog') || (content:phrase(ZEE, termOffsetMap, 'red', 'dog') && ZEE == 'red' && ZEE == 'dog')";
        test(buildTermOffsetIncompleteContext(), query, true, true);
    }

    // #INCLUDE
    @Test
    public void testIncludeFunctionAgainstIncompleteField() {
        String query = "FOO == 'bar' && filter:regexInclude(FIELD_A,'ba.*')";
        test(buildDefaultIncompleteContext(), query, true, true);
    }

    // UNKNOWN && DATE_FUNCTION -> UNKNOWN -> TRUE
    @Test
    public void testArithmeticWithFunctionOutputs() {

        //  @formatter:off
        Object[][] array = {
                // check for boolean coercion errors, long value is 80+ years
                {"FIELD_A == 'a1b2c3' && filter:getMaxTime(DEATH_DATE) - filter:getMinTime(BIRTH_DATE) > 2522880000000L", true, true},
                {"FIELD_A == 'a1b2c3' && filter:getMaxTime(DEATH_DATE) - filter:getMinTime(BIRTH_DATE) >= 2522880000000L", true, true},
                {"FIELD_A == 'a1b2c3' && filter:getMinTime(BIRTH_DATE) - filter:getMaxTime(DEATH_DATE) < 2522880000000L", true, true},
                {"FIELD_A == 'a1b2c3' && filter:getMinTime(BIRTH_DATE) - filter:getMaxTime(DEATH_DATE) <= 2522880000000L", true, true}
        };
        //  @formatter:on

        for (Object[] o : array) {
            test((String) o[0], (Boolean) o[1], (Boolean) o[2]);
        }
    }

    // negation tests

    @Test
    public void testNot() {
        String query = "!(FIELD_A == 'no')";
        test(buildDefaultIncompleteContext(), query, true, true);
    }

    @Test
    public void testGnarlyNegationsDude() {
        // !( (delayed(FALSE) || UNKNOWN || UNKNOWN) && UNKNOWN) ==> TRUE via UNKNOWN
        String query = "!((((_Delayed_ = true) && (_ANYFIELD_ =~ 'a-.*')) || FIELD_A == 'no' || FIELD_A == 'nope') && FIELD_A == 'nada')";
        test(buildDefaultIncompleteContext(), query, true, true);
    }

    @Test
    public void testAndNot() {
        // (TRUE && !(FALSE)) = TRUE
        String query = "FOO == 'bar' && !(FOO == 'nothing')";
        test(buildDefaultIncompleteContext(), query, true, false);

        // (TRUE && !(UNKNOWN)) = TRUE
        query = "FOO == 'bar' && !(FIELD_A == 'nothing')";
        test(buildDefaultIncompleteContext(), query, true, true);

        // (TRUE && !(delayed && UNKNOWN)) = TRUE
        query = "FOO == 'bar' && !((_Delayed_ = true) && (FIELD_A == 'nothing'))";
        test(buildDefaultIncompleteContext(), query, true, true);
    }

    @Test
    public void testOrNot() {
        // (TRUE || !(FALSE)) = TRUE
        String query = "FOO == 'bar' || !(FOO == 'nothing')";
        test(buildDefaultIncompleteContext(), query, true, false);

        // (TRUE || !(UNKNOWN)) = TRUE
        query = "FOO == 'bar' || !(FIELD_A == 'nothing')";
        test(buildDefaultIncompleteContext(), query, true, false);

        // (TRUE || !(delayed && UNKNOWN)) = TRUE
        query = "FOO == 'bar' || !((_Delayed_ = true) && (FIELD_A == 'nothing'))";
        test(buildDefaultIncompleteContext(), query, true, false);
    }

    /**
     * taken from {@link datawave.query.CompositeFunctionsTest}
     */
    @Test
    public void testFilterFunctionIncludeRegexSize_incomplete() {
        String query = "filter:includeRegex(FOO, 'bar').size() >= 1";
        testInputWithContext(buildDefaultIncompleteContext(), query, true);

        query = "1 >= filter:includeRegex(FOO, 'bar').size()";
        testInputWithContext(buildDefaultIncompleteContext(), query, true);
    }

    @Test
    public void testFilterFunctionMatchesAtLeastCountOf_incomplete() {
        String query = "FOO =~ 'ba.*' && filter:matchesAtLeastCountOf(2,FOO,'BAR','BAZ')";
        testInput(query, true);
    }

    // (f1.size() + f2.size()) > x is logically equivalent to f:matchesAtLeastCountOf(x,FIELD,'value')
    @Test
    public void testFilterFunctionInsteadOfMatchesAtLeastCountOf_incomplete() {
        String query = "(filter:includeRegex(FIELD_A, 'bar').size() + filter:includeRegex(FOO, 'baz').size()) >= 1";
        test(query, true, true);

        query = "(filter:includeRegex(FIELD_A, 'bar').size() + filter:includeRegex(FOO, 'baz').size()) > 0";
        test(query, true, true);

        query = "(filter:includeRegex(FIELD_A, 'bar').size() + filter:includeRegex(FOO, 'baz').size()) <= 1";
        test(query, true, true);

        query = "(filter:includeRegex(FIELD_A, 'bar').size() + filter:includeRegex(FOO, 'baz').size()) < 10";
        test(query, true, true);
    }

    @Test
    public void testGroupingFunctionsWithIncompleteFields() {
        //  @formatter:off
        Object[][] array = {
                //  getGroupsForMatchesInGroup() without a sibling method will flatten the resulting collection to a boolean
                {"grouping:getGroupsForMatchesInGroup(FIELD_A, 'MALE').size() == 2", true, true},
                //  only one group matches AGE == 21
                {"grouping:getGroupsForMatchesInGroup(FIELD_A, 'MALE', AGE, '21').size() == 1", true, true},
                //  for groups that match GENDER == 'male', there is a value for AGE less than 19
                {"FIELD_A.getValuesForGroups(grouping:getGroupsForMatchesInGroup(FIELD_A, 'MALE')) < 19", true, true},
                //  for group that matches GENDER = 'male' and AGE == '16', there is an AGE less than 19
                {"FIELD_A.getValuesForGroups(grouping:getGroupsForMatchesInGroup(FIELD_A, 'MALE', AGE, '16')) < 19", true, true},
                //  incomplete field NOT in the context is short-circuited as false
                {"FIELD_B.getValuesForGroups(grouping:getGroupsForMatchesInGroup(FIELD_A, 'MALE')) < 19", true, true},
                {"FIELD_B.getValuesForGroups(grouping:getGroupsForMatchesInGroup(FIELD_A, 'MALE', AGE, '16')) < 19", true, true}
        };
        //  @formatter:on

        for (Object[] o : array) {
            test((String) o[0], (Boolean) o[1], (Boolean) o[2]);
        }
    }

    @Test
    public void testMinMaxSizeMethods_incomplete() {
        //  @formatter:off
        Object[][] array = {
                //  min() with incomplete field, present in context
                {"FIELD_A.min() > 0", true, true},
                {"FIELD_A.min() >= 0", true, true},
                {"FIELD_A.min() == 0", true, true},
                {"FIELD_A.min() < 0", true, true},
                {"FIELD_A.min() <= 0", true, true},
                //  max() with incomplete field, present in context
                {"FIELD_A.max() > 0", true, true},
                {"FIELD_A.max() >= 0", true, true},
                {"FIELD_A.max() == 0", true, true},
                {"FIELD_A.max() < 0", true, true},
                {"FIELD_A.max() <= 0", true, true},
                //  size() with incomplete field, present in context
                {"FIELD_A.size() > 0", true, true},
                {"FIELD_A.size() >= 0", true, true},
                {"FIELD_A.size() == 0", true, true},
                {"FIELD_A.size() < 0", true, true},
                {"FIELD_A.size() <= 0", true, true},

                //  min() with incomplete field, absent from context
                {"FIELD_B.min() > 0", false, false},
                {"FIELD_B.min() >= 0", false, false},
                {"FIELD_B.min() == 0", false, false},
                {"FIELD_B.min() < 0", false, false},
                {"FIELD_B.min() <= 0", false, false},
                //  max() with incomplete field, absent from context
                {"FIELD_B.max() > 0", false, false},
                {"FIELD_B.max() >= 0", false, false},
                {"FIELD_B.max() == 0", false, false},
                {"FIELD_B.max() < 0", false, false},
                {"FIELD_B.max() <= 0", false, false},
                //  size() with incomplete field, absent from context
                {"FIELD_B.size() > 0", false, false},
                {"FIELD_B.size() >= 0", false, false},
                {"FIELD_B.size() == 0", false, false},
                {"FIELD_B.size() < 0", false, false},
                {"FIELD_B.size() <= 0", false, false},
        };
        //  @formatter:on

        for (Object[] o : array) {
            test(buildDefaultIncompleteContext(), (String) o[0], (Boolean) o[1], (Boolean) o[2]);
        }
    }

    // TODO -- consider breaking this apart and expanding
    @Test
    public void testMultiFieldedMinMaxFunctions() {
        //  @formatter:off
        Object[][] array = {
            //  incomplete and present with an absent field
            {"(FIELD_A || ABSENT).min() > 0", true, true},
            {"(ABSENT || FIELD_A).min() > 0", true, true},
            //  incomplete and absent with an absent field
            {"(FIELD_B || ABSENT).min() > 0", false, false},
            {"(ABSENT || FIELD_B).min() > 0", false, false},
            //  incomplete and present with a present field
            {"(FIELD_A || SPEED).min() > 0", true, true},
            {"(SPEED || FIELD_A).min() > 0", true, true},
            //  incomplete and absent with a present field
            {"(FIELD_B || SPEED).min() > 0", true, false},
            {"(SPEED || FIELD_B).min() > 0", true, false},
        };
        //  @formatter:on

        for (Object[] o : array) {
            test(buildDefaultIncompleteContext(), (String) o[0], (Boolean) o[1], (Boolean) o[2]);
        }
    }

    @Test
    public void testGreaterThanWithIncompleteField() {
        //  @formatter:off
        Object[][] array = {
                //  incomplete field present in context
                {"FIELD_A.greaterThan(17).size() == 2", true, true},
                {"FIELD_A.greaterThan(17).size() == 1", true, true},

                {"FIELD_A.compareWith(17,'>').size() == 2", true, true},
                {"FIELD_A.compareWith(17,'>').size() == 1", true, true},

                {"FIELD_A.compareWith(17,'>=').size() == 2", true, true},
                {"FIELD_A.compareWith(17,'>=').size() == 1", true, true},

                //  incomplete field not present in context
                {"FIELD_B.greaterThan(17).size() == 2", false, false},
                {"FIELD_B.greaterThan(17).size() == 1", false, false},

                {"FIELD_B.compareWith(17,'>').size() == 2", false, false},
                {"FIELD_B.compareWith(17,'>').size() == 1", false, false},

                {"FIELD_B.compareWith(17,'>=').size() == 2", false, false},
                {"FIELD_B.compareWith(17,'>=').size() == 1", false, false},
        };
        //  @formatter:on

        for (Object[] o : array) {
            test(buildDefaultIncompleteContext(), (String) o[0], (Boolean) o[1], (Boolean) o[2]);
        }
    }

    @Test
    public void testFieldEqualsFieldWithIncompleteFields() {
        //  @formatter:off
        Object[][] array = {
                {"FOO == FIELD_A", true, true},
                {"FOO == FIELD_B", false, false},
                {"filter:compare(FOO,'==','ANY',FIELD_A)", true, true},
                {"filter:compare(FOO,'==','ANY',FIELD_B)", false, false}
        };
        //  @formatter:on

        for (Object[] o : array) {
            test(buildDefaultIncompleteContext(), (String) o[0], (Boolean) o[1], (Boolean) o[2]);
        }
    }

    /**
     * Add incomplete fields to the default context
     *
     * @param query
     *            the query
     * @param expectedResult
     *            the expected evaluation
     */
    @Override
    protected void testInput(String query, boolean expectedResult) {
        testInputWithContext(buildDefaultIncompleteContext(), query, expectedResult);
    }

    protected void test(String query, boolean expectedResult, boolean expectedCallbackState) {
        test(buildDefaultIncompleteContext(), query, expectedResult, expectedCallbackState);
    }

    /**
     * Builds a JexlContext with some incomplete fields.
     *
     * @return a JexlContext with incomplete fields
     */
    protected JexlContext buildDefaultIncompleteContext() {
        JexlContext context = buildDefaultContext();
        //@formatter:off
        context.set(FIELD_A, new FunctionalSet(Collections.singletonList(
                new ValueTuple(FIELD_A, "a1b2c3", "a1b2c3", new TypeAttribute<>(new LcNoDiacriticsType("a1b2c3"), docKey, true)))));
        //@formatter:on
        return context;
    }

    /**
     * adds term offsets for an incomplete field
     *
     * @return a JexlContext with additional term offsets
     */
    protected JexlContext buildTermOffsetIncompleteContext() {
        JexlContext context = buildDefaultContext();

        // a term offset map already exists for the TEXT field. No values are necessary for the
        // incomplete FIELD_A. The only necessary addition to the context is the kv pairs for
        // FIELD_A.

        //@formatter:off
        context.set(FIELD_A, new FunctionalSet(Arrays.asList(
                new ValueTuple(FIELD_A, "big", "big", new TypeAttribute<>(new LcNoDiacriticsType("big"), docKey, true)),
                new ValueTuple(FIELD_A, "red", "red", new TypeAttribute<>(new LcNoDiacriticsType("red"), docKey, true)),
                new ValueTuple(FIELD_A, "dog", "dog", new TypeAttribute<>(new LcNoDiacriticsType("dog"), docKey, true)))));
        //@formatter:on
        return context;
    }
}
