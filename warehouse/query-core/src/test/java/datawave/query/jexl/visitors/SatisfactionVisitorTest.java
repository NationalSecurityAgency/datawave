package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.query.jexl.JexlASTHelper;

public class SatisfactionVisitorTest {

    private static final Set<String> indexOnlyFields = Collections.singleton("IDXONLY");
    private static final Set<String> includeReferences = Collections.unmodifiableSet(Sets.newHashSet("INCLUDEME", "MAKE", "COLOR", "OWNER", "BBOX_USER"));
    private static final Set<String> excludeReferences = Collections.singleton("EXCLUDEME");

    @Test
    public void test() throws Exception {
        assertFalse(isQueryFullySatisfied("MAKE == 'Ford' && COLOR == 'red' && OWNER != null"));
        assertFalse(isQueryFullySatisfied("MAKE == 'Ford' && COLOR == 'red' && OWNER == null"));
        assertFalse(isQueryFullySatisfied("filter:includeRegex(MAKE, 'vw') && COLOR == 'red'"));
        assertFalse(isQueryFullySatisfied("geo:intersects_bounding_box(BBOX_USER, 50.932610, 51.888420, 35.288080, 35.991210)"));
        assertFalse(isQueryFullySatisfied("MAKE == 'Ford' && EXCLUDEME == 'foo'"));
        assertFalse(isQueryFullySatisfied("FOO_USER >= '09021f44' && FOO_USER <= '09021f47'"));
        assertFalse(isQueryFullySatisfied("(MAKE == null || ((_Delayed_ = true) && (MAKE == '020')) || MAKE == null)"));

        assertTrue(isQueryFullySatisfied("MAKE == 'Ford' && COLOR == 'red'"));
        assertTrue(isQueryFullySatisfied("f:between(COLOR, 'red', 'rouge')"));
        assertTrue(isQueryFullySatisfied("((_Value_ = true) && (FOO_USER >= '09021f44' && FOO_USER <= '09021f47'))"));
        assertTrue(isQueryFullySatisfied("((_List_ = true) && (FOO_USER >= '09021f44' && FOO_USER <= '09021f47'))"));
    }

    private boolean isQueryFullySatisfied(String query) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        SatisfactionVisitor visitor = new SatisfactionVisitor(indexOnlyFields, includeReferences, excludeReferences, true);
        visitor.visit(script, null);
        return visitor.isQueryFullySatisfied;
    }

    private final Set<String> indexed = Set.of("INDEXED_FIELD", "INDEX_ONLY_FIELD");
    private final Set<String> indexOnly = Set.of("INDEX_ONLY_FIELD");

    @Test
    public void testEq() {
        test(true, "INDEXED_FIELD == 'a'");
        test(true, "INDEX_ONLY_FIELD == 'a'");
        test(false, "EVENT_ONLY_FIELD == 'a'");
    }

    @Test
    public void testNe() {
        test(true, "INDEXED_FIELD != 'a'");
        test(true, "INDEX_ONLY_FIELD != 'a'");
        test(false, "EVENT_ONLY_FIELD != 'a'");
    }

    @Test
    public void testNotEq() {
        // this term is absolutely executable on the field index
        test(false, "!(INDEXED_FIELD == 'a')");
        // this term is absolutely executable on the field index
        test(false, "!(INDEX_ONLY_FIELD == 'a')");
        // a special iterator that runs against the event column could be used if we really wanted to, as in the case of tokenized event fields (builds two
        // iterators on against TF and the other against the event column)
        test(false, "!(EVENT_ONLY_FIELD == 'a')");
    }

    @Test
    public void testEqNullLiteral() {
        test(false, "INDEXED_FIELD == null");
        test(false, "INDEX_ONLY_FIELD == null");
        test(false, "EVENT_ONLY_FIELD == null");
    }

    @Test
    public void testNeNullLiteral() {
        test(false, "INDEXED_FIELD != null");
        test(false, "INDEX_ONLY_FIELD != null");
        test(false, "EVENT_ONLY_FIELD != null");
    }

    @Test
    public void testNotEqNullLiteral() {
        test(false, "!(INDEXED_FIELD == null)");
        test(false, "!(INDEX_ONLY_FIELD == null)");
        test(false, "!(EVENT_ONLY_FIELD == null)");
    }

    @Test
    public void testRangeOperators() {
        // test LT, GT, LE, GE operators in isolation (not part of a bounded range)
        test(false, "INDEXED_FIELD < 'a'");
        test(false, "INDEX_ONLY_FIELD < 'a'");
        test(false, "EVENT_ONLY_FIELD < 'a'");

        test(false, "INDEXED_FIELD > 'a'");
        test(false, "INDEX_ONLY_FIELD > 'a'");
        test(false, "EVENT_ONLY_FIELD > 'a'");

        test(false, "INDEXED_FIELD <= 'a'");
        test(false, "INDEX_ONLY_FIELD <= 'a'");
        test(false, "EVENT_ONLY_FIELD <= 'a'");

        test(false, "INDEXED_FIELD >= 'a'");
        test(false, "INDEX_ONLY_FIELD >= 'a'");
        test(false, "EVENT_ONLY_FIELD >= 'a'");
    }

    @Test
    public void testExceededValueMarker() {
        // exceeded value regex
        test(true, "((_Value_ = true) && (INDEXED_FIELD =~ 'ba.*'))");
        test(true, "((_Value_ = true) && (INDEX_ONLY_FIELD =~ 'ba.*'))");
        // a regex against an event only field should get marked as evaluation only, but document
        // the behavior here. This term should return false, but returns true instead.
        test(true, "((_Value_ = true) && (EVENT_ONLY_FIELD =~ 'ba.*'))");

        test(true, "((_Value_ = true) && ((_Bounded_ = true) && (INDEXED_FIELD > '1' && INDEXED_FIELD < '2')))");
        test(true, "((_Value_ = true) && ((_Bounded_ = true) && (INDEX_ONLY_FIELD > '1' && INDEX_ONLY_FIELD < '2')))");
        // this should be false. In practice this will never happen, but if it did the range should be marked _eval_
        test(true, "((_Value_ = true) && ((_Bounded_ = true) && (EVENT_ONLY_FIELD > '1' && EVENT_ONLY_FIELD < '2')))");
    }

    @Test
    public void testIndexHoleMarker() {
        test(true, "((_Hole_ = true) && (INDEXED_FIELD == 'a'))");
        test(true, "((_Hole_ = true) && (INDEX_ONLY_FIELD == 'a'))");
        // this should be false. will be caught if we verify the source node for markers
        test(true, "((_Hole_ = true) && (EVENT_ONLY_FIELD == 'a'))");
    }

    @Test
    public void testDelayedMarker() {
        test(false, "((_Delayed_ = true) && (INDEXED_FIELD == 'a'))");
        test(false, "((_Delayed_ = true) && (INDEX_ONLY_FIELD == 'a'))");
        test(false, "((_Delayed_ = true) && (EVENT_ONLY_FIELD == 'a'))");
    }

    @Test
    public void testEvaluationOnlyMarker() {
        // the only fields that should be marked eval only are event-only fields
        test(false, "((_Eval_ = true) && (INDEXED_FIELD =~ 'ba.*'))");
        test(false, "((_Eval_ = true) && (INDEX_ONLY_FIELD =~ 'ba.*'))");
        test(false, "((_Eval_ = true) && (EVENT_ONLY_FIELD =~ 'ba.*'))");
    }

    @Test
    public void testBoundedRangeMarker() {
        test(true, "((_Bounded_ = true) && (INDEXED_FIELD > '1' && INDEXED_FIELD < '2'))");
        test(true, "((_Bounded_ = true) && (INDEX_ONLY_FIELD > '1' && INDEX_ONLY_FIELD < '2'))");
        // this is not executable against the field index and should be wrapped with a delayed or eval marker
        // leave this test case in to document behavior when the fields for the source node are not visited
        test(true, "((_Bounded_ = true) && (EVENT_ONLY_FIELD > '1' && EVENT_ONLY_FIELD < '2'))");
    }

    @Test
    public void testListOrMarker() {
        test(true, "((_List_ = true) && ((id = 'uuid') && (field = 'INDEXED_FIELD') && (params = '{\"values\":[\"a\",\"b\"]}')))");
        test(true, "((_List_ = true) && ((id = 'uuid') && (field = 'INDEX_ONLY_FIELD') && (params = '{\"values\":[\"a\",\"b\"]}')))");
        test(true, "((_List_ = true) && ((id = 'uuid') && (field = 'EVENT_ONLY_FIELD') && (params = '{\"values\":[\"a\",\"b\"]}')))");
    }

    @Test
    public void testContentAdjacentFunction() {
        // unfielded content functions are thing of the past, but record this behavior anyway
        test(false, "content:adjacent(termOffsetMap, 'a', 'b')");
        test(false, "content:adjacent(INDEXED_FIELD, termOffsetMap, 'a', 'b')");
        test(false, "content:adjacent(INDEX_ONLY_FIELD, termOffsetMap, 'a', 'b')");
        test(false, "content:adjacent(EVENT_ONLY_FIELD, termOffsetMap, 'a', 'b')");

        // in practice multi-fielded content functions are rewritten as a union of single fielded content functions
        // order should not matter, but we shouldn't make assumptions..
        test(false, "content:adjacent((INDEXED_FIELD || INDEXED_FIELD), termOffsetMap, 'a', 'b')");
        test(false, "content:adjacent((INDEXED_FIELD || INDEX_ONLY_FIELD), termOffsetMap, 'a', 'b')");
        test(false, "content:adjacent((INDEXED_FIELD || EVENT_ONLY_FIELD), termOffsetMap, 'a', 'b')");

        test(false, "content:adjacent((INDEX_ONLY_FIELD || INDEXED_FIELD), termOffsetMap, 'a', 'b')");
        test(false, "content:adjacent((INDEX_ONLY_FIELD || INDEX_ONLY_FIELD), termOffsetMap, 'a', 'b')");
        test(false, "content:adjacent((INDEX_ONLY_FIELD || EVENT_ONLY_FIELD), termOffsetMap, 'a', 'b')");

        test(false, "content:adjacent((EVENT_ONLY_FIELD || INDEXED_FIELD), termOffsetMap, 'a', 'b')");
        test(false, "content:adjacent((EVENT_ONLY_FIELD || INDEX_ONLY_FIELD), termOffsetMap, 'a', 'b')");
        test(false, "content:adjacent((EVENT_ONLY_FIELD || EVENT_ONLY_FIELD), termOffsetMap, 'a', 'b')");
    }

    @Test
    public void testContentPhraseFunction() {
        // unfielded content functions are thing of the past, but record this behavior anyway
        test(false, "content:phrase(termOffsetMap, 'a', 'b')");
        test(false, "content:phrase(INDEXED_FIELD, termOffsetMap, 'a', 'b')");
        test(false, "content:phrase(INDEX_ONLY_FIELD, termOffsetMap, 'a', 'b')");
        test(false, "content:phrase(EVENT_ONLY_FIELD, termOffsetMap, 'a', 'b')");

        // in practice multi-fielded content functions are rewritten as a union of single fielded content functions
        // order should not matter, but we shouldn't make assumptions..
        test(false, "content:phrase((INDEXED_FIELD || INDEXED_FIELD), termOffsetMap, 'a', 'b')");
        test(false, "content:phrase((INDEXED_FIELD || INDEX_ONLY_FIELD), termOffsetMap, 'a', 'b')");
        test(false, "content:phrase((INDEXED_FIELD || EVENT_ONLY_FIELD), termOffsetMap, 'a', 'b')");

        test(false, "content:phrase((INDEX_ONLY_FIELD || INDEXED_FIELD), termOffsetMap, 'a', 'b')");
        test(false, "content:phrase((INDEX_ONLY_FIELD || INDEX_ONLY_FIELD), termOffsetMap, 'a', 'b')");
        test(false, "content:phrase((INDEX_ONLY_FIELD || EVENT_ONLY_FIELD), termOffsetMap, 'a', 'b')");

        test(false, "content:phrase((EVENT_ONLY_FIELD || INDEXED_FIELD), termOffsetMap, 'a', 'b')");
        test(false, "content:phrase((EVENT_ONLY_FIELD || INDEX_ONLY_FIELD), termOffsetMap, 'a', 'b')");
        test(false, "content:phrase((EVENT_ONLY_FIELD || EVENT_ONLY_FIELD), termOffsetMap, 'a', 'b')");
    }

    @Test
    public void testContentScoredPhraseFunction() {
        // unfielded content functions are thing of the past, but record this behavior anyway
        test(false, "content:scoredPhrase(-1.1, termOffsetMap, 'a', 'b')");
        test(false, "content:scoredPhrase(INDEXED_FIELD, -1.1, termOffsetMap, 'a', 'b')");
        test(false, "content:scoredPhrase(INDEX_ONLY_FIELD, -1.1, termOffsetMap, 'a', 'b')");
        test(false, "content:scoredPhrase(EVENT_ONLY_FIELD, -1.1, termOffsetMap, 'a', 'b')");

        // in practice multi-fielded content functions are rewritten as a union of single fielded content functions
        // order should not matter, but we shouldn't make assumptions..
        test(false, "content:scoredPhrase((INDEXED_FIELD || INDEXED_FIELD), -1.1, termOffsetMap, 'a', 'b')");
        test(false, "content:scoredPhrase((INDEXED_FIELD || INDEX_ONLY_FIELD), -1.1, termOffsetMap, 'a', 'b')");
        test(false, "content:scoredPhrase((INDEXED_FIELD || EVENT_ONLY_FIELD), -1.1, termOffsetMap, 'a', 'b')");

        test(false, "content:scoredPhrase((INDEX_ONLY_FIELD || INDEXED_FIELD), -1.1, termOffsetMap, 'a', 'b')");
        test(false, "content:scoredPhrase((INDEX_ONLY_FIELD || INDEX_ONLY_FIELD), -1.1, termOffsetMap, 'a', 'b')");
        test(false, "content:scoredPhrase((INDEX_ONLY_FIELD || EVENT_ONLY_FIELD), -1.1, termOffsetMap, 'a', 'b')");

        test(false, "content:scoredPhrase((EVENT_ONLY_FIELD || INDEXED_FIELD), -1.1, termOffsetMap, 'a', 'b')");
        test(false, "content:scoredPhrase((EVENT_ONLY_FIELD || INDEX_ONLY_FIELD), -1.1, termOffsetMap, 'a', 'b')");
        test(false, "content:scoredPhrase((EVENT_ONLY_FIELD || EVENT_ONLY_FIELD), -1.1, termOffsetMap, 'a', 'b')");
    }

    @Test
    public void testContentWithinFunction() {
        // unfielded content functions are thing of the past, but record this behavior anyway
        test(false, "content:within(1, termOffsetMap, 'a', 'b')");
        test(false, "content:within(INDEXED_FIELD, 1, termOffsetMap, 'a', 'b')");
        test(false, "content:within(INDEX_ONLY_FIELD, 1, termOffsetMap, 'a', 'b')");
        test(false, "content:within(EVENT_ONLY_FIELD, 1, termOffsetMap, 'a', 'b')");

        // in practice multi-fielded content functions are rewritten as a union of single fielded content functions
        // order should not matter, but we shouldn't make assumptions...
        test(false, "content:within((INDEXED_FIELD || INDEXED_FIELD), 1, termOffsetMap, 'a', 'b')");
        test(false, "content:within((INDEXED_FIELD || INDEX_ONLY_FIELD), 1, termOffsetMap, 'a', 'b')");
        test(false, "content:within((INDEXED_FIELD || EVENT_ONLY_FIELD), 1, termOffsetMap, 'a', 'b')");

        test(false, "content:within((INDEX_ONLY_FIELD || INDEXED_FIELD), 1, termOffsetMap, 'a', 'b')");
        test(false, "content:within((INDEX_ONLY_FIELD || INDEX_ONLY_FIELD), 1, termOffsetMap, 'a', 'b')");
        test(false, "content:within((INDEX_ONLY_FIELD || EVENT_ONLY_FIELD), 1, termOffsetMap, 'a', 'b')");

        test(false, "content:within((EVENT_ONLY_FIELD || INDEXED_FIELD), 1, termOffsetMap, 'a', 'b')");
        test(false, "content:within((EVENT_ONLY_FIELD || INDEX_ONLY_FIELD), 1, termOffsetMap, 'a', 'b')");
        test(false, "content:within((EVENT_ONLY_FIELD || EVENT_ONLY_FIELD), 1, termOffsetMap, 'a', 'b')");
    }

    @Test
    public void testFilterIncludeRegex() {
        test(false, "filter:includeRegex(INDEXED_FIELD, 'ba.*')");
        test(false, "filter:includeRegex(INDEX_ONLY_FIELD, 'ba.*')");
        test(false, "filter:includeRegex(EVENT_ONLY_FIELD, 'ba.*')");

        test(false, "filter:includeRegex((INDEXED_FIELD || INDEXED_FIELD), 'ba.*')");
        test(false, "filter:includeRegex((INDEXED_FIELD || INDEX_ONLY_FIELD), 'ba.*')");
        test(false, "filter:includeRegex((INDEXED_FIELD || EVENT_ONLY_FIELD), 'ba.*')");

        test(false, "filter:includeRegex((INDEX_ONLY_FIELD || INDEXED_FIELD), 'ba.*')");
        test(false, "filter:includeRegex((INDEX_ONLY_FIELD || INDEX_ONLY_FIELD), 'ba.*')");
        test(false, "filter:includeRegex((INDEX_ONLY_FIELD || EVENT_ONLY_FIELD), 'ba.*')");

        test(false, "filter:includeRegex((EVENT_ONLY_FIELD || INDEXED_FIELD), 'ba.*')");
        test(false, "filter:includeRegex((EVENT_ONLY_FIELD || INDEX_ONLY_FIELD), 'ba.*')");
        test(false, "filter:includeRegex((EVENT_ONLY_FIELD || EVENT_ONLY_FIELD), 'ba.*')");
    }

    @Test
    public void testFilterExcludeRegex() {
        test(false, "filter:excludeRegex(INDEXED_FIELD, 'ba.*')");
        test(false, "filter:excludeRegex(INDEX_ONLY_FIELD, 'ba.*')");
        test(false, "filter:excludeRegex(EVENT_ONLY_FIELD, 'ba.*')");

        test(false, "filter:excludeRegex((INDEXED_FIELD || INDEXED_FIELD), 'ba.*')");
        test(false, "filter:excludeRegex((INDEXED_FIELD || INDEX_ONLY_FIELD), 'ba.*')");
        test(false, "filter:excludeRegex((INDEXED_FIELD || EVENT_ONLY_FIELD), 'ba.*')");

        test(false, "filter:excludeRegex((INDEX_ONLY_FIELD || INDEXED_FIELD), 'ba.*')");
        test(false, "filter:excludeRegex((INDEX_ONLY_FIELD || INDEX_ONLY_FIELD), 'ba.*')");
        test(false, "filter:excludeRegex((INDEX_ONLY_FIELD || EVENT_ONLY_FIELD), 'ba.*')");

        test(false, "filter:excludeRegex((EVENT_ONLY_FIELD || INDEXED_FIELD), 'ba.*')");
        test(false, "filter:excludeRegex((EVENT_ONLY_FIELD || INDEX_ONLY_FIELD), 'ba.*')");
        test(false, "filter:excludeRegex((EVENT_ONLY_FIELD || EVENT_ONLY_FIELD), 'ba.*')");
    }

    @Test
    public void testFilterIsNull() {
        // in practice isNull/isNotNull functions are rewritten into a union of 'FIELD == null' terms
        // we still support these functions in the interpreter, so record behavior here
        test(false, "filter:isNull(INDEXED_FIELD)");
        test(false, "filter:isNull(INDEX_ONLY_FIELD)");
        test(false, "filter:isNull(EVENT_ONLY_FIELD)");

        test(false, "filter:isNull((INDEXED_FIELD || INDEXED_FIELD))");
        test(false, "filter:isNull((INDEXED_FIELD || INDEX_ONLY_FIELD))");
        test(false, "filter:isNull((INDEXED_FIELD || EVENT_ONLY_FIELD))");

        test(false, "filter:isNull((INDEX_ONLY_FIELD || INDEXED_FIELD))");
        test(false, "filter:isNull((INDEX_ONLY_FIELD || INDEX_ONLY_FIELD))");
        test(false, "filter:isNull((INDEX_ONLY_FIELD || EVENT_ONLY_FIELD))");

        test(false, "filter:isNull((EVENT_ONLY_FIELD || INDEXED_FIELD))");
        test(false, "filter:isNull((EVENT_ONLY_FIELD || INDEX_ONLY_FIELD))");
        test(false, "filter:isNull((EVENT_ONLY_FIELD || EVENT_ONLY_FIELD))");
    }

    @Test
    public void testFilterIsNotNull() {
        // in practice isNull/isNotNull functions are rewritten into a union of 'FIELD == null' terms
        // we still support these functions in the interpreter, so record behavior here
        test(false, "filter:isNotNull(INDEXED_FIELD)");
        test(false, "filter:isNotNull(INDEX_ONLY_FIELD)");
        test(false, "filter:isNotNull(EVENT_ONLY_FIELD)");

        test(false, "filter:isNotNull((INDEXED_FIELD || INDEXED_FIELD))");
        test(false, "filter:isNotNull((INDEXED_FIELD || INDEX_ONLY_FIELD))");
        test(false, "filter:isNotNull((INDEXED_FIELD || EVENT_ONLY_FIELD))");

        test(false, "filter:isNotNull((INDEX_ONLY_FIELD || INDEXED_FIELD))");
        test(false, "filter:isNotNull((INDEX_ONLY_FIELD || INDEX_ONLY_FIELD))");
        test(false, "filter:isNotNull((INDEX_ONLY_FIELD || EVENT_ONLY_FIELD))");

        test(false, "filter:isNotNull((EVENT_ONLY_FIELD || INDEXED_FIELD))");
        test(false, "filter:isNotNull((EVENT_ONLY_FIELD || INDEX_ONLY_FIELD))");
        test(false, "filter:isNotNull((EVENT_ONLY_FIELD || EVENT_ONLY_FIELD))");
    }

    @Test
    public void testFilterOccurrence() {
        test(false, "filter:occurrence(INDEXED_FIELD, '==', 3)");
        test(false, "filter:occurrence(INDEX_ONLY_FIELD, '==', 3)");
        test(false, "filter:occurrence(EVENT_ONLY_FIELD, '==', 3)");

        test(false, "filter:occurrence(INDEXED_FIELD, '>', 3)");
        test(false, "filter:occurrence(INDEX_ONLY_FIELD, '>', 3)");
        test(false, "filter:occurrence(EVENT_ONLY_FIELD, '>', 3)");
    }

    @Test
    public void testFilterMatchesAtLeastCountOf() {
        test(false, "filter:occurrence(2, INDEXED_FIELD, INDEXED_FIELD)");
        test(false, "filter:occurrence(2, INDEXED_FIELD, INDEX_ONLY_FIELD)");
        test(false, "filter:occurrence(2, INDEXED_FIELD, EVENT_ONLY_FIELD)");

        test(false, "filter:occurrence(2, INDEX_ONLY_FIELD, INDEXED_FIELD)");
        test(false, "filter:occurrence(2, INDEX_ONLY_FIELD, INDEX_ONLY_FIELD)");
        test(false, "filter:occurrence(2, INDEX_ONLY_FIELD, EVENT_ONLY_FIELD)");

        test(false, "filter:occurrence(2, EVENT_ONLY_FIELD, INDEXED_FIELD)");
        test(false, "filter:occurrence(2, EVENT_ONLY_FIELD, INDEX_ONLY_FIELD)");
        test(false, "filter:occurrence(2, EVENT_ONLY_FIELD, EVENT_ONLY_FIELD)");
    }

    @Test
    public void testFilterTimeFunction() {
        test(false, "filter:timeFunction(INDEXED_FIELD,INDEXED_FIELD,'-','>',2522880000000L)");
        test(false, "filter:timeFunction(INDEXED_FIELD,INDEX_ONLY_FIELD,'-','>',2522880000000L)");
        test(false, "filter:timeFunction(INDEXED_FIELD,EVENT_ONLY_FIELD,'-','>',2522880000000L)");

        test(false, "filter:timeFunction(INDEX_ONLY_FIELD,INDEXED_FIELD,'-','>',2522880000000L)");
        test(false, "filter:timeFunction(INDEX_ONLY_FIELD,INDEX_ONLY_FIELD,'-','>',2522880000000L)");
        test(false, "filter:timeFunction(INDEX_ONLY_FIELD,EVENT_ONLY_FIELD,'-','>',2522880000000L)");

        test(false, "filter:timeFunction(EVENT_ONLY_FIELD,INDEXED_FIELD,'-','>',2522880000000L)");
        test(false, "filter:timeFunction(EVENT_ONLY_FIELD,INDEX_ONLY_FIELD,'-','>',2522880000000L)");
        test(false, "filter:timeFunction(EVENT_ONLY_FIELD,EVENT_ONLY_FIELD,'-','>',2522880000000L)");

    }

    @Test
    public void testFilterGetMaxTime() {
        test(false, "filter:getMaxTime(INDEXED_FIELD) < 123456L");
        test(false, "filter:getMaxTime(INDEX_ONLY_FIELD) < 123456L");
        test(false, "filter:getMaxTime(EVENT_ONLY_FIELD) < 123456L");
    }

    @Test
    public void testFieldArithmetic() {
        test(false, "INDEXED_FIELD.min() > 10");
        test(false, "INDEX_ONLY_FIELD.min() > 10");
        test(false, "EVENT_ONLY_FIELD.min() > 10");

        test(false, "INDEXED_FIELD.max() > 10");
        test(false, "INDEX_ONLY_FIELD.max() > 10");
        test(false, "EVENT_ONLY_FIELD.max() > 10");
    }

    @Test
    public void testFieldMethodArithmetic() {
        test(false, "INDEXED_FIELD.greaterThan(10).size() == 1");
        test(false, "INDEX_ONLY_FIELD.greaterThan(10).size() == 1");
        test(false, "EVENT_ONLY_FIELD.greaterThan(10).size() == 1");

        test(false, "INDEXED_FIELD.max() > 10");
        test(false, "INDEX_ONLY_FIELD.max() > 10");
        test(false, "EVENT_ONLY_FIELD.max() > 10");
    }

    @Test
    public void testMethodsAsArguments() {
        test(false, "INDEXED_FIELD.getValuesForGroups(grouping:getGroupsForMatchesInGroup(INDEXED_FIELD, 'a')).isEmpty() == false");
        test(false, "INDEXED_FIELD.getValuesForGroups(grouping:getGroupsForMatchesInGroup(INDEX_ONLY_FIELD, 'a')).isEmpty() == false");
        test(false, "INDEXED_FIELD.getValuesForGroups(grouping:getGroupsForMatchesInGroup(EVENT_ONLY_FIELD, 'a')).isEmpty() == false");

        test(false, "INDEX_ONLY_FIELD.getValuesForGroups(grouping:getGroupsForMatchesInGroup(INDEXED_FIELD, 'a')).isEmpty() == false");
        test(false, "INDEX_ONLY_FIELD.getValuesForGroups(grouping:getGroupsForMatchesInGroup(INDEX_ONLY_FIELD, 'a')).isEmpty() == false");
        test(false, "INDEX_ONLY_FIELD.getValuesForGroups(grouping:getGroupsForMatchesInGroup(EVENT_ONLY_FIELD, 'a')).isEmpty() == false");

        test(false, "EVENT_ONLY_FIELD.getValuesForGroups(grouping:getGroupsForMatchesInGroup(INDEXED_FIELD, 'a')).isEmpty() == false");
        test(false, "EVENT_ONLY_FIELD.getValuesForGroups(grouping:getGroupsForMatchesInGroup(INDEX_ONLY_FIELD, 'a')).isEmpty() == false");
        test(false, "EVENT_ONLY_FIELD.getValuesForGroups(grouping:getGroupsForMatchesInGroup(EVENT_ONLY_FIELD, 'a')).isEmpty() == false");
    }

    @Test
    public void testAndNot() {
        // negations can be handled by the field index
        test(true, "INDEXED_FIELD == 'a' && !(INDEXED_FIELD == 'b')");
        test(true, "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b')");
        // this is technically satisfiable if we run an iterator against the event column
        test(false, "INDEXED_FIELD == 'a' && !(EVENT_ONLY_FIELD == 'b')");

        // negations can be handled by the field index
        test(true, "INDEX_ONLY_FIELD == 'a' && !(INDEXED_FIELD == 'b')");
        test(true, "INDEX_ONLY_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b')");
        // this is technically satisfiable if we run an iterator against the event column
        test(false, "INDEX_ONLY_FIELD == 'a' && !(EVENT_ONLY_FIELD == 'b')");

        // this is technically a full table scan and should never reach the query iterator, document for posterity
        test(false, "EVENT_ONLY_FIELD == 'a' && !(INDEXED_FIELD == 'b')");
        test(false, "EVENT_ONLY_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b')");
        test(false, "EVENT_ONLY_FIELD == 'a' && !(EVENT_ONLY_FIELD == 'b')");

        // alternate type of negated term that is not satisfiable against the field index
        test(false, "INDEXED_FIELD == 'a' && !(filter:includeRegex(INDEXED_FIELD,'ba.*'))");
        test(false, "INDEX_ONLY_FIELD == 'a' && !(filter:includeRegex(INDEXED_FIELD,'ba.*'))");
        test(false, "EVENT_ONLY_FIELD == 'a' && !(filter:includeRegex(INDEXED_FIELD,'ba.*'))");
    }

    @Test
    public void testOrNot() {
        // any negated term within a top level union is considered a top level negation

        test(false, "INDEXED_FIELD == 'a' || !(INDEXED_FIELD == 'b')");
        test(false, "INDEXED_FIELD == 'a' || !(INDEX_ONLY_FIELD == 'b')");
        test(false, "INDEXED_FIELD == 'a' || !(EVENT_ONLY_FIELD == 'b')");

        test(false, "INDEX_ONLY_FIELD == 'a' || !(INDEXED_FIELD == 'b')");
        test(false, "INDEX_ONLY_FIELD == 'a' || !(INDEX_ONLY_FIELD == 'b')");
        test(false, "INDEX_ONLY_FIELD == 'a' || !(EVENT_ONLY_FIELD == 'b')");

        test(false, "EVENT_ONLY_FIELD == 'a' || !(INDEXED_FIELD == 'b')");
        test(false, "EVENT_ONLY_FIELD == 'a' || !(INDEX_ONLY_FIELD == 'b')");
        test(false, "EVENT_ONLY_FIELD == 'a' || !(EVENT_ONLY_FIELD == 'b')");

        // alternate type of negated term that is not satisfiable against the field index
        test(false, "INDEXED_FIELD == 'a' || !(filter:includeRegex(INDEXED_FIELD,'ba.*'))");
        test(false, "INDEX_ONLY_FIELD == 'a' || !(filter:includeRegex(INDEXED_FIELD,'ba.*'))");
        test(false, "EVENT_ONLY_FIELD == 'a' || !(filter:includeRegex(INDEXED_FIELD,'ba.*'))");
    }

    /**
     * Assert the outcome of applying the {@link SatisfactionVisitor} to the provided query
     *
     * @param expected
     *            the expected outcome
     * @param query
     *            the query
     */
    private void test(boolean expected, String query) {
        ASTJexlScript script = parse(query);
        // in practice the set of exclude fields is never used
        SatisfactionVisitor visitor = new SatisfactionVisitor(indexOnly, indexed, Collections.emptySet(), true);
        visitor.visit(script, null);
        assertEquals(expected, visitor.isQueryFullySatisfied());

        // this method of calling the visitor is used by the delayed non-event context
        SatisfactionVisitor pessimistic = new SatisfactionVisitor(indexOnly, indexed, Collections.emptySet(), false);
        pessimistic.visit(script, null);
        assertFalse(pessimistic.isQueryFullySatisfied());
    }

    /**
     * Utility method to parse a query without forcing every test to throw a parse exception
     *
     * @param query
     *            the query string
     * @return a JexlScript
     */
    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
            throw new RuntimeException(e);
        }
    }
}
