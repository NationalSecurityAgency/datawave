package datawave.query.jexl.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.TreeMultimap;

import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.DatawaveJexlEngine;
import datawave.query.jexl.DefaultArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.TermFrequencyList.Zone;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.postprocessing.tf.PhraseOffset;
import datawave.query.postprocessing.tf.TermOffsetMap;
import datawave.query.util.MockDateIndexHelper;
import datawave.query.util.MockMetadataHelper;

public class ContentFunctionsTest {
    private static DatawaveJexlEngine engine;

    private JexlContext context;
    private TermOffsetMap termOffSetMap;

    private final String phraseFunction = ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME;
    private final String scoredPhraseFunction = ContentFunctions.CONTENT_SCORED_PHRASE_FUNCTION_NAME;
    private static final String EVENT_ID = "shard\u0000dt\u0000uid";
    private final String eventId = EVENT_ID;

    @BeforeClass
    public static void setUp() {
        Map<String,Object> functions = new HashMap<>(ArithmeticJexlEngines.functions());
        functions.put("f", QueryFunctions.class);
        functions.put("geo", GeoFunctions.class);
        functions.put("content", ContentFunctions.class);
        engine = new DatawaveJexlEngine(
                        new JexlBuilder().debug(false).namespaces(functions).arithmetic(new DefaultArithmetic()).permissions(JexlPermissions.UNRESTRICTED));
    }

    @Before
    public void setup() {
        this.context = new MapContext();
        this.termOffSetMap = new TermOffsetMap();
        this.termOffSetMap.setGatherPhraseOffsets(true);
    }

    /**
     * Ensures that result is Boolean and equal to the expected value
     *
     * @param result
     *            The object in question
     * @param expected
     *            The expected result
     * @return True if result is Boolean and equal to expected
     */
    public static boolean expect(Object result, Boolean expected) {
        return expected.equals(ArithmeticJexlEngines.isMatched(result));
    }

    private String buildFunction(String functionName, String... args) {
        StringBuilder sb = new StringBuilder();

        sb.append(ContentFunctions.CONTENT_FUNCTION_NAMESPACE).append(":").append(functionName);
        sb.append("(");

        for (String arg : args) {
            sb.append(arg).append(",");
        }

        sb.setLength(sb.length() - 1);
        sb.append(")");

        return sb.toString();
    }

    private List<TermWeightPosition> asList(int... offsets) {
        return asList(true, offsets);
    }

    private List<TermWeightPosition> asList(boolean zeroOffsetMatch, int... offsets) {
        List<TermWeightPosition> list = new ArrayList<>();
        for (int offset : offsets) {
            list.add(getPosition(offset, zeroOffsetMatch));
        }
        return list;
    }

    private List<TermWeightPosition> asList(List<Integer> offsets, List<Integer> skips) {
        return asList(true, offsets, skips);
    }

    private List<TermWeightPosition> asList(boolean zeroOffsetMatch, List<Integer> offsets, List<Integer> skips) {
        if (offsets.size() != skips.size()) {
            fail("Offsets and skips size need to match.");
        }

        List<TermWeightPosition> list = new ArrayList<>();
        for (int i = 0; i < offsets.size(); i++) {
            list.add(getPosition(offsets.get(i), skips.get(i), zeroOffsetMatch));
        }

        return list;
    }

    private List<TermWeightPosition> asList(List<Integer> offsets, List<Integer> skips, List<Float> scores) {
        if (offsets.size() != skips.size() || offsets.size() != scores.size()) {
            fail("Offsets and skips size need to match.");
        }

        List<TermWeightPosition> list = new ArrayList<>();
        for (int i = 0; i < offsets.size(); i++) {
            list.add(getPosition(offsets.get(i), skips.get(i), scores.get(i)));
        }

        return list;
    }

    private TermWeightPosition getPosition(int offset) {
        return new TermWeightPosition.Builder().setOffset(offset).build();
    }

    private TermWeightPosition getPosition(int offset, boolean zeroOffsetMatch) {
        return new TermWeightPosition.Builder().setOffset(offset).setZeroOffsetMatch(zeroOffsetMatch).build();
    }

    private TermWeightPosition getPosition(int offset, int prevSkips, boolean zeroOffsetMatch) {
        return new TermWeightPosition.Builder().setOffset(offset).setPrevSkips(prevSkips).setZeroOffsetMatch(zeroOffsetMatch).build();
    }

    private TermWeightPosition getPosition(int offset, int prevSkips, float score) {
        return new TermWeightPosition.Builder().setOffset(offset).setPrevSkips(prevSkips).setScore(TermWeightPosition.positionScoreToTermWeightScore(score))
                        .build();
    }

    private void assertPhraseOffset(String field, final int startOffset, final int endOffset) {
        Collection<PhraseOffset> phraseOffsets = termOffSetMap.getPhraseIndexes(field);
        boolean found = phraseOffsets.stream()
                        .anyMatch(pair -> pair.getEventId().equals(eventId) && pair.getStartOffset() == startOffset && pair.getEndOffset() == endOffset);
        assertTrue("Expected phrase offset [" + startOffset + ", " + endOffset + "] for field " + field + " and eventId " + eventId.replace('\u0000', '/'),
                        found);
    }

    private void assertNoPhraseOffsetsFor(String field) {
        assertTrue(termOffSetMap.getPhraseIndexes(field).isEmpty());
    }

    private void assertPhraseOffsetsEmpty() {
        assertTrue("Expected empty phrase offset map", termOffSetMap.getPhraseIndexes() == null || termOffSetMap.getPhraseIndexes().isEmpty());
    }

    @Test
    public void testEvaluation1() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "1", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(List.of(1, 2, 3), List.of(0, 0, 0));
        list2 = asList(List.of(5, 6, 7), List.of(0, 2, 0)); // match (6-2) should match (3+1)

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 3, 4);
    }

    @Test
    public void reverseSharedTokenIndex() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'a'", "'b'", "'c'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> t1, t2, t3;
        t1 = asList(List.of(234, 239, 252, 257, 265, 281, 286, 340, 363, 367), List.of(0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        t2 = asList(List.of(212, 229, 252, 272), List.of(0, 0, 0, 0));
        t3 = asList(List.of(1, 101, 202, 213, 253, 312, 336), List.of(0, 0, 0, 0, 0, 0, 0));

        termOffSetMap.putTermFrequencyList("a", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), t1)));
        termOffSetMap.putTermFrequencyList("b", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), t2)));
        termOffSetMap.putTermFrequencyList("c", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), t3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 252, 253);
    }

    @Test
    public void forwardSharedTokenIndex() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'c'", "'b'", "'a'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> t1, t2, t3;
        t1 = asList(List.of(234, 239, 252, 257, 265, 281, 286, 340, 363, 367), List.of(0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        t2 = asList(List.of(212, 229, 252, 272), List.of(0, 0, 0, 0));
        t3 = asList(List.of(1, 101, 202, 213, 251, 312, 336), List.of(0, 0, 0, 0, 0, 0, 0));

        termOffSetMap.putTermFrequencyList("a", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), t1)));
        termOffSetMap.putTermFrequencyList("b", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), t2)));
        termOffSetMap.putTermFrequencyList("c", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), t3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 251, 252);
    }

    @Test
    public void reverseAllSharedTokenIndex() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'a'", "'b'", "'c'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> t1, t2, t3;
        t1 = asList(List.of(234, 239, 252, 257, 265, 281, 286, 340, 363, 367), List.of(0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        t2 = asList(List.of(212, 229, 252, 272), List.of(0, 0, 0, 0));
        t3 = asList(List.of(1, 101, 202, 213, 252, 312, 336), List.of(0, 0, 0, 0, 0, 0, 0));

        termOffSetMap.putTermFrequencyList("a", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), t1)));
        termOffSetMap.putTermFrequencyList("b", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), t2)));
        termOffSetMap.putTermFrequencyList("c", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), t3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 252, 252);
    }

    @Test
    public void forwardAllSharedTokenIndex() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'c'", "'b'", "'a'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> t1, t2, t3;
        t1 = asList(List.of(234, 239, 252, 257, 265, 281, 286, 340, 363, 367), List.of(0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        t2 = asList(List.of(212, 229, 252, 272), List.of(0, 0, 0, 0));
        t3 = asList(List.of(1, 101, 202, 213, 252, 312, 336), List.of(0, 0, 0, 0, 0, 0, 0));

        termOffSetMap.putTermFrequencyList("a", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), t1)));
        termOffSetMap.putTermFrequencyList("b", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), t2)));
        termOffSetMap.putTermFrequencyList("c", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), t3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 252, 252);
    }

    /**
     * Same as testEvaluation1, however the term frequency list marks teh fields as non-content expansion fields and hence we expect no results
     */
    @Test
    public void testEvaluationNoContentFields() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "1", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(List.of(1, 2, 3), List.of(0, 0, 0));
        list2 = asList(List.of(5, 6, 7), List.of(0, 0, 3)); // match (7-3_ should match (3+1)

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", false, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", false, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testQuotedEvaluation() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "1", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog\\'s'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 2, 3);
        list2 = asList(3, 4, 5);

        termOffSetMap.putTermFrequencyList("dog's", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 2, 3);
    }

    /**
     * Ensure that we have a failure
     */
    @Test(expected = JexlException.class)
    public void testQuotedEvaluation_1_fail() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "1", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog's'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        fail("Query should have failed to parse");
    }

    @Test
    public void testEvaluation1_1() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "1", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1);
        list2 = asList(2);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 2);
    }

    @Test
    public void testEvaluation2() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "1", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 2, 3);
        list2 = asList(5, 6, 7);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluation3() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "1", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 2, 3);
        list2 = asList(5);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationWithSkips() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "1", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(List.of(4), List.of(1));
        list2 = asList(List.of(2), List.of(1)); // (10-6) = (3+1)

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 2, 3);
    }

    @Test
    public void testEvaluationEmptyOffsetList() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "1", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 2, 3);
        list2 = new ArrayList<>();

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationThreeTerms() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "3", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'",
                        "'rat'");
        JexlExpression expr = engine.createExpression(query);

        // (15-3)-9 <= 3
        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 5, 9);
        list2 = asList(3, 7, 11);
        list3 = asList(10, 15, 20, 25);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 7, 10);
    }

    @Test
    public void testEvaluationThreeTermsTooSmallDistance() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "2", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'",
                        "'rat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 2, 3);
        list2 = asList(3, 4, 5);
        list3 = asList(10, 15, 20, 25);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationFailedThreeTerms() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "3", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'",
                        "'rat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 2, 3);
        list2 = asList(3, 4, 5);
        list3 = asList(10, 15, 20, 25);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationMiddleMatch() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "2", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'",
                        "'rat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 5, 10);
        list2 = asList(2, 4, 20);
        list3 = asList(6, 8, 15);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 4, 6);
    }

    @Test
    public void testEvaluationAdjacent1() {
        String query = buildFunction(ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1);
        list2 = asList(2);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 2);
    }

    @Test
    public void testEvaluationAdjacent2() {
        String query = buildFunction(ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 2, 3);
        list2 = asList(5, 6, 7);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationAdjacent3() {
        String query = buildFunction(ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 2, 3);
        list2 = asList(5);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationAdjacentEmptyOffsetList() {
        String query = buildFunction(ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 2, 3);
        list2 = new ArrayList<>();

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationAdjacentThreeTerms() {
        String query = buildFunction(ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'", "'rat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 5, 9);
        list2 = asList(3, 7, 11);
        list3 = asList(10, 15, 20, 25);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 9, 11);
    }

    @Test
    public void testEvaluationAdjacentFailedThreeTerms() {
        String query = buildFunction(ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'", "'rat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 2, 3);
        list2 = asList(3, 4, 5);
        list3 = asList(10, 15, 20, 25);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseBasic() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 2, 3);
        list2 = asList(3, 4, 5);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 2, 3);
    }

    @Test
    public void testEvaluationPhraseBasicWithSkips() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(List.of(1, 2, 3), List.of(0, 1, 0));
        list2 = asList(List.of(5, 6, 7), List.of(2, 2, 2));

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 2, 5);
    }

    @Test
    public void testEvaluationPhraseBasic2() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 2, 3);
        list2 = asList(4, 5, 6);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 3, 4);
    }

    @Test
    public void testEvaluationPhraseBasic2WithSkips() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(List.of(1, 2, 3), List.of(0, 1, 0));
        list2 = asList(List.of(5, 6, 7), List.of(1, 3, 1));

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 2, 6);
    }

    @Test
    public void testEvaluationPhraseBasic3() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'", "'fish'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1);
        list2 = asList(2);
        list3 = asList(3);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("fish", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 3);
    }

    @Test
    public void testEvaluationPhraseBasic3WithSkips() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'", "'fish'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(List.of(1), List.of(0));
        list2 = asList(List.of(3), List.of(1)); // ~3-5
        list3 = asList(List.of(4, 10), List.of(0, 0));

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("fish", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 4);
    }

    @Test
    public void testEvaluationPhraseBasic3FavorContentOrderedFunction() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'", "'fish'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39);
        list2 = asList(2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40);
        list3 = asList(41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("fish", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 39, 41);
    }

    @Test
    public void testEvaluationPhraseBasicOrderFail() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(3, 4, 5);
        list2 = asList(1, 2);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseBasicFailWithSkips() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(List.of(3, 4, 5), List.of(0, 0, 2));
        list2 = asList(List.of(1, 2), List.of(0, 1));

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseBasicOrderFail2() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'", "'fish'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(4);
        list2 = asList(3);
        list3 = asList(2);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("fish", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseBasicFail2WithSkips() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'", "'fish'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(List.of(4), List.of(0));
        list2 = asList(List.of(3), List.of(1));
        list3 = asList(List.of(2), List.of(0));

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("fish", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseBasicOrderFail3() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'", "'fish'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(2);
        list2 = asList(4);
        list3 = asList(3);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("fish", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseBasicFail3WithSkips() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'", "'fish'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(List.of(2), List.of(0));
        list2 = asList(List.of(4), List.of(0));
        list3 = asList(List.of(3), List.of(0));

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("fish", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseBasicTermOrderFail() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'dog'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 2, 3);
        list2 = asList(4, 5, 6);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseBasicTermOrderFailWithSkips() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'dog'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(List.of(1, 2, 3), List.of(1, 1, 1));
        list2 = asList(List.of(4, 5, 6), List.of(0, 1, 1));

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseSameTermFailure() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1;
        list1 = asList(1, 3, 5);

        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseSameTermSuccessFirst() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1;
        list1 = asList(1, 2, 5);

        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 2);
    }

    @Test
    public void testEvaluationPhraseSameTermSuccessLast() {
        String query = buildFunction(ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1;
        list1 = asList(1, 4, 5);

        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 4, 5);
    }

    @Test
    public void testEvaluationAdjacencySameTermFailureTest() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "2", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1;
        list1 = asList(1, 4);

        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationAdjacencySameTermSuccessTest() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "2", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1;
        list1 = asList(1, 3);

        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 3);
    }

    @Test
    public void testEvaluationAdjacencySameTermWithSkipsSuccessTest() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "2", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1;
        list1 = asList(List.of(1, 4), List.of(0, 1));

        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 3);
    }

    @Test
    public void testEvaluationAdjacencySameTermMixedSuccessTest() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "4", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'dog'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 5);
        list2 = asList(3);

        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 3);
    }

    /**
     * This test case was originally testPhraseBasicTermOrderFail. This test case knowingly returns a false positive. We can't seem to differentiate term order
     * at the same off, but we were missing some valid phraes. Maybe someone can come up with a way. For now, false negatives seemed worse than false positives.
     */
    @Test
    public void testEvaluationPhraseBasicTermOrderFalsePositive() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'dog'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 2, 3);
        list2 = asList(3, 4, 5);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 3, 3);
    }

    @Test
    public void testEvaluationPhraseThreeTerm() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'", "'rat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 2, 4);
        list2 = asList(5, 7, 9);
        list3 = asList(6, 8, 10);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 4, 6);
    }

    @Test
    public void testEvaluationPhraseThreeTermFail() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'dog'", "'rat'");

        JexlExpression expr = engine.createExpression(query);
        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 2, 4);
        list2 = asList(5, 7, 9);
        list3 = asList(6, 8, 10);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseThreeTermPass() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'rat'", "'dog'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 2, 4); // cat
        list2 = asList(4, 7, 8, 10); // rat
        list3 = asList(4, 6); // dog

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 4, 4);
    }

    @Test
    public void testEvaluationPhraseThreeTermFail2() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'rat'", "'dog'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 2, 4); // cat
        list2 = asList(5, 7, 9); // rat
        list3 = asList(4, 6, 10); // dog

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseTermOverlap() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'rat'", "'dog'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1); // cat
        list2 = asList(1); // rat
        list3 = asList(1); // dog

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 1);
    }

    @Test
    public void testEvaluationPhraseTermOverlapWithSkips() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'rat'", "'dog'");

        JexlExpression expr = engine.createExpression(query);
        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(false, List.of(135), List.of(6)); // cat
        list2 = asList(List.of(135), List.of(6)); // rat
        list3 = asList(List.of(1), List.of(1)); // dog

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationPhraseTermOverlapPass2() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'rat'", "'dog'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1); // cat
        list2 = asList(1); // rat
        list3 = asList(2); // dog

        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 2);
    }

    @Test
    public void testEvaluationPhraseTermOverlapPass3() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'rat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1); // cat
        list2 = asList(1, 5); // rat

        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 1);
    }

    @Test
    public void testEvaluationPhraseTermOverlapPass4() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'rat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(5); // cat
        list2 = asList(1, 5); // rat

        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 5, 5);
    }

    @Test
    public void testEvaluationPhraseTermOverlapFail() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'cat'", "'rat'", "'dog'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(2);
        list2 = asList(2);
        list3 = asList(1);

        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("rat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationScorePass() {
        String query = buildFunction(scoredPhraseFunction, "'CONTENT'", "-0.200", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(List.of(1, 2, 3), List.of(0, 0, 0), List.of(-0.223f, -1.4339f, -0.0001f));
        list2 = asList(List.of(3, 4, 5), List.of(0, 0, 0), List.of(-0.001f, -1.4339f, -0.2001f));

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 3, 3);
    }

    @Test
    public void testEvaluationScoreNoZonePass() {
        String query = buildFunction(scoredPhraseFunction, "-0.200", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(List.of(1, 2, 3), List.of(0, 0, 0), List.of(-0.223f, -1.4339f, -0.0001f));
        list2 = asList(List.of(3, 4, 5), List.of(0, 0, 0), List.of(-0.001f, -1.4339f, -0.2001f));

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 3, 3);
    }

    @Test
    public void testEvaluationScoreFail() {
        String query = buildFunction(scoredPhraseFunction, "'CONTENT'", "-0.200", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(List.of(1, 2, 3), List.of(0, 0, 0), List.of(-0.223f, -1.4339f, -0.2001f));
        list2 = asList(List.of(3, 4, 5), List.of(0, 0, 0), List.of(-0.001f, -1.4339f, -0.2001f));

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationMultipleContentFunctions() {
        String query1 = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "3", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'bat'", "'dog'",
                        "'cat'");
        String query2 = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "3", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        String query = query1 + "||" + query2;
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 2, 4);
        list2 = asList(4, 5, 6);
        list3 = asList(11, 12, 14);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("bat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(ArithmeticJexlEngines.isMatched(o), true));
        assertPhraseOffset("CONTENT", 1, 4);
    }

    /**
     * This tests the adjustment mechanism in the ContentOrderedEvaluator within the TraverseAndPrune method.
     */
    @Test
    public void testEvaluationPhrasePruningEdgeCondition() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'", "'bat'");

        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(9, 10);
        list2 = asList(9, 10, 11);
        list3 = asList(7, 12);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("bat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 10, 12);
    }

    /**
     * This tests the adjustment mechanism in the ContentOrderedEvaluator within the TraverseAndPrune method.
     */
    @Test
    public void testEvaluationReverseOffsetAdjustment() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'", "'bat'");

        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(5, 9, 10, 25, 27, 29);
        list2 = asList(3, 9, 10, 12, 13, 20, 23, 25);
        list3 = asList(1, 12, 13, 27);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("bat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testEvaluationMultiEvent() {
        String query1 = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "3", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        String query2 = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "3", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'bat'");
        String query = query1 + "||" + query2;

        List<TermWeightPosition> list1, list2, list3;
        list1 = asList(1, 2, 3);
        list2 = asList(4, 5, 6);
        list3 = asList(4, 5, 6);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("bat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId + ".1"), list3)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);

        JexlExpression expr = engine.createExpression(query1);
        Object o = expr.evaluate(context);
        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 4);

        termOffSetMap.getPhraseIndexes().clear();
        expr = engine.createExpression(query2);
        o = expr.evaluate(context);
        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();

        termOffSetMap.getPhraseIndexes().clear();
        expr = engine.createExpression(query);
        o = expr.evaluate(context);
        assertTrue(expect(ArithmeticJexlEngines.isMatched(o), true));
        assertPhraseOffset("CONTENT", 1, 4);
    }

    @Test
    public void testJexlFunctionArgumentDescriptor() throws ParseException {
        String query = "content:within('BODY', 5, termOffsetMap, 'hello', 'world')";
        String expected = "(BODY == 'hello' && BODY == 'world')";

        testJexlFunctionArgumentDescriptors(query, expected);
    }

    @Test
    public void testJexlFunctionArgumentDescriptor2() throws ParseException {
        String query = "content:within(5, termOffsetMap, 'hello', 'world')";
        String expected = "(META == 'hello' and META == 'world') or (BODY == 'hello' and BODY == 'world')";

        testJexlFunctionArgumentDescriptors(query, expected);
    }

    @Test
    public void testJexlFunctionArgumentDescriptor3() throws ParseException {
        String query = "content:adjacent('BODY', termOffsetMap, 'hello', 'world')";
        String expected = "(BODY == 'hello' and BODY == 'world')";

        testJexlFunctionArgumentDescriptors(query, expected);
    }

    @Test
    public void testJexlFunctionArgumentDescriptor4() throws ParseException {
        String query = "content:adjacent(termOffsetMap, 'hello', 'world')";
        String expected = "(META == 'hello' and META == 'world') or (BODY == 'hello' and BODY == 'world')";

        testJexlFunctionArgumentDescriptors(query, expected);
    }

    @Test
    public void testJexlFunctionArgumentDescriptor5() throws ParseException {
        String query = "content:" + phraseFunction + "('BODY', termOffsetMap, 'hello', 'world')";
        String expected = "(BODY == 'hello' and BODY == 'world')";

        testJexlFunctionArgumentDescriptors(query, expected);
    }

    @Test
    public void testJexlFunctionArgumentDescriptor6() throws ParseException {
        String query = "content:" + phraseFunction + "(termOffsetMap, 'hello', 'world')";
        String expected = "(META == 'hello' and META == 'world') or (BODY == 'hello' and BODY == 'world')";

        testJexlFunctionArgumentDescriptors(query, expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJexlFunctionArgumentDescriptor7() throws ParseException {
        String query = "content:" + phraseFunction + "('termOffsetMap', 'hello', 'world')";
        testJexlFunctionArgumentDescriptors(query, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJexlFunctionArgumentDescriptor8() throws ParseException {
        String query = "content:within(3, 'hello', 'world')";
        testJexlFunctionArgumentDescriptors(query, "");
    }

    @Test
    public void testJexlFunctionArgumentDescriptor9() throws ParseException {
        String query = "content:" + phraseFunction + "(termOffsetMap, 'hello', 'world')";
        String expected = "(BODY == 'hello' and BODY == 'world')";
        testJexlFunctionArgumentDescriptors(query, expected, Set.of("BODY"));
    }

    @Test
    public void testJexlFunctionArgumentDescriptor10() throws ParseException {
        String query = "content:" + scoredPhraseFunction + "(-1.1, termOffsetMap, 'hello', 'world')";
        String expected = "(META == 'hello' and META == 'world') or (BODY == 'hello' and BODY == 'world')";

        testJexlFunctionArgumentDescriptors(query, expected);
    }

    private void testJexlFunctionArgumentDescriptors(String query, String expected) throws ParseException {
        testJexlFunctionArgumentDescriptors(query, expected, null);
    }

    private void testJexlFunctionArgumentDescriptors(String query, String expected, Set<String> contentFields) throws ParseException {
        MockMetadataHelper metadataHelper = new MockMetadataHelper();
        metadataHelper.addTermFrequencyFields(List.of("BODY", "META"));
        metadataHelper.setIndexedFields(Set.of("BODY", "META"));

        if (contentFields != null) {
            metadataHelper.addContentFields(contentFields);
        }

        MockDateIndexHelper dateIndexHelper = new MockDateIndexHelper();

        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        JexlNode child = script.jjtGetChild(0);
        assertEquals("First child of ASTJexlScript is not an AStFunctionNode", ASTFunctionNode.class, child.getClass());

        ASTFunctionNode function = (ASTFunctionNode) child;

        JexlArgumentDescriptor desc = new ContentFunctionsDescriptor().getArgumentDescriptor(function);
        JexlNode indexQuery = desc.getIndexQuery(null, metadataHelper, dateIndexHelper, null);

        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        JexlNode scriptChild = expectedScript.jjtGetChild(0);

        assertTrue("Expected " + JexlStringBuildingVisitor.buildQuery(scriptChild) + " but was " + JexlStringBuildingVisitor.buildQuery(indexQuery),
                        JexlASTHelper.equals(scriptChild, indexQuery));
    }

    @Test
    public void testDoubleWordInPhrase() {
        String query = buildFunction(phraseFunction, Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'foo'", "'bar'", "'foo'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1, 3);
        list2 = asList(2);

        termOffSetMap.putTermFrequencyList("foo", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("bar", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("foo", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);

        Object o = expr.evaluate(context);
        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 3);
    }

    @Test
    public void testSomeEmptyOffsetsPhrase() {
        String query = buildFunction(phraseFunction, "BODY", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'foo'", "'bar'", "'car'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3, list4;
        list1 = asList(296);
        list2 = asList(1079);
        list3 = asList(260, 284, 304);
        list4 = asList(1165);

        termOffSetMap.putTermFrequencyList("foo", new TermFrequencyList(Map.entry(new Zone("BODY", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("bar", new TermFrequencyList(Map.entry(new Zone("BODY", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("car",
                        new TermFrequencyList(Map.entry(new Zone("BODY", true, eventId), list3), Map.entry(new Zone("META", true, eventId), list4)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        context.set("BODY", List.of("foo", "bar", "car"));

        Object o = expr.evaluate(context);
        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testSomeEmptyOffsetsAdjacency() {
        String query = buildFunction(ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME, "BODY", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'foo'", "'bar'",
                        "'car'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3, list4;
        list1 = asList(296);
        list2 = asList(1079);
        list3 = asList(260, 284, 304);
        list4 = asList(1165);

        termOffSetMap.putTermFrequencyList("foo", new TermFrequencyList(Map.entry(new Zone("BODY", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("bar", new TermFrequencyList(Map.entry(new Zone("BODY", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("car",
                        new TermFrequencyList(Map.entry(new Zone("BODY", true, eventId), list3), Map.entry(new Zone("META", true, eventId), list4)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        context.set("BODY", List.of("foo", "bar", "car"));

        Object o = expr.evaluate(context);
        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testSomeEmptyOffsetsWithin() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "BODY", "5", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'foo'", "'bar'",
                        "'car'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3, list4;
        list1 = asList(296);
        list2 = asList(1079);
        list3 = asList(260, 284, 304);
        list4 = asList(1165);

        termOffSetMap.putTermFrequencyList("foo", new TermFrequencyList(Map.entry(new Zone("BODY", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("bar", new TermFrequencyList(Map.entry(new Zone("BODY", true, eventId), list2)));
        termOffSetMap.putTermFrequencyList("car",
                        new TermFrequencyList(Map.entry(new Zone("BODY", true, eventId), list3), Map.entry(new Zone("META", true, eventId), list4)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        context.set("BODY", List.of("foo", "bar", "car"));

        Object o = expr.evaluate(context);
        assertTrue(expect(o, false));
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void testDuplicatePhraseOffset() {
        TreeMultimap<Zone,TermWeightPosition> multimap = TreeMultimap.create();

        TermOffsetMap termOffsetMap = new TermOffsetMap();

        multimap.put(genTestZone(), getPosition(19));
        termOffsetMap.putTermFrequencyList("go", new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(genTestZone(), getPosition(20));
        multimap.put(genTestZone(), getPosition(27));
        multimap.put(genTestZone(), getPosition(29));
        termOffsetMap.putTermFrequencyList("and", new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(genTestZone(), getPosition(21));
        termOffsetMap.putTermFrequencyList("tell", new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(genTestZone(), getPosition(22));
        termOffsetMap.putTermFrequencyList("your", new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(genTestZone(), getPosition(23));
        termOffsetMap.putTermFrequencyList("brother", new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(genTestZone(), getPosition(20));
        multimap.put(genTestZone(), getPosition(24));
        termOffsetMap.putTermFrequencyList("that", new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(genTestZone(), getPosition(25));
        termOffsetMap.putTermFrequencyList("dinners", new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(genTestZone(), getPosition(26));
        termOffsetMap.putTermFrequencyList("ready", new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(genTestZone(), getPosition(28));
        termOffsetMap.putTermFrequencyList("come", new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(genTestZone(), getPosition(30));
        termOffsetMap.putTermFrequencyList("wash", new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(genTestZone(), getPosition(31));
        termOffsetMap.putTermFrequencyList("his", new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(genTestZone(), getPosition(32));
        multimap.put(genTestZone(), getPosition(42));
        multimap.put(genTestZone(), getPosition(52));
        termOffsetMap.putTermFrequencyList("hands", new TermFrequencyList(multimap));

        // ///////////////////////////
        // Phrase functions
        // ///////////////////////////

        // full terms list
        assertNotNull(termOffsetMap.getTermFrequencyList("his"));
        String[] terms = new String[] {"go", "and", "tell", "your", "brother", "that", "dinners", "ready", "and", "come", "and", "wash", "his", "hands"};
        assertEquals(Set.of("BODY"), ContentFunctions.phrase("BODY", termOffsetMap, terms));

        // duplicate consecutive terms fail here
        terms = new String[] {"go", "and", "and", "tell", "your", "brother", "that", "dinners", "ready", "and", "come", "and", "wash", "his", "hands"};
        assertEquals(Set.of(), ContentFunctions.phrase("BODY", termOffsetMap, terms));

        // duplicate consecutive terms fail here
        terms = new String[] {"go", "and", "and", "tell", "your", "brother", "that", "dinners", "ready", "and", "come"};
        assertEquals(Set.of(), ContentFunctions.phrase("BODY", termOffsetMap, terms));

        // subset(1, end)
        terms = new String[] {"and", "tell", "your", "brother", "that", "dinners", "ready", "and", "come", "and", "wash", "his", "hands"};
        assertEquals(Set.of("BODY"), ContentFunctions.phrase("BODY", termOffsetMap, terms));

        // subset(1,end-5)
        terms = new String[] {"and", "tell", "your", "brother", "that", "dinners", "ready", "and"};
        assertEquals(Set.of("BODY"), ContentFunctions.phrase("BODY", termOffsetMap, terms));

        // ///////////////////////////
        // Within functions
        // ///////////////////////////

        // full terms list
        terms = new String[] {"go", "and", "tell", "your", "brother", "that", "dinners", "ready", "and", "come", "and", "wash", "his", "hands"};
        assertEquals(Set.of("BODY"), ContentFunctions.within("BODY", 14, termOffsetMap, terms));

        // duplicate consecutive terms fail here
        terms = new String[] {"go", "and", "and", "tell", "your", "brother", "that", "dinners", "ready", "and", "come", "and", "wash", "his", "hands"};
        assertEquals(Set.of(), ContentFunctions.within("BODY", 15, termOffsetMap, terms));

        // placement does not matter
        terms = new String[] {"go", "and", "and", "tell", "your", "brother", "that", "dinners", "ready", "and", "come"};
        assertEquals(Set.of("BODY"), ContentFunctions.within("BODY", 11, termOffsetMap, terms));

        // subset(1, end)
        terms = new String[] {"and", "tell", "your", "brother", "that", "dinners", "ready", "and", "come", "and", "wash", "his", "hands"};
        assertEquals(Set.of("BODY"), ContentFunctions.within("BODY", 12, termOffsetMap, terms));

        // subset(1,end-5)
        terms = new String[] {"and", "tell", "your", "brother", "that", "dinners", "ready", "and", "come", "and"};
        assertEquals(Set.of("BODY"), ContentFunctions.within("BODY", 10, termOffsetMap, terms));
    }

    private Zone genTestZone() {
        return new Zone("BODY", true, "shard\u0000dt\u0000uid");
    }

    private Zone genTestZone(String zone) {
        return new Zone(zone, true, "shard\u0000dt\u0000uid");
    }

    @Test
    public void testIgnoreIrrelevantZones() {
        Zone zone1 = genTestZone("ZONE1");
        Zone zone2 = genTestZone("ZONE2");

        String[] terms = new String[] {"some", "phrase"};

        TreeMultimap<Zone,TermWeightPosition> multimap;
        TermOffsetMap termOffsetMap = new TermOffsetMap();

        // Build term 1 offsets...

        multimap = TreeMultimap.create();
        multimap.put(zone1, getPosition(1));
        multimap.put(zone1, getPosition(100));
        termOffsetMap.putTermFrequencyList(terms[0], new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(zone2, getPosition(19));
        termOffsetMap.putTermFrequencyList(terms[0], new TermFrequencyList(multimap));

        // Build term 2 offsets...

        multimap = TreeMultimap.create();
        multimap.put(zone1, getPosition(10));
        multimap.put(zone1, getPosition(1000));
        termOffsetMap.putTermFrequencyList(terms[1], new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(zone2, getPosition(20));
        multimap.put(zone2, getPosition(27));
        termOffsetMap.putTermFrequencyList(terms[1], new TermFrequencyList(multimap));

        // The only match, [19, 20], is in ZONE2.
        // Thus, evaluating ZONE1 should return false here (see #1171)...
        assertEquals(Set.of(), ContentFunctions.phrase(zone1.getZone(), termOffsetMap, terms));

        // Ensure that we do get the hit if we evaluate the other zone
        assertEquals(Set.of(zone2.getZone()), ContentFunctions.phrase(zone2.getZone(), termOffsetMap, terms));

        // Ensure that we get the hit if we evaluate both zones
        assertEquals(Set.of(zone2.getZone()), ContentFunctions.phrase(List.of(zone1.getZone(), zone2.getZone()), termOffsetMap, terms));

        // Ensure that we get the hit if we evaluate null zone
        assertEquals(Set.of(zone2.getZone()), ContentFunctions.phrase((Object) null, termOffsetMap, terms));
    }

    /**
     * Verify that if gatherPhraseOffsets is false, that even when there is a phrase index for matching excerpt field, it is not recorded.
     */
    @Test
    public void testGatherPhraseOffsetsIsFalse() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "1", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1);
        list2 = asList(2);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(false);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffsetsEmpty();
    }

    /**
     * Verify that if gatherPhraseOffsets is true, if a phrase index for is found for a non-excerpt field, it is not recorded.
     */
    @Test
    public void testNonMatchingExcerptFields() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "1", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2;
        list1 = asList(1);
        list2 = asList(2);

        termOffSetMap.putTermFrequencyList("dog", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list1)));
        termOffSetMap.putTermFrequencyList("cat", new TermFrequencyList(Map.entry(new Zone("CONTENT", true, eventId), list2)));
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("BODY"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffsetsEmpty();
    }

    /**
     * Verify that if an event occurs with the same phrase for two different fields, only the phrase for the excerpt field is retrieved.
     */
    @Test
    public void testNonMatchingExcerptFieldsWithMultipleFieldsPresent() {
        String query = buildFunction(ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME, "1", Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, "'dog'", "'cat'");
        JexlExpression expr = engine.createExpression(query);

        List<TermWeightPosition> list1, list2, list3, list4;
        list1 = asList(1);
        list2 = asList(2);
        list3 = asList(3);
        list4 = asList(4);

        TermFrequencyList dogList = new TermFrequencyList();
        dogList.addOffsets(new Zone("CONTENT", true, eventId), list1);
        dogList.addOffsets(new Zone("BODY", true, eventId), list4);

        TermFrequencyList catList = new TermFrequencyList();
        catList.addOffsets(new Zone("CONTENT", true, eventId), list2);
        catList.addOffsets(new Zone("BODY", true, eventId), list3);

        termOffSetMap.putTermFrequencyList("dog", dogList);
        termOffSetMap.putTermFrequencyList("cat", catList);
        termOffSetMap.setGatherPhraseOffsets(true);
        termOffSetMap.setExcerptFields(Set.of("CONTENT"));

        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, termOffSetMap);
        Object o = expr.evaluate(context);

        assertTrue(expect(o, true));
        assertPhraseOffset("CONTENT", 1, 2);
        assertNoPhraseOffsetsFor("BODY");
    }

    // Validate intersection of event ids is working properly
    @Test
    public void testAdjacentHitsAcrossChildDocuments() {
        TreeMultimap<Zone,TermWeightPosition> multimap = TreeMultimap.create();

        TermOffsetMap termOffsetMap = new TermOffsetMap();

        multimap.put(new Zone("BODY", true, "shard\u0000dt\u0000uid0.1"), getPosition(5));
        termOffsetMap.putTermFrequencyList("blue", new TermFrequencyList(multimap));

        multimap = TreeMultimap.create();
        multimap.put(new Zone("BODY", true, "shard\u0000dt\u0000uid0.2"), getPosition(6));
        termOffsetMap.putTermFrequencyList("fish", new TermFrequencyList(multimap));

        // full terms list
        assertNotNull(termOffsetMap.getTermFrequencyList("blue"));
        assertNotNull(termOffsetMap.getTermFrequencyList("fish"));
        String[] terms = new String[] {"blue", "fish"};
        assertEquals(Set.of(), ContentFunctions.phrase("BODY", termOffsetMap, terms));
    }
}
