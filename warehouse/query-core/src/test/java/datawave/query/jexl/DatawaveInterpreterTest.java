package datawave.query.jexl;

import datawave.data.normalizer.NumberNormalizer;
import datawave.data.type.DateType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.DatawaveJexlScript;
import org.apache.commons.jexl2.ExpressionImpl;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.easymock.EasyMock;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DatawaveInterpreterTest {

    @Test
    public void mergeAndNodeFunctionalSetsTest() {
        String query = "((GEO == '0321􏿿+bE4.4' || GEO == '0334􏿿+bE4.4' || GEO == '0320􏿿+bE4.4' || GEO == '0335􏿿+bE4.4') && ((_Delayed_ = true) && ((GEO >= '030a' && GEO <= '0335') && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))))";

        DatawaveJexlContext context = new DatawaveJexlContext();

        Script script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);

        context.set("GEO", "0321􏿿+bE4.4");
        context.set("WKT_BYTE_LENGTH", "+bE4.4");

        assertTrue(DatawaveInterpreter.isMatched(script.execute(context)));
    }

    @Test
    public void largeOrListTest() {
        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < 1000000; i++)
            uuids.add("'" + UUID.randomUUID().toString() + "'");

        String query = String.join(" || ", uuids);

        DatawaveJexlContext context = new DatawaveJexlContext();

        Script script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);

        assertTrue(DatawaveInterpreter.isMatched(script.execute(context)));
    }

    @Test
    public void invocationFails_alwaysThrowsException() {
        JexlEngine engine = mock(JexlEngine.class);
        JexlContext context = mock(JexlContext.class);
        DatawaveInterpreter interpreter = new DatawaveInterpreter(engine, context, false, false);
        JexlException exception = new JexlException(new ASTStringLiteral(1), "Function failure");

        // Make mocks available.
        EasyMock.replay(engine, context);

        // Capture the expected exception.
        Exception thrown = null;
        try {
            interpreter.invocationFailed(exception);
        } catch (Exception e) {
            thrown = e;
        }

        // Verify that an exception is thrown even when strict == false.
        assertEquals(thrown, exception);
    }

    @Test
    public void testSimpleQuery() {
        String query = "FOO == 'bar'";
        test(query, true);

        query = "FOO == 'baz'";
        test(query, true);

        query = "FOO != 'bar'";
        test(query, false);

        query = "FOO != 'baz'";
        test(query, false);
    }

    @Test
    public void testOrWithTwoTerms() {
        String query = "FOO == 'bar' || FOO == 'baz'";
        test(query, true);

        query = "FOO == 'abc' || FOO == 'xyz'";
        test(query, false);
    }

    @Test
    public void testOrWithFourTerms() {
        String query = "FOO == 'bar' || FOO == 'baz' || FOO == 'barzee' || FOO == 'zeebar'";
        test(query, true);

        query = "FOO == 'barzee' || FOO == 'zeebar' || FOO == 'bar' || FOO == 'baz'";
        test(query, true);
    }

    @Test
    public void testAndWithTwoTerms() {
        String query = "FOO == 'bar' && FOO == 'baz'";
        test(query, true);

        query = "FOO != 'bar' && FOO == 'baz'";
        test(query, false);
    }

    @Test
    public void testAndWithFourTerms() {
        String query = "FOO == 'bar' && FOO == 'baz' && FOO == 'barzee' && FOO == 'zeebar'";
        test(query, false);

        query = "FOO == 'barzee' && FOO == 'zeebar' && FOO == 'bar' && FOO == 'baz'";
        test(query, false);
    }

    @Test
    public void testMixOfAndOrExpressions() {
        String query = "((FOO == 'bar' && FOO == 'baz') || (FOO == 'barzee' && FOO == 'zeebar'))";
        test(query, true);

        query = "((FOO == 'bar' && FOO == 'zeebar') || (FOO == 'barzee' && FOO == 'baz'))";
        test(query, false);

        query = "((FOO == 'bar' || FOO == 'baz') && (FOO == 'barzee' || FOO == 'zeebar'))";
        test(query, false);

        query = "((FOO == 'bar' || FOO == 'zeebar') && (FOO == 'barzee' || FOO == 'baz'))";
        test(query, true);

        query = "DEATH_DATE.min() < '20160301120000'";
        test(query, true);
    }

    @Test
    public void testFilterBeforeDate() {
        //  @formatter:off
        Object[][] array = {
                //  BIRTH_DATE is pre-epoch
                {"filter:beforeDate(BIRTH_DATE, '19200101')", true},    //  single value match, pre-epoch
                {"filter:beforeDate(BIRTH_DATE, '19600101')", true},    //  multi value match, pre-epoch
                {"filter:beforeDate(BIRTH_DATE, '20220101')", true},    //  multi value match, post-epoch

                //  with a pattern
                {"filter:beforeDate(BIRTH_DATE, '19200101', 'yyyyMMdd')", true},    //  single value match, pre-epoch
                {"filter:beforeDate(BIRTH_DATE, '19600101', 'yyyyMMdd')", true},    //  multi value match, pre-epoch
                {"filter:beforeDate(BIRTH_DATE, '20220101', 'yyyyMMdd')", true},    //  multi value match, post-epoch

                //  with multi fields
                {"filter:beforeDate((BIRTH_DATE||BIRTH_DATE), '19200101')", true},  //  single value match, pre-epoch
                {"filter:beforeDate((BIRTH_DATE||BIRTH_DATE), '19600101')", true},  //  multi value match, pre-epoch
                {"filter:beforeDate((BIRTH_DATE||BIRTH_DATE), '20220101')", true},  //  multi value match, post-epoch

                //  with a pattern
                {"filter:beforeDate((BIRTH_DATE||BIRTH_DATE), '19200101', 'yyyyMMdd')", true},  //  single value match, pre-epoch
                {"filter:beforeDate((BIRTH_DATE||BIRTH_DATE), '19600101', 'yyyyMMdd')", true},  //  multi value match, pre-epoch
                {"filter:beforeDate((BIRTH_DATE||BIRTH_DATE), '20220101', 'yyyyMMdd')", true},  //  multi value match, post-epoch

                //  DEATH_DATE is post-epoch
                {"filter:beforeDate(DEATH_DATE, '20010101')", true},    //  single value match
                {"filter:beforeDate(DEATH_DATE, '20030101')", true},    //  multi value match

                //  single field with pattern
                {"filter:beforeDate(DEATH_DATE, '20010101', 'yyyyMMdd')", true},    //  single value match
                {"filter:beforeDate(DEATH_DATE, '20030101', 'yyyyMMdd')", true},    //  multi value match

                //  multi field
                {"filter:beforeDate((DEATH_DATE||DEATH_DATE), '20010101')", true},    //  single value match
                {"filter:beforeDate((DEATH_DATE||DEATH_DATE), '20030101')", true},    //  multi value match

                //  multi field with pattern
                {"filter:beforeDate((DEATH_DATE||DEATH_DATE), '20010101', 'yyyyMMdd')", true},    //  single value match
                {"filter:beforeDate((DEATH_DATE||DEATH_DATE), '20030101', 'yyyyMMdd')", true},    //  multi value match
        };
        //  @formatter:on

        test(array);
    }

    @Test
    public void testFilterAfterDate() {
        //  @formatter:off
        Object[][] array = {
                //  BIRTH_DATE is pre-epoch
                {"filter:afterDate(BIRTH_DATE, '19400101')", true},    //  single value match, pre-epoch
                {"filter:afterDate(BIRTH_DATE, '19200101')", true},    //  multi value match, pre-epoch

                //  with a pattern
                {"filter:afterDate(BIRTH_DATE, '19400101', 'yyyyMMdd')", true},    //  single value match, pre-epoch
                {"filter:afterDate(BIRTH_DATE, '19200101', 'yyyyMMdd')", true},    //  multi value match, pre-epoch

                //  with multi fields
                {"filter:afterDate((BIRTH_DATE||BIRTH_DATE), '19400101')", true},  //  single value match, pre-epoch
                {"filter:afterDate((BIRTH_DATE||BIRTH_DATE), '19200101')", true},  //  multi value match, pre-epoch

                //  with a pattern
                {"filter:afterDate((BIRTH_DATE||BIRTH_DATE), '19400101', 'yyyyMMdd')", true},  //  single value match, pre-epoch
                {"filter:afterDate((BIRTH_DATE||BIRTH_DATE), '19200101', 'yyyyMMdd')", true},  //  multi value match, pre-epoch

                //  DEATH_DATE is post-epoch
                {"filter:afterDate(DEATH_DATE, '20010101')", true},    //  single value match, post-epoch
                {"filter:afterDate(DEATH_DATE, '20000101')", true},    //  multi value match, post-epoch
                {"filter:afterDate(DEATH_DATE, '19500101')", true},    //  multi value match, pre-epoch

                //  single field with pattern
                {"filter:afterDate(DEATH_DATE, '20010101', 'yyyyMMdd')", true},    //  single value match, post-epoch
                {"filter:afterDate(DEATH_DATE, '20000101', 'yyyyMMdd')", true},    //  multi value match, post-epoch
                {"filter:afterDate(DEATH_DATE, '19500101', 'yyyyMMdd')", true},    //  multi value match, pre-epoch

                //  multi field
                {"filter:afterDate((DEATH_DATE||DEATH_DATE), '20010101')", true},    //  single value match, post-epoch
                {"filter:afterDate((DEATH_DATE||DEATH_DATE), '20000101')", true},    //  multi value match, post-epoch
                {"filter:afterDate((DEATH_DATE||DEATH_DATE), '19500101')", true},    //  multi value match, pre-epoch

                //  multi field with pattern
                {"filter:afterDate((DEATH_DATE||DEATH_DATE), '20010101', 'yyyyMMdd')", true},    //  single value match, post-epoch
                {"filter:afterDate((DEATH_DATE||DEATH_DATE), '20000101', 'yyyyMMdd')", true},    //  multi value match, post-epoch
                {"filter:afterDate((DEATH_DATE||DEATH_DATE), '19500101', 'yyyyMMdd')", true},    //  multi value match, pre-epoch
        };
        //  @formatter:on

        test(array);
    }

    @Test
    public void testBoundedRange() {
        String query = "((_Bounded_ = true) && (SPEED >= '" + numberIndex("120") + "' && SPEED <= '" + numberIndex("150") + "'))";
        test(query, buildBoundedRangeContext(), true);

        query = "((_Bounded_ = true) && (SPEED >= '" + numberIndex("90") + "' && SPEED <= '" + numberIndex("120") + "'))";
        test(query, buildBoundedRangeContext(), false);
    }

    @Test
    public void testBoundedRangeAndTerm() {
        // range matches, term matches
        String query = "((_Bounded_ = true) && (SPEED >= '" + numberIndex("120") + "' && SPEED <= '" + numberIndex("150") + "')) && FOO == 'bar'";
        test(query, buildBoundedRangeContext(), true);

        // range matches, term misses
        query = "((_Bounded_ = true) && (SPEED >= '" + numberIndex("120") + "' && SPEED <= '" + numberIndex("150") + "')) && FOO != 'bar'";
        test(query, buildBoundedRangeContext(), false);

        // range misses, term hits
        query = "((_Bounded_ = true) && (SPEED >= '" + numberIndex("90") + "' && SPEED <= '" + numberIndex("120") + "')) && FOO == 'bar'";
        test(query, buildBoundedRangeContext(), false);

        // range misses, term misses
        query = "((_Bounded_ = true) && (SPEED >= '" + numberIndex("90") + "' && SPEED <= '" + numberIndex("120") + "')) && FOO != 'bar'";
        test(query, buildBoundedRangeContext(), false);
    }

    @Test
    public void testBoundedRangeOrTerm() {
        // range matches, term matches
        String query = "((_Bounded_ = true) && (SPEED >= '" + numberIndex("120") + "' && SPEED <= '" + numberIndex("150") + "')) || FOO == 'bar'";
        test(query, buildBoundedRangeContext(), true);

        // range matches, term misses
        query = "((_Bounded_ = true) && (SPEED >= '" + numberIndex("120") + "' && SPEED <= '" + numberIndex("150") + "')) || FOO != 'bar'";
        test(query, buildBoundedRangeContext(), true);

        // range misses, term hits
        query = "((_Bounded_ = true) && (SPEED >= '" + numberIndex("90") + "' && SPEED <= '" + numberIndex("120") + "')) || FOO == 'bar'";
        test(query, buildBoundedRangeContext(), true);

        // range misses, term misses
        query = "((_Bounded_ = true) && (SPEED >= '" + numberIndex("90") + "' && SPEED <= '" + numberIndex("120") + "')) || FOO != 'bar'";
        test(query, buildBoundedRangeContext(), false);
    }

    @Test
    public void testAssignments() {
        // test plain assignment will return false
        String assignment = "(_value_ = true)";
        String query = assignment;
        test(query, false);

        // validate simply query hits
        String test = "FOO == 'bar'";
        query = test;
        test(query, true);

        // validate intersection will hit with assignment
        query = assignment + " && " + test;
        test(query, true);

        // validate union will hit with assignment
        query = assignment + " || " + test;
        test(query, true);

        // validate simply query misses
        test = "FOO == 'barbar'";
        query = test;
        test(query, false);

        // validate intersection will miss with assignment
        query = assignment + " && " + test;
        test(query, false);

        // validate union will miss with assignment
        query = assignment + " || " + test;
        test(query, false);
    }

    @Test
    public void testDroppedAssignments() {
        // test plain assignment will return false
        String assignment = "((_dropped_ = true) && ((_reason_ = 'because') && (_query_ = 'field == value')))";
        String query = assignment;
        test(query, false);

        // validate simply query hits
        String test = "FOO == 'bar'";
        query = test;
        test(query, true);

        // validate intersection will hit with assignment
        query = assignment + " && " + test;
        test(query, true);

        // validate union will hit with assignment
        query = assignment + " || " + test;
        test(query, true);

        // validate simply query misses
        test = "FOO == 'barbar'";
        query = test;
        test(query, false);

        // validate intersection will miss with assignment
        query = assignment + " && " + test;
        test(query, false);

        // validate union will miss with assignment
        query = assignment + " || " + test;
        test(query, false);
    }

    // test filter:isNull
    @Test
    public void testFilterFunctionIsNull() {
        // single field, present
        String query = "FOO == 'bar' && filter:isNull(FOO)";
        test(query, buildDefaultContext(), false);

        // single field, absent
        query = "FOO == 'bar' && filter:isNull(ABSENT)";
        test(query, buildDefaultContext(), true);
    }

    @Ignore
    @Test
    public void testFilterFunctionMultiFieldedIsNull() {
        // Once #1604 is complete these tests will evaluate correctly

        // multi field, all present
        String query = "FOO == 'bar' && filter:isNull(FOO || FOO)";
        test(query, buildDefaultContext(), false);

        // multi field, (present || absent)
        query = "FOO == 'bar' && filter:isNull(FOO || FOO)";
        test(query, buildDefaultContext(), true);

        query = "FOO == 'bar' && filter:isNull(FOO || ABSENT)";
        test(query, buildDefaultContext(), true);

        // multi field, (absent || present)
        query = "FOO == 'bar' && filter:isNull(ABSENT || FOO)";
        test(query, buildDefaultContext(), true);

        // multi field, all absent
        query = "FOO == 'bar' && filter:isNull(ABSENT || ABSENT)";
        test(query, buildDefaultContext(), true);
    }

    // test filter:isNotNull and not(filter:isNull)
    @Test
    public void testFilterFunctionIsNotNull() {
        // single field, present
        String query = "FOO == 'bar' && filter:isNotNull(FOO)";
        test(query, buildDefaultContext(), true);

        query = "FOO == 'bar' && !(filter:isNull(FOO))";
        test(query, buildDefaultContext(), true);

        // single field, absent
        query = "FOO == 'bar' && filter:isNotNull(ABSENT)";
        test(query, buildDefaultContext(), false);

        query = "FOO == 'bar' && !(filter:isNull(ABSENT))";
        test(query, buildDefaultContext(), false);
    }

    @Ignore
    @Test
    public void testFilterFunctionsMultiFieldedIsNotNull() {
        // Once #1604 is complete these tests will evaluate correctly

        // multi field, all present
        String query = "FOO == 'bar' && filter:isNotNull(FOO || FOO)";
        test(query, buildDefaultContext(), true);

        query = "FOO == 'bar' && !(filter:isNull(FOO || FOO))";
        test(query, buildDefaultContext(), true);

        // multi field, (present || absent)
        query = "FOO == 'bar' && filter:isNotNull(FOO || ABSENT)";
        test(query, buildDefaultContext(), true);

        query = "FOO == 'bar' && !(filter:isNull(FOO || ABSENT))";
        test(query, buildDefaultContext(), true);

        query = "FOO == 'bar' && ( !(filter:isNull(FOO)) || !(filter:isNull(ABSENT)) )";
        test(query, buildDefaultContext(), true);

        // multi field, (absent || present)
        query = "FOO == 'bar' && filter:isNotNull(ABSENT || FOO)";
        test(query, buildDefaultContext(), false); // this is wrong. isNotNull expands into an AND so both values must be null.

        query = "FOO == 'bar' && !(filter:isNull(ABSENT || FOO))";
        test(query, buildDefaultContext(), false); // this is wrong. isNotNull expands into an AND so both values must be null.

        // multi field, all absent
        query = "FOO == 'bar' && filter:isNotNull(ABSENT || ABSENT)";
        test(query, buildDefaultContext(), false);

        query = "FOO == 'bar' && !(filter:isNull(ABSENT || ABSENT))";
        test(query, buildDefaultContext(), false);
    }

    /**
     * Semi-parameterized test method, uses the default context
     *
     * @param array
     *            an array of query strings to boolean expected output
     */
    private void test(Object[][] array) {
        for (Object[] sub : array) {
            test((String) sub[0], (boolean) sub[1]);
        }
    }

    /**
     * Evaluate a query against a default context
     *
     * @param query
     *            the query
     * @param expectedResult
     *            the expected result
     */
    private void test(String query, boolean expectedResult) {
        test(query, buildDefaultContext(), expectedResult);
    }

    /**
     * Evaluates a query against a jexl context. Tests with both a binary tree and a flattened tree.
     *
     * @param query
     *            a jexl query
     * @param context
     *            a context representing an event
     * @param expectedResult
     *            the expected evaluation of the query given the context
     */
    private void test(String query, JexlContext context, boolean expectedResult) {

        // create binary tree and execute the query
        Script script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);
        Object executed = script.execute(context);
        boolean isMatched = ArithmeticJexlEngines.isMatched(executed);
        assertEquals("Unexpected result for query (binary tree): " + query, expectedResult, isMatched);

        // create flattened tree and execute the query
        DatawaveJexlScript dwScript = DatawaveJexlScript.create((ExpressionImpl) script);
        executed = dwScript.execute(context);
        isMatched = ArithmeticJexlEngines.isMatched(executed);
        assertEquals("Unexpected result for query (flattened tree): " + query, expectedResult, isMatched);
    }

    protected Key docKey = new Key("dt\0uid");

    /**
     * Build a jexl context with default values for several different type attributes
     *
     * <pre>
     *     FOO = {bar, baz}
     *     BIRTH_DATE = {1910, 1930, 1950}
     *     DEATH_DATE = {2000, 2002}
     * </pre>
     *
     * @return a context
     */
    protected JexlContext buildDefaultContext() {
        //  @formatter:off
        DatawaveJexlContext context = new DatawaveJexlContext();

        //  standard field value pair
        context.set("FOO", new FunctionalSet(Arrays.asList(
                new ValueTuple("FOO", "bar", "bar", new TypeAttribute<>(new LcNoDiacriticsType("bar"), docKey, true)),
                new ValueTuple("FOO", "baz", "baz", new TypeAttribute<>(new LcNoDiacriticsType("baz"), docKey, true)))));

        //  TODO -- add numerics to default context

        //  dates
        DateType firstDate = new DateType("1910-12-28T00:00:05.000Z");
        DateType secondDate = new DateType("1930-12-28T00:00:05.000Z");
        DateType thirdDate = new DateType("1950-12-28T00:00:05.000Z");

        ValueTuple birthDate01 = new ValueTuple("BIRTH_DATE", firstDate, "1910-12-28T00:00:05.000Z", new TypeAttribute<>(firstDate, docKey, true));
        ValueTuple birthDate02 = new ValueTuple("BIRTH_DATE", secondDate, "1930-12-28T00:00:05.000Z", new TypeAttribute<>(secondDate, docKey, true));
        ValueTuple birthDate03 = new ValueTuple("BIRTH_DATE", thirdDate, "1950-12-28T00:00:05.000Z", new TypeAttribute<>(thirdDate, docKey, true));
        context.set("BIRTH_DATE", new FunctionalSet(Arrays.asList(birthDate01, birthDate02, birthDate03)));

        DateType fourthDate = new DateType("2000-12-28T00:00:05.000Z");
        DateType fifthDate = new DateType("2002-12-28T00:00:05.000Z");
        ValueTuple deathDate01 = new ValueTuple("DEATH_DATE", fourthDate, "2000-12-28T00:00:05.000Z", new TypeAttribute<>(fourthDate, docKey, true));
        ValueTuple deathDate02 = new ValueTuple("DEATH_DATE", fifthDate, "2002-12-28T00:00:05.000Z", new TypeAttribute<>(fifthDate, docKey, true));
        context.set("DEATH_DATE", new FunctionalSet(Arrays.asList(deathDate01, deathDate02)));

        //  TODO -- add term offsets

        //  @formatter:on
        return context;
    }

    /**
     * Build a jexl context with values that will match bounded ranges
     *
     * @return a context
     */
    private JexlContext buildUnNormalizedBoundedRangeContext() {
        DatawaveJexlContext context = new DatawaveJexlContext();
        context.set("SPEED", new FunctionalSet(Arrays.asList("123", "147")));
        context.set("FOO", new FunctionalSet(Arrays.asList("bar", "baz")));
        return context;
    }

    /**
     * Build a jexl context with values that will match bounded ranges
     *
     * @return a context
     */
    private JexlContext buildBoundedRangeContext() {
        DatawaveJexlContext context = new DatawaveJexlContext();
        context.set("SPEED", new FunctionalSet(Arrays.asList(numberTuple("SPEED", "123"), numberTuple("SPEED", "147"))));
        context.set("FOO", new FunctionalSet(Arrays.asList("bar", "baz")));
        return context;
    }

    private String numberIndex(String value) {
        return new NumberNormalizer().normalize(value);
    }

    private ValueTuple numberTuple(String field, String value) {
        String index = numberIndex(value);
        return new ValueTuple(field, value, index, new TypeAttribute<>(new NumberType(index), docKey, true));
    }

}
