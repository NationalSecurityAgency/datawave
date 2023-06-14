package datawave.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Comparator;
import java.util.Date;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(Enclosed.class)
public class OperationEvaluatorTest {

    /**
     * Tests for {@link OperationEvaluator#calculate(int, int, String)} and {@link OperationEvaluator#compare(int, int, String)}
     */
    public static class IntegerTests extends EvaluatorTestSuite<Integer,Integer> {

        @Before
        public void beforeEach() {
            givenCalculateFunction((args) -> OperationEvaluator.calculate(args.left, args.right, args.operator));
            givenCompareFunction((args) -> OperationEvaluator.compare(args.left, args.right, args.operator));
        }

        @After
        public void afterEach() {
            clearArgs();
        }

        @Test
        public void testCalculate() {
            givenLeft(4);
            givenRight(2);

            givenOperator(OperationEvaluator.ADD);
            assertCalculateResult(6);

            givenOperator(OperationEvaluator.SUBTRACT);
            assertCalculateResult(2);

            givenOperator(OperationEvaluator.DIVIDE);
            assertCalculateResult(2);

            givenOperator(OperationEvaluator.MULTIPLY);
            assertCalculateResult(8);

            givenOperator(OperationEvaluator.MODULO);
            assertCalculateResult(0);

            givenOperator("[");
            Exception exception = assertThrows(IllegalArgumentException.class, this::calculate);
            assertEquals("[ is not a valid calculation operator", exception.getMessage());
        }

        @Test
        public void testCompare() {
            givenLeft(4);
            givenRight(2);

            givenOperator(OperationEvaluator.EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.DOUBLE_EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.NOT_EQUAL);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN_EQUALS);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.LESS_THAN);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.LESS_THAN_EQUALS);
            assertCompareResult(false);

            givenOperator("[");
            Exception exception = assertThrows(IllegalArgumentException.class, this::compare);
            assertEquals("[ is not a valid comparison operator", exception.getMessage());
        }
    }

    /**
     * Tests for {@link OperationEvaluator#calculate(long, long, String)} and {@link OperationEvaluator#compare(long, long, String)}
     */
    public static class LongTests extends EvaluatorTestSuite<Long,Long> {

        @Before
        public void beforeEach() {
            givenCalculateFunction((args) -> OperationEvaluator.calculate(args.left, args.right, args.operator));
            givenCompareFunction((args) -> OperationEvaluator.compare(args.left, args.right, args.operator));
        }

        @After
        public void afterEach() {
            clearArgs();
        }

        @Test
        public void testCalculate() {
            givenLeft(4L);
            givenRight(2L);

            givenOperator(OperationEvaluator.ADD);
            assertCalculateResult(6L);

            givenOperator(OperationEvaluator.SUBTRACT);
            assertCalculateResult(2L);

            givenOperator(OperationEvaluator.DIVIDE);
            assertCalculateResult(2L);

            givenOperator(OperationEvaluator.MULTIPLY);
            assertCalculateResult(8L);

            givenOperator(OperationEvaluator.MODULO);
            assertCalculateResult(0L);

            givenOperator("[");
            Exception exception = assertThrows(IllegalArgumentException.class, this::calculate);
            assertEquals("[ is not a valid calculation operator", exception.getMessage());
        }

        @Test
        public void testCompare() {
            givenLeft(4L);
            givenRight(2L);

            givenOperator(OperationEvaluator.EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.DOUBLE_EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.NOT_EQUAL);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN_EQUALS);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.LESS_THAN);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.LESS_THAN_EQUALS);
            assertCompareResult(false);

            givenOperator("[");
            Exception exception = assertThrows(IllegalArgumentException.class, this::compare);
            assertEquals("[ is not a valid comparison operator", exception.getMessage());
        }
    }

    /**
     * Tests for {@link OperationEvaluator#calculate(float, float, String)} and {@link OperationEvaluator#compare(float, float, String)}
     */
    public static class FloatTests extends EvaluatorTestSuite<Float,Float> {

        @Before
        public void beforeEach() {
            givenCalculateFunction((args) -> OperationEvaluator.calculate(args.left, args.right, args.operator));
            givenCompareFunction((args) -> OperationEvaluator.compare(args.left, args.right, args.operator));
        }

        @After
        public void afterEach() {
            clearArgs();
        }

        @Test
        public void testCalculate() {
            givenLeft(4F);
            givenRight(2F);

            givenOperator(OperationEvaluator.ADD);
            assertCalculateResult(6F);

            givenOperator(OperationEvaluator.SUBTRACT);
            assertCalculateResult(2F);

            givenOperator(OperationEvaluator.DIVIDE);
            assertCalculateResult(2F);

            givenOperator(OperationEvaluator.MULTIPLY);
            assertCalculateResult(8F);

            givenOperator(OperationEvaluator.MODULO);
            assertCalculateResult(0F);

            givenOperator("[");
            Exception exception = assertThrows(IllegalArgumentException.class, this::calculate);
            assertEquals("[ is not a valid calculation operator", exception.getMessage());
        }

        @Test
        public void testCompare() {
            givenLeft(4F);
            givenRight(2F);

            givenOperator(OperationEvaluator.EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.DOUBLE_EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.NOT_EQUAL);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN_EQUALS);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.LESS_THAN);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.LESS_THAN_EQUALS);
            assertCompareResult(false);

            givenOperator("[");
            Exception exception = assertThrows(IllegalArgumentException.class, this::compare);
            assertEquals("[ is not a valid comparison operator", exception.getMessage());
        }
    }

    /**
     * Tests for {@link OperationEvaluator#calculate(double, double, String)} and {@link OperationEvaluator#compare(double, double, String)}
     */
    public static class DoubleTests extends EvaluatorTestSuite<Double,Double> {

        @Before
        public void beforeEach() {
            givenCalculateFunction((args) -> OperationEvaluator.calculate(args.left, args.right, args.operator));
            givenCompareFunction((args) -> OperationEvaluator.compare(args.left, args.right, args.operator));
        }

        @After
        public void afterEach() {
            clearArgs();
        }

        @Test
        public void testCalculate() {
            givenLeft(4D);
            givenRight(2D);

            givenOperator(OperationEvaluator.ADD);
            assertCalculateResult(6D);

            givenOperator(OperationEvaluator.SUBTRACT);
            assertCalculateResult(2D);

            givenOperator(OperationEvaluator.DIVIDE);
            assertCalculateResult(2D);

            givenOperator(OperationEvaluator.MULTIPLY);
            assertCalculateResult(8D);

            givenOperator(OperationEvaluator.MODULO);
            assertCalculateResult(0D);

            givenOperator("[");
            Exception exception = assertThrows(IllegalArgumentException.class, this::calculate);
            assertEquals("[ is not a valid calculation operator", exception.getMessage());
        }

        @Test
        public void testCompare() {
            givenLeft(4D);
            givenRight(2D);

            givenOperator(OperationEvaluator.EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.DOUBLE_EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.NOT_EQUAL);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN_EQUALS);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.LESS_THAN);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.LESS_THAN_EQUALS);
            assertCompareResult(false);

            givenOperator("[");
            Exception exception = assertThrows(IllegalArgumentException.class, this::compare);
            assertEquals("[ is not a valid comparison operator", exception.getMessage());
        }
    }

    /**
     * Tests for {@link OperationEvaluator#calculate(Date, Date, String)} and {@link OperationEvaluator#compare(Date, Date, String)}
     */
    public static class DateTests extends EvaluatorTestSuite<Date,Long> {

        @Before
        public void beforeEach() {
            givenCalculateFunction((args) -> OperationEvaluator.calculate(args.left, args.right, args.operator));
            givenCompareFunction((args) -> OperationEvaluator.compare(args.left, args.right, args.operator));
        }

        @After
        public void afterEach() {
            clearArgs();
        }

        @Test
        public void testCalculate() {
            givenLeft(new Date(4L));
            givenRight(new Date(2L));

            givenOperator(OperationEvaluator.ADD);
            assertCalculateResult(6L);

            givenOperator(OperationEvaluator.SUBTRACT);
            assertCalculateResult(2L);

            givenOperator(OperationEvaluator.DIVIDE);
            assertCalculateResult(2L);

            givenOperator(OperationEvaluator.MULTIPLY);
            assertCalculateResult(8L);

            givenOperator(OperationEvaluator.MODULO);
            assertCalculateResult(0L);

            givenOperator("[");
            Exception exception = assertThrows(IllegalArgumentException.class, this::calculate);
            assertEquals("[ is not a valid calculation operator", exception.getMessage());
        }

        @Test
        public void testCompare() {
            givenLeft(new Date(4L));
            givenRight(new Date(2L));

            givenOperator(OperationEvaluator.EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.DOUBLE_EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.NOT_EQUAL);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN_EQUALS);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.LESS_THAN);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.LESS_THAN_EQUALS);
            assertCompareResult(false);

            givenOperator("[");
            Exception exception = assertThrows(IllegalArgumentException.class, this::compare);
            assertEquals("[ is not a valid comparison operator", exception.getMessage());
        }
    }

    /**
     * Tests for {@link OperationEvaluator#compare(Comparable, Comparable, String)}.
     */
    public static class ComparableTests extends EvaluatorTestSuite<String,Void> {

        @Before
        public void beforeEach() {
            givenCompareFunction((args) -> OperationEvaluator.compare(args.left, args.right, args.operator));
        }

        @After
        public void afterEach() {
            clearArgs();
        }

        @Test
        public void testCompare() {
            givenLeft("a");
            givenRight("b");

            givenOperator(OperationEvaluator.EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.DOUBLE_EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.NOT_EQUAL);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.LESS_THAN);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.LESS_THAN_EQUALS);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.GREATER_THAN_EQUALS);
            assertCompareResult(false);

            givenOperator("[");
            Exception exception = assertThrows(IllegalArgumentException.class, this::compare);
            assertEquals("[ is not a valid comparison operator", exception.getMessage());
        }
    }

    /**
     * Tests for {@link OperationEvaluator#compare(Object, Object, String, Comparator)}.
     */
    public static class ComparatorTests extends EvaluatorTestSuite<String,Void> {

        @Before
        public void beforeEach() {
            givenCompareFunction((args) -> OperationEvaluator.compare(args.left, args.right, args.operator, Comparator.reverseOrder()));
        }

        @After
        public void afterEach() {
            clearArgs();
        }

        @Test
        public void testCompare() {
            givenLeft("a");
            givenRight("b");

            givenOperator(OperationEvaluator.EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.DOUBLE_EQUALS);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.NOT_EQUAL);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.GREATER_THAN_EQUALS);
            assertCompareResult(true);

            givenOperator(OperationEvaluator.LESS_THAN);
            assertCompareResult(false);

            givenOperator(OperationEvaluator.LESS_THAN_EQUALS);
            assertCompareResult(false);

            givenOperator("[");
            Exception exception = assertThrows(IllegalArgumentException.class, this::compare);
            assertEquals("[ is not a valid comparison operator", exception.getMessage());
        }
    }

    private static abstract class EvaluatorTestSuite<INPUT,CALCULATE_OUTPUT> {
        private final Arguments<INPUT> args = new Arguments<>();
        private Function<Arguments<INPUT>,CALCULATE_OUTPUT> calculateFunction;
        private Function<Arguments<INPUT>,Boolean> compareFunction;

        public void givenLeft(INPUT left) {
            args.left = left;
        }

        public void givenRight(INPUT right) {
            args.right = right;
        }

        public void givenOperator(String operator) {
            args.operator = operator;
        }

        public void clearArgs() {
            args.left = null;
            args.right = null;
            args.operator = null;
        }

        public void givenCalculateFunction(Function<Arguments<INPUT>,CALCULATE_OUTPUT> calculateFunction) {
            this.calculateFunction = calculateFunction;
        }

        public void givenCompareFunction(Function<Arguments<INPUT>,Boolean> compareFunction) {
            this.compareFunction = compareFunction;
        }

        public CALCULATE_OUTPUT calculate() {
            return calculateFunction.apply(args);
        }

        public boolean compare() {
            return compareFunction.apply(args);
        }

        public void assertCalculateResult(CALCULATE_OUTPUT expected) {
            CALCULATE_OUTPUT actual = calculate();
            assertEquals("Expected " + expected + " but received " + actual, expected, actual);
        }

        public void assertCompareResult(boolean expected) {
            boolean actual = compare();
            assertEquals(expected, actual);
        }
    }

    private static class Arguments<INPUT> {
        INPUT left;
        INPUT right;
        String operator;
    }
}
