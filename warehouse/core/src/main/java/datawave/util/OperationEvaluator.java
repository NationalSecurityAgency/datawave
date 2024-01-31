package datawave.util;

import java.util.Comparator;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;

/**
 * Provides utility functions for evaluating arithmetic and logical operations given relevant arguments and a non-constant operator.
 */
public class OperationEvaluator {

    // Arithmetic operators.
    public static final String ADD = "+";
    public static final String SUBTRACT = "-";
    public static final String MULTIPLY = "*";
    public static final String DIVIDE = "/";
    public static final String MODULO = "%";

    // Logical operators.
    public static final String EQUALS = "=";
    public static final String DOUBLE_EQUALS = "==";
    public static final String NOT_EQUAL = "!=";
    public static final String GREATER_THAN = ">";
    public static final String GREATER_THAN_EQUALS = ">=";
    public static final String LESS_THAN = "<";
    public static final String LESS_THAN_EQUALS = "<=";

    /**
     * Return the result of the calculation for the provided integers using the provided calculation operator. Supported operators:
     * <ul>
     * <li>+ returns the sum of the left and right</li>
     * <li>- returns the difference of the left and right</li>
     * <li>* returns the product of the left and right</li>
     * <li>/ returns the quotient of the left and right</li>
     * <li>% returns the modulo of the left and right</li>
     * </ul>
     *
     * @param left
     *            the left side of the expression
     * @param right
     *            the right side of the expression
     * @param operator
     *            the calculation operator
     * @return the calculation result
     */
    public static int calculate(int left, int right, String operator) {
        Preconditions.checkArgument(!StringUtils.isBlank(operator), "operator must not be blank");
        operator = CharMatcher.whitespace().removeFrom(operator);
        switch (operator) {
            case ADD:
                return left + right;
            case SUBTRACT:
                return left - right;
            case MULTIPLY:
                return left * right;
            case DIVIDE:
                return left / right;
            case MODULO:
                return left % right;
            default:
                throw new IllegalArgumentException(operator + " is not a valid calculation operator");
        }
    }

    /**
     * Return the result of the calculation for the provided longs using the provided calculation operator. Supported operators:
     * <ul>
     * <li>+ returns the sum of the left and right</li>
     * <li>- returns the difference of the left and right</li>
     * <li>* returns the product of the left and right</li>
     * <li>/ returns the quotient of the left and right</li>
     * <li>% returns the modulo of the left and right</li>
     * </ul>
     *
     * @param left
     *            the left side of the expression
     * @param right
     *            the right side of the expression
     * @param operator
     *            the calculation operator
     * @return the calculation result
     */
    public static long calculate(long left, long right, String operator) {
        Preconditions.checkArgument(!StringUtils.isBlank(operator), "operator must not be blank");
        operator = CharMatcher.whitespace().removeFrom(operator);
        switch (operator) {
            case ADD:
                return left + right;
            case SUBTRACT:
                return left - right;
            case MULTIPLY:
                return left * right;
            case DIVIDE:
                return left / right;
            case MODULO:
                return left % right;
            default:
                throw new IllegalArgumentException(operator + " is not a valid calculation operator");
        }
    }

    /**
     * Return the result of the calculation for the provided floats using the provided calculation operator. Supported operators:
     * <ul>
     * <li>+ returns the sum of the left and right</li>
     * <li>- returns the difference of the left and right</li>
     * <li>* returns the product of the left and right</li>
     * <li>/ returns the quotient of the left and right</li>
     * <li>% returns the modulo of the left and right</li>
     * </ul>
     *
     * @param left
     *            the left side of the expression
     * @param right
     *            the right side of the expression
     * @param operator
     *            the calculation operator
     * @return the calculation result
     */
    public static float calculate(float left, float right, String operator) {
        Preconditions.checkArgument(!StringUtils.isBlank(operator), "operator must not be blank");
        operator = CharMatcher.whitespace().removeFrom(operator);
        switch (operator) {
            case ADD:
                return left + right;
            case SUBTRACT:
                return left - right;
            case MULTIPLY:
                return left * right;
            case DIVIDE:
                return left / right;
            case MODULO:
                return left % right;
            default:
                throw new IllegalArgumentException(operator + " is not a valid calculation operator");
        }
    }

    /**
     * Return the result of the calculation for the provided doubles using the provided calculation operator. Supported operators:
     * <ul>
     * <li>+ returns the sum of the left and right</li>
     * <li>- returns the difference of the left and right</li>
     * <li>* returns the product of the left and right</li>
     * <li>/ returns the quotient of the left and right</li>
     * <li>% returns the modulo of the left and right</li>
     * </ul>
     *
     * @param left
     *            the left side of the expression
     * @param right
     *            the right side of the expression
     * @param operator
     *            the calculation operator
     * @return the calculation result
     */
    public static double calculate(double left, double right, String operator) {
        Preconditions.checkArgument(!StringUtils.isBlank(operator), "operator must not be blank");
        operator = CharMatcher.whitespace().removeFrom(operator);
        switch (operator) {
            case ADD:
                return left + right;
            case SUBTRACT:
                return left - right;
            case MULTIPLY:
                return left * right;
            case DIVIDE:
                return left / right;
            case MODULO:
                return left % right;
            default:
                throw new IllegalArgumentException(operator + " is not a valid calculation operator");
        }
    }

    /**
     * Return the result of the calculation for the time in milliseconds of the provided dates using the provided calculation operator. Supported operators:
     * <ul>
     * <li>+ returns the sum of the left and right</li>
     * <li>- returns the difference of the left and right</li>
     * <li>* returns the product of the left and right</li>
     * <li>/ returns the quotient of the left and right</li>
     * <li>% returns the modulo of the left and right</li>
     * </ul>
     *
     * @param left
     *            the left side of the expression
     * @param right
     *            the right side of the expression
     * @param operator
     *            the calculation operator
     * @return the calculation result
     */
    public static long calculate(Date left, Date right, String operator) {
        Preconditions.checkArgument(!StringUtils.isBlank(operator), "operator must not be blank");
        operator = CharMatcher.whitespace().removeFrom(operator);
        return calculate(left.getTime(), right.getTime(), operator);
    }

    /**
     * Return the result of the comparison of the provided integers using the provided logical operator. Supported operators:
     * <ul>
     * <li>=and == returns whether the left and right are equal</li>
     * <li>!= returns whether the left and right are not equal</li>
     * <li>&lt; returns whether the left is less than the right</li>
     * <li>&lt;= returns whether the left is less or equal to the right</li>
     * <li>&gt; returns whether the left is greater than the right</li>
     * <li>&gt;= returns whether the left is greater than or equal to the right</li>
     * </ul>
     *
     * @param left
     *            the left side of the expression
     * @param right
     *            the right side of the expression
     * @param operator
     *            the logical operator
     * @return true if the logical expression evaluates to true, or false otherwise
     */
    public static boolean compare(int left, int right, String operator) {
        Preconditions.checkArgument(!StringUtils.isBlank(operator), "operator must not be blank");
        operator = CharMatcher.whitespace().removeFrom(operator);
        switch (operator) {
            case EQUALS:
            case DOUBLE_EQUALS:
                return left == right;
            case NOT_EQUAL:
                return left != right;
            case LESS_THAN:
                return left < right;
            case LESS_THAN_EQUALS:
                return left <= right;
            case GREATER_THAN:
                return left > right;
            case GREATER_THAN_EQUALS:
                return left >= right;
            default:
                throw new IllegalArgumentException(operator + " is not a valid comparison operator");
        }
    }

    /**
     * Return the result of the comparison of the provided longs using the provided logical operator. Supported operators:
     * <ul>
     * <li>=and == returns whether the left and right are equal</li>
     * <li>!= returns whether the left and right are not equal</li>
     * <li>&lt; returns whether the left is less than the right</li>
     * <li>&lt;= returns whether the left is less or equal to the right</li>
     * <li>&gt; returns whether the left is greater than the right</li>
     * <li>&gt;= returns whether the left is greater than or equal to the right</li>
     * </ul>
     *
     * @param left
     *            the left side of the expression
     * @param right
     *            the right side of the expression
     * @param operator
     *            the logical operator
     * @return true if the logical expression evaluates to true, or false otherwise
     */
    public static boolean compare(long left, long right, String operator) {
        Preconditions.checkArgument(!StringUtils.isBlank(operator), "operator must not be blank");
        operator = CharMatcher.whitespace().removeFrom(operator);
        switch (operator) {
            case EQUALS:
            case DOUBLE_EQUALS:
                return left == right;
            case NOT_EQUAL:
                return left != right;
            case LESS_THAN:
                return left < right;
            case LESS_THAN_EQUALS:
                return left <= right;
            case GREATER_THAN:
                return left > right;
            case GREATER_THAN_EQUALS:
                return left >= right;
            default:
                throw new IllegalArgumentException(operator + " is not a valid comparison operator");
        }
    }

    /**
     * Return the result of the comparison of the provided floats using the provided logical operator. Supported operators:
     * <ul>
     * <li>=and == returns whether the left and right are equal</li>
     * <li>!= returns whether the left and right are not equal</li>
     * <li>&lt; returns whether the left is less than the right</li>
     * <li>&lt;= returns whether the left is less or equal to the right</li>
     * <li>&gt; returns whether the left is greater than the right</li>
     * <li>&gt;= returns whether the left is greater than or equal to the right</li>
     * </ul>
     *
     * @param left
     *            the left side of the expression
     * @param right
     *            the right side of the expression
     * @param operator
     *            the logical operator
     * @return true if the logical expression evaluates to true, or false otherwise
     */
    public static boolean compare(float left, float right, String operator) {
        Preconditions.checkArgument(!StringUtils.isBlank(operator), "operator must not be blank");
        operator = CharMatcher.whitespace().removeFrom(operator);
        switch (operator) {
            case EQUALS:
            case DOUBLE_EQUALS:
                return left == right;
            case NOT_EQUAL:
                return left != right;
            case LESS_THAN:
                return left < right;
            case LESS_THAN_EQUALS:
                return left <= right;
            case GREATER_THAN:
                return left > right;
            case GREATER_THAN_EQUALS:
                return left >= right;
            default:
                throw new IllegalArgumentException(operator + " is not a valid comparison operator");
        }
    }

    /**
     * Return the result of the comparison of the provided doubles using the provided logical operator. Supported operators:
     * <ul>
     * <li>=and == returns whether the left and right are equal</li>
     * <li>!= returns whether the left and right are not equal</li>
     * <li>&lt; returns whether the left is less than the right</li>
     * <li>&lt;= returns whether the left is less or equal to the right</li>
     * <li>&gt; returns whether the left is greater than the right</li>
     * <li>&gt;= returns whether the left is greater than or equal to the right</li>
     * </ul>
     *
     * @param left
     *            the left side of the expression
     * @param right
     *            the right side of the expression
     * @param operator
     *            the logical operator
     * @return true if the logical expression evaluates to true, or false otherwise
     */
    public static boolean compare(double left, double right, String operator) {
        Preconditions.checkArgument(!StringUtils.isBlank(operator), "operator must not be blank");
        operator = CharMatcher.whitespace().removeFrom(operator);
        switch (operator) {
            case EQUALS:
            case DOUBLE_EQUALS:
                return left == right;
            case NOT_EQUAL:
                return left != right;
            case LESS_THAN:
                return left < right;
            case LESS_THAN_EQUALS:
                return left <= right;
            case GREATER_THAN:
                return left > right;
            case GREATER_THAN_EQUALS:
                return left >= right;
            default:
                throw new IllegalArgumentException(operator + " is not a valid comparison operator");
        }
    }

    /**
     * Return the result of the comparison of the time in milliseconds of the provided dates using the provided logical operator. Supported operators:
     * <ul>
     * <li>=and == returns whether the left and right are equal</li>
     * <li>!= returns whether the left and right are not equal</li>
     * <li>&lt; returns whether the left is less than the right</li>
     * <li>&lt;= returns whether the left is less or equal to the right</li>
     * <li>&gt; returns whether the left is greater than the right</li>
     * <li>&gt;= returns whether the left is greater than or equal to the right</li>
     * </ul>
     *
     * @param left
     *            the left side of the expression
     * @param right
     *            the right side of the expression
     * @param operator
     *            the logical operator
     * @return true if the logical expression evaluates to true, or false otherwise
     */
    public static boolean compare(Date left, Date right, String operator) {
        Preconditions.checkArgument(!StringUtils.isBlank(operator), "operator must not be blank");
        operator = CharMatcher.whitespace().removeFrom(operator);
        return compare(left.getTime(), right.getTime(), operator);
    }

    /**
     * Return the result of the comparison of the provided comparables using the provided logical operator. Supported operators:
     * <ul>
     * <li>=and == returns whether the left and right are equal</li>
     * <li>!= returns whether the left and right are not equal</li>
     * <li>&lt; returns whether the left is less than the right</li>
     * <li>&lt;= returns whether the left is less or equal to the right</li>
     * <li>&gt; returns whether the left is greater than the right</li>
     * <li>&gt;= returns whether the left is greater than or equal to the right</li>
     * </ul>
     *
     * @param left
     *            the left side of the expression
     * @param right
     *            the right side of the expression
     * @param operator
     *            the logical operator
     * @return true if the logical expression evaluates to true, or false otherwise
     */
    public static <T extends Comparable<T>> boolean compare(T left, T right, String operator) {
        Preconditions.checkArgument(!StringUtils.isBlank(operator), "operator must not be blank");
        operator = CharMatcher.whitespace().removeFrom(operator);
        switch (CharMatcher.whitespace().removeFrom(operator)) {
            case EQUALS:
            case DOUBLE_EQUALS:
                return left == right || left.compareTo(right) == 0;
            case NOT_EQUAL:
                return left != right && left.compareTo(right) != 0;
            case LESS_THAN:
                return left.compareTo(right) < 0;
            case LESS_THAN_EQUALS:
                return left.compareTo(right) <= 0;
            case GREATER_THAN:
                return left.compareTo(right) > 0;
            case GREATER_THAN_EQUALS:
                return left.compareTo(right) >= 0;
            default:
                throw new IllegalArgumentException(operator + " is not a valid comparison operator");
        }
    }

    /**
     * Return the result of the comparison of the provided objects using the provided logical operator and comparator. Supported operators:
     * <ul>
     * <li>=and == returns whether the left and right are equal</li>
     * <li>!= returns whether the left and right are not equal</li>
     * <li>&lt; returns whether the left is less than the right</li>
     * <li>&lt;= returns whether the left is less or equal to the right</li>
     * <li>&gt; returns whether the left is greater than the right</li>
     * <li>&gt;= returns whether the left is greater than or equal to the right</li>
     * </ul>
     *
     * @param left
     *            the left side of the expression
     * @param right
     *            the right side of the expression
     * @param operator
     *            the logical operator
     * @return true if the logical expression evaluates to true, or false otherwise
     */
    public static <T> boolean compare(T left, T right, String operator, Comparator<T> comparator) {
        Preconditions.checkArgument(!StringUtils.isBlank(operator), "operator must not be blank");
        Preconditions.checkNotNull(comparator, "comparator must not be null");
        operator = CharMatcher.whitespace().removeFrom(operator);
        switch (CharMatcher.whitespace().removeFrom(operator)) {
            case EQUALS:
            case DOUBLE_EQUALS:
                return left == right || comparator.compare(left, right) == 0;
            case NOT_EQUAL:
                return left != right && comparator.compare(left, right) != 0;
            case LESS_THAN:
                return comparator.compare(left, right) < 0;
            case LESS_THAN_EQUALS:
                return comparator.compare(left, right) <= 0;
            case GREATER_THAN:
                return comparator.compare(left, right) > 0;
            case GREATER_THAN_EQUALS:
                return comparator.compare(left, right) >= 0;
            default:
                throw new IllegalArgumentException(operator + " is not a valid comparison operator");
        }
    }

    private OperationEvaluator() {
        throw new UnsupportedOperationException();
    }
}
