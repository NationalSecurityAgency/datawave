package datawave.query.planner.pushdown;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class CostTest {

    @Test
    public void testStaticConstructors() {
        Cost expectedZeroCost = Cost.of(0L, 0L);
        assertEquals(expectedZeroCost, Cost.zero());

        Cost expectedInfiniteRegexCost = Cost.of(Long.MAX_VALUE, 0L);
        assertEquals(expectedInfiniteRegexCost, Cost.infiniteRegex());

        Cost expectedInfiniteOther = Cost.of(0L, Long.MAX_VALUE);
        assertEquals(expectedInfiniteOther, Cost.infiniteOther());
    }

    @Test
    public void testRegexConstructor() {
        long regexCost = 123456L;
        long expectedRegexCost = regexCost * 2;
        Cost cost = Cost.regex(regexCost);
        assertEquals(expectedRegexCost, cost.getERCost(), "If this test fails then the ER_COST_MULTIPLIER was updated");

        Cost infinitePositive = Cost.regex(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, infinitePositive.getERCost());

        assertThrows(IllegalArgumentException.class, () -> Cost.regex(Long.MIN_VALUE));
    }

    @Test
    public void testNonRegexConstructor() {
        long otherCost = 123456L;
        Cost cost = Cost.other(otherCost);
        assertEquals(otherCost, cost.getOtherCost());

        Cost infinitePositive = Cost.other(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, infinitePositive.getOtherCost());

        assertThrows(IllegalArgumentException.class, () -> Cost.other(Long.MIN_VALUE));
    }

    @Test
    public void testIsZeroCost() {
        Cost zero = Cost.zero();
        Cost regex = Cost.regex(1234L);
        Cost other = Cost.other(5678L);
        assertTrue(zero.isZeroCost());
        assertFalse(regex.isZeroCost());
        assertFalse(other.isZeroCost());
    }

    @Test
    public void testIsInfinite() {
        Cost regex = Cost.regex(Long.MAX_VALUE);
        assertTrue(regex.isInfinite());

        Cost other = Cost.other(Long.MAX_VALUE);
        assertTrue(other.isInfinite());
    }

    @Test
    public void testRegexConstructorLongOverflow() {
        long costThatWillOverflow = (Long.MAX_VALUE / 3) * 2;
        Cost cost = Cost.regex(costThatWillOverflow);
        assertEquals(Long.MAX_VALUE, cost.getERCost());
    }

    @Test
    public void testIncrementBy() {
        Cost regex = Cost.regex(123L);
        Cost other = Cost.other(456L);

        Cost sum = Cost.zero();
        sum.incrementBy(regex);
        sum.incrementBy(other);

        assertEquals(regex.getERCost(), sum.getERCost());
        assertEquals(other.getOtherCost(), sum.getOtherCost());
    }

    @Test
    public void testIncrementByMaxValue() {
        Cost regex = Cost.regex(Long.MAX_VALUE);
        Cost other = Cost.other(Long.MAX_VALUE);

        Cost sum = Cost.zero();
        sum.incrementBy(regex);
        sum.incrementBy(other);

        assertEquals(Long.MAX_VALUE, sum.getERCost());
        assertEquals(Long.MAX_VALUE, sum.getOtherCost());
    }

    @Test
    public void testIncrementERCost() {
        long third = Long.MAX_VALUE / 3;
        Cost regex = Cost.regex(third);
        assertEquals(third * 2, regex.getERCost());

        regex.incrementERCost(third);
        assertEquals(third * 3, regex.getERCost());

        regex.incrementERCost(third);
        assertEquals(Long.MAX_VALUE, regex.getERCost());
    }

    @Test
    public void testIncrementERCostWithNegativeValue() {
        long cost = Long.MAX_VALUE / 2;
        Cost regex = Cost.regex(cost);

        regex.incrementERCost(Long.MIN_VALUE);
        assertEquals(Long.MAX_VALUE, regex.getERCost());
    }

    @Test
    public void testIncrementOtherCost() {
        long third = Long.MAX_VALUE / 3;
        Cost other = Cost.other(third);
        assertEquals(third, other.getOtherCost());

        other.incrementOtherCost(third);
        assertEquals(third * 2, other.getOtherCost());

        other.incrementOtherCost(third);
        assertEquals(third * 3, other.getOtherCost());

        other.incrementOtherCost(third);
        assertEquals(Long.MAX_VALUE, other.getOtherCost());
    }

    @Test
    public void testIncrementOtherCostWithNegativeValue() {
        long cost = Long.MAX_VALUE / 2;
        Cost other = Cost.other(cost);

        other.incrementOtherCost(Long.MIN_VALUE);
        assertEquals(Long.MAX_VALUE, other.getOtherCost());
    }

    @Test
    public void testSetMaxValue() {
        Cost cost = Cost.zero();
        cost.setERCost(Long.MAX_VALUE);
        cost.setOtherCost(Long.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, cost.getERCost());
        assertEquals(Long.MAX_VALUE, cost.getOtherCost());
    }

    @Test
    public void testSetMinValue() {
        Cost cost = Cost.zero();
        assertThrows(IllegalArgumentException.class, () -> cost.setERCost(Long.MIN_VALUE));
    }

    @Test
    public void testGetTotalCost() {
        long twoThirdsMax = (Long.MAX_VALUE / 3) * 2;
        Cost cost = Cost.zero();
        cost.setERCost(twoThirdsMax);
        cost.setOtherCost(twoThirdsMax);
        assertEquals(Long.MAX_VALUE, cost.totalCost());

        Cost other = Cost.of(11L, 22L);
        assertEquals(44L, other.totalCost());
    }

    @Test
    public void testEquals() {
        // full equality check
        Cost first = Cost.of(123L, 456L);
        Cost second = Cost.of(123L, 456L);
        assertEquals(first, second);

        // equality check fails on regex cost
        first = Cost.of(123L, 456L);
        second = Cost.of(999L, 456L);
        assertNotEquals(first, second);

        // equality check fails on other cost
        first = Cost.of(123L, 999L);
        second = Cost.of(123L, 456L);
        assertNotEquals(first, second);

        Long other = 123L;
        assertNotEquals(first, other);
    }

    @Test
    public void testCompareTo() {
        Cost first = Cost.of(123L, 456L);
        Cost second = Cost.of(123L, 456L);
        assertEquals(0, first.compareTo(second));
    }

    @Test
    public void testHashCodeViaSet() {
        Set<Cost> costs = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            Cost cost = Cost.of(123L, 456L);
            costs.add(cost);
        }
        assertEquals(1, costs.size());
    }

    @Test
    public void testToString() {
        Cost cost = Cost.of(123L, 456L);
        assertEquals("Regex cost: 246, Other cost: 456", cost.toString());
    }

}
