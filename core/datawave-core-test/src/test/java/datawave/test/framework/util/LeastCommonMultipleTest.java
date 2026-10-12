package datawave.test.framework.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

public class LeastCommonMultipleTest {

    @Test
    public void testSingleNumber() {
        assertLCM(List.of(7), 7);
    }

    @Test
    public void testCoprimePair() {
        assertLCM(List.of(2, 3), 6);
    }

    @Test
    public void testPairSharingAFactor() {
        // regression: 4 * 6 = 24, but the least common multiple is 12
        assertLCM(List.of(4, 6), 12);
    }

    @Test
    public void testPairWhereOneDividesTheOther() {
        assertLCM(List.of(2, 4), 4);
    }

    @Test
    public void testThreeCoprimeNumbers() {
        assertLCM(List.of(2, 3, 17), 102);
    }

    @Test
    public void testThreeNumbersSharingFactors() {
        assertLCM(List.of(4, 6, 8), 24);
    }

    @Test
    public void testDuplicateNumbers() {
        assertLCM(List.of(2, 2, 3), 6);
    }

    @Test
    public void testIncludingOne() {
        assertLCM(List.of(1, 5), 5);
    }

    @Test
    public void testEmptyListReturnsZero() {
        assertLCM(List.of(), 0);
    }

    @Test
    public void testNonPositiveNumberThrows() {
        assertThrows(IllegalArgumentException.class, () -> LeastCommonMultiple.lcm(List.of(2, 0)));
        assertThrows(IllegalArgumentException.class, () -> LeastCommonMultiple.lcm(List.of(2, -3)));
    }

    @Test
    public void testOverflowThrows() {
        assertThrows(IllegalStateException.class, () -> LeastCommonMultiple.lcm(List.of(Integer.MAX_VALUE, 2)));
    }

    private void assertLCM(List<Integer> numbers, int expected) {
        int result = LeastCommonMultiple.lcm(numbers);
        assertEquals(expected, result);
    }
}
