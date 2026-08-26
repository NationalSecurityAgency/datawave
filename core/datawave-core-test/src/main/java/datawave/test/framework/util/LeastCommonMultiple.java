package datawave.test.framework.util;

import java.util.List;

import com.google.common.base.Preconditions;

/**
 * Utility for computing the least common multiple (LCM) of a list of positive integers.
 */
public class LeastCommonMultiple {

    private LeastCommonMultiple() {
        // enforce static access
    }

    /**
     * Compute the least common multiple of the provided numbers.
     *
     * @param numbers
     *            a list of positive integers
     * @return the least common multiple, or 0 if the list is empty
     * @throws IllegalArgumentException
     *             if any number is not positive
     * @throws IllegalStateException
     *             if the result overflows {@link Integer#MAX_VALUE}
     */
    public static int lcm(List<Integer> numbers) {
        if (numbers.isEmpty()) {
            return 0;
        }

        int result = 1;
        for (int number : numbers) {
            Preconditions.checkArgument(number > 0, "numbers must be positive but found %s", number);
            result = lcm(result, number);
        }
        return result;
    }

    private static int lcm(int a, int b) {
        // divide before multiplying to reduce the chance of overflow; widen to long to detect any that remains
        long lcm = (long) (a / gcd(a, b)) * b;
        if (lcm > Integer.MAX_VALUE) {
            throw new IllegalStateException("could not determine LCM before overflowing Integer.MAX_VALUE");
        }
        return (int) lcm;
    }

    private static int gcd(int a, int b) {
        while (b != 0) {
            int temp = b;
            b = a % b;
            a = temp;
        }
        return a;
    }
}
