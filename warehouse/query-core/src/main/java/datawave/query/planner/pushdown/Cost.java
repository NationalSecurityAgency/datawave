package datawave.query.planner.pushdown;

/**
 * A class that contains the estimated cost of a term.
 * <p>
 * Costs are handled separately for regex terms and all other terms.
 * <p>
 * At no point should any cost be negative. All operations should check inputs for bounds and overflow conditions.
 */
public class Cost implements Comparable<Cost> {

    private long regexCost = 0L;
    private long otherCost = 0L;

    private static final long REGEX_COST_MULTIPLIER = 2;

    /**
     * Create a zero {@link Cost}
     *
     * @return a cost
     */
    public static Cost zero() {
        return new Cost(0L, 0L);
    }

    /**
     * Create an infinite regex {@link Cost}
     *
     * @return a cost
     */
    public static Cost infiniteRegex() {
        return new Cost(Long.MAX_VALUE, 0L);
    }

    /**
     * Create an infinite cost for a non-regex term
     *
     * @return a cost
     */
    public static Cost infiniteOther() {
        return new Cost(0L, Long.MAX_VALUE);
    }

    /**
     * Create a {@link Cost} for a regex using the {@link #REGEX_COST_MULTIPLIER}
     *
     * @param value
     *            the estimated value
     * @return a regex cost
     */
    public static Cost regex(long value) {
        return new Cost(value, 0L);
    }

    /**
     * Create a {@link Cost} for a non-regex term
     *
     * @param value
     *            the estimated value
     * @return a non-regex cost
     */
    public static Cost other(long value) {
        return new Cost(0L, value);
    }

    /**
     * Static entrypoint for creating a non-standard {@link Cost}. Preference should be given to one of the existing static entry points for code clarity.
     *
     * @param regexCost
     *            the regex cost
     * @param otherCost
     *            the standard cost
     * @return the cost
     */
    public static Cost of(long regexCost, long otherCost) {
        return new Cost(regexCost, otherCost);
    }

    /**
     * Constructor that accepts a regex cost and a standard cost
     *
     * @param regexCost
     *            the regex cost
     * @param otherCost
     *            the standard cost
     */
    private Cost(long regexCost, long otherCost) {
        setRegexCost(regexCost);
        setOtherCost(otherCost);
    }

    /**
     * True if both the regex cost and standard cost is zero
     *
     * @return true if the cost is zero
     */
    public boolean isZeroCost() {
        return regexCost == 0L && otherCost == 0L;
    }

    /**
     * True if either the regex cost or the standard cost is {@link Long#MAX_VALUE}
     *
     * @return true if the cost is infinite
     */
    public boolean isInfinite() {
        return regexCost == Long.MAX_VALUE || otherCost == Long.MAX_VALUE;
    }

    /**
     * Increment the current cost by the other cost
     *
     * @param other
     *            the other cost
     */
    public void incrementBy(Cost other) {
        incrementRegexCost(other.getRegexCost());
        incrementOtherCost(other.getOtherCost());
    }

    /**
     * Increment the regex cost with error handling for Long overflows
     *
     * @param other
     *            the other regex cost
     */
    public void incrementRegexCost(long other) {
        long next = regexCost + other;

        if (next < 0) {
            regexCost = Long.MAX_VALUE;
        } else {
            regexCost = next;
        }
    }

    /**
     * Set the regex cost
     *
     * @param newCost
     *            the new regex cost
     */
    public void setRegexCost(long newCost) {
        if (newCost < 0) {
            throw new IllegalArgumentException("newCost must be >= 0");
        }

        long weightedCost = newCost * REGEX_COST_MULTIPLIER;
        if (weightedCost < 0) {
            regexCost = Long.MAX_VALUE;
        } else {
            regexCost = weightedCost;
        }
    }

    /**
     * Get the regex cost
     *
     * @return the regex cost
     */
    public long getRegexCost() {
        return regexCost;
    }

    /**
     * Get the sum of the regex cost and the standard cost.
     *
     * @return the total cost
     */
    public long totalCost() {
        long sum = regexCost + otherCost;
        if (sum < 0) {
            // check for overflow
            return Long.MAX_VALUE;
        }
        return sum;
    }

    /**
     * Increment the standard cost with error handling
     *
     * @param newCost
     *            the other cost
     */
    public void incrementOtherCost(long newCost) {
        long next = otherCost + newCost;

        if (next < 0) {
            // check for overflow
            otherCost = Long.MAX_VALUE;
        } else {
            otherCost = next;
        }
    }

    /**
     * Set the standard cost
     *
     * @param newCost
     *            the standard cost
     */
    public void setOtherCost(long newCost) {
        if (newCost < 0) {
            throw new IllegalArgumentException("newCost must be >= 0");
        }
        otherCost = newCost;
    }

    /**
     * Get the standard cost
     *
     * @return the standard cost
     */
    public long getOtherCost() {
        return otherCost;
    }

    @Override
    public String toString() {
        return "Regex cost: " + getRegexCost() + ", Other cost: " + getOtherCost();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Cost) {
            Cost other = (Cost) o;
            return getRegexCost() == other.getRegexCost() && getOtherCost() == other.getOtherCost();
        }

        return false;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(regexCost);
        result = 31 * result + Long.hashCode(otherCost);
        return result;
    }

    @Override
    public int compareTo(Cost other) {
        return Long.compare(totalCost(), other.totalCost());
    }
}
