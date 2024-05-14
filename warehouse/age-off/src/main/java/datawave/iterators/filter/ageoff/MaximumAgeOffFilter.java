package datawave.iterators.filter.ageoff;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.Logger;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.util.CompositeTimestamp;

/**
 * Data type age off filter. Traverses through indexed tables
 *
 *
 * and non-indexed tables. Example follows. Note that
 *
 * any data type TTL will follow the same units specified in ttl units
 *
 * <pre>
 * {@code
 *
 * <rules>
 *     <rule>
 *         <filterClass>datawave.iterators.filter.ageoff.MaximumAgeOffFilter</filterClass>
 *         <ttl units="d">720</ttl>
 *     </rule>
 * </rules>
 * }
 * </pre>
 */
public class MaximumAgeOffFilter extends AppliedRule {

    /**
     * Logger
     */
    private static final Logger log = Logger.getLogger(MaximumAgeOffFilter.class);

    /**
     * Determine whether or not the rules are applied
     */
    protected boolean ruleApplied = true;

    /**
     * Required by the {@code FilterRule} interface. This method returns a {@code boolean} value indicating whether or not to allow the {@code (Key, Value)}
     * pair through the rule. A value of {@code true} indicates that he pair should be passed onward through the {@code Iterator} stack, and {@code false}
     * indicates that the {@code (Key, Value)} pair should not be passed on.
     *
     * <p>
     * If the value provided in the paramter {@code k} does not match the REGEX pattern specified in this filter's configuration options, then a value of
     * {@code true} is returned.
     *
     * @param k
     *            {@code Key} object containing the row, column family, and column qualifier.
     * @param v
     *            {@code Value} object containing the value corresponding to the {@code Key: k}
     * @return {@code boolean} value indicating whether or not to allow the {@code Key, Value} through the {@code Filter}.
     */
    @Override
    public boolean accept(AgeOffPeriod period, Key k, Value v) {

        // this rule determines whether to accept / deny (ageoff) a K/V
        // based solely on whether a timestamp is before (older than) the cutoff for aging off
        if (CompositeTimestamp.getAgeOffDate(k.getTimestamp()) > period.getCutOffMilliseconds()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Required by the {@code FilterRule} interface. Used to initialize the the {@code FilterRule} implementation
     *
     * @param options
     *            {@code Map} object containing the TTL, TTL_UNITS, and MATCHPATTERN for the filter rule.
     * @see datawave.iterators.filter.AgeOffConfigParams
     */
    public void init(FilterOptions options) {
        init(options, null);
    }

    /**
     * Required by the {@code FilterRule} interface. Used to initialize the the {@code FilterRule} implementation
     *
     * @param options
     *            {@code Map} object containing the TTL, TTL_UNITS, and MATCHPATTERN for the filter rule.
     * @param iterEnv
     *            iterator environment
     *
     * @see datawave.iterators.filter.AgeOffConfigParams
     */
    public void init(FilterOptions options, IteratorEnvironment iterEnv) {
        String scanStartStr = options.getOption(AgeOffConfigParams.SCAN_START_TIMESTAMP);
        long scanStart = scanStartStr == null ? System.currentTimeMillis() : Long.parseLong(scanStartStr);
        this.init(options, scanStart, iterEnv);
    }

    protected void init(FilterOptions options, final long scanStart, IteratorEnvironment iterEnv) {
        super.init(options, iterEnv);
        if (options == null) {
            throw new IllegalArgumentException("ttl must be set for a FilterRule implementation");
        }

        if (options.getTTL() < 0) {
            throw new IllegalArgumentException("ttl must be set for a FilterRule implementation");
        }
    }

    @Override
    public boolean isFilterRuleApplied() {
        return ruleApplied;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " [cutOffDateMillis=" + getPeriod().getCutOffMilliseconds() + "]";
    }
}
