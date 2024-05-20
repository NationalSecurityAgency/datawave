package datawave.iterators.filter;

import java.util.Arrays;
import java.util.Date;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.Logger;

import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterOptions;
import datawave.util.StringUtils;

/**
 * This class provides an abstract base class to be extended to filter based on matching a STRING to the {@code Strings} tokens that represents some portion of
 * a ACCUMULO record's {@code Key} or {@code Value}. For example, a subclass could be used to filter on all or a portion of each record's: Row, Column Family,
 * Column Qualifier, or Column Visibility
 * <P>
 * This filter caches the previous column visibility to avoid expensive parse operations.
 */
public abstract class TokenFilterBase extends AppliedRule {

    private static final Logger log = Logger.getLogger(TokenFilterBase.class);
    private byte[][] patternBytes;
    private boolean ruleApplied;

    private boolean cacheEnabled = false;
    private byte[] prevBytes;
    private boolean prevState;

    // These are the possible delimiters in a column visibility exception for the double quote. NOTE, this is fragile
    // but currently there is no way to get this list out of the accumulo ColumnVisibility class.
    private static final byte[] DELIMITERS = "|&()".getBytes();

    /**
     * This method is to be implemented by sub-classes of this class. Child classes should test the provided tokens against the provided key's column visibility
     * and return true if the pattern was satisfied.
     *
     * @param k
     *            {@code Key} object containing the row, column family, and column qualifier.
     * @param v
     *            {@code Value} object containing the value corresponding to the {@code Key: k}
     * @param testTokens
     *            the tokens to search for (or vs and rules defined by the underlying class)
     * @return {@code boolean} True if the key satisfies the pattern(s)
     */

    public abstract boolean hasToken(Key k, Value v, byte[][] testTokens);

    /**
     * Required by the {@code FilterRule} interface. This method returns a {@code boolean} value indicating whether or not to allow the {@code (Key, Value)}
     * pair through the rule. A value of {@code true} indicates that he pair should be passed onward through the {@code Iterator} stack, and {@code false}
     * indicates that the {@code (Key, Value)} pair should not be passed on.
     *
     * <p>
     * If the value provided in the paramter {@code k} does not match the STRING pattern specified in this filter's configuration options, then a value of
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
        ruleApplied = false;

        if (this.patternBytes == null) {
            // patterns are not being used
            log.trace("patternBytes == null");
            return true;
        }

        byte[] backing = k.getColumnVisibilityData().getBackingArray();

        if (prevBytes == null) {
            prevBytes = backing;
        } else if (cacheEnabled && Arrays.equals(prevBytes, backing)) {
            if (prevState) {
                ruleApplied = true;
                // still need to check timestamps, same viz could be loaded at a different time
                return calculateCutoff(k, period);
            } else {
                return true;
            }
        }

        if (hasToken(k, v, patternBytes)) {
            ruleApplied = true;
            boolean keyAccepted = calculateCutoff(k, period);
            prevBytes = backing;
            prevState = true;
            return keyAccepted;
        } else {
            if (log.isTraceEnabled()) {
                log.trace("did not match the patterns:" + toString(patternBytes));
            }

            prevBytes = backing;
            prevState = false;
            // if the pattern did not match anything, then go ahead and accept this record
            return true;
        }
    }

    /**
     * Calculates if the key falls within the cutoff period
     *
     * @param key
     *            the key
     * @param period
     *            the {@link AgeOffPeriod}
     * @return true if the key is not excluded by the age off period
     */
    protected boolean calculateCutoff(Key key, AgeOffPeriod period) {
        boolean dtFlag = key.getTimestamp() > period.getCutOffMilliseconds();
        if (log.isTraceEnabled()) {
            log.trace("timeStamp = " + key.getTimestamp());
            log.trace("timeStamp = " + period.getCutOffMilliseconds());
            log.trace("timeStamp as Date = " + new Date(key.getTimestamp()));
            log.trace("dtFlag = " + dtFlag);
        }
        return dtFlag;
    }

    /**
     * Required by the {@code FilterRule} interface. Used to initialize the the {@code FilterRule} implementation
     *
     * @param options
     *            {@code Map} object containing the TTL, TTL_UNITS, and MATCHPATTERN for the filter rule.
     * @see datawave.iterators.filter.AgeOffConfigParams
     */
    @Override
    public void init(FilterOptions options) {
        init(options, null);
    }

    /**
     * Required by the {@code FilterRule} interface. Used to initialize the {@code FilterRule} implementation
     *
     * @param options
     *            {@code Map} object containing the TTL, TTL_UNITS, and MATCHPATTERN for the filter rule.
     * @param iterEnv
     *            iterator environment
     * @see datawave.iterators.filter.AgeOffConfigParams
     */
    @Override
    public void init(FilterOptions options, IteratorEnvironment iterEnv) {
        super.init(options, iterEnv);
        if (options == null) {
            throw new IllegalArgumentException("options must be set for FilterRule implementation");
        }

        if (options.getOption(AgeOffConfigParams.MATCHPATTERN) != null) {
            String[] patternStrs = StringUtils.split(options.getOption(AgeOffConfigParams.MATCHPATTERN), ',');
            patternBytes = new byte[patternStrs.length][];
            for (int i = 0; i < patternStrs.length; i++) {
                patternBytes[i] = patternStrs[i].trim().getBytes();
            }
        }

        String opt = options.getOption(AgeOffConfigParams.COLUMN_VISIBILITY_CACHE_ENABLED);
        if (org.apache.commons.lang3.StringUtils.isNotBlank(opt)) {
            cacheEnabled = Boolean.parseBoolean(opt);
        }

        ruleApplied = false;
    }

    protected void setRuleApplied(boolean ruleApplied) {
        this.ruleApplied = ruleApplied;
    }

    @Override
    public boolean isFilterRuleApplied() {
        return ruleApplied;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " [patternStr=" + toString(patternBytes) + ", cutOffDateMillis=" + getPeriod().getCutOffMilliseconds() + "]";
    }

    public String toString(byte[][] patterns) {
        if (patterns == null) {
            return "null";
        }
        StringBuilder builder = new StringBuilder();
        char delimiter = '[';
        for (byte[] pattern : patterns) {
            builder.append(delimiter).append(new String(pattern));
            delimiter = ',';
        }
        builder.append(']');
        return builder.toString();
    }

    protected boolean startsWith(byte[] bytes, int start, byte[] pattern) {
        if (bytes.length - start < pattern.length) {
            return false;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (bytes[i + start] != pattern[i]) {
                return false;
            }
        }
        return true;
    }

    protected int findNextNonDelimiter(byte[] cv, int index) {
        while (index < cv.length && isDelimiter(cv[index])) {
            index++;
        }
        return index;
    }

    protected int findNextDelimiter(byte[] cv, int index) {
        while (index < cv.length && !isDelimiter(cv[index])) {
            index++;
        }
        return index;
    }

    boolean isDelimiter(byte value) {
        for (byte delimiter : DELIMITERS) {
            if (delimiter == value) {
                return true;
            }
        }
        return false;
    }

    public byte[][] getPatternBytes() {
        return patternBytes;
    }
}
