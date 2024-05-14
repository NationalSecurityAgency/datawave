package datawave.iterators.filter;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.Logger;

import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterOptions;
import datawave.util.CompositeTimestamp;

/**
 * This class provides an abstract base class to be extended to filter based on matching a REGEX to the {@code String} object that represents some portion of a
 * ACCUMULO record's {@code Key} or {@code Value}. For example, a subclass could be used to filter on all or a portion of each record's: Row, Column Family, or
 * Column Qualifier
 *
 */
public abstract class RegexFilterBase extends AppliedRule {

    private static final Logger log = Logger.getLogger(RegexFilterBase.class);
    private String patternStr;
    private Pattern pattern;
    private boolean ruleApplied;

    /**
     * This method is to be implemented by sub-classes of this class. It should return the String that needs to be tested against the REGEX for the instance of
     * this class.
     *
     * @param k
     *            {@code Key} object containing the row, column family, and column qualifier.
     * @param v
     *            {@code Value} object containing the value corresponding to the {@code Key: k}
     * @return {@code String} object containing the value to be tested with this classes REGEX.
     */
    protected abstract String getKeyField(Key k, Value v);

    /**
     * Required by the {@code FilterRule} interface. This method returns a {@code boolean} value indicating whether or not to allow the {@code (Key, Value)}
     * pair through the rule. A value of {@code true} indicates that he pair should be passed onward through the {@code Iterator} stack, and {@code false}
     * indicates that the {@code (Key, Value)} pair should not be passed on.
     *
     * <p>
     * If the value provided in the parameter {@code k} does not match the REGEX pattern specified in this filter's configuration options, then a value of
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
        // Keep the pair if its date is after the cutoff date
        boolean dtFlag = false;
        ruleApplied = false;

        if (this.pattern == null) // stringLiteral is not being used
        {
            if (log.isTraceEnabled())
                log.trace("dataTypeRegex == null");
            dtFlag = true;
        } else {
            String keyField = getKeyField(k, v);
            Matcher matcher = pattern.matcher(keyField);
            if (matcher.find()) {
                long timeStamp = CompositeTimestamp.getAgeOffDate(k.getTimestamp());
                dtFlag = timeStamp > period.getCutOffMilliseconds();
                if (log.isTraceEnabled()) {
                    log.trace("timeStamp = " + timeStamp);
                    log.trace("timeStamp = " + period.getCutOffMilliseconds());
                    log.trace("timeStamp as Date = " + new Date(timeStamp));
                }
                ruleApplied = true;
            } else {
                if (log.isTraceEnabled()) {
                    log.trace(keyField + "did not match the pattern:" + this.patternStr);
                }
                dtFlag = true;
            }

        }
        if (log.isTraceEnabled()) {
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
     * Required by the {@code FilterRule} interface. Used to initialize the the {@code FilterRule} implementation
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

        this.patternStr = options.getOption(AgeOffConfigParams.MATCHPATTERN);
        try {
            this.pattern = Pattern.compile(patternStr);
        } catch (PatternSyntaxException pse) {
            throw new IllegalArgumentException("Error initializing RegexKeyFilterBase with the regex string value:" + this.patternStr, pse);
        }

        ruleApplied = false;
    }

    @Override
    public boolean isFilterRuleApplied() {
        return ruleApplied;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " [patternStr=" + patternStr + ", cutOffDateMillis=" + getPeriod().getCutOffMilliseconds() + "]";
    }

}
