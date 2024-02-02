package datawave.iterators.filter.ageoff;

import java.lang.reflect.InvocationTargetException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import com.google.common.base.Objects;

import datawave.iterators.filter.AgeOffConfigParams;

/**
 * Description: Identifies an applied rule.
 *
 * Purpose: Contains the appropriate internal components to be a rule that is applied to filtering.
 *
 * Justification:FilterRule is an interface, this class enables us to contain the age off period and the options for this applied rule. Similarly, since we can
 * have different options within any application, we should see each applied rule as a discrete implementation of a rule
 */
public abstract class AppliedRule implements FilterRule {

    /**
     * Age off period for this applied rule.
     */
    private AgeOffPeriod ageOffPeriod;

    /**
     * Current options for this applied rule.
     */
    protected FilterOptions currentOptions;

    protected IteratorEnvironment iterEnv;

    private static final Logger log = Logger.getLogger(AppliedRule.class);

    /*
     * (non-Javadoc)
     *
     * @see datawave.iterators.filter.ageoff.FilterRule#init(datawave.iterators.filter.ageoff.FilterOptions)
     */
    @Override
    public void init(FilterOptions options) {
        init(options, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.iterators.filter.ageoff.FilterRule#init(datawave.iterators.filter.ageoff.FilterOptions,
     * org.apache.accumulo.core.iterators.IteratorEnvironment)
     */
    @Override
    public void init(FilterOptions options, IteratorEnvironment iterEnv) {
        this.currentOptions = options;
        ageOffPeriod = options.getAgeOffPeriod();
        this.iterEnv = iterEnv;
    }

    /** Perform initialization in support of a deepCopy, copying any expensive state from the parent. */
    protected void deepCopyInit(FilterOptions newOptions, AppliedRule parentCopy) {
        init(newOptions, iterEnv);
    }

    public abstract boolean isFilterRuleApplied();

    /*
     * (non-Javadoc)
     *
     * @see datawave.iterators.filter.ageoff.FilterRule#accept(datawave.iterators.filter.ageoff.AgeOffPeriod, org.apache.accumulo.core.data.Key,
     * org.apache.accumulo.core.data.Value)
     */

    @Override
    public boolean accept(SortedKeyValueIterator<Key,Value> iter) {
        if (!iter.hasTop()) {
            return false;
        }

        return accept(ageOffPeriod, iter.getTopKey(), iter.getTopValue());
    }

    public boolean accept(Key key, Value value) {
        if (log.isTraceEnabled())
            log.trace("Applying " + ageOffPeriod.getCutOffMilliseconds());

        return accept(ageOffPeriod, key, value);
    }

    public abstract boolean accept(AgeOffPeriod period, Key k, Value V);

    /*
     * (non-Javadoc)
     *
     * @see datawave.iterators.filter.ageoff.FilterRule#deepCopy(datawave.iterators.filter.ageoff.AgeOffPeriod)
     */
    @Override
    public FilterRule deepCopy(AgeOffPeriod period, IteratorEnvironment iterEnv) {
        AppliedRule newFilter;
        try {
            newFilter = (AppliedRule) super.getClass().getDeclaredConstructor().newInstance();
            newFilter.iterEnv = iterEnv;
            newFilter.deepCopyInit(currentOptions, this);
            // for some reason this needs to come after deep copy init
            newFilter.ageOffPeriod = new AgeOffPeriod(period.getCutOffMilliseconds());
            log.trace("Age off is " + newFilter.ageOffPeriod.getCutOffMilliseconds());
            return newFilter;
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            log.error(e);
        }
        return null;
    }

    /**
     * @param scanStart
     * @return
     */
    public FilterRule deepCopy(long scanStart, IteratorEnvironment iterEnv) {
        AppliedRule newFilter;
        try {
            newFilter = (AppliedRule) super.getClass().newInstance();
            FilterOptions newOptions = new FilterOptions(currentOptions);
            newOptions.setOption(AgeOffConfigParams.SCAN_START_TIMESTAMP, Long.toString(scanStart));
            newFilter.iterEnv = iterEnv;
            newFilter.deepCopyInit(newOptions, this);
            // for some reason this needs to come after deep copy init
            newFilter.ageOffPeriod = new AgeOffPeriod(scanStart, currentOptions.ttl, currentOptions.ttlUnits);
            log.trace("Age off is " + newFilter.ageOffPeriod.getCutOffMilliseconds());
            return newFilter;
        } catch (InstantiationException | IllegalAccessException e) {
            log.error(e);
        }
        return null;
    }

    /**
     * @return
     */
    protected AgeOffPeriod getPeriod() {
        return ageOffPeriod;
    }

    @Override
    public boolean equals(Object o) {
        if (null == o)
            return false;
        if (o == this)
            return true;

        if (o instanceof AppliedRule) {
            AppliedRule ar = AppliedRule.class.cast(o);
            return ageOffPeriod.equals(ar.ageOffPeriod) && this.currentOptions.equals(ar.currentOptions);

        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ageOffPeriod, currentOptions);
    }
}
