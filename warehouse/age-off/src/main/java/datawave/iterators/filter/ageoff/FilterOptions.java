package datawave.iterators.filter.ageoff;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Objects;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.filter.AgeOffTtlUnits;

/**
 * Description: Identifies all options for filter rules
 *
 * Justification: Strongly types filter options. encapsulates all functionality for the filter optinos, including the TTL and the units for these periodicities.
 *
 *
 */
public class FilterOptions {

    /**
     * Base time in this configuration.
     */
    protected long baseTime;

    /**
     * Options for this configuration.
     */
    protected Map<String,String> options;

    /**
     * TTL configured here.
     */
    protected long ttl;

    /**
     * Units for the above
     */
    protected String ttlUnits;

    public FilterOptions() {
        options = new HashMap<>();
        ttl = -1;
        ttlUnits = "d";
    }

    public FilterOptions(long baseTime) {
        options = new HashMap<>();
        ttl = -1;
        ttlUnits = "d";
        this.baseTime = baseTime;
    }

    public FilterOptions(FilterOptions otherOptions) {
        options = new HashMap<>(otherOptions.options);
        ttl = otherOptions.ttl;
        ttlUnits = otherOptions.ttlUnits;
        this.baseTime = otherOptions.baseTime;
    }

    public AgeOffPeriod getAgeOffPeriod() {
        return getAgeOffPeriod(baseTime);
    }

    public AgeOffPeriod getAgeOffPeriod(long timestamp) {
        return new AgeOffPeriod(timestamp, ttl, ttlUnits);
    }

    protected void setBaseTime(long baseTime) {
        this.baseTime = baseTime;
    }

    public void setTTL(long ttl) {
        this.ttl = ttl;
    }

    public long getTTL() {
        return ttl;
    }

    public String getTTLUnits() {
        return ttlUnits;
    }

    public void setTTLUnits(String ttlUnits) {
        if (ttlUnits.equals(AgeOffTtlUnits.DAYS)) {
            this.ttlUnits = AgeOffTtlUnits.DAYS;
        } else if (ttlUnits.equals(AgeOffTtlUnits.HOURS)) {
            this.ttlUnits = AgeOffTtlUnits.HOURS;
        } else if (ttlUnits.equals(AgeOffTtlUnits.MINUTES)) {
            this.ttlUnits = AgeOffTtlUnits.MINUTES;
        } else if (ttlUnits.equals(AgeOffTtlUnits.SECONDS)) {
            this.ttlUnits = AgeOffTtlUnits.SECONDS;
        } else if (ttlUnits.equals(AgeOffTtlUnits.MILLISECONDS)) {
            this.ttlUnits = AgeOffTtlUnits.MILLISECONDS;
        } else {
            throw new IllegalArgumentException(AgeOffConfigParams.TTL_UNITS + " must be set to a valid value. (" + AgeOffTtlUnits.DAYS + ", "
                            + AgeOffTtlUnits.HOURS + ", " + AgeOffTtlUnits.MINUTES + ", " + AgeOffTtlUnits.SECONDS + ", or " + AgeOffTtlUnits.MILLISECONDS
                            + "[default = d])");
        }
    }

    public void setOption(String option, String value) {
        options.put(option, value);
    }

    public String getOption(String option) {
        return options.get(option);
    }

    public String getOption(String option, String def) {
        String value = options.get(option);
        return (value == null ? def : value);
    }

    @Override
    public boolean equals(Object o) {
        if (null == o)
            return false;
        if (o == this)
            return true;

        if (o instanceof FilterOptions) {
            FilterOptions fo = FilterOptions.class.cast(o);
            return baseTime == fo.baseTime && options.equals(fo.options) && ttl == fo.ttl && Objects.equal(ttlUnits, fo.ttlUnits);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(baseTime, options, ttlUnits);
    }

    @Override
    public String toString() {
        return "ttl: " + ttl + " ttlUnits: " + ttlUnits + " baseTime: " + baseTime + " options: " + options;
    }
}
