package datawave.iterators.filter.ageoff;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.filter.AgeOffTtlUnits;

import org.apache.log4j.Logger;

/**
 * Description: Identifies an age off period
 *
 * Justification: Strongly types of age off periods are identified.
 */
public class AgeOffPeriod {

    private long cutOffMillis;
    private long ttl;
    private String ttlUnits;

    private static final Logger log = Logger.getLogger(AgeOffPeriod.class);

    public AgeOffPeriod(long cutOffMillis) {
        this.ttlUnits = "ms";
        this.ttl = System.currentTimeMillis() - cutOffMillis;
        this.cutOffMillis = cutOffMillis;
    }

    public AgeOffPeriod(long scanStart, long ttl, String ttlUnits) {

        this.ttl = ttl;
        this.ttlUnits = ttlUnits;
        this.cutOffMillis = scanStart - (ttl * getTtlUnitsFactor());
    }

    public long getTtlUnitsFactor() {
        return getTtlUnitsFactor(ttlUnits);
    }

    public static long getTtlUnitsFactor(String units) {
        long ttlUnitsFactor;
        if (units.equals(AgeOffTtlUnits.DAYS)) {
            ttlUnitsFactor = (1000 * 60 * 60 * 24); // ms per day
        } else if (units.equals(AgeOffTtlUnits.HOURS)) {
            ttlUnitsFactor = (1000 * 60 * 60); // ms per hour
        } else if (units.equals(AgeOffTtlUnits.MINUTES)) {
            ttlUnitsFactor = (1000 * 60); // ms per minute
        } else if (units.equals(AgeOffTtlUnits.SECONDS)) {
            ttlUnitsFactor = (1000); // ms per second
        } else if (units.equals(AgeOffTtlUnits.MILLISECONDS)) {
            ttlUnitsFactor = 1;
        } else {
            throw new IllegalArgumentException(AgeOffConfigParams.TTL_UNITS + "=" + units + " must be set to a valid value. (" + AgeOffTtlUnits.DAYS + ", "
                            + AgeOffTtlUnits.HOURS + ", " + AgeOffTtlUnits.MINUTES + ", " + AgeOffTtlUnits.SECONDS + ", or " + AgeOffTtlUnits.MILLISECONDS
                            + "[default = d])");
        }
        return ttlUnitsFactor;
    }

    public long getCutOffMilliseconds() {
        return cutOffMillis;
    }

    public long getTtl() {
        return ttl;
    }

    public String getTtlUnits() {
        return ttlUnits;
    }

    @Override
    public boolean equals(Object o) {
        if (null == o)
            return false;
        if (o == this)
            return true;

        if (o instanceof AgeOffPeriod) {
            AgeOffPeriod agp = AgeOffPeriod.class.cast(o);
            return cutOffMillis == agp.cutOffMillis;

        }

        return false;
    }

    @Override
    public int hashCode() {
        return (int) (cutOffMillis ^ (cutOffMillis >>> 32));
    }
}
