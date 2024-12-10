package datawave.query.jexl.lookups.cache;

import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Cache that records the result of a bounded range lookup
 */
public class BoundedRangeLookupCache extends LookupFailureCache {

    private static final String CACHE_NAME = "Range";

    public BoundedRangeLookupCache(int size, int expireAfterWriteMinutes, int expireAfterAccessMinutes) {
        super(CACHE_NAME, size, expireAfterWriteMinutes, expireAfterAccessMinutes);
    }

    public static class BoundedRangeCacheKey implements LookupCacheKey {

        private final String range;
        private final String startDate;
        private final String endDate;
        private final Set<String> types;

        public BoundedRangeCacheKey(String range, String startDate, String endDate, Set<String> types) {
            this.range = range;
            this.types = types;
            if (startDate.length() > 8) {
                this.startDate = startDate.substring(0, 8);
            } else {
                this.startDate = startDate;
            }

            if (endDate.length() > 8) {
                this.endDate = endDate.substring(0, 8);
            } else {
                this.endDate = endDate;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            BoundedRangeCacheKey that = (BoundedRangeCacheKey) o;

            //  @formatter:off
            return new EqualsBuilder()
                    .append(range, that.range)
                    .append(startDate, that.startDate)
                    .append(endDate, that.endDate)
                    .append(types, that.types)
                    .isEquals();
            //  @formatter:on
        }

        @Override
        public int hashCode() {
            //  @formatter:off
            return new HashCodeBuilder(17, 37)
                    .append(range)
                    .append(startDate)
                    .append(endDate)
                    .append(types)
                    .toHashCode();
            //  @formatter:on
        }
    }
}
