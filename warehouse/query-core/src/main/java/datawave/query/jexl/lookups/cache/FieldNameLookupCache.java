package datawave.query.jexl.lookups.cache;

import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

//  might not need this given the unbounded nature of field name lookups
public class FieldNameLookupCache extends LookupCache {

    private static final String CACHE_NAME = "Field";

    public FieldNameLookupCache(int size, int expireAfterWriteMinutes, int expireAfterAccessMinutes) {
        super(CACHE_NAME, size, expireAfterWriteMinutes, expireAfterAccessMinutes);
    }

    public static class FieldNameCacheKey implements LookupCacheKey {

        private final String value;
        private final String startDate;
        private final String endDate;
        private final Set<String> types;

        public FieldNameCacheKey(String value, String startDate, String endDate, Set<String> types) {
            this.value = value;
            this.startDate = startDate;
            this.endDate = endDate;
            this.types = types;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            FieldNameCacheKey that = (FieldNameCacheKey) o;

            //  @formatter:off
            return new EqualsBuilder()
                    .append(value, that.value)
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
                    .append(value)
                    .append(startDate)
                    .append(endDate)
                    .append(types)
                    .toHashCode();
            //  @formatter:on
        }
    }
}
