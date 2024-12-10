package datawave.query.jexl.lookups.cache;

import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class RegexLookupCache extends LookupCache {

    private static final String CACHE_NAME = "Regex";

    public RegexLookupCache(int size, int expireAfterWriteMinutes, int expireAfterAccessMinutes) {
        super(CACHE_NAME, size, expireAfterWriteMinutes, expireAfterAccessMinutes);
    }

    public static class RegexCacheKey implements LookupCacheKey {
        private final String regex;
        private final boolean reverse;
        private final String startDate;
        private final String endDate;
        private final Set<String> types;

        public RegexCacheKey(String regex, boolean reverse, String startDate, String endDate, Set<String> types) {
            this.regex = regex;
            this.reverse = reverse;
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

            RegexCacheKey that = (RegexCacheKey) o;

            //  @formatter:off
            return new EqualsBuilder()
                    .append(reverse, that.reverse)
                    .append(regex, that.regex)
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
                    .append(regex)
                    .append(reverse)
                    .append(startDate)
                    .append(endDate)
                    .append(types)
                    .toHashCode();
            //  @formatter:on
        }
    }
}
