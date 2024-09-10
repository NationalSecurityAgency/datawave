package datawave.query.jexl;

import java.util.regex.Pattern;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * This class maintains a cache of compiled {@link Pattern} instances for regex strings. All patterns are compiled with case-insensitive and multiline matching.
 */
public class JexlPatternCache {

    private static final Cache<String,Pattern> PATTERN_CACHE = CacheBuilder.newBuilder().maximumSize(10000L).initialCapacity(100).concurrencyLevel(10).build();

    /**
     * Returns a cached {@link Pattern} that has been compiled with case-insensitive and multiline matching for the given regex. If a {@link Pattern} is not
     * already cached, one will be created.
     *
     * @param regex
     *            the regex string
     * @return the cached {@link Pattern}
     */
    public static Pattern getPattern(String regex) {
        Pattern pattern = PATTERN_CACHE.getIfPresent(regex);
        if (null != pattern) {
            return pattern;
        }

        pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
        PATTERN_CACHE.put(regex, pattern);

        return pattern;
    }

    // Do not allow this class to be instantiated.
    private JexlPatternCache() {
        throw new UnsupportedOperationException();
    }
}
