package datawave.webservice.query.limit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * {@link Matcher} implementation that matches a string against a pattern that was determined to neither be an exact-matching pattern, nor a wildcard-only
 * pattern. The matching type for this {@link Matcher} will always be {@link Matcher.Type#PARTIAL}.
 */
public class PatternMatcher implements Matcher {
    
    private final Cache<String, Boolean> cache = Caffeine.newBuilder().build();
    private final Pattern pattern;
    
    public PatternMatcher(Pattern pattern) {
        Objects.requireNonNull(pattern, "pattern must not be null");
        this.pattern = pattern;
    }
    
    @Override
    public Type getType() {
        return Type.PARTIAL;
    }
    
    @Override
    public boolean matches(String value) {
        Objects.requireNonNull(value, "value must not be null");
        // Check if we have cached the match for this before.
        Boolean matches = cache.getIfPresent(value);
        // If not, check if the value matches.
        if(matches == null) {
            matches = pattern.matcher(value).matches();
            // Cache the result.
            cache.put(value, matches);
        }
        return matches;
    }
}
