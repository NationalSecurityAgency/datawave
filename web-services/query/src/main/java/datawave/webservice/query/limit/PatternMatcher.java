package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * {@link Matcher} implementation that matches a string against a pattern that was determined to neither be an exact-matching pattern, nor a wildcard-only
 * pattern. The matching type for this {@link Matcher} will always be {@link Matcher.Type#PARTIAL}.
 */
public class PatternMatcher implements Matcher {

    private final Cache<String,Boolean> cache = Caffeine.newBuilder().maximumSize(100).build();
    private final Pattern pattern;

    public PatternMatcher(Pattern pattern) {
        Objects.requireNonNull(pattern, "pattern must not be null");
        this.pattern = pattern;
    }

    public Pattern getPattern() {
        return pattern;
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
        if (matches == null) {
            matches = pattern.matcher(value).matches();
            // Cache the result.
            cache.put(value, matches);
        }
        return matches;
    }

    @Override
    public boolean matchesAnyOf(Collection<String> values) {
        for (String value : values) {
            if (matches(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<String> getMatches(Collection<String> values) {
        return values.stream().filter(this::matches).collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PatternMatcher that = (PatternMatcher) o;
        return Objects.equals(pattern, that.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", PatternMatcher.class.getSimpleName() + "[", "]").add("pattern=" + pattern).toString();
    }
}
