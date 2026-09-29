package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.Set;
import java.util.StringJoiner;

/**
 * {@link Matcher} implementation that represents a pattern that was determined to be wildcard-only. The matching type for this {@link Matcher} will always be
 * {@link Matcher.Type#ALL}.
 */
public class WildcardMatcher implements Matcher {

    private static final WildcardMatcher INSTANCE = new WildcardMatcher();

    public static WildcardMatcher of() {
        return INSTANCE;
    }

    @Override
    public Type getType() {
        return Type.ALL;
    }

    @Override
    public boolean matches(String value) {
        return true;
    }

    @Override
    public boolean matchesAnyOf(Collection<String> values) {
        return true;
    }

    @Override
    public Set<String> getMatches(Collection<String> values) {
        return Set.copyOf(values);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", WildcardMatcher.class.getSimpleName() + "[", "]").toString();
    }
}
