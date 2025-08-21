package datawave.webservice.query.limit;

import java.util.Collection;

/**
 * {@link Matcher} implementation that represents a pattern that was determined to be wildcard-only. The matching type for this {@link Matcher} will always be
 * {@link Matcher.Type#ALL}.
 */
public class WildcardMatcher implements Matcher {

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
}
