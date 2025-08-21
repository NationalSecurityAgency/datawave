package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * {@link Matcher} implementation that exactly matches a string. The matching type for this {@link Matcher} will always be {@link Matcher.Type#EXACT}.
 */
public class StringMatcher implements Matcher {

    private final String value;

    public StringMatcher(String value) {
        Objects.requireNonNull(value, "value must not be null");
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public Type getType() {
        return Type.EXACT;
    }

    @Override
    public boolean matches(String value) {
        return Objects.equals(this.value, value);
    }

    @Override
    public boolean matchesAnyOf(Collection<String> values) {
        return values.contains(this.value);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StringMatcher that = (StringMatcher) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", StringMatcher.class.getSimpleName() + "[", "]").add("value='" + value + "'").toString();
    }
}
