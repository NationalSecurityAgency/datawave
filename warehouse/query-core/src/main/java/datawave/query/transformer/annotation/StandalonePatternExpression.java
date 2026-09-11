package datawave.query.transformer.annotation;

import java.util.Objects;

/** A single regular-expression criterion. The source and flags, rather than Pattern, are its identity. */
public final class StandalonePatternExpression implements SearchExpression {
    private static final long serialVersionUID = 1L;

    private final String patternSource;
    private final int flags;

    public StandalonePatternExpression(String patternSource, int flags) {
        this.patternSource = Objects.requireNonNull(patternSource, "patternSource");
        this.flags = flags;
    }

    public StandalonePatternExpression(String patternSource) {
        this(patternSource, java.util.regex.Pattern.CASE_INSENSITIVE | java.util.regex.Pattern.UNICODE_CASE);
    }

    public String getPatternSource() {
        return patternSource;
    }

    /** Alias useful to callers that refer to the source as the normalized term. */
    public String getSource() {
        return patternSource;
    }

    public int getFlags() {
        return flags;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StandalonePatternExpression)) {
            return false;
        }
        StandalonePatternExpression that = (StandalonePatternExpression) other;
        return flags == that.flags && patternSource.equals(that.patternSource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(patternSource, flags);
    }
}
