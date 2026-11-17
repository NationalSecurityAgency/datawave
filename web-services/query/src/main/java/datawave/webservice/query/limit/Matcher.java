package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import datawave.query.parser.JavaRegexAnalyzer;

/**
 * This class defines the behavior for a matcher that will determine if it matches against a string or any string elements of a collection.
 */
public interface Matcher {

    enum Type {
        // Do not reorder these. The ordinal value is important for use when sorting query limits by best match.
        EXACT, PARTIAL, ALL
    }

    Type getType();

    /**
     * Return whether this {@link Matcher} finds a match for the given string.
     *
     * @param value
     *            the string to match
     * @return true if there is a match, or false otherwise
     */
    boolean matches(String value);

    /**
     * Return whether this {@link Matcher} finds a match for any of the elements in the given collection.
     *
     * @param values
     *            the collection to evaluate
     * @return true if there is a match, or false otherwise
     */
    boolean matchesAnyOf(Collection<String> values);

    /**
     * Return a {@link Set} containing the elements in the given collection that match against this {@link Matcher}. Possibly empty, but never null.
     *
     * @param values
     *            the collection to evaluate
     * @return the set of matches
     */
    Set<String> getMatches(Collection<String> values);

    /**
     * Construct and return a {@link Matcher} based off the given string. The type of returned matcher will depend on the string provided:
     * <ul>
     * <li>If the string {@code "*"} or {@code ".*"} is provided, a {@link WildcardMatcher} will be returned.</li>
     * <li>If the string is a regex pattern that does not consist solely of literals and escaped literals, a {@link PatternMatcher} will be returned that will
     * match against the compiled pattern.</li>
     * <li>If the string is a regex pattern that consists solely of literals and escaped literals, a {@link StringMatcher} will be returned that will match
     * against the unescaped form of the regex pattern. For instance, {@code "abc\\k"} will result in a {@link StringMatcher} that will match against {@code
     * "abck"}.</li>
     * <li>Otherwise, a {@link StringMatcher} will be returned that will match against the string.</li>
     * </ul>
     *
     * @param str
     *            the string
     * @param maxCacheSize
     *            the maximum size for the internal cache if a {@link PatternMatcher} is returned
     * @return the matcher
     */
    static Matcher getMatcher(String str, long maxCacheSize) {
        Objects.requireNonNull(str, "Parameter str cannot be null");

        if (QueryLimitConstants.wildcardOnlyPattern.matcher(str).matches()) {
            return WildcardMatcher.of();
        } else {
            // Analyze the regex to determine what, if any, regex constructs are present.
            JavaRegexAnalyzer analyzer;
            try {
                analyzer = new JavaRegexAnalyzer(str);
            } catch (JavaRegexAnalyzer.JavaRegexParseException e) {
                throw new IllegalArgumentException("Failed to analyze regex for string '" + str + "'", e);
            }
            JavaRegexAnalyzer.RegexPart[] regexParts = analyzer.getRegexParts();
            boolean escapedLiteralsSeen = false;
            boolean regexSeen = false;

            // Determine if the regex contains any escaped literals or non-literal regex constructs.
            for (JavaRegexAnalyzer.RegexPart regexPart : regexParts) {
                JavaRegexAnalyzer.RegexType type = regexPart.getType();
                if (type == JavaRegexAnalyzer.RegexType.ESCAPED_LITERAL) {
                    escapedLiteralsSeen = true;
                } else if (type != JavaRegexAnalyzer.RegexType.LITERAL) {
                    // We have seen a non-literal, and can stop early.
                    regexSeen = true;
                    break;
                }
            }

            // If a non-literal regex construct was seen, use a Pattern matcher that falls into the 'partial-match' bucket.
            if (regexSeen) {
                return new PatternMatcher(Pattern.compile(str), maxCacheSize);
            } else if (escapedLiteralsSeen) {
                // If the pattern consists solely of literals and escaped literals, remove the escaping backslashes and use a string matcher that falls into the
                // 'exact-match' bucket.
                String literal = toUnescapedLiteralString(regexParts);
                return new StringMatcher(literal);
            } else {
                // If the pattern consists only of literals, use a string matcher that falls into the 'exact-match' bucket.
                return new StringMatcher(str);
            }
        }
    }

    /**
     * Return the given parts from a regex pattern as a simple, non-escaped string.
     *
     * @param regexParts
     *            the regex parts
     * @return the simplified string
     */
    private static String toUnescapedLiteralString(JavaRegexAnalyzer.RegexPart[] regexParts) {
        StringBuilder sb = new StringBuilder();
        for (JavaRegexAnalyzer.RegexPart part : regexParts) {
            if (part.getType() == JavaRegexAnalyzer.RegexType.LITERAL) {
                sb.append(part.regex);
            } else if (part.getType() == JavaRegexAnalyzer.RegexType.ESCAPED_LITERAL) {
                sb.append(part.getRegex().charAt(1));
            } else {
                throw new IllegalArgumentException(
                                "Regex parts must be of type " + JavaRegexAnalyzer.RegexType.LITERAL + " or " + JavaRegexAnalyzer.RegexType.ESCAPED_LITERAL);
            }
        }
        return sb.toString();
    }
}
