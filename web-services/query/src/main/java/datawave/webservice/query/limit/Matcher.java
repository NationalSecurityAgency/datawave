package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.regex.Pattern;

import datawave.query.parser.JavaRegexAnalyzer;

/**
 * A matcher is intended to determine if it matches against a string.
 */
public interface Matcher {

    enum Type {
        // Do not reorder these. The ordinal value is important for use when sorting query limits.
        EXACT, PARTIAL, ALL
    }

    Type getType();

    boolean matches(String value);

    boolean matchesAnyOf(Collection<String> values);

    /**
     * Construct and return a {@link Matcher} based off the given string.
     *
     * @param str
     *            the string
     * @return the matcher
     */
    static Matcher getMatcher(String str) {
        if (QueryLimitConstants.wildcardOnlyPattern.matcher(str).matches()) {
            return new WildcardMatcher();
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
                return new PatternMatcher(Pattern.compile(str));
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
