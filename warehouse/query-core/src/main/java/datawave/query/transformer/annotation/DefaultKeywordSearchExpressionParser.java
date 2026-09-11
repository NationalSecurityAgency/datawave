package datawave.query.transformer.annotation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import datawave.data.normalizer.Normalizer;

/**
 * Parses one already-decoded logical explicit-keyword value. In particular, this class deliberately does not know about JSON, URL encoding, or the legacy
 * semicolon-delimited parameter format.
 */
public class DefaultKeywordSearchExpressionParser implements KeywordParser {
    private static final long serialVersionUID = 1L;
    private static final int PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;

    private final Normalizer<String> normalizer;

    /** Uses the normalizer used by the default string field type. */
    public DefaultKeywordSearchExpressionParser() {
        this(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
    }

    public DefaultKeywordSearchExpressionParser(Normalizer<String> normalizer) {
        this.normalizer = Objects.requireNonNull(normalizer, "normalizer");
    }

    @Override
    public SearchExpressions parse(String keyword) {
        if (keyword == null || keyword.isEmpty()) {
            return new SearchExpressions();
        }

        List<String> components = scan(keyword);
        List<StandalonePatternExpression> normalized = new ArrayList<>(components.size());
        for (String component : components) {
            String value = normalizer.normalize(component);
            // A normalizer is allowed to eliminate a value. Do not create an
            // expression for such a component (or for an all-empty input).
            if (value != null && !value.isEmpty()) {
                normalized.add(new StandalonePatternExpression(value, PATTERN_FLAGS));
            }
        }

        if (normalized.isEmpty()) {
            return new SearchExpressions();
        }

        List<SearchExpression> expressions = new ArrayList<>(1);
        if (normalized.size() == 1) {
            expressions.add(normalized.get(0));
        } else {
            expressions.add(new ProximityExpression(true, normalized, 1));
        }
        return new SearchExpressions(expressions);
    }

    /**
     * Splits on unescaped whitespace. Only whitespace and backslash have an escaping meaning here; other backslashes are retained so regex syntax is not
     * changed. A backslash at the end is likewise retained literally.
     */
    private List<String> scan(String input) {
        List<String> components = new ArrayList<>();
        StringBuilder component = new StringBuilder();

        for (int i = 0; i < input.length(); i++) {
            char current = input.charAt(i);
            if (current == '\\') {
                if (i + 1 == input.length()) {
                    component.append(current);
                } else {
                    char next = input.charAt(i + 1);
                    if (Character.isWhitespace(next) || next == '\\') {
                        component.append(next);
                        i++;
                    } else {
                        component.append(current);
                    }
                }
            } else if (Character.isWhitespace(current)) {
                if (component.length() > 0) {
                    components.add(component.toString());
                    component.setLength(0);
                }
            } else {
                component.append(current);
            }
        }

        if (component.length() > 0) {
            components.add(component.toString());
        }
        return components;
    }
}
