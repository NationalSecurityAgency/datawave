package datawave.annotation.projection;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Checks the keys and Java value types of an already parsed semantic configuration object. These helpers reject unknown or missing keys and values with the
 * wrong type; they do not convert strings to numbers or Booleans. The caller's setup loader owns JSON parsing and validation of the enclosing profile document.
 */
final class SemanticDefinition {
    private SemanticDefinition() {}

    static Map<?,?> object(Object value) {
        if (!(value instanceof Map)) {
            throw new ProjectionException("Expected a semantic object");
        }
        return (Map<?,?>) value;
    }

    static void keys(Map<?,?> object, String... expected) {
        Set<String> keys = Set.of(expected);
        if (!object.keySet().equals(keys)) {
            throw new ProjectionException("Expected exactly keys " + keys + "; received " + object.keySet());
        }
    }

    static String string(Object value) {
        if (!(value instanceof String)) {
            throw new ProjectionException("Expected a configured string");
        }
        return (String) value;
    }

    static String nonblank(Object value) {
        String text = string(value);
        if (blank(text)) {
            throw new ProjectionException("Configured identity must be nonblank");
        }
        return text;
    }

    static boolean bool(Object value) {
        if (!(value instanceof Boolean)) {
            throw new ProjectionException("Expected a configured Boolean");
        }
        return (Boolean) value;
    }

    static List<?> nonemptyList(Object value) {
        if (!(value instanceof List) || ((List<?>) value).isEmpty()) {
            throw new ProjectionException("Expected a nonempty configured array");
        }
        return (List<?>) value;
    }

    /**
     * Returns whether every Java character is whitespace according to {@link Character#isWhitespace(char)}. This test runs before final {@link String#trim()}:
     * em-space is blank, while nonbreaking space and some control characters are not. Empty strings are also blank.
     */
    static boolean blank(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isWhitespace(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
