package datawave.visibility;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.apache.accumulo.access.ParsedAccessExpression.ExpressionType.AND;
import static org.apache.accumulo.access.ParsedAccessExpression.ExpressionType.AUTHORIZATION;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.access.Access;
import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.access.ParsedAccessExpression;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Utility for bridging between Accumulo's {@link ColumnVisibility} (from accumulo-core) and {@link AccessExpression} (from accumulo-access), and for
 * normalizing expressions.
 */
public final class AccessExpressionUtil {

    private static final Access ACCESS = Access.builder().build();
    private static final AccessExpression EMPTY_EXPRESSION = ACCESS.newExpression("");
    private static final ColumnVisibility EMPTY_VISIBILITY = new ColumnVisibility();

    private AccessExpressionUtil() {}

    /** Normalize the conjunction of all nonempty inputs, preserving parentheses around each input expression. */
    public static ColumnVisibility conjunction(Collection<ColumnVisibility> visibilities) {
        Set<String> expressions = new LinkedHashSet<>();
        if (visibilities != null) {
            for (ColumnVisibility visibility : visibilities) {
                if (visibility != null && visibility.getExpression().length != 0) {
                    expressions.add(toAccessExpression(visibility).getExpression());
                }
            }
        }
        if (expressions.isEmpty()) {
            return new ColumnVisibility();
        }
        String combined = expressions.size() == 1 ? expressions.iterator().next()
                        : expressions.stream().map(expression -> "(" + expression + ")").collect(Collectors.joining("&"));
        return new ColumnVisibility(normalize(ACCESS.newParsedExpression(combined)).expression);
    }

    /**
     * Convert an {@link AccessExpression} to a {@link ColumnVisibility}.
     *
     * @param ae
     *            the access expression
     * @return the equivalent column visibility, or null if {@code ae} is null
     */
    public static ColumnVisibility toColumnVisibility(AccessExpression ae) {
        if (null == ae) {
            return null;
        }
        String expr = ae.getExpression();
        if (expr.isEmpty()) {
            return EMPTY_VISIBILITY;
        }
        return new ColumnVisibility(expr);
    }

    /**
     * Convert a {@link ColumnVisibility} to an {@link AccessExpression}.
     *
     * @param cv
     *            the column visibility
     * @return the equivalent access expression, or null if {@code cv} is null
     */
    public static AccessExpression toAccessExpression(ColumnVisibility cv) {
        if (null == cv) {
            return null;
        }
        return toAccessExpression(cv.getExpression());
    }

    /**
     * Convert a visibility String to an {@link AccessExpression}.
     *
     * @param visibility
     *            the visibility String
     * @return the equivalent access expression; the empty expression for null or empty input
     */
    public static AccessExpression toAccessExpression(String visibility) {
        if (visibility == null || visibility.isEmpty()) {
            return EMPTY_EXPRESSION;
        }
        return ACCESS.newExpression(visibility);
    }

    /**
     * Convert a raw visibility byte array (e.g. from {@code Key.getColumnVisibilityData()}) to an {@link AccessExpression}.
     *
     * @param visibilityBytes
     *            the raw visibility bytes
     * @return the equivalent access expression; the empty expression for null or empty input
     */
    public static AccessExpression toAccessExpression(byte[] visibilityBytes) {
        if (visibilityBytes == null || visibilityBytes.length == 0) {
            return EMPTY_EXPRESSION;
        }
        return ACCESS.newExpression(new String(visibilityBytes, UTF_8));
    }

    /**
     * Normalize an {@link AccessExpression} by deduplicating and sorting terms.
     *
     * @param expression
     *            the expression to normalize
     * @return a normalized {@link AccessExpression}, or null if {@code expression} is null
     */
    public static AccessExpression normalize(AccessExpression expression) {
        if (null == expression) {
            return null;
        }
        return normalize(expression.getExpression());
    }

    /**
     * Normalize a {@link ColumnVisibility} by deduplicating and sorting terms.
     *
     * @param cv
     *            the column visibility to normalize
     * @return a normalized {@link AccessExpression}, or null if {@code cv} is null
     */
    public static AccessExpression normalize(ColumnVisibility cv) {
        if (null == cv) {
            return null;
        }
        return normalize(new String(cv.getExpression(), UTF_8));
    }

    /**
     * Normalize a visibility String by deduplicating and sorting terms.
     *
     * @param cv
     *            the visibility to normalize
     * @return a normalized {@link AccessExpression}, or null if {@code cv} is null
     */
    public static AccessExpression normalize(String cv) {
        if (null == cv) {
            return null;
        }
        ParsedAccessExpression parsed = ACCESS.newParsedExpression(cv);
        return ACCESS.newExpression(normalize(parsed).expression);
    }

    /**
     * @return the shared {@link Access} instance used by this utility
     */
    public static Access getAccess() {
        return ACCESS;
    }

    /**
     * @return a cached empty {@link AccessExpression}
     */
    public static AccessExpression emptyExpression() {
        return EMPTY_EXPRESSION;
    }

    /**
     * As part of normalizing an access expression this class is used to sort and dedupe sub-expressions in a tree set.
     */
    public static class NormalizedExpression implements Comparable<NormalizedExpression> {
        public final String expression;
        public final ParsedAccessExpression.ExpressionType type;

        NormalizedExpression(String expression, ParsedAccessExpression.ExpressionType type) {
            this.expression = expression;
            this.type = type;
        }

        // determines the sort order of different kinds of subexpressions.
        private static int typeOrder(ParsedAccessExpression.ExpressionType type) {
            switch (type) {
                case AUTHORIZATION:
                    return 1;
                case OR:
                    return 2;
                case AND:
                    return 3;
                case EMPTY:
                default:
                    throw new IllegalArgumentException("Unexpected type " + type);
            }
        }

        @Override
        public int compareTo(NormalizedExpression o) {
            // Changing this comparator would significantly change how expressions are normalized.
            int cmp = typeOrder(type) - typeOrder(o.type);
            if (cmp == 0) {
                if (type == AUTHORIZATION) {
                    // sort based on the unquoted and unescaped form of the authorization
                    cmp = ACCESS.unquote(expression).compareTo(ACCESS.unquote(o.expression));
                } else {
                    cmp = expression.compareTo(o.expression);
                }
            }
            return cmp;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || (o instanceof NormalizedExpression && compareTo((NormalizedExpression) o) == 0);
        }

        @Override
        public int hashCode() {
            return expression.hashCode();
        }
    }

    /**
     * Helps with the flattening aspect of normalization by recursing down the parse tree as far as possible while the expression type stays the same.
     */
    private static void flatten(ParsedAccessExpression.ExpressionType parentType, ParsedAccessExpression parsed,
                    SortedSet<NormalizedExpression> normalizedExpressions) {
        if (parsed.getType() == parentType) {
            for (var child : parsed.getChildren()) {
                flatten(parentType, child, normalizedExpressions);
            }
        } else {
            // The type changed, so start again on the subexpression.
            normalizedExpressions.add(normalize(parsed));
        }
    }

    /**
     * Deduplicate, sort, flatten, and remove unneeded parentheses or quotes in an expression. For example:
     * <ul>
     * <li>flattening: {@code A&(B&C)} becomes {@code A&B&C}</li>
     * <li>sorting: {@code (Z&Y)|(C&B)} becomes {@code (B&C)|(Y&Z)}</li>
     * <li>deduplication: {@code X&Y&X} becomes {@code X&Y}</li>
     * <li>unneeded quotes: {@code "ABC"&"XYZ"} becomes {@code ABC&XYZ}</li>
     * <li>unneeded parentheses: {@code (((ABC)|(XYZ)))} becomes {@code ABC|XYZ}</li>
     * </ul>
     * This attempts to match the behavior of the Accumulo 2.1 {@code ColumnVisibility} normalization, although the implementation is different.
     *
     * @param parsed
     *            the parsed expression
     * @return the normalized expression
     */
    public static NormalizedExpression normalize(ParsedAccessExpression parsed) {
        if (parsed.getType() == AUTHORIZATION) {
            // If the authorization is quoted and it does not need to be quoted then this removes the unnecessary quoting.
            String unquoted = ACCESS.unquote(parsed.getExpression());
            String quoted = ACCESS.quote(unquoted);
            return new NormalizedExpression(quoted, parsed.getType());
        } else {
            // The tree set does the work of sorting and deduplicating sub expressions.
            TreeSet<NormalizedExpression> normalizedChildren = new TreeSet<>();
            for (var child : parsed.getChildren()) {
                flatten(parsed.getType(), child, normalizedChildren);
            }

            if (normalizedChildren.size() == 1) {
                return normalizedChildren.first();
            } else {
                return new NormalizedExpression(join(parsed, normalizedChildren), parsed.getType());
            }
        }
    }

    private static String join(ParsedAccessExpression parsed, TreeSet<NormalizedExpression> normalizedChildren) {
        String operator = parsed.getType() == AND ? "&" : "|";
        String sep = "";
        StringBuilder builder = new StringBuilder();
        for (var child : normalizedChildren) {
            builder.append(sep);
            if (child.type == AUTHORIZATION) {
                builder.append(child.expression);
            } else {
                builder.append("(").append(child.expression).append(")");
            }
            sep = operator;
        }
        return builder.toString();
    }
}
