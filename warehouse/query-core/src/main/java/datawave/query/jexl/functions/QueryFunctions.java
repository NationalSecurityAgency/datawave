package datawave.query.jexl.functions;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;

import datawave.data.type.util.NumericalEncoder;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.JexlPatternCache;

/**
 * NOTE: The JexlFunctionArgumentDescriptorFactory is implemented by QueryFunctionsDescriptor. This is kept as a separate class to reduce accumulo dependencies
 * on other jars.
 **/
@JexlFunctions(descriptorFactory = "datawave.query.jexl.functions.QueryFunctionsDescriptor")
public class QueryFunctions {

    public static final String QUERY_FUNCTION_NAMESPACE = "f";
    public static final String OPTIONS_FUNCTION = "options";

    public static final String MOST_RECENT_PREFIX = "most_recent_";
    public static final String UNIQUE_FUNCTION = "unique";
    public static final String GROUPBY_FUNCTION = "groupby";
    public static final String EXCERPT_FIELDS_FUNCTION = "excerpt_fields";
    public static final String SUMMARY_FUNCTION = "summary";
    public static final String LENIENT_FIELDS_FUNCTION = "lenient";
    public static final String STRICT_FIELDS_FUNCTION = "strict";
    public static final String MATCH_REGEX = "matchRegex";
    public static final String INCLUDE_TEXT = "includeText";
    public static final String NO_EXPANSION = "noExpansion";
    public static final String SUM = "sum";
    public static final String MAX = "max";
    public static final String MIN = "min";
    public static final String COUNT = "count";
    public static final String AVERAGE = "average";
    public static final String RENAME_FUNCTION = "rename";

    protected static Logger log = Logger.getLogger(QueryFunctions.class);

    protected static String getHitTermString(Object valueTuple) {
        Object value = ValueTuple.getStringValue(valueTuple);
        Object fieldName = ValueTuple.getFieldName(valueTuple);
        if (value != null && fieldName != null) {
            return fieldName + ":" + value;
        } else {
            return "";
        }
    }

    public static Collection<?> length(Object field, long lower, long upper) {
        Set<String> matches = Collections.emptySet();
        if (field != null) {
            String fieldValue = ValueTuple.getNormalizedStringValue(field);
            if (upper < lower) {
                throw new IllegalArgumentException("upper bound must be greater than the lower bound");
            }
            if (fieldValue != null && fieldValue.length() >= lower && fieldValue.length() <= upper) {
                matches = Collections.singleton(getHitTermString(field));
            }
        }
        return matches;
    }

    public static Collection<?> length(Iterable<?> values, long lower, long upper) {
        if (values != null) {
            for (Object value : values) {
                Collection<?> matches = length(value, lower, upper);
                if (!matches.isEmpty()) {
                    return matches;
                }
            }
        }
        return Collections.emptySet();
    }

    public static Collection<?> between(Object field, String left, String right) {
        return between(field, left, true, right, true);
    }

    public static Collection<?> between(Object field, String left, boolean leftInclusive, String right, boolean rightInclusive) {
        if (field != null) {
            // we only want to test against the normalized variant to be consistent with the DatawaveArithmetic
            String fieldValue = ValueTuple.getNormalizedStringValue(field);
            if (fieldValue != null) {
                int leftComparison = fieldValue.compareTo(left);
                if (leftComparison > 0 || (leftInclusive && leftComparison == 0)) {
                    int rightComparison = fieldValue.compareTo(right);
                    if (rightComparison < 0 || (rightInclusive && rightComparison == 0)) {
                        return Collections.singleton(getHitTermString(field));
                    }
                }
            }
        }
        return Collections.emptySet();
    }

    public static Collection<?> between(Iterable<?> values, String left, String right) {
        return between(values, left, true, right, true);
    }

    public static Collection<?> between(Iterable<?> values, String left, boolean leftInclusive, String right, boolean rightInclusive) {
        if (values != null) {
            for (Object value : values) {
                Collection<?> matches = between(value, left, leftInclusive, right, rightInclusive);
                if (!matches.isEmpty()) {
                    return matches;
                }
            }
        }
        return Collections.emptySet();
    }

    public static Collection<?> between(Object field, float left, float right) {
        return between(field, left, true, right, true);
    }

    public static Collection<?> between(Object field, float left, boolean leftInclusive, float right, boolean rightInclusive) {
        Number value = null;
        if (field != null) {
            String fieldValue = ValueTuple.getNormalizedStringValue(field);
            if (fieldValue != null) {
                if (NumericalEncoder.isPossiblyEncoded(fieldValue)) {
                    try {
                        value = NumericalEncoder.decode(fieldValue);
                    } catch (NumberFormatException nfe) {
                        try {
                            value = NumberUtils.createNumber(fieldValue);
                        } catch (Exception nfe2) {
                            throw new NumberFormatException("Cannot decode " + fieldValue + " using NumericalEncoder or float");
                        }
                    }
                } else {
                    try {
                        value = NumberUtils.createNumber(fieldValue);
                    } catch (Exception nfe2) {
                        throw new NumberFormatException("Cannot decode " + fieldValue + " using float");
                    }
                }
            }
        }
        if (value != null) {
            float floatValue = value.floatValue();
            if (((floatValue > left) || (leftInclusive && floatValue == left)) && ((floatValue < right) || (rightInclusive && floatValue == right))) {
                return Collections.singleton(getHitTermString(field));
            }
        }
        return Collections.emptySet();
    }

    public static Collection<?> between(Iterable<?> values, float left, float right) {
        return between(values, left, true, right, true);
    }

    public static Collection<?> between(Iterable<?> values, float left, boolean leftInclusive, float right, boolean rightInclusive) {
        if (values != null) {
            for (Object value : values) {
                Collection<?> matches = between(value, left, leftInclusive, right, rightInclusive);
                if (!matches.isEmpty()) {
                    return matches;
                }
            }
        }
        return Collections.emptySet();
    }

    public static Collection<?> between(Object fieldValue, long left, long right) {
        return between(fieldValue, left, true, right, true);
    }

    public static Collection<?> between(Object fieldValue, long left, boolean leftInclusive, long right, boolean rightInclusive) {
        return between(fieldValue, (float) left, leftInclusive, (float) right, rightInclusive);
    }

    public static Collection<?> between(Iterable<?> values, long left, long right) {
        return between(values, left, true, right, true);
    }

    public static Collection<?> between(Iterable<?> values, long left, boolean leftInclusive, long right, boolean rightInclusive) {
        return between(values, (float) left, leftInclusive, (float) right, rightInclusive);
    }

    /**
     * Returns a set that contains the hit term for the given field value if the regex matches against the value of the field value. If the regex string
     * contains case-insensitive flags, e.g. {@code (?i).*(?-i)}, a search for a match will also be done against the normalized value of the field value.
     * <p>
     * Note: the regex will be compiled into a {@link Pattern} with case-insensitive and multiline matching.
     *
     * @param fieldValue
     *            the field value to evaluate
     * @param regex
     *            the regex
     * @return a {@link FunctionalSet} with the matching hit term, or an empty set if no matches were found
     */
    public static FunctionalSet<ValueTuple> matchRegex(Object fieldValue, String regex) {
        if (fieldValue != null) {
            Pattern pattern = JexlPatternCache.getPattern(regex);
            boolean caseInsensitive = regex.matches(EvaluationPhaseFilterFunctions.CASE_INSENSITIVE);
            if (isMatchForPattern(pattern, caseInsensitive, fieldValue)) {
                return FunctionalSet.singleton(ValueTuple.toValueTuple(fieldValue));
            }
        }
        return FunctionalSet.emptySet();
    }

    /**
     * Returns a set that contains the hit term if the non-normalized value of the field value matches the given string.
     *
     * @param fieldValue
     *            the field value to evaluate
     * @param valueToMatch
     *            the string to match
     * @return a {@link FunctionalSet} with the matching hit term, or an empty set if no matches were found
     */
    public static FunctionalSet<ValueTuple> includeText(Object fieldValue, String valueToMatch) {
        if (fieldValue != null && ValueTuple.getStringValue(fieldValue).equals(valueToMatch)) {
            return FunctionalSet.singleton(ValueTuple.toValueTuple(fieldValue));
        }
        return FunctionalSet.emptySet();
    }

    /**
     * Returns a set that contains the hit term for the first field value where the regex matches against the value of the field value. If the regex string
     * contains case-insensitive flags, e.g. {@code (?i).*(?-i)}, a search for a match will also be done against the normalized value of the field value.
     * <p>
     * Note: the regex will be compiled into a {@link Pattern} with case-insensitive and multiline matching. Additionally, a regex of {@code ".*"} still
     * requires a value to be present. In other words, searching for {@code FIELD:'.*'} requires a value for {@code FIELD} to exist in the document to match.
     *
     * @param values
     *            the values to evaluate
     * @param regex
     *            the regex
     * @return a {@link FunctionalSet} with the matching hit term, or an empty set if no matches were found
     */
    public static FunctionalSet<ValueTuple> matchRegex(Iterable<?> values, String regex) {
        if (values != null) {
            final Pattern pattern = JexlPatternCache.getPattern(regex);
            final boolean caseInsensitive = regex.matches(EvaluationPhaseFilterFunctions.CASE_INSENSITIVE);
            // @formatter:off
            return StreamSupport.stream(values.spliterator(), false)
                    .filter(Objects::nonNull)
                    .filter((value) -> isMatchForPattern(pattern, caseInsensitive, value))
                    .findFirst()
                    .map(EvaluationPhaseFilterFunctions::getHitTerm)
                    .map(FunctionalSet::singleton)
                    .orElseGet(FunctionalSet::emptySet);
            // @formatter:on
        }
        return FunctionalSet.emptySet();
    }

    /**
     * Returns a set that contains the hit term for the first field value where the non-normalized value matches the given string.
     *
     * @param values
     *            the values to evaluate
     * @param valueToMatch
     *            the string to match
     * @return a {@link FunctionalSet} with the matching hit term, or an empty set if no matches were found
     */
    public static FunctionalSet<ValueTuple> includeText(Iterable<?> values, String valueToMatch) {
        if (values != null) {
            // @formatter:off
            return StreamSupport.stream(values.spliterator(), false)
                    .filter(Objects::nonNull)
                    .filter((value) -> ValueTuple.getStringValue(value).equals(valueToMatch))
                    .findFirst()
                    .map(ValueTuple::toValueTuple)
                    .map(FunctionalSet::singleton)
                    .orElseGet(FunctionalSet::emptySet);
            // @formatter:on
        }
        return FunctionalSet.emptySet();
    }

    // Returns whether the pattern matches against either the non-normalized value or, if caseInsensitive is false, the normalized value.
    private static boolean isMatchForPattern(Pattern pattern, boolean caseInsensitive, Object value) {
        Matcher matcher = pattern.matcher(ValueTuple.getStringValue(value));
        if (!matcher.matches() && !caseInsensitive) {
            matcher.reset(ValueTuple.getNormalizedStringValue(value));
        }
        return matcher.matches();
    }
}
