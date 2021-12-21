package datawave.query.jexl.functions;

import com.google.common.base.CharMatcher;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datawave.data.type.Type;
import datawave.query.attributes.ValueTuple;
import datawave.query.jexl.JexlPatternCache;
import datawave.query.collections.FunctionalSet;
import org.apache.commons.collections4.SetUtils;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * NOTE: The {@link JexlFunctionArgumentDescriptorFactory} is implemented by {@link EvaluationPhaseFilterFunctionsDescriptor}. This is kept as a separate class
 * to reduce accumulo dependencies on other jars.
 * <p>
 * NOTE: You will see that most of these functions return a {@link List} of {@link ValueTuple} hits instead of a boolean value. This is because these functions
 * do not have index queries. Hence, the "hits" cannot be determined by the index function. So instead, the {@link datawave.query.jexl.HitListArithmetic} takes
 * the return values from the functions and adds them to the hit list.
 *
 **/
@JexlFunctions(descriptorFactory = "datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor")
public class EvaluationPhaseFilterFunctions {
    public static final String EVAL_PHASE_FUNCTION_NAMESPACE = "filter";
    public static final String CASE_SENSITIVE_EXPRESSION = ".*\\(\\?[idmsux]*-[dmsux]*i[idmsux]*\\).*";
    
    protected static final Logger log = Logger.getLogger(EvaluationPhaseFilterFunctions.class);
    
    public static boolean occurrence(Iterable<?> fieldValues, String operator, int count) {
        return evaluateSizeOf(fieldValues, operator, count);
    }
    
    public static boolean occurrence(Object fieldValue, String operator, int count) {
        return evaluateSizeOf(fieldValue, operator, count);
    }
    
    public static boolean occurrence(Object fieldValue, int count) {
        return evaluateSizeOf(fieldValue, "==", count);
    }
    
    public static boolean occurrence(Iterable<?> values, int count) {
        return evaluateSizeOf(values, "==", count);
    }
    
    private static int getSizeOf(Iterable<?> iterable) {
        Map<String,Integer> countMap = Maps.newHashMap();
        int count = 0;
        if (iterable != null) {
            // additional datatypes may have been added to this field to assist with evaluation and for use as
            // a 'marker' for some other condition. We care only about how many were in the original so
            // bin them all up to count what was really there in the beginning
            for (Object o : iterable) {
                if (o instanceof ValueTuple) {
                    ValueTuple valueTuple = (ValueTuple) o;
                    if (valueTuple.second() instanceof Type<?>) {
                        String typeName = valueTuple.second().getClass().toString();
                        if (countMap.containsKey(typeName)) {
                            countMap.put(typeName, countMap.get(typeName) + 1);
                        } else {
                            countMap.put(typeName, 1);
                        }
                        count = countMap.get(typeName);
                    } else {
                        count++;
                    }
                } else {
                    count++;
                }
            }
        }
        return count;
    }
    
    /**
     * Returns the size of the given object. If the value is null or a non-iterable object, a value of 1 will be returned. If the value is an {@link Iterable},
     * the result of {@link EvaluationPhaseFilterFunctions#getSizeOf(Iterable)} will be returned.
     *
     * @param fieldValue
     *            the object to evaluate
     * @return the evaluated size of the object
     * @see EvaluationPhaseFilterFunctions#getSizeOf(Iterable) additional documentation on how the size is determined for an Iterable
     */
    private static int getSizeOf(Object fieldValue) {
        if (fieldValue instanceof Iterable<?>) {
            return getSizeOf((Iterable<?>) fieldValue);
        } else {
            return 1;
        }
    }
    
    private static boolean evaluateSizeOf(Object obj, String operatorString, int count) {
        int size = getSizeOf(obj);
        if (log.isDebugEnabled())
            log.debug("evaluate(" + obj + ", size=" + size + " " + operatorString + ", " + count + ")");
        // clean up, in case they input extra spaces in or around the operator
        switch (CharMatcher.WHITESPACE.removeFrom(operatorString)) {
            case "<":
                return size < count;
            case "<=":
                return size <= count;
            case "==":
                return size == count;
            case "=":
                return size == count;
            case ">=":
                return size >= count;
            case ">":
                return size > count;
            case "!=":
                return size != count;
        }
        throw new IllegalArgumentException("cannot use " + operatorString + " in this equation");
    }
    
    private static boolean evaluate(long term1, long term2, String operatorString, String equalityString, long goalResult) {
        long result = calculate(term1, term2, operatorString);
        // clean up, in case they input extra spaces in or around the equalityString
        switch (CharMatcher.WHITESPACE.removeFrom(equalityString)) {
            case "<":
                return result < goalResult;
            case "<=":
                return result <= goalResult;
            case "==":
                return result == goalResult;
            case "=":
                return result == goalResult;
            case ">=":
                return result >= goalResult;
            case ">":
                return result > goalResult;
            case "!=":
                return result != goalResult;
        }
        throw new IllegalArgumentException("cannot use " + equalityString + " in this equation");
    }
    
    private static long calculate(long term1, long term2, String operatorString) {
        // clean up, in case they input extra spaces in or around the operatorString
        switch (CharMatcher.WHITESPACE.removeFrom(operatorString)) {
            case "+":
                return term1 + term2;
            case "-":
                return term1 - term2;
            case "*":
                return term1 * term2;
            case "/":
                return term1 / term2;
        }
        throw new IllegalArgumentException("cannot use " + operatorString + " in this equation");
    }
    
    /**
     * Returns a {@link FunctionalSet} of hit terms found for {@code fieldValue}. If {@code fieldValue} is a singular value tuple, a singleton
     * {@link FunctionalSet} with the hit term from it will be returned. If {@code fieldValue} is a non-empty collection of value tuples, a
     * {@link FunctionalSet} containing the hit terms from each value in the collection will be returned. Otherwise, an empty {@link FunctionalSet} will be
     * returned.
     *
     * @param fieldValue
     *            the field value to evaluate
     * @return the {@link FunctionalSet} of hit terms found
     */
    public static FunctionalSet<ValueTuple> isNotNull(Object fieldValue) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            // fieldValue might be an empty collection.
            if (fieldValue instanceof Collection) {
                if (((Collection) fieldValue).isEmpty()) {
                    return matches; // which is an empty set
                } else {
                    matches = new FunctionalSet<>();
                    for (Object value : (Collection) fieldValue) {
                        matches.add(getHitTerm(value));
                    }
                }
            } else {
                matches = FunctionalSet.singleton(getHitTerm(fieldValue));
            }
        }
        return matches;
    }
    
    /**
     * Returns whether {@code fieldValue} is considered an equivalently null field value.
     *
     * @param fieldValue
     *            the fieldValue
     * @return true if {@code fieldValue} is a null {@link Object} or an empty {@link Collection}, or false otherwise
     */
    public static boolean isNull(Object fieldValue) {
        if (fieldValue instanceof Collection)
            return ((Collection) fieldValue).isEmpty();
        return fieldValue == null;
    }
    
    /**
     * Return whether no match was found for the given regex against the value of the field value. If the regex string contains case-insensitive flags, e.g.
     * {@code (?i).*(?-i)}, a search for a match will also be done against the normalized value of the field value.
     * <p>
     * Note: the regex will be compiled into a {@link Pattern} with case-insensitive and multiline matching.
     *
     * @param fieldValue
     *            the field value to evaluate
     * @param regex
     *            the regex
     * @return true if no match was found for the given regex, or false otherwise
     */
    public static boolean excludeRegex(Object fieldValue, String regex) {
        return includeRegex(fieldValue, regex).isEmpty();
    }
    
    /**
     * Returns whether no match was found for the given regex against the value of any field value provided in the given {@link Iterable}. If the regex string
     * contains case-insensitive flags, e.g. {@code (?i).*(?-i)}, a search for a match will also be done against the normalized value of the field values.
     * <p>
     * Note: the regex will be compiled into a {@link Pattern} with case-insensitive and multiline matching. Additionally, a regex of {@code ".*"} still
     * requires a value to be present. In other words, searching for {@code FIELD:'.*'} requires a value for {@code FIELD} to exist in the document to match.
     *
     * @param values
     *            the values to evaluate
     * @param regex
     *            the regex
     * @return true if no match was found for the given regex, or false otherwise
     */
    public static boolean excludeRegex(Iterable<?> values, String regex) {
        return includeRegex(values, regex).isEmpty();
    }
    
    /**
     * Returns a {@link FunctionalSet} with {@link ValueTuple} of matches found in the {@code fieldValue} for each given regex, if the number of matches meets
     * the minimum parsed from the given {@code minimumRequired}. If the minimum was not met, then an empty {@link FunctionalSet} will be returned.
     * <p>
     * NOTE: this method does not guarantee returning all possible matches, but does guarantee returning at least the minimum number of matches, or none. The
     * expected args are:
     *
     * <pre>
     * args[0] - the minimum number of required matches
     * args[1] - the fieldValue object
     * args[2...] - the regexes to match against the fieldValue
     * </pre>
     * <p>
     * This function exists primarily exists for backwards compatibility.
     *
     * @param args
     *            the arguments array
     * @return the {@link FunctionalSet} of matches.
     */
    public static FunctionalSet<ValueTuple> matchesAtLeastCountOf(Object... args) {
        FunctionalSet<ValueTuple> matches = new FunctionalSet<>();
        // first arg is the count
        Integer count = Integer.parseInt(args[0].toString());
        // next arg is the field name
        Object fieldValue = args[1];
        // the rest of the args are the possible matches
        for (int i = 2; i < args.length; i++) {
            String regex = args[i].toString();
            if (fieldValue instanceof Iterable) {
                // cast as Iterable in order to call the right includeRegex method
                matches.addAll(includeRegex((Iterable) fieldValue, regex));
            } else {
                matches.addAll(includeRegex(fieldValue, regex));
            }
            if (matches.size() >= count) {
                break;
            }
        }
        if (matches.size() < count) {
            matches.clear();
        }
        return FunctionalSet.unmodifiableSet(matches);
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
    public static FunctionalSet<ValueTuple> includeRegex(Object fieldValue, String regex) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null
                        && (JexlPatternCache.getPattern(regex).matcher(ValueTuple.getStringValue(fieldValue)).matches() || (JexlPatternCache.getPattern(regex)
                                        .matcher(ValueTuple.getNormalizedStringValue(fieldValue)).matches() && !regex.matches(CASE_SENSITIVE_EXPRESSION)))) {
            matches = FunctionalSet.singleton(getHitTerm(fieldValue));
        }
        return matches;
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
    public static FunctionalSet<ValueTuple> includeRegex(Iterable<?> values, String regex) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        // Important to note that a regex of ".*" still requires
        // a value to be present. In other words, searching for FIELD:'.*'
        // requires a value for FIELD to exist in the document to match
        if (values != null) {
            final Pattern pattern = JexlPatternCache.getPattern(regex);
            final boolean caseSensitiveExpression = regex.matches(CASE_SENSITIVE_EXPRESSION);
            
            Matcher m = null;
            for (Object value : values) {
                if (null == value)
                    continue;
                
                if (null == m) {
                    m = pattern.matcher(ValueTuple.getStringValue(value));
                } else {
                    m.reset(ValueTuple.getStringValue(value));
                }
                
                if (m.matches()) {
                    matches = FunctionalSet.singleton(getHitTerm(value));
                    return matches;
                }
                
                if (!caseSensitiveExpression) {
                    m.reset(ValueTuple.getNormalizedStringValue(value));
                    
                    if (m.matches()) {
                        matches = FunctionalSet.singleton(getHitTerm(value));
                        return matches;
                    }
                }
            }
        }
        return matches;
    }
    
    /**
     * Returns a set that contains the hit term for each field value where the regex matches against the value of the field value. If the regex string contains
     * case-insensitive flags, e.g. {@code (?i).*(?-i)}, a search for a match will also be done against the normalized value of the field value.
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
    public static FunctionalSet<ValueTuple> getAllMatches(Iterable<?> values, String regex) {
        FunctionalSet<ValueTuple> matches = new FunctionalSet();
        // Important to note that a regex of ".*" still requires
        // a value to be present. In other words, searching for FIELD:'.*'
        // requires a value for FIELD to exist in the document to match
        if (values != null) {
            final Pattern pattern = JexlPatternCache.getPattern(regex);
            final boolean caseSensitiveExpression = regex.matches(CASE_SENSITIVE_EXPRESSION);
            
            Matcher m = null;
            for (Object value : values) {
                if (null == value)
                    continue;
                
                if (null == m) {
                    m = pattern.matcher(ValueTuple.getStringValue(value));
                } else {
                    m.reset(ValueTuple.getStringValue(value));
                }
                
                if (m.matches()) {
                    matches.add(getHitTerm(value));
                }
                
                if (!caseSensitiveExpression) {
                    m.reset(ValueTuple.getNormalizedStringValue(value));
                    
                    if (m.matches()) {
                        matches.add(getHitTerm(value));
                    }
                }
            }
        }
        return FunctionalSet.unmodifiableSet(matches);
    }
    
    /**
     * Functionally equivalent to {@link #includeRegex(Object, String)}.
     *
     * @see EvaluationPhaseFilterFunctions#includeRegex(Object, String) additional documentation on expected result
     */
    public static FunctionalSet<ValueTuple> getAllMatches(Object fieldValue, String regex) {
        return includeRegex(fieldValue, regex);
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
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null && ValueTuple.getStringValue(fieldValue).equals(valueToMatch)) {
            matches = FunctionalSet.singleton(getHitTerm(fieldValue));
        }
        return matches;
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
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (values != null) {
            for (Object value : values) {
                if (null == value)
                    continue;
                
                if (ValueTuple.getStringValue(value).equals(valueToMatch)) {
                    matches = FunctionalSet.singleton(getHitTerm(value));
                    return matches;
                }
            }
        }
        return matches;
    }
    
    /**
     * Searches for a load date after start (exclusively)
     * 
     * @param fieldValue
     *            : A field value as a "time since epoch" long: should be LOAD_DATE
     * @param start
     *            : A start date in one of the formats specified above
     * 
     * @return True if the datetime occurs after the provided datetime value
     */
    public static FunctionalSet<ValueTuple> afterLoadDate(Object fieldValue, String start) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                if (betweenInclusive(Long.parseLong(ValueTuple.getStringValue(fieldValue)), getTime(start, true), Long.MAX_VALUE)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a load date after start (exclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values as "time since epoch" longs: should be LOAD_DATE
     * @param start
     *            : A start date in one of the formats specified above
     * 
     * @return True if the datetime occurs after the provided datetime value
     */
    public static FunctionalSet<ValueTuple> afterLoadDate(Iterable<?> fieldValue, String start) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                for (Object o : fieldValue) {
                    if (betweenInclusive(Long.parseLong(ValueTuple.getStringValue(o)), getTime(start, true), Long.MAX_VALUE)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                    break; // really?
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a load date after start (exclusively)
     * 
     * @param fieldValue
     *            : A field value as a "time since epoch" long: should be LOAD_DATE
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @return True if the datetime occurs after the provided datetime value
     */
    public static FunctionalSet<ValueTuple> afterLoadDate(Object fieldValue, String start, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                int granularity = getGranularity(rangePattern);
                if (betweenInclusive(Long.parseLong(ValueTuple.getStringValue(fieldValue)), getNextTime(start, rangeFormat, granularity), Long.MAX_VALUE)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a load date after start (exclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values as "time since epoch" longs: should be LOAD_DATE
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @return True if the datetime occurs after the provided datetime value
     */
    public static FunctionalSet<ValueTuple> afterLoadDate(Iterable<?> fieldValue, String start, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                int granularity = getGranularity(rangePattern);
                for (Object o : fieldValue) {
                    if (betweenInclusive(Long.parseLong(ValueTuple.getStringValue(o)), getNextTime(start, rangeFormat, granularity), Long.MAX_VALUE)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a load date before end (exclusively)
     * 
     * @param fieldValue
     *            : A field value as a "time since epoch" long: should be LOAD_DATE
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @return True if the datetime occurs before the provided datetime value
     */
    public static FunctionalSet<ValueTuple> beforeLoadDate(Object fieldValue, String end) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                if (betweenInclusive(Long.parseLong(ValueTuple.getStringValue(fieldValue)), 0, getTime(end) - 1)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a load date before end (exclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values "time since epoch" longs: should be LOAD_DATE
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @return True if the datetime occurs before the provided datetime value
     */
    public static FunctionalSet<ValueTuple> beforeLoadDate(Iterable<?> fieldValue, String end) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                for (Object o : fieldValue) {
                    if (betweenInclusive(Long.parseLong(ValueTuple.getStringValue(o)), 0, getTime(end) - 1)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a load date before end (exclusively)
     * 
     * @param fieldValue
     *            : A field value as a "time since epoch" long: should be LOAD_DATE
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @return True if the datetime occurs before the provided datetime value
     */
    public static FunctionalSet<ValueTuple> beforeLoadDate(Object fieldValue, String end, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                if (betweenInclusive(Long.parseLong(ValueTuple.getStringValue(fieldValue)), 0, getTime(end, rangeFormat) - 1)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a load date before end (exclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values as "time since epoch" longs: should be LOAD_DATE
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @return True if the datetime occurs before the provided datetime value
     */
    public static FunctionalSet<ValueTuple> beforeLoadDate(Iterable<?> fieldValue, String end, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                for (Object o : fieldValue) {
                    if (betweenInclusive(Long.parseLong(ValueTuple.getStringValue(o)), 0, getTime(end, rangeFormat) - 1)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a load date between start and end (inclusively)
     * 
     * @param fieldValue
     *            : A field value as a "time since epoch" long: should be LOAD_DATE
     * @param start
     *            : A start date in one of the formats specified above
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @return True if the datetime occurs between the provided datetime values
     */
    public static FunctionalSet<ValueTuple> betweenLoadDates(Object fieldValue, String start, String end) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                if (betweenInclusive(Long.parseLong(ValueTuple.getStringValue(fieldValue)), getTime(start), getTime(end, true) - 1)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + " or " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a load date between start and end (inclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values as "time since epoch" longs: should be LOAD_DATE
     * @param start
     *            : A start date in one of the formats specified above
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @return True if the datetime occurs between the provided datetime values
     */
    public static FunctionalSet<ValueTuple> betweenLoadDates(Iterable<?> fieldValue, String start, String end) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                long lStart = getTime(start);
                long lEnd = getTime(end, true) - 1;
                for (Object o : fieldValue) {
                    if (betweenInclusive(Long.parseLong(ValueTuple.getStringValue(o)), lStart, lEnd)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + " or " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a load date between start and end (inclusively)
     * 
     * @param fieldValue
     *            : A field value as a "time since epoch" long: should be LOAD_DATE
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @return True if the datetime occurs between the provided datetime values
     */
    public static FunctionalSet<ValueTuple> betweenLoadDates(Object fieldValue, String start, String end, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                int granularity = getGranularity(rangePattern);
                if (betweenInclusive(Long.parseLong(ValueTuple.getStringValue(fieldValue)), getTime(start, rangeFormat),
                                getNextTime(end, rangeFormat, granularity) - 1)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + " or " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a load date between start and end (inclusively)
     * 
     * @param fieldValue
     *            : An iterable of field value as "time since epoch" longs: should be LOAD_DATE
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @return True if the datetime occurs between the provided datetime values
     */
    public static FunctionalSet<ValueTuple> betweenLoadDates(Iterable<?> fieldValue, String start, String end, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                int granularity = getGranularity(rangePattern);
                long lStart = getTime(start, rangeFormat);
                long lEnd = getNextTime(end, rangeFormat, granularity) - 1;
                for (Object o : fieldValue) {
                    if (betweenInclusive(Long.parseLong(ValueTuple.getStringValue(o)), lStart, lEnd)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + " or " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date between start and end (inclusively)
     * 
     * @param fieldValue
     *            : A field value in one of the formats specified above
     * @param start
     *            : A start date in one of the formats specified above
     *
     * @return True if the datetime occurs after the provided datetime value
     */
    public static FunctionalSet<ValueTuple> afterDate(Object fieldValue, String start) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                if (betweenInclusive(getTime(fieldValue), getTime(start, true), Long.MAX_VALUE)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date between start and end (inclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values in one of the formats specified above
     * @param start
     *            : A start date in one of the formats specified above
     *
     * @return True if the datetime occurs after the provided datetime value
     */
    public static FunctionalSet<ValueTuple> afterDate(Iterable<?> fieldValue, String start) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                for (Object o : fieldValue) {
                    if (betweenInclusive(getTime(o), getTime(start, true), Long.MAX_VALUE)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date after start (exclusively)
     * 
     * @param fieldValue
     *            : A field value in one of the formats above
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @return True if the datetime occurs after the provided datetime value
     */
    public static FunctionalSet<ValueTuple> afterDate(Object fieldValue, String start, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                int granularity = getGranularity(rangePattern);
                if (betweenInclusive(getTime(fieldValue), getNextTime(start, rangeFormat, granularity), Long.MAX_VALUE)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date after start (exclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values in one of the formats above
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @return True if the datetime occurs after the provided datetime value
     */
    public static FunctionalSet<ValueTuple> afterDate(Iterable<?> fieldValue, String start, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                int granularity = getGranularity(rangePattern);
                for (Object o : fieldValue) {
                    if (betweenInclusive(getTime(o), getNextTime(start, rangeFormat, granularity), Long.MAX_VALUE)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date after start (exclusively)
     * 
     * @param fieldValue
     *            : A field value in the supplied format
     * @param pattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     *
     * @return True if the datetime occurs after the provided datetime value
     */
    public static FunctionalSet<ValueTuple> afterDate(Object fieldValue, String pattern, String start, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat format = newSimpleDateFormat(pattern);
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                int granularity = getGranularity(rangePattern);
                long lStart = getNextTime(start, rangeFormat, granularity);
                long lEnd = Long.MAX_VALUE;
                if (betweenInclusive(getTime(fieldValue, format), lStart, lEnd)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date after start (exclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values in the supplied format
     * @param pattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     *
     * @return True if the datetime occurs after the provided datetime value
     */
    public static FunctionalSet<ValueTuple> afterDate(Iterable<?> fieldValue, String pattern, String start, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat format = newSimpleDateFormat(pattern);
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                int granularity = getGranularity(rangePattern);
                long lStart = getNextTime(start, rangeFormat, granularity);
                long lEnd = Long.MAX_VALUE;
                for (Object o : fieldValue) {
                    if (betweenInclusive(getTime(o, format), lStart, lEnd)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date before end (exclusively)
     * 
     * @param fieldValue
     *            : A field value in one of the formats specified above
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @return True if the datetime occurs before the provided datetime value
     */
    public static FunctionalSet<ValueTuple> beforeDate(Object fieldValue, String end) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                if (betweenInclusive(getTime(fieldValue), 0, getTime(end) - 1)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date before end (exclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values in one of the formats specified above
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @return True if the datetime occurs before the provided datetime value
     */
    public static FunctionalSet<ValueTuple> beforeDate(Iterable<?> fieldValue, String end) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                for (Object o : fieldValue) {
                    if (betweenInclusive(getTime(o), 0, getTime(end) - 1)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date before end (exclusively)
     * 
     * @param fieldValue
     *            : A field value in one of the formats above
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @return True if the datetime occurs before the provided datetime value
     */
    public static FunctionalSet<ValueTuple> beforeDate(Object fieldValue, String end, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                if (betweenInclusive(getTime(fieldValue), 0, getTime(end, rangeFormat) - 1)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date before end (exclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values in one of the formats above
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @return True if the datetime occurs before the provided datetime value
     */
    public static FunctionalSet<ValueTuple> beforeDate(Iterable<?> fieldValue, String end, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                for (Object o : fieldValue) {
                    if (betweenInclusive(getTime(o), 0, getTime(end, rangeFormat) - 1)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            }
        }
        return matches;
        
    }
    
    /**
     * Searches for a date before end (exclusively)
     * 
     * @param fieldValue
     *            : A field value in the supplied format
     * @param pattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     *
     * @return True if the datetime occurs before the provided datetime value
     */
    public static FunctionalSet<ValueTuple> beforeDate(Object fieldValue, String pattern, String end, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat format = newSimpleDateFormat(pattern);
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                long lStart = 0;
                long lEnd = getTime(end, rangeFormat) - 1;
                if (betweenInclusive(getTime(fieldValue, format), lStart, lEnd)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date before end (exclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values in the supplied format
     * @param pattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     *
     * @return True if the datetime occurs before the provided datetime value
     */
    public static FunctionalSet<ValueTuple> beforeDate(Iterable<?> fieldValue, String pattern, String end, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat format = newSimpleDateFormat(pattern);
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                long lStart = 0;
                long lEnd = getTime(end, rangeFormat) - 1;
                for (Object o : fieldValue) {
                    if (betweenInclusive(getTime(o, format), lStart, lEnd)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date between start and end (inclusively)
     * 
     * @param fieldValue
     *            : A field value in one of the formats specified above
     * @param start
     *            : A start date in one of the formats specified above
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @return True if the datetime occurs between the provided datetime values
     */
    public static FunctionalSet<ValueTuple> betweenDates(Object fieldValue, String start, String end) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                if (betweenInclusive(getTime(fieldValue), getTime(start), getTime(end, true) - 1)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date between start and end (inclusively)
     * 
     * @param fieldValue
     *            : Am iterable of field values in one of the formats specified above
     * @param start
     *            : A start date in one of the formats specified above
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @return True if the datetime occurs between the provided datetime values
     */
    public static FunctionalSet<ValueTuple> betweenDates(Iterable<?> fieldValue, String start, String end) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                if (fieldValue != null) {
                    long lStart = getTime(start);
                    long lEnd = getTime(end, true) - 1;
                    for (Object o : fieldValue) {
                        if (betweenInclusive(getTime(o), lStart, lEnd)) {
                            matches = FunctionalSet.singleton(getHitTerm(o));
                            break;
                        }
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + " or " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date between start and end (inclusively)
     * 
     * @param fieldValue
     *            : A field value in one of the formats above
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @return True if the datetime occurs between the provided datetime values
     */
    public static FunctionalSet<ValueTuple> betweenDates(Object fieldValue, String start, String end, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                int granularity = getGranularity(rangePattern);
                if (betweenInclusive(getTime(fieldValue), getTime(start, rangeFormat), getNextTime(end, rangeFormat, granularity) - 1)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date between start and end (inclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values in one of the formats above
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @return True if the datetime occurs between the provided datetime values
     */
    public static FunctionalSet<ValueTuple> betweenDates(Iterable<?> fieldValue, String start, String end, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                int granularity = getGranularity(rangePattern);
                long lStart = getTime(start, rangeFormat);
                long lEnd = getNextTime(end, rangeFormat, granularity) - 1;
                for (Object o : fieldValue) {
                    if (betweenInclusive(getTime(o), lStart, lEnd)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + " or " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date between start and end (inclusively)
     * 
     * @param fieldValue
     *            : A field value in the supplied format
     * @param pattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     *
     * @return True if the datetime occurs between the provided datetime values
     */
    public static FunctionalSet<ValueTuple> betweenDates(Object fieldValue, String pattern, String start, String end, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat format = newSimpleDateFormat(pattern);
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                int granularity = getGranularity(rangePattern);
                if (betweenInclusive(getTime(fieldValue, format), getTime(start, rangeFormat), getNextTime(end, rangeFormat, granularity) - 1)) {
                    matches = FunctionalSet.singleton(getHitTerm(fieldValue));
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * Searches for a date between start and end (inclusively)
     * 
     * @param fieldValue
     *            : An iterable of field values in the supplied format
     * @param pattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     *
     * @return True if the datetime occurs between the provided datetime values
     */
    public static FunctionalSet<ValueTuple> betweenDates(Iterable<?> fieldValue, String pattern, String start, String end, String rangePattern) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null) {
            try {
                DateFormat format = newSimpleDateFormat(pattern);
                DateFormat rangeFormat = newSimpleDateFormat(rangePattern);
                int granularity = getGranularity(rangePattern);
                long lStart = getTime(start, rangeFormat);
                long lEnd = getNextTime(end, rangeFormat, granularity) - 1;
                for (Object o : fieldValue) {
                    if (betweenInclusive(getTime(o, format), lStart, lEnd)) {
                        matches = FunctionalSet.singleton(getHitTerm(o));
                        break;
                    }
                }
            } catch (ParseException pe) {
                log.error(pe.getMessage());
            } catch (NumberFormatException nfe) {
                log.error("Unable to numeric argument " + start + " or " + end + ": " + nfe.getMessage());
            }
        }
        return matches;
    }
    
    /**
     * A special format denoting the time since epoch format
     */
    public static final String TIME_SINCE_EPOCH_FORMAT = "e";
    
    /**
     * The list of formats attempted At a mimumum the following are found in existing data: "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ssz",
     * "yyyy-MM-dd", "yyyy-MM-dd'T'HH'|'mm", "EEE MMM dd HH:mm:ss zzz yyyy"}; "yyyyMMddhhmmss" "yyyyMMddHHmm", "yyyyMMddHH", "yyyyMMdd",
     */
    protected static final String[] DATE_FORMAT_STRINGS = {"yyyyMMdd:HH:mm:ss:SSSZ", "yyyyMMdd:HH:mm:ss:SSS", "EEE MMM dd HH:mm:ss zzz yyyy",
            "d MMM yyyy HH:mm:ss 'GMT'", "yyyy-MM-dd HH:mm:ss.SSS Z", "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss.S Z",
            "yyyy-MM-dd HH:mm:ss.S",
            "yyyy-MM-dd HH:mm:ss Z", // ISO 8601
            "yyyy-MM-dd HH:mm:ssz", "yyyy-MM-dd HH:mm:ss", "yyyyMMdd HHmmss", "yyyy-MM-dd'T'HH'|'mm", "yyyy-MM-dd'T'HH':'mm':'ss'.'SSS'Z'",
            "yyyy-MM-dd'T'HH':'mm':'ss'Z'", "MM'/'dd'/'yyyy HH':'mm':'ss", "E MMM d HH:mm:ss z yyyy", "E MMM d HH:mm:ss Z yyyy", "yyyyMMdd_HHmmss",
            "yyyy-MM-dd", "MM/dd/yyyy", "yyyy-MMMM", "yyyy-MMM", "yyyyMMddHHmmss", "yyyyMMddHHmm", "yyyyMMddHH", "yyyyMMdd",};
    
    static final List<DateFormat> dateFormatList = new ArrayList<>();
    static final List<Integer> dateGranularityList = new ArrayList<>();
    static {
        for (String fs : DATE_FORMAT_STRINGS) {
            DateFormat format = newSimpleDateFormat(fs);
            dateFormatList.add(format);
            dateGranularityList.add(getGranularity(fs));
        }
    }
    
    /**
     * Create a new simple date format, with a GMT time zone
     * 
     * @param format
     * @return the DateFormat
     */
    protected static DateFormat newSimpleDateFormat(String format) {
        DateFormat newFormat;
        if (format.equals(TIME_SINCE_EPOCH_FORMAT)) {
            newFormat = new SimpleDateFormat() {
                @Override
                public Date parse(String source) throws ParseException {
                    calendar.clear();
                    calendar.setTimeInMillis(Long.parseLong(source));
                    return calendar.getTime();
                }
                
            };
        } else {
            newFormat = new SimpleDateFormat(format);
        }
        newFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return newFormat;
    }
    
    /**
     * Determine the granularity of the provided date format and return a {@link Calendar} constant suitable for the {@link Calendar#add(int, int)} method. For
     * example, if the granularity of the date format is to the minute, then {@link Calendar#MINUTE} is returned. NOTE: This was verified in Java 6.0: the
     * granularity monotonically increases with the value of the {@link Calendar} constant. Hence, a simple max works in this routine.
     *
     * @param dateFormat
     *            the date format to determine the granularity from
     * @return the calendar granularity
     */
    protected static int getGranularity(String dateFormat) {
        // check for out special format
        if (dateFormat.equals(TIME_SINCE_EPOCH_FORMAT)) {
            return Calendar.MILLISECOND;
        }
        
        // start with a year granularity
        int granularity = Calendar.YEAR;
        boolean escaped = false;
        for (int i = 0; i < dateFormat.length(); i++) {
            char c = dateFormat.charAt(i);
            if (c == '\'') {
                escaped = !escaped;
            }
            if (!escaped) {
                if (c == 'S') {
                    // a millisecond granularity is as fase as we go...return immediately
                    return Calendar.MILLISECOND;
                } else if (c == 's') {
                    granularity = Math.max(granularity, Calendar.SECOND);
                } else if (c == 'm') {
                    granularity = Math.max(granularity, Calendar.MINUTE);
                } else if (c == 'h' || c == 'K' || c == 'k' || c == 'H') {
                    granularity = Math.max(granularity, Calendar.HOUR);
                } else if (c == 'E' || c == 'F' || c == 'd' || c == 'D') {
                    granularity = Math.max(granularity, Calendar.DATE);
                } else if (c == 'W' || c == 'w') {
                    granularity = Math.max(granularity, Calendar.WEEK_OF_YEAR);
                } else if (c == 'M') {
                    granularity = Math.max(granularity, Calendar.MONTH);
                }
            }
        }
        return granularity;
    }
    
    public static FunctionalSet<ValueTuple> timeFunction(Object time1, Object time2, String operatorString, String equalityString, long goal) {
        FunctionalSet<ValueTuple> matches = new FunctionalSet<>();
        if (time1 != null && time2 != null) {
            try {
                boolean truth = evaluate(getMaxTime(time1), getMinTime(time2), operatorString, equalityString, goal);
                if (truth) {
                    matches.addAll(Sets.newHashSet(getHitTerm(getMaxValue(time1)), getHitTerm(getMinValue(time2))));
                }
            } catch (ParseException e) {
                log.warn("could not evaluate:" + time1 + " " + operatorString + " " + time2 + " " + equalityString + " " + goal);
            }
        }
        return FunctionalSet.unmodifiableSet(matches);
    }
    
    public static long getMaxTime(Object dates) throws ParseException {
        if (dates instanceof Iterable<?>)
            return getMaxTime((Iterable<?>) dates);
        else
            return getTime(dates);
    }
    
    public static long getMaxTime(Iterable<?> dates) throws ParseException {
        long max = Long.MIN_VALUE;
        if (dates != null) {
            for (Object date : dates) {
                max = Math.max(max, getTime(date));
            }
        }
        return max;
    }
    
    public static long getMinTime(Object dates) throws ParseException {
        if (dates instanceof Iterable<?>)
            return getMinTime((Iterable<?>) dates);
        else
            return getTime(dates);
    }
    
    public static long getMinTime(Iterable<?> dates) throws ParseException {
        long min = Long.MAX_VALUE;
        for (Object date : dates) {
            min = Math.min(min, getTime(date));
        }
        return min;
    }
    
    public static Object getMaxValue(Object dates) throws ParseException {
        if (dates instanceof Iterable<?>)
            return getMaxValue((Iterable<?>) dates);
        else
            return dates;
    }
    
    public static Object getMaxValue(Iterable<?> dates) throws ParseException {
        long max = Long.MIN_VALUE;
        Object value = null;
        for (Object date : dates) {
            long newTime = getTime(date);
            if (newTime > max) {
                max = newTime;
                value = date;
            }
        }
        return value;
    }
    
    public static Object getMinValue(Object dates) throws ParseException {
        if (dates instanceof Iterable<?>)
            return getMinValue((Iterable<?>) dates);
        else
            return dates;
    }
    
    public static Object getMinValue(Iterable<?> dates) throws ParseException {
        long min = Long.MAX_VALUE;
        Object value = null;
        for (Object date : dates) {
            long newTime = getTime(date);
            if (newTime < min) {
                min = newTime;
                value = date;
            }
        }
        return value;
    }
    
    /**
     * Given a Calendar constant as returned by getGranularity(format), get the next unit of time determine by incrementing by the specified granularity. For
     * example getNextUnit(x, DAY) would return {@code x+<ms/day>}.
     * 
     * @param granularity
     * @return next date/time in milliseconds
     */
    public static long getNextTime(long time, int granularity) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(time);
        c.add(granularity, 1);
        return c.getTimeInMillis();
    }
    
    /**
     * Get the time using the supplied format
     * 
     * @param value
     *            The value to be parsed
     * @param format
     *            The format to parse with
     * @return the time as ms since epoch
     * @throws ParseException
     *             if the value failed to be parsed using the supplied format
     */
    public static long getTime(Object value, DateFormat format) throws ParseException {
        synchronized (format) {
            return format.parse(ValueTuple.getStringValue(value)).getTime();
        }
    }
    
    /**
     * Given a Calendar constant as returned by getGranularity(format), get the next unit of time determine by incrementing by the specified granularity. For
     * example getNextUnit(x, DAY) would return {@code x+<ms/day>}.
     * 
     * @param value
     *            The value to be parsed
     * @param format
     *            The format to parse with
     * @param granularity
     *            The granularity of the supplied format
     * @return the next time as ms since epoch
     * @throws ParseException
     *             if the value failed to be parsed using the suppied format
     */
    public static long getNextTime(Object value, DateFormat format, int granularity) throws ParseException {
        return getNextTime(getTime(value, format), granularity);
    }
    
    /**
     * Get the time for a value
     * 
     * @param value
     *            The value to be parsed
     * @return the time as ms since epoch
     * @throws ParseException
     *             if the value failed to be parsed using any of the known formats
     */
    public static long getTime(Object value) throws ParseException {
        return getTime(value, false);
    }
    
    /**
     * Return the time parsed for the given value in ms. If {@code nextTime} is true, the next unit of time will be returned based on the granularity of the
     * given time, e.g. if the time given is to the day, then the next day will be returned.
     *
     * @param value
     *            the value to be parsed
     * @param nextTime
     *            whether to increment to the next time
     * @return the time in ms since epoch
     * @throws ParseException
     *             if the value failed to be parsed by any of the known formats.
     */
    public static long getTime(Object value, boolean nextTime) throws ParseException {
        // determine if a number first
        for (int i = 0; i < dateFormatList.size(); i++) {
            DateFormat format = dateFormatList.get(i);
            try {
                long time = getTime(value, format);
                if (nextTime) {
                    time = getNextTime(time, dateGranularityList.get(i));
                }
                return time;
            } catch (ParseException e) {
                // try the next one
            }
        }
        throw new ParseException("Unable to parse value using known date formats: " + value, 0);
    }
    
    /**
     * Return whether the given value is inclusively between the given left and right of the range.
     *
     * @param value
     *            the value to evaluate
     * @param left
     *            the left (smaller) side of the range
     * @param right
     *            the right (larger) side of the range
     * @return true if the value is inclusively between the range, or false otherwise
     */
    static boolean betweenInclusive(long value, long left, long right) {
        return (value >= left && value <= right);
    }
    
    /**
     * Return the given object as a {@link ValueTuple}.
     *
     * @param valueTuple
     *            the value tuple to convert
     * @return a {@link ValueTuple}
     * @see ValueTuple#toValueTuple(Object) documentation on conversion details
     */
    static ValueTuple getHitTerm(Object valueTuple) {
        return ValueTuple.toValueTuple(valueTuple);
    }
    
    /**
     * Returns a string that is a substring of the given string. The substring starts at the index of the first '.', and extends to the index of the Nth
     * occurrence of the character '.' from the left, where N is specified by {@code pos}.
     *
     * <pre>
     * Given the string "FIRST.SECOND.THIRD.FOURTH"
     * - pos 0 will result in the substring 'SECOND.THIRD'
     * - pos 1 will result in the substring 'SECOND'
     * - pos 2 will result in an exception being thrown
     * </pre>
     *
     * @param input
     *            the input string
     * @param pos
     *            the Nth position of '.' to end the substring at
     * @return the substring
     */
    public static String getMatchToLeftOfPeriod(String input, int pos) {
        // always peel off the fieldName before the first '.'
        input = input.substring(input.indexOf('.') + 1);
        int[] indices = indicesOf(input, '.');
        if (indices.length < pos + 1)
            throw new RuntimeException("Input " + input + " does not have a '.' at position " + pos + " from the left.");
        return input.substring(0, indices[indices.length - pos - 1]);
    }
    
    /**
     * Returns a string that is a substring of the given string. The substring starts at the index of the Nth occurrence of the character '.' from the left,
     * where N is specified by {@code pos} and extends to the end of the string.
     *
     * <pre>
     * Given the string "FIRST.SECOND.THIRD.FOURTH"
     * - pos 0 will result in the substring 'FOURTH'
     * - pos 1 will result in the substring 'THIRD.FOURTH'
     * - pos 2 will result in the substring 'SECOND.THIRD.FOURTH'
     * - pos 3 will result in an exception being thrown
     * </pre>
     *
     * @param input
     *            the input string
     * @param pos
     *            the Nth position of '.' to begin the substring at
     * @return the substring
     */
    public static String getMatchToRightOfPeriod(String input, int pos) {
        int[] indices = indicesOf(input, '.');
        if (indices.length < pos + 1)
            throw new RuntimeException("Input " + input + " does not have a '.' at position " + pos + " from the right.");
        return input.substring(indices[indices.length - pos - 1] + 1);
    }
    
    private static int[] indicesOf(String input, char c) {
        CharMatcher matcher = CharMatcher.is(c);
        int count = matcher.countIn(input);
        int[] indices = new int[count];
        int lastIndex = 0;
        for (int i = 0; i < count; i++) {
            indices[i] = input.indexOf(c, lastIndex + 1);
            lastIndex = indices[i];
        }
        return indices;
    }
    
    /**
     * Compare the set of normalized values between two fields using the comparison strategy indicated by {@code operator}, taking into account
     * {@code compareMode} to indicate if the comparison strategy may either match against any value or must match against all values.
     * <p>
     * Read below for expected results for different comparison strategies and compare mode:
     * <ul>
     * <li>Equals comparison: use operator = or ==
     * <ul>
     * <li>{@code compareMode} ANY: Return true if {@code field1} and {@code field2} are both either null or an empty collection, or if {@code field1} and
     * {@code field2} have any normalized value in common. Return false otherwise.</li>
     * <li>{@code compareMode} ALL: Return true if {@code field1} and {@code field2} are both either null or an empty collection, or if the set of normalized
     * values in {@code field1} is equal to the set of normalized values in {@code field2}. Return false otherwise.</li>
     * </ul>
     * </li>
     * <li>Not equals comparison: use operator !=
     * <ul>
     * <li>{@code compareMode} ANY: Return true if there is at least one normalized value that is found in either {@code field1} or {@code field2}, but not the
     * other. Return false otherwise.</li>
     * <li>{@code compareMode} ALL: Return true if {@code field1} and {@code field2} do not have any normalized value in common.</li>
     * </ul>
     * </li>
     * <li>Less Than comparison: use operator &lt;
     * <ul>
     * <li>{@code compareMode} ANY: Return true if there is any normalized value in {@code field1} that is considered less than a normalized value in
     * {@code field2}. Return false otherwise.</li>
     * <li>{@code compareMode} ALL: Return true if all normalized values in {@code field1} are considered less than a normalized value in {@code field2}. Return
     * false otherwise.</li>
     * </ul>
     * </li>
     * <li>Less Than Equals comparison: use operator &lt;=
     * <ul>
     * <li>{@code compareMode} ANY: Return true if there is any normalized value in {@code field1} that is considered less than or equal to a normalized value
     * in {@code field2}. Return false otherwise.</li>
     * <li>{@code compareMode} ALL: Return true if all normalized values in {@code field1} are considered less than or equal to a normalized value in
     * {@code field2}. Return false otherwise.</li>
     * </ul>
     * </li>
     * <li>Greater Than comparison: use operator &gt;
     * <ul>
     * <li>{@code compareMode} ANY: Return true if there is any normalized value in {@code field1} that is considered greater than a normalized value in
     * {@code field2}. Return false otherwise.</li>
     * <li>{@code compareMode} ALL: Return true if all normalized values in {@code field1} are considered greater than a normalized value in {@code field2}.
     * Return false otherwise.</li>
     * </ul>
     * </li>
     * <li>Greater Than Equals comparison: use operator &gt;=
     * <ul>
     * <li>{@code compareMode} ANY: Return true if there is any normalized value in {@code field1} that is considered greater than or equal to a normalized
     * value in {@code field2}. Return false otherwise.</li>
     * <li>{@code compareMode} ALL: Return true if all normalized values in {@code field1} are considered greater than or equal to a normalized value in
     * {@code field2}. Return false otherwise.</li>
     * </ul>
     * </li>
     * </ul>
     *
     * @param field1
     *            the first field
     * @param operator
     *            the comparison operator, must be one of the following: =, ==, !=, &lt;, &lt;=, &gt;, &gt;=
     * @param compareMode
     *            the comparison mode, must be {@code "ANY"} or {@code "ALL"}. "ANY" indicates that the comparison strategy may match against any value in
     *            {@code field1}, "ALL" indicates that the comparison strategy must match against all values in {@code field1}
     * @param field2
     *            the second field
     * @return true if the specified comparison operation is satisfied, or false otherwise
     */
    public static boolean compare(Object field1, String operator, String compareMode, Object field2) {
        FunctionalSet<ValueTuple> set1 = toFunctionalSet(field1);
        FunctionalSet<ValueTuple> set2 = toFunctionalSet(field2);
        boolean matchAny = CompareFunctionValidator.Mode.valueOf(compareMode.toUpperCase()).equals(CompareFunctionValidator.Mode.ANY);
        return compareFields(set1, set2, operator, matchAny);
    }
    
    /**
     * Convert the given value as a {@link FunctionalSet}.
     *
     * @param value
     *            the value to convert
     * @return the set
     */
    private static FunctionalSet<ValueTuple> toFunctionalSet(Object value) {
        FunctionalSet<ValueTuple> set = new FunctionalSet<>();
        if (value instanceof Iterable) {
            for (Object o : (Iterable<?>) value) {
                set.add(ValueTuple.toValueTuple(o));
            }
        } else {
            if (value != null) {
                set.add(ValueTuple.toValueTuple(value));
            }
        }
        return set;
    }
    
    /**
     * Compare the normalized values in {@code set1} to {@code set2}.
     *
     * @param set1
     *            the first set
     * @param set2
     *            the second set
     * @param operator
     *            the comparison operator
     * @param matchAny
     *            if true, do not require all normalized values in {@code set1} to satisfy the comparison operation when compared against {@code set2}
     * @return the comparison result
     */
    private static boolean compareFields(FunctionalSet<ValueTuple> set1, FunctionalSet<ValueTuple> set2, String operator, boolean matchAny) {
        switch (CharMatcher.WHITESPACE.removeFrom(operator)) {
            case "==":
            case "=":
                return areNormalizedValuesEqual(set1, set2, matchAny);
            case "!=":
                return areNormalizedValuesNonEqual(set1, set2, matchAny);
            case "<":
                if (!set1.isEmpty() && !set2.isEmpty()) {
                    // matchesAny: min(set1) < max(set2)
                    // matchesAll: min(set2) > max(set1)
                    return matchAny ? compareMinToMaxNormalizedValue(set1, set2) < 0 : compareMinToMaxNormalizedValue(set2, set1) > 0;
                }
            case "<=":
                if (!set1.isEmpty() && !set2.isEmpty()) {
                    // matchesAny: min(set1) <= max(set2)
                    // matchesAll: min(set2) >= max(set1)
                    return matchAny ? compareMinToMaxNormalizedValue(set1, set2) <= 0 : compareMinToMaxNormalizedValue(set2, set1) >= 0;
                }
            case ">":
                if (!set1.isEmpty() && !set2.isEmpty()) {
                    // matchesAny: min(set1) <= max(set2)
                    // matchesAll: min(set2) >= max(set1)
                    return matchAny ? compareMinToMaxNormalizedValue(set2, set1) < 0 : compareMinToMaxNormalizedValue(set1, set2) > 0;
                }
            case ">=":
                if (!set1.isEmpty() && !set2.isEmpty()) {
                    // matchesAny: min(set1) <= max(set2)
                    // matchesAll: min(set2) >= max(set1)
                    return matchAny ? compareMinToMaxNormalizedValue(set2, set1) <= 0 : compareMinToMaxNormalizedValue(set1, set2) >= 0;
                }
            default:
                return false;
        }
    }
    
    /**
     * Return the result of comparing the normalized value of the minimum element in {@code set1} to the normalized value of the maximum element in {@code set2}
     * .
     *
     * @param set1
     *            the first set
     * @param set2
     *            the second set
     * @return the comparison result
     */
    private static int compareMinToMaxNormalizedValue(FunctionalSet<ValueTuple> set1, FunctionalSet<ValueTuple> set2) {
        ValueTuple min = (ValueTuple) set1.min();
        ValueTuple max = (ValueTuple) set2.max();
        // noinspection unchecked
        return ((Comparable<Object>) min.getNormalizedValue()).compareTo(max.getNormalizedValue());
    }
    
    /**
     * Return whether the set of normalized values between {@code set1} and {@code set2} are considered equal.
     *
     * @param set1
     *            the first set
     * @param set2
     *            the second set
     * @param matchAny
     *            if true, consider the sets equal if there is any normalized value in common, or otherwise only consider the sets equal if they are exactly
     *            equivalent
     * @return true if the sets are considered equal, or false otherwise
     */
    private static boolean areNormalizedValuesEqual(FunctionalSet<ValueTuple> set1, FunctionalSet<ValueTuple> set2, boolean matchAny) {
        if (set1.isEmpty() && set2.isEmpty()) {
            return true;
        }
        Set<Object> fields1Values = getNormalizedValues(set1);
        Set<Object> fields2Values = getNormalizedValues(set2);
        if (matchAny) {
            return !SetUtils.intersection(fields1Values, fields2Values).isEmpty();
        } else {
            return SetUtils.isEqualSet(fields1Values, fields2Values);
        }
    }
    
    /**
     * Return whether the set of normalized values between {@code set1} and {@code set2} are considered non-equal.
     *
     * @param set1
     *            the first set
     * @param set2
     *            the second set
     * @param matchAny
     *            if true, consider the sets non-equal if there is any normalized value in one set that is not contained by the other, or otherwise only
     *            consider the sets non-equal if they have no normalized value in common
     * @return true if the sets are considered non-equal, or false otherwise
     */
    private static boolean areNormalizedValuesNonEqual(FunctionalSet<ValueTuple> set1, FunctionalSet<ValueTuple> set2, boolean matchAny) {
        if (set1.isEmpty() && set2.isEmpty()) {
            return false;
        }
        Set<Object> fields1Values = getNormalizedValues(set1);
        Set<Object> fields2Values = getNormalizedValues(set2);
        if (matchAny) {
            return fields1Values.size() != fields2Values.size() || !SetUtils.difference(fields1Values, fields2Values).isEmpty();
        } else {
            return SetUtils.intersection(fields1Values, fields2Values).isEmpty();
        }
    }
    
    /**
     * Return a {@link Set} containing all normalized values in the given {@link FunctionalSet}.
     *
     * @param set
     *            the set
     * @return the normalized values
     */
    private static Set<Object> getNormalizedValues(FunctionalSet<ValueTuple> set) {
        return set.stream().map((value) -> value.getNormalizedValue()).collect(Collectors.toSet());
    }
}
