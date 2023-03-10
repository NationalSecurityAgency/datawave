package datawave.query.jexl.functions;

import com.google.common.base.CharMatcher;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datawave.data.type.Type;
import datawave.query.attributes.ValueTuple;
import datawave.query.jexl.JexlPatternCache;
import datawave.query.collections.FunctionalSet;
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
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * NOTE: The JexlFunctionArgumentDescriptorFactory is implemented by EvaluationPhaseFilterFunctionsDescriptor. This is kept as a separate class to reduce
 * accumulo dependencies on other jars.
 *
 * NOTE: You will see that most of these functions return a list of ValueTuple hits instead of a boolean. This is because these functions do not have index
 * queries (@see EvaluationPhaseFilterFunctionsDescriptor). Hence the "hits" cannot be determined by the index function. So instead, the HitListArithmetic takes
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
    
    public static Collection<ValueTuple> isNotNull(Object fieldValue) {
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
    
    public static boolean isNull(Object fieldValue) {
        if (fieldValue instanceof Collection)
            return ((Collection) fieldValue).isEmpty();
        return fieldValue == null;
    }
    
    // Evaluation does not perform any goofy rewriting of the JEXL query string into
    // a JexlScript to handle multi-valued fields; therefore, we don't need to remove
    // this functionality
    // we do not want to return positive matches in the hit list, so return a boolean here
    public static boolean excludeRegex(Object fieldValue, String regex) {
        return includeRegex(fieldValue, regex).isEmpty();
    }
    
    // Evaluation does not perform any goofy rewriting of the JEXL query string into
    // a JexlScript to handle multi-valued fields; therefore, we don't need to remove
    // this functionality
    // we do not want to return positive matches in the hit list, so return a boolean here
    public static boolean excludeRegex(Iterable<?> values, String regex) {
        return includeRegex(values, regex).isEmpty();
    }
    
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
    
    // Evaluate a regex. Note this is being done against the un-normalized value unless the regex is not case sensitive.
    public static FunctionalSet<ValueTuple> includeRegex(Object fieldValue, String regex) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null
                        && (JexlPatternCache.getPattern(regex).matcher(ValueTuple.getStringValue(fieldValue)).matches() || (JexlPatternCache.getPattern(regex)
                                        .matcher(ValueTuple.getNormalizedStringValue(fieldValue)).matches() && !regex.matches(CASE_SENSITIVE_EXPRESSION)))) {
            matches = FunctionalSet.singleton(getHitTerm(fieldValue));
        }
        return matches;
    }
    
    // Evaluate a regex. Note this is being done against the un-normalized value unless the regex is not case sensitive.
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
     * This is a version of includeRegex that returns all matches instead of only what was used to evaluate the query
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
                } else if (pattern.matcher(ValueTuple.getNormalizedStringValue(value)).matches()) {
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
    
    public static FunctionalSet<ValueTuple> getAllMatches(Object fieldValue, String regex) {
        return includeRegex(fieldValue, regex);
    }
    
    // Evaluate text. Note this is being done against the un-normalized value.
    public static FunctionalSet<ValueTuple> includeText(Object fieldValue, String valueToMatch) {
        FunctionalSet<ValueTuple> matches = FunctionalSet.emptySet();
        if (fieldValue != null && ValueTuple.getStringValue(fieldValue).equals(valueToMatch)) {
            matches = FunctionalSet.singleton(getHitTerm(fieldValue));
        }
        return matches;
    }
    
    // Evaluate text. Note this is being done against the un-normalized value.
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
                        matches = FunctionalSet.singleton(getHitTerm(fieldValue));
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
                        matches = FunctionalSet.singleton(getHitTerm(fieldValue));
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
                        matches = FunctionalSet.singleton(getHitTerm(fieldValue));
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
                        matches = FunctionalSet.singleton(getHitTerm(fieldValue));
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
                        matches = FunctionalSet.singleton(getHitTerm(fieldValue));
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
                        matches = FunctionalSet.singleton(getHitTerm(fieldValue));
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
     * Determine the granularity of the provided date format and return a Calendar constant suitable for the Calendar.add method. So for example is the
     * granularity of the date format is to the minute, then Calendar.MINUTE is returned. NOTE: This was verified in Java 6.0: the granularity monotonically
     * increases with the value of the Calendar constant. Hence a simple max works in this routine.
     * 
     * @param dateFormat
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
        FunctionalSet<ValueTuple> matches = new FunctionalSet();
        try {
            boolean truth = evaluate(getMaxTime(time1), getMinTime(time2), operatorString, equalityString, goal);
            if (truth) {
                matches.addAll(Sets.newHashSet(getHitTerm(getMaxValue(time1)), getHitTerm(getMinValue(time2))));
            }
        } catch (ParseException e) {
            log.warn("could not evaluate:" + time1 + " " + operatorString + " " + time2 + " " + equalityString + " " + goal);
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
        for (Object date : dates) {
            max = Math.max(max, getTime(date));
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
    static long getNextTime(long time, int granularity) {
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
     *             if the value failed to be parsed using the suppied format
     */
    protected static long getTime(Object value, DateFormat format) throws ParseException {
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
    static long getNextTime(Object value, DateFormat format, int granularity) throws ParseException {
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
    protected static long getTime(Object value) throws ParseException {
        return getTime(value, false);
    }
    
    /**
     * Get the time for a value
     * 
     * @param value
     *            The value to be parsed
     * @param nextTime
     *            If true the next unit of time will be returned (e.g. next day if matching date format is only to the day)
     * @return the time as ms since epoch
     * @throws ParseException
     *             if the value failed to be parsed using any of the known formats
     */
    protected static long getTime(Object value, boolean nextTime) throws ParseException {
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
     * A basic between comparison
     * 
     * @param fieldValue
     * @param left
     * @param right
     * @return true if between (inclusively)
     */
    static boolean betweenInclusive(long fieldValue, long left, long right) {
        return (fieldValue >= left && fieldValue <= right);
    }
    
    protected static ValueTuple getHitTerm(Object valueTuple) {
        return ValueTuple.toValueTuple(valueTuple);
    }
    
    /**
     * <pre>
     * for a field like:
     * NAME.GGPARENT.GPARENT.PARENT.CHILD
     * if the pos is 0, then GGPARENT.GPARENT.PARENT will be returned
     * if the pos is 2, then GGPARENT will be returned
     * </pre>
     * 
     * @param input
     * @param pos
     * @return
     */
    public static String getMatchToLeftOfPeriod(String input, int pos) {
        // always peel off the fieldName before the first '.'
        input = input.substring(input.indexOf('.') + 1);
        int[] indices = indicesOf(input, '.');
        if (indices.length < pos + 1)
            throw new RuntimeException("Input" + input + " does not have a '.' at position " + pos + " from the left.");
        return input.substring(0, indices[indices.length - pos - 1]);
    }
    
    /**
     * <pre>
     *     for a field like NAME.NAME_1.5
     *     if the pos is 0, then '5' will be returned
     *     if the pos it 1, then NAME_1.5 will be returned
     * </pre>
     * 
     * @param input
     * @param pos
     * @return
     */
    public static String getMatchToRightOfPeriod(String input, int pos) {
        int[] indices = indicesOf(input, '.');
        if (indices.length < pos + 1)
            throw new RuntimeException("Input" + input + " does not have a '.' at position " + pos + " from the right.");
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
    
}
