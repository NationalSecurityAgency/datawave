package nsa.datawave.query.functions;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * NOTE: The JexlFunctionArgumentDescriptorFactory is implemented by EvaluationPhaseFilterFunctionsDescriptor. This is kept as a separate class to reduce
 * accumulo dependencies on other jars.
 *
 **/
@Deprecated
@JexlFunctions(descriptorFactory = "nsa.datawave.query.functions.EvaluationPhaseFilterFunctionsDescriptor")
public class EvaluationPhaseFilterFunctions {
    
    protected static Logger log = Logger.getLogger(EvaluationPhaseFilterFunctions.class);
    
    public static boolean isNull(String fieldValue) {
        return fieldValue == null;
    }
    
    public static boolean includeRegex(String fieldValue, String regex) {
        if (null == regex) {
            throw new NullPointerException("includeRegex received a null regex");
        }
        return null != fieldValue && Pattern.matches(regex, fieldValue);
    }
    
    /**
     * Searches for a load date after start
     * 
     * Accepted date formats (as supplied to java.text.SimpleDateFormat) "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ssz", "yyyy-MM-dd",
     * "yyyy-MM-dd'T'HH'|'mm", "EEE MMM dd HH:mm:ss zzz yyyy"}; "yyyyMMddhhmmss" <long value of ms since epoch>
     * 
     * @param fieldValue
     *            : A field value as a "time since epoch" long: should be LOAD_DATE
     * @param start
     *            : A start date in one of the formats specified above
     * 
     * @returns True if the datetime occurs after the provided datetime value
     */
    public static boolean afterLoadDate(Object fieldValue, String start) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.afterLoadDate(fieldValue, start).size() > 0;
    }
    
    /**
     * Searches for a load date after start
     * 
     * @param fieldValue
     *            : A field value as a "time since epoch" long: should be LOAD_DATE
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @returns True if the datetime occurs after the provided datetime value
     */
    public static boolean afterLoadDate(Object fieldValue, String start, String rangePattern) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.afterLoadDate(fieldValue, start, rangePattern).size() > 0;
    }
    
    /**
     * Searches for a load date before end
     * 
     * Accepted date formats (as supplied to java.text.SimpleDateFormat) "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ssz", "yyyy-MM-dd",
     * "yyyy-MM-dd'T'HH'|'mm", "EEE MMM dd HH:mm:ss zzz yyyy"}; "yyyyMMddhhmmss" <long value of ms since epoch>
     * 
     * @param fieldValue
     *            : A field value as a "time since epoch" long: should be LOAD_DATE
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @returns True if the datetime occurs before the provided datetime value
     */
    public static boolean beforeLoadDate(Object fieldValue, String end) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.beforeLoadDate(fieldValue, end).size() > 0;
    }
    
    /**
     * Searches for a load date before end
     * 
     * @param fieldValue
     *            : A field value as a "time since epoch" long: should be LOAD_DATE
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @returns True if the datetime occurs before the provided datetime value
     */
    public static boolean beforeLoadDate(Object fieldValue, String end, String rangePattern) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.beforeLoadDate(fieldValue, end, rangePattern).size() > 0;
    }
    
    /**
     * Searches for a load date between start and end
     * 
     * Accepted date formats (as supplied to java.text.SimpleDateFormat) "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ssz", "yyyy-MM-dd",
     * "yyyy-MM-dd'T'HH'|'mm", "EEE MMM dd HH:mm:ss zzz yyyy"}; "yyyyMMddhhmmss" <long value of ms since epoch>
     * 
     * @param fieldValue
     *            : A field value as a "time since epoch" long: should be LOAD_DATE
     * @param start
     *            : A start date in one of the formats specified above
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @returns True if the datetime occurs between the provided datetime values
     */
    public static boolean betweenLoadDates(String fieldValue, String start, String end) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.betweenLoadDates(fieldValue, start, end).size() > 0;
    }
    
    /**
     * Searches for a load date between start and end
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
     * @returns True if the datetime occurs between the provided datetime values
     */
    public static boolean betweenLoadDates(String fieldValue, String start, String end, String rangePattern) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.betweenLoadDates(fieldValue, start, end, rangePattern).size() > 0;
    }
    
    /**
     * Searches for a date between start and end
     * 
     * Accepted date formats (as supplied to java.text.SimpleDateFormat) "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ssz", "yyyy-MM-dd",
     * "yyyy-MM-dd'T'HH'|'mm", "EEE MMM dd HH:mm:ss zzz yyyy"}; "yyyyMMddhhmmss" <long value of ms since epoch>
     * 
     * @param fieldValue
     *            : A field value in one of the formats specified above
     * @param start
     *            : A start date in one of the formats specified above
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @returns True if the datetime occurs after the provided datetime value
     */
    public static boolean afterDate(Object fieldValue, String start) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.afterDate(fieldValue, start).size() > 0;
    }
    
    /**
     * Searches for a date after start
     * 
     * Accepted date formats (as supplied to java.text.SimpleDateFormat) "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ssz", "yyyy-MM-dd",
     * "yyyy-MM-dd'T'HH'|'mm", "EEE MMM dd HH:mm:ss zzz yyyy"}; "yyyyMMddhhmmss" <long value of ms since epoch>
     * 
     * @param fieldValue
     *            : A field value in one of the formats above
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @returns True if the datetime occurs after the provided datetime value
     */
    public static boolean afterDate(Object fieldValue, String start, String rangePattern) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.afterDate(fieldValue, start, rangePattern).size() > 0;
    }
    
    /**
     * Searches for a date after start
     * 
     * @param fieldValue
     *            : A field value in the supplied format
     * @param format
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @returns True if the datetime occurs after the provided datetime value
     */
    public static boolean afterDate(Object fieldValue, String pattern, String start, String rangePattern) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.afterDate(fieldValue, pattern, start, rangePattern).size() > 0;
    }
    
    /**
     * Searches for a date before end
     * 
     * Accepted date formats (as supplied to java.text.SimpleDateFormat) "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ssz", "yyyy-MM-dd",
     * "yyyy-MM-dd'T'HH'|'mm", "EEE MMM dd HH:mm:ss zzz yyyy"}; "yyyyMMddhhmmss" <long value of ms since epoch>
     * 
     * @param fieldValue
     *            : A field value in one of the formats specified above
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @returns True if the datetime occurs before the provided datetime value
     */
    public static boolean beforeDate(Object fieldValue, String end) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.beforeDate(fieldValue, end).size() > 0;
    }
    
    /**
     * Searches for a date before end
     * 
     * Accepted date formats (as supplied to java.text.SimpleDateFormat) "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ssz", "yyyy-MM-dd",
     * "yyyy-MM-dd'T'HH'|'mm", "EEE MMM dd HH:mm:ss zzz yyyy"}; "yyyyMMddhhmmss" <long value of ms since epoch>
     * 
     * @param fieldValue
     *            : A field value in one of the formats above
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @returns True if the datetime occurs before the provided datetime value
     */
    public static boolean beforeDate(Object fieldValue, String end, String rangePattern) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.beforeDate(fieldValue, end, rangePattern).size() > 0;
    }
    
    /**
     * Searches for a date before end
     * 
     * @param fieldValue
     *            : A field value in the supplied format
     * @param format
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @returns True if the datetime occurs before the provided datetime value
     */
    public static boolean beforeDate(Object fieldValue, String pattern, String end, String rangePattern) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.beforeDate(fieldValue, pattern, end, rangePattern).size() > 0;
    }
    
    /**
     * Searches for a date between start and end
     * 
     * Accepted date formats (as supplied to java.text.SimpleDateFormat) "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ssz", "yyyy-MM-dd",
     * "yyyy-MM-dd'T'HH'|'mm", "EEE MMM dd HH:mm:ss zzz yyyy"}; "yyyyMMddhhmmss" <long value of ms since epoch>
     * 
     * @param fieldValue
     *            : A field value in one of the formats specified above
     * @param start
     *            : A start date in one of the formats specified above
     * @param end
     *            : An end date in one of the formats specified above
     * 
     * @returns True if the datetime occurs between the provided datetime values
     */
    public static boolean betweenDates(String fieldValue, String start, String end) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.betweenDates(fieldValue, start, end).size() > 0;
    }
    
    /**
     * Searches for a date between start and end
     * 
     * Accepted date formats (as supplied to java.text.SimpleDateFormat) "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ssz", "yyyy-MM-dd",
     * "yyyy-MM-dd'T'HH'|'mm", "EEE MMM dd HH:mm:ss zzz yyyy"}; "yyyyMMddhhmmss" <long value of ms since epoch>
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
     * @returns True if the datetime occurs between the provided datetime values
     */
    public static boolean betweenDates(String fieldValue, String start, String end, String rangePattern) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.betweenDates(fieldValue, start, end, rangePattern).size() > 0;
    }
    
    /**
     * Searches for a date between start and end
     * 
     * @param fieldValue
     *            : A field value in the supplied format
     * @param format
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * @param start
     *            : A start date in the supplied rangePattern format
     * @param end
     *            : An end date in the supplied rangePattern format
     * @param rangePattern
     *            : A date format to be supplied to java.text.SimpleDateFormat
     * 
     * @returns True if the datetime occurs between the provided datetime values
     */
    public static boolean betweenDates(String fieldValue, String pattern, String start, String end, String rangePattern) {
        return nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions.betweenDates(fieldValue, pattern, start, end, rangePattern).size() > 0;
    }
    
}
