package datawave.query.attributes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import datawave.data.normalizer.DateNormalizer;

/**
 * Represents different levels of granularity supported by the {@code #unique()} and {@code #groupby} function. This class is also responsible for providing the
 * functionality to transform values such that they conform to the specified granularity.
 */
public enum TemporalGranularity {

    /**
     * A {@link TemporalGranularity} implementation that will always return the original value.
     */
    ALL("ALL", Function.identity()),

    /**
     * A {@link TemporalGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the day padded with 0s. Otherwise,
     * the original value will be returned.
     * <p>
     * Format: <code>yyyy-MM-dd'T'00:00:00.000</code>
     */
    TRUNCATE_TEMPORAL_TO_DAY("DAY", new DateTimeValueFormatter("yyyy-MM-dd'T'00:00:00.000")),

    /**
     * A {@link TemporalGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the hour padded with 0s.
     * Otherwise, the original value will be returned.
     * <p>
     * Format: <code>yyyy-MM-dd'T'HH:00:00.000</code>
     */
    TRUNCATE_TEMPORAL_TO_HOUR("HOUR", new DateTimeValueFormatter("yyyy-MM-dd'T'HH:00:00.000")),

    /**
     * A {@link TemporalGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the month padded with 0s.
     * Otherwise, the original value will be returned.
     * <p>
     * Format: <code>yyyy-MM-00'T'00:00:00.000</code>
     */
    TRUNCATE_TEMPORAL_TO_MONTH("MONTH", new DateTimeValueFormatter("yyyy-MM-00'T'00:00:00.000")),

    /**
     * A {@link TemporalGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the year padded with 0s.
     * Otherwise, the original value will be returned.
     * <p>
     * Format: <code>yyyy-00-00'T'00:00:00.000</code>
     */
    TRUNCATE_TEMPORAL_TO_YEAR("YEAR", new DateTimeValueFormatter("yyyy-00-00'T'00:00:00.000")),

    /**
     * A {@link TemporalGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the second padded with 0s.
     * Otherwise, the original value will be returned.
     * <p>
     * Format: <code>yyyy-MM-dd'T'HH:mm:ss.000</code>
     */
    TRUNCATE_TEMPORAL_TO_SECOND("SECOND", new DateTimeValueFormatter("yyyy-MM-dd'T'HH:mm:ss.000")),

    /**
     * A {@link TemporalGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the millisecond. Otherwise, the
     * original value will be returned.
     * <p>
     * Format: <code>yyyy-MM-dd'T'HH:mm:ss.SSS</code>
     */
    TRUNCATE_TEMPORAL_TO_MILLISECOND("MILLISECOND", new DateTimeValueFormatter("yyyy-MM-dd'T'HH:mm:ss.SSS")),

    /**
     * A {@link TemporalGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the tenth of an hour padded with
     * 0s. Otherwise, the original value will be returned. Since the length of this datetime value can differ, we must ensure the correct amount of 0s are
     * included. We accomplish this by utilizing {@link #replaceCharWithZero(String, int)}. If the index provided is larger than the length of the date or less
     * than 1, it will return the original value.
     * <p>
     * Potential formats:
     * <p>
     * <code>yyyy-MM-dd'T'HH:m0:00.000</code>
     * <p>
     * <code>yyyy-MM-dd'T'HH:00:00.000</code>
     */
    TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR("TENTH_OF_HOUR", new DateTimeValueFormatter("yyyy-MM-dd'T'HH:mm:00.000", (date) -> replaceCharWithZero(date, 16))),

    /**
     * A {@link TemporalGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the minute padded with 0s.
     * Otherwise, the original value will be returned.
     * <p>
     * Format: <code>yyyy-MM-dd'T'HH:mm:00.000</code>
     */
    TRUNCATE_TEMPORAL_TO_MINUTE("MINUTE", new DateTimeValueFormatter("yyyy-MM-dd'T'HH:mm:00.000"));

    private final String name;
    private final Function<String,String> function;

    @JsonCreator
    public static TemporalGranularity of(String name) {
        name = name.toUpperCase();
        switch (name) {
            case "ALL":
                return TemporalGranularity.ALL;
            case "YEAR":
                return TemporalGranularity.TRUNCATE_TEMPORAL_TO_YEAR;
            case "MONTH":
                return TemporalGranularity.TRUNCATE_TEMPORAL_TO_MONTH;
            case "DAY":
                return TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY;
            case "HOUR":
                return TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR;
            case "TENTH_OF_HOUR":
                return TemporalGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR;
            case "MINUTE":
                return TemporalGranularity.TRUNCATE_TEMPORAL_TO_MINUTE;
            case "SECOND":
                return TemporalGranularity.TRUNCATE_TEMPORAL_TO_SECOND;
            case "MILLISECOND":
                return TemporalGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND;
            default:
                throw new IllegalArgumentException("No " + TemporalGranularity.class.getSimpleName() + " exists with the name " + name);
        }
    }

    TemporalGranularity(String name, Function<String,String> function) {
        this.name = name;
        this.function = function;
    }

    /**
     * Return the unique name of this {@link TemporalGranularity}.
     *
     * @return the name
     */
    @JsonValue
    public String getName() {
        return name;
    }

    /**
     * Apply the underlying transformation function to this value and return the result.
     *
     * @param value
     *            the value to transformed
     * @return the transformed result
     */
    public String transform(String value) {
        return function.apply(value);
    }

    /**
     * Replaces the character at the given index in the string with the character '0'. Useful for truncating values in date strings. If the index provided is
     * invalid (greater than the length of the date or less than 1), the original value will be returned with no replacements.
     *
     * @param str
     *            the string
     * @param index
     *            the character index
     * @return the new string
     */
    private static String replaceCharWithZero(String str, int index) {
        char[] strCharArray = str.toCharArray();
        if (index <= str.length() && index > 0) {
            strCharArray[index - 1] = '0';
            return String.valueOf(strCharArray);
        } else {
            return str;
        }
    }

    /**
     * A {@link Function} implementation to will handle datetime value formatting.
     */
    private static class DateTimeValueFormatter implements Function<String,String> {

        private static final Logger log = Logger.getLogger(DateTimeValueFormatter.class);
        private final SimpleDateFormat formatter;
        private final Function<String,String> postFormatFunction;

        private DateTimeValueFormatter(String pattern) {
            this(pattern, Function.identity());
        }

        private DateTimeValueFormatter(String pattern, Function<String,String> postFormatFunction) {
            this.formatter = new SimpleDateFormat(pattern);
            this.postFormatFunction = postFormatFunction;
        }

        @Override
        public String apply(String value) {
            try {
                // Attempt to format the denormalized date value.
                Date date = DateNormalizer.DATE_NORMALIZER.denormalize(value);
                String formattedDate = formatter.format(date);
                // Perform any additional formatting required.
                return postFormatFunction.apply(formattedDate);
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to format value " + value + " as date with pattern " + formatter.toPattern(), e);
                }
                // If a date could not be parsed, return the original value.
                return value;
            }
        }
    }
}
