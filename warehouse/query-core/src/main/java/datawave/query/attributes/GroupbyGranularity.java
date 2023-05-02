package datawave.query.attributes;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import datawave.data.normalizer.DateNormalizer;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

/**
 * Represents different levels of granularity supported by the {@code #unique()} function. This class is also responsible for providing the functionality to
 * transform values such that they conform to the specified granularity.
 */
public enum GroupbyGranularity {

    /**
     * A {@link GroupbyGranularity} implementation that will always return the original value.
     */
    ALL("ALL", Function.identity()),

    /**
     * A {@link GroupbyGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the day. Otherwise, the original
     * value will be returned.
     */
    TRUNCATE_TEMPORAL_TO_DAY("DAY", new DateTimeValueFormatter("yyyy-MM-dd")),

    /**
     * A {@link GroupbyGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the hour. Otherwise, the original
     * value will be returned.
     */
    TRUNCATE_TEMPORAL_TO_HOUR("HOUR", new DateTimeValueFormatter("yyyy-MM-dd'T'HH")),

    /**
     * A {@link GroupbyGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the month. Otherwise, the original
     * value will be returned.
     */
    TRUNCATE_TEMPORAL_TO_MONTH("MONTH", new DateTimeValueFormatter("yyyy-MM")),

    /**
     * A {@link GroupbyGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the year. Otherwise, the original
     * value will be returned.
     */
    TRUNCATE_TEMPORAL_TO_YEAR("YEAR", new DateTimeValueFormatter("yyyy")),

    /**
     * A {@link GroupbyGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the second. Otherwise, the original
     * value will be returned.
     */
    TRUNCATE_TEMPORAL_TO_SECOND("SECOND", new DateTimeValueFormatter("yyyy-MM-dd'T'HH:mm:ss")),

    /**
     * A {@link GroupbyGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the millisecond. Otherwise, the
     * original value will be returned.
     */
    TRUNCATE_TEMPORAL_TO_MILLISECOND("MILLISECOND", new DateTimeValueFormatter("yyyy-MM-dd'T'HH:mm:ss.SSS")),

    /**
     * A {@link GroupbyGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the tenth of an hour. Otherwise, the
     * original value will be returned.
     */
    TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR("TENTH_OF_HOUR", new DateTimeValueFormatter("yyyy-MM-dd'T'HH:m", true)),

    /**
     * A {@link GroupbyGranularity} implementation that, if provided a datetime value, will return the datetime truncated to the minute. Otherwise, the original
     * value will be returned.
     */
    TRUNCATE_TEMPORAL_TO_MINUTE("MINUTE", new DateTimeValueFormatter("yyyy-MM-dd'T'HH:mm"));

    private final String name;
    private final Function<String,String> function;

    @JsonCreator
    public static GroupbyGranularity of(String name) {
        switch (name) {
            case "ALL":
                return GroupbyGranularity.ALL;
            case "YEAR":
                return GroupbyGranularity.TRUNCATE_TEMPORAL_TO_YEAR;
            case "MONTH":
                return GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MONTH;
            case "DAY":
                return GroupbyGranularity.TRUNCATE_TEMPORAL_TO_DAY;
            case "HOUR":
                return GroupbyGranularity.TRUNCATE_TEMPORAL_TO_HOUR;
            case "TENTH_OF_HOUR":
                return GroupbyGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR;
            case "MINUTE":
                return GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MINUTE;
            case "SECOND":
                return GroupbyGranularity.TRUNCATE_TEMPORAL_TO_SECOND;
            case "MILLISECOND":
                return GroupbyGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND;
            default:
                throw new IllegalArgumentException("No " + GroupbyGranularity.class.getSimpleName() + " exists with the name " + name);
        }
    }

    GroupbyGranularity(String name, Function<String,String> function) {
        this.name = name;
        this.function = function;
    }
    
    /**
     * Return the unique name of this {@link GroupbyGranularity}.
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
     * A {@link Function} implementation to will handle datetime value formatting.
     */
    private static class DateTimeValueFormatter implements Function<String,String> {
        
        private static final Logger log = Logger.getLogger(DateTimeValueFormatter.class);
        private final SimpleDateFormat formatter;
        private boolean isTenth = false;
        
        private DateTimeValueFormatter(String pattern) {
            this.formatter = new SimpleDateFormat(pattern);
        }
        
        private DateTimeValueFormatter(String pattern, boolean isTenth) {
            this.formatter = new SimpleDateFormat(pattern);
            this.isTenth = isTenth;
        }
        
        @Override
        public String apply(String value) {
            try {
                // Attempt to format the denormalized date value.
                Date date = DateNormalizer.DATE_NORMALIZER.denormalize(value);
                String formattedDate = formatter.format(date);
                if (!isTenth) {
                    return formattedDate;
                } else {
                    return formattedDate.substring(0, formattedDate.length() - 1);
                }
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
