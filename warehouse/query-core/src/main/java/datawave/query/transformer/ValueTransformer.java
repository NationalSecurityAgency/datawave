package datawave.query.transformer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import datawave.data.normalizer.DateNormalizer;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

public enum ValueTransformer {
    
    /**
     * A {@link ValueTransformer} implementation that will always return the original value.
     */
    ORIGINAL("ORIGINAL", Function.identity()),
    
    /**
     * A {@link ValueTransformer} implementation that, if provided a datetime value, will return the datetime truncated to the day. Otherwise, the original
     * value will be returned.
     */
    TRUNCATE_TEMPORAL_TO_DAY("DAY", new DateTimeValueFormatter("yyyy-MM-dd")),
    
    /**
     * A {@link ValueTransformer} implementation that, if provided a datetime value, will return the datetime truncated to the hour. Otherwise, the original
     * value will be returned.
     */
    TRUNCATE_TEMPORAL_TO_HOUR("HOUR", new DateTimeValueFormatter("yyyy-MM-dd'T'HH")),
    
    /**
     * A {@link ValueTransformer} implementation that, if provided a datetime value, will return the datetime truncated to the minute. Otherwise, the original
     * value will be returned.
     */
    TRUNCATE_TEMPORAL_TO_MINUTE("MINUTE", new DateTimeValueFormatter("yyyy-MM-dd'T'HH:mm"));
    
    private final String name;
    private final Function<String,String> function;
    
    @JsonCreator
    public static ValueTransformer of(String name) {
        switch (name) {
            case "ORIGINAL":
                return ValueTransformer.ORIGINAL;
            case "DAY":
                return ValueTransformer.TRUNCATE_TEMPORAL_TO_DAY;
            case "HOUR":
                return ValueTransformer.TRUNCATE_TEMPORAL_TO_HOUR;
            case "MINUTE":
                return ValueTransformer.TRUNCATE_TEMPORAL_TO_MINUTE;
            default:
                throw new IllegalArgumentException("No " + ValueTransformer.class.getSimpleName() + " exists with the name " + name);
        }
    }
    
    ValueTransformer(String name, Function<String,String> function) {
        this.name = name;
        this.function = function;
    }
    
    /**
     * Return the unique name of this {@link ValueTransformer}.
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
        
        private DateTimeValueFormatter(String pattern) {
            this.formatter = new SimpleDateFormat(pattern);
        }
        
        @Override
        public String apply(String value) {
            try {
                // Attempt to format the denormalized date value.
                Date date = DateNormalizer.DATE_NORMALIZER.denormalize(value);
                return formatter.format(date);
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
