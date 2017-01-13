package nsa.datawave.query.rewrite.jexl.functions;

import nsa.datawave.data.type.util.NumericalEncoder;
import nsa.datawave.query.rewrite.attributes.ValueTuple;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * NOTE: The JexlFunctionArgumentDescriptorFactory is implemented by GeoFunctionsDescriptor. This is kept as a separate class to reduce accumulo dependencies on
 * other jars.
 * 
 **/
@JexlFunctions(descriptorFactory = "nsa.datawave.query.rewrite.jexl.functions.QueryFunctionsDescriptor")
public class QueryFunctions {
    
    public static final String QUERY_FUNCTION_NAMESPACE = "f";
    
    protected static Logger log = Logger.getLogger(QueryFunctions.class);
    
    protected static String getHitTermString(Object valueTuple) {
        Object value = ValueTuple.getStringValue(valueTuple);
        Object fieldName = ValueTuple.getFieldName(valueTuple);
        if (value != null && fieldName != null) {
            return fieldName.toString() + ":" + value.toString();
        } else {
            return "";
        }
    }
    
    public static Collection<?> length(Object field, long lower, long upper) {
        Set<String> matches = Collections.<String> emptySet();
        String fieldValue = ValueTuple.getNormalizedStringValue(field);
        if (upper < lower)
            throw new IllegalArgumentException("upper bound must be greater than the lower bound");
        if (fieldValue != null && fieldValue.length() >= lower && fieldValue.length() <= upper) {
            matches = Collections.singleton(getHitTermString(field));
        }
        return matches;
    }
    
    public static Collection<?> length(Iterable<?> values, long lower, long upper) {
        for (Object value : values) {
            Collection<?> matches = length(value, lower, upper);
            if (!matches.isEmpty()) {
                return matches;
            }
        }
        return Collections.<String> emptySet();
    }
    
    public static Collection<?> between(Object field, String left, String right) {
        return between(field, left, true, right, true);
    }
    
    public static Collection<?> between(Object field, String left, boolean leftInclusive, String right, boolean rightInclusive) {
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
        return Collections.<String> emptySet();
    }
    
    public static Collection<?> between(Iterable<?> values, String left, String right) {
        return between(values, left, true, right, true);
    }
    
    public static Collection<?> between(Iterable<?> values, String left, boolean leftInclusive, String right, boolean rightInclusive) {
        for (Object value : values) {
            Collection<?> matches = between(value, left, leftInclusive, right, rightInclusive);
            if (!matches.isEmpty()) {
                return matches;
            }
        }
        return Collections.<String> emptySet();
    }
    
    public static Collection<?> between(Object field, float left, float right) {
        return between(field, left, true, right, true);
    }
    
    public static Collection<?> between(Object field, float left, boolean leftInclusive, float right, boolean rightInclusive) {
        String fieldValue = ValueTuple.getNormalizedStringValue(field);
        Number value;
        if (fieldValue != null && NumericalEncoder.isPossiblyEncoded(fieldValue)) {
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
        if (value != null) {
            float floatValue = value.floatValue();
            if (((floatValue > left) || (leftInclusive && floatValue == left)) && ((floatValue < right) || (rightInclusive && floatValue == right))) {
                return Collections.singleton(getHitTermString(field));
            }
        }
        return Collections.<String> emptySet();
    }
    
    public static Collection<?> between(Iterable<?> values, float left, float right) {
        return between(values, left, true, right, true);
    }
    
    public static Collection<?> between(Iterable<?> values, float left, boolean leftInclusive, float right, boolean rightInclusive) {
        for (Object value : values) {
            Collection<?> matches = between(value, left, leftInclusive, right, rightInclusive);
            if (!matches.isEmpty()) {
                return matches;
            }
        }
        return Collections.<String> emptySet();
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
    
}
