package nsa.datawave.query.functions;

import nsa.datawave.data.type.util.NumericalEncoder;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;

/**
 * NOTE: The JexlFunctionArgumentDescriptorFactory is implemented by GeoFunctionsDescriptor. This is kept as a separate class to reduce accumulo dependencies on
 * other jars.
 *
 **/
@Deprecated
@JexlFunctions(descriptorFactory = "nsa.datawave.query.functions.QueryFunctionsDescriptor")
public class QueryFunctions {
    
    protected static Logger log = Logger.getLogger(QueryFunctions.class);
    
    public static boolean length(String fieldValue, long lower, long upper) {
        if (upper < lower)
            throw new IllegalArgumentException("upper bound must be greater than the lower bound");
        return fieldValue.length() >= lower && fieldValue.length() <= upper;
    }
    
    public static boolean between(String fieldValue, double left, double right) {
        Number value;
        if (NumericalEncoder.isPossiblyEncoded(fieldValue)) {
            try {
                value = NumericalEncoder.decode(fieldValue);
            } catch (NumberFormatException nfe) {
                try {
                    value = NumberUtils.createNumber(fieldValue);
                } catch (Exception nfe2) {
                    throw new NumberFormatException("Cannot decode " + fieldValue + " using NumericalEncoder or Double");
                }
            }
        } else {
            try {
                value = NumberUtils.createNumber(fieldValue);
            } catch (Exception nfe2) {
                throw new NumberFormatException("Cannot decode " + fieldValue + " using Double");
            }
        }
        
        return value.doubleValue() >= left && value.doubleValue() <= right;
    }
    
    public static boolean between(String fieldValue, long left, long right) {
        return between(fieldValue, (double) left, (double) right);
    }
}
