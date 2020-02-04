package datawave.query.composite;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.type.DiscreteIndexType;
import datawave.data.type.Type;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class contains a collection of methods and constants which are used when dealing with composite terms and ranges.
 *
 */
public class CompositeUtils {
    private static final Logger log = Logger.getLogger(CompositeUtils.class);
    
    public static final Set<Class<?>> ALL_VALID_LEAF_NODE_CLASSES = Sets.union(CompositeTerm.VALID_LEAF_NODE_CLASSES, CompositeRange.VALID_LEAF_NODE_CLASSES);
    public static final Set<Class<?>> UNBOUNDED_RANGE_NODE_CLASSES = Sets.newHashSet(ASTGTNode.class, ASTGENode.class, ASTLTNode.class, ASTLENode.class);
    
    public static Map<String,DiscreteIndexType<?>> getFieldToDiscreteIndexTypeMap(Multimap<String,?> fieldDatatypes) {
        Map<String,DiscreteIndexType<?>> fieldToDiscreteIndexTypeMap = new HashMap<>();
        for (String field : fieldDatatypes.keySet()) {
            DiscreteIndexType discreteIndexType = null;
            for (Object typeObj : fieldDatatypes.get(field)) {
                Type type = null;
                if (typeObj instanceof Type) {
                    type = (Type) typeObj;
                } else if (typeObj instanceof String) {
                    try {
                        type = Class.forName(typeObj.toString()).asSubclass(Type.class).newInstance();
                    } catch (Exception e) {
                        if (log.isTraceEnabled())
                            log.trace("Could not instantiate object for class [" + typeObj.toString() + "]");
                    }
                }
                if (type != null && type instanceof DiscreteIndexType && ((DiscreteIndexType) type).producesFixedLengthRanges()) {
                    if (discreteIndexType == null) {
                        discreteIndexType = (DiscreteIndexType) type;
                    } else if (!discreteIndexType.getClass().equals(type.getClass())) {
                        discreteIndexType = null;
                        break;
                    }
                }
            }
            
            if (discreteIndexType != null)
                fieldToDiscreteIndexTypeMap.put(field, discreteIndexType);
        }
        return fieldToDiscreteIndexTypeMap;
    }
    
    public static String getInclusiveLowerBound(String lowerBound, DiscreteIndexType discreteIndexType) {
        if (discreteIndexType != null) {
            String newLowerBound = discreteIndexType.incrementIndex(lowerBound);
            if (newLowerBound.compareTo(lowerBound) > 0)
                return newLowerBound;
        }
        return incrementBound(lowerBound);
    }
    
    public static String getExclusiveLowerBound(String lowerBound, DiscreteIndexType discreteIndexType) {
        if (discreteIndexType != null) {
            String newLowerBound = discreteIndexType.decrementIndex(lowerBound);
            if (newLowerBound.compareTo(lowerBound) < 0)
                return newLowerBound;
        }
        return decrementBound(lowerBound);
    }
    
    public static String getInclusiveUpperBound(String upperBound, DiscreteIndexType discreteIndexType) {
        if (discreteIndexType != null) {
            String newUpperBound = discreteIndexType.decrementIndex(upperBound);
            if (newUpperBound.compareTo(upperBound) < 0)
                return newUpperBound;
        }
        return decrementBound(upperBound);
    }
    
    public static String getExclusiveUpperBound(String upperBound, DiscreteIndexType discreteIndexType) {
        if (discreteIndexType != null) {
            String newUpperBound = discreteIndexType.incrementIndex(upperBound);
            if (newUpperBound.compareTo(upperBound) > 0)
                return newUpperBound;
        }
        return incrementBound(upperBound);
    }
    
    // NOTE: The output string will have the same number of characters as the input string.
    // An exception will be thrown if we can't decrement without maintaining the number of characters
    public static String decrementBound(String orig) {
        // decrement string
        int[] codePoints = orig.codePoints().toArray();
        int length = codePoints.length;
        int lastCodePoint = codePoints[length - 1];
        while (lastCodePoint == Character.MIN_CODE_POINT) {
            if (length == 1)
                throw new RuntimeException("Cannot decrement bound without decreasing the number of characters.");
            codePoints[length - 1] = Character.MAX_CODE_POINT;
            lastCodePoint = codePoints[--length - 1];
        }
        
        // keep decrementing until we reach a valid code point
        while (!Character.isValidCodePoint(--lastCodePoint))
            ;
        codePoints[length - 1] = lastCodePoint;
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < codePoints.length; i++)
            sb.appendCodePoint(codePoints[i]);
        
        return sb.toString();
    }
    
    // NOTE: The output string will have the same number of characters as the input string.
    // An exception will be thrown if we can't increment without maintaining the number of characters
    public static String incrementBound(String orig) {
        // increment string
        int[] codePoints = orig.codePoints().toArray();
        int length = codePoints.length;
        int lastCodePoint = codePoints[length - 1];
        while (lastCodePoint == Character.MAX_CODE_POINT) {
            if (length == 1)
                throw new RuntimeException("Cannot increment bound without increasing the number of characters.");
            codePoints[length - 1] = Character.MIN_CODE_POINT;
            lastCodePoint = codePoints[--length - 1];
        }
        
        // keep incrementing until we reach a valid code point
        while (!Character.isValidCodePoint(++lastCodePoint))
            ;
        codePoints[length - 1] = lastCodePoint;
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < codePoints.length; i++)
            sb.appendCodePoint(codePoints[i]);
        
        return sb.toString();
    }
}
