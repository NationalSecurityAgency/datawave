package datawave.attribute;

import datawave.util.StringUtils;

/**
 * FIELD_NAME.COMMONALITY.GROUPING_CONTEXT
 */
public class EventField {
    
    public static String[] getCommonalityAndGroupingContext(String field) {
        String[] splits = StringUtils.split(field, '.');
        if (splits.length >= 3) {
            // return the first group and last group (a.k.a the instance in the first group)
            return new String[] {splits[1], splits[splits.length - 1]};
        }
        return null;
    }
    
    public static String getBaseFieldName(String field) {
        String[] splits = StringUtils.split(field, '.');
        return splits[0];
    }
    
    public static String getGroupingContext(String field) {
        
        String[] splits = StringUtils.split(field, '.');
        if (splits.length >= 3) {
            return splits[splits.length - 1];
        }
        return null;
    }
    
    public static String getCommonality(String field) {
        String[] splits = StringUtils.split(field, '.');
        if (splits.length >= 3) {
            return splits[1];
        }
        return null;
    }
    
}
