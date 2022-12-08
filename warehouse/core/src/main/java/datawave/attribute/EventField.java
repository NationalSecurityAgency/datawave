package datawave.attribute;

import datawave.util.StringUtils;

/**
 * FIELD_NAME.GROUP.{intermediate groups}.SUBGROUP
 */

public class EventField {
    
    private static final char GROUPING_SEPARATOR = '.';
    
    public static String[] getGroupAndSubgroup(String field) {
        String[] splits = StringUtils.split(field, GROUPING_SEPARATOR);
        if (splits.length >= 3) {
            // return the first group and last group (a.k.a the instance in the first group)
            return new String[] {splits[1], splits[splits.length - 1]};
        }
        return null;
    }
    
    public static String getBaseFieldName(String field) {
        String[] splits = StringUtils.split(field, GROUPING_SEPARATOR);
        return splits[0];
    }
    
    public static String getSubgroup(String field) {
        
        String[] splits = StringUtils.split(field, GROUPING_SEPARATOR);
        if (splits.length >= 3) {
            return splits[splits.length - 1];
        }
        return null;
    }
    
    public static String getGroup(String field) {
        String[] splits = StringUtils.split(field, GROUPING_SEPARATOR);
        if (splits.length >= 3) {
            return splits[1];
        }
        return null;
    }
    
}
