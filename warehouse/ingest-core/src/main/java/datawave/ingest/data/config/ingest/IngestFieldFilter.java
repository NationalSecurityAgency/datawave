package datawave.ingest.data.config.ingest;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.GroupedNormalizedContentInterface;
import datawave.ingest.data.config.NormalizedContentInterface;
import org.apache.commons.collections4.Unmodifiable;
import org.apache.commons.collections4.keyvalue.AbstractMapEntry;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The field filter can drop fields based on the presence of other preferred fields. This is configuration driven and expects colon delimited pairs, A:B, where
 * if A is present in the fields, then any occurrences of B will be removed. A and/or B can represent a set of fields delimited by the ampersand sign. In the
 * case of the data.fieldvalue.filter, the values must match as well which implies that the number of KEEP fields must be the same as the number of DROP fields
 * which is not the case for the fieldName filter. For backward compatibility datatype1.data.field.filter can be used in place of
 * datatype1.data.fieldname.filter however that is deprecated and will be removed in a future release.
 *
 * <p>
 * Example:
 * 
 * <pre>
 * {@code
 * 
 *     <property>
 *         <name>datatype1.data.fieldname.filter</name>
 *         <value>KEEP1:DROP1,KEEP2&KEEP3:DROP2</value>
 *     </property>
 *     <property>
 *         <name>datatype1.data.fieldvalue.filter</name>
 *         <value>KEEP1:DROP1,KEEP2&KEEP3:DROP2&DROP3</value>
 *     </property>
 * 
 * }
 * </pre>
 */
public class IngestFieldFilter {
    
    private static final Logger logger = Logger.getLogger(IngestFieldFilter.class);
    
    @Deprecated
    public static final String FILTER_FIELD_SUFFIX = ".data.field.filter";
    
    public static final String FILTER_FIELD_NAME_SUFFIX = ".data.fieldname.filter";
    public static final String FILTER_FIELD_VALUE_SUFFIX = ".data.fieldvalue.filter";
    public static final char PAIR_DELIM = ',';
    public static final char VALUE_DELIM = ':';
    public static final char FIELD_DELIM = '&';
    
    private final Type dataType;
    private FieldConfiguration fieldNameFilters;
    private FieldConfiguration fieldValueFilters;
    
    public IngestFieldFilter(Type dataType) {
        this.dataType = dataType;
    }
    
    /**
     * Configures the field filter.
     *
     * @param conf
     */
    public void setup(Configuration conf) {
        fieldNameFilters = new FieldConfiguration();
        fieldNameFilters.load(conf.get(dataType.typeName() + FILTER_FIELD_SUFFIX), false);
        fieldNameFilters.load(conf.get(dataType.typeName() + FILTER_FIELD_NAME_SUFFIX), false);
        logger.info("Field Name Filters for " + dataType.typeName() + ": " + fieldNameFilters);
        
        fieldValueFilters = new FieldConfiguration(conf.get(dataType.typeName() + FILTER_FIELD_VALUE_SUFFIX), true);
        logger.info("Field Value Filters for " + dataType.typeName() + ": " + fieldValueFilters);
    }
    
    /**
     * Applies the configured filter rules to the given fields.
     *
     * @param fields
     */
    public void apply(Multimap<String,?> fields) {
        // for the field name filters, remove the drop fields if the keep fields exist
        for (FieldFilter filter : fieldNameFilters) {
            if (fields.keySet().containsAll(filter.getKeepFields())) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Removing " + filter.getDropFields() + " because " + filter.getKeepFields() + " exists in event");
                }
                fields.keySet().removeAll(filter.getDropFields());
            }
        }
        // for the field value filters, remove the drop fields if the keep fields exist with the same values
        for (FieldFilter filter : fieldValueFilters) {
            if (fields.keySet().containsAll(filter.getKeepFields())) {
                for (List<FieldValue> keepValues : gatherValueLists(fields, filter.getKeepFields(), -1, null)) {
                    for (List<FieldValue> toRemoveValues : gatherValueLists(fields, filter.getDropFields(), -1, null)) {
                        if (equalValues(keepValues, toRemoveValues)) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Removing " + toRemoveValues + " because " + keepValues + " exists in event");
                            }
                            for (FieldValue toRemoveValue : toRemoveValues) {
                                fields.remove(toRemoveValue.getKey(), toRemoveValue.getValue());
                            }
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Determine if two sets of field values are the same
     * 
     * @param left
     * @param right
     * @return true if the values are equal
     */
    private boolean equalValues(List<FieldValue> left, List<FieldValue> right) {
        boolean matches = true;
        
        if (left.size() != right.size()) {
            matches = false;
        } else {
            for (int i = 0; matches && i < left.size(); i++) {
                matches = left.get(i).equalRawValue(right.get(i));
            }
        }
        
        return matches;
    }
    
    /**
     * Gather sets of values for the fields specified. This is basically the dot product of the values for each field to gather, with the complexity that we
     * need to keep groups together. So if one of the fields' values contains a group (or the group is specified) then only those other fields' values with the
     * same group should be considered in the dot product.
     * 
     * @param fieldValues
     *            The field values
     * @param fieldsToGather
     *            The fields to gather (this ordering is maintained in the return value)
     * @param group
     *            The group to use or null if not specified
     * @return a collection of value lists which maintain the order as specified in the fieldsToGather
     */
    private Collection<List<FieldValue>> gatherValueLists(Multimap<String,?> fieldValues, List<String> fieldsToGather, int index, String group) {
        List<List<FieldValue>> valueLists = Lists.newArrayList();
        
        if (fieldsToGather.isEmpty()) {
            return valueLists;
        }
        
        // since we are adding the entries starting at the tail of the recursion, we will start with the last field and work backwards
        if (index < 0) {
            index = fieldsToGather.size() - 1;
        }
        String field = fieldsToGather.get(index);
        
        // for each of the values for the current field
        for (FieldValue value : getValues(fieldValues, field, group)) {
            // if this is the last field, then simply create a value set per value
            if (index == 0) {
                // initialize with an initial capacity equal to the number of fields
                List<FieldValue> singleValueList = new ArrayList<>(fieldsToGather.size());
                singleValueList.add(value);
                valueLists.add(singleValueList);
            }
            // else add this value to each of the value sets found recursively for the remaining fields
            else {
                for (List<FieldValue> singleValueList : gatherValueLists(fieldValues, fieldsToGather, index - 1,
                // restrict to the group already determined, or the group of this value if not yet determined
                                (group == null ? value.getGroup() : group))) {
                    singleValueList.add(value);
                    valueLists.add(singleValueList);
                }
            }
        }
        
        return valueLists;
    }
    
    /**
     * Get the values for a given field and optionally specified group
     * 
     * @param fieldValues
     *            The field values to pull from
     * @param field
     *            The field to gather for
     * @param group
     *            The optionally specified group
     * @return A collection of field/value pairs
     */
    private Collection<FieldValue> getValues(Multimap<String,?> fieldValues, String field, String group) {
        List<FieldValue> values = Lists.newArrayList();
        
        if (group == null) {
            for (Object value : fieldValues.get(field)) {
                values.add(new FieldValue(field, value));
            }
        } else {
            for (Object value : fieldValues.get(field)) {
                String newGroup = new FieldValue(null, value).getGroup();
                if (newGroup == null || newGroup.equals(group)) {
                    values.add(new FieldValue(field, value));
                }
            }
        }
        return values;
    }
    
    /***************************************************************************
     * An unmodifiable field value
     ***************************************************************************/
    class FieldValue extends AbstractMapEntry implements Unmodifiable {
        public FieldValue(String field, Object value) {
            super(field, value);
        }
        
        public String getField() {
            return (String) getKey();
        }
        
        public Object setValue(Object value) {
            throw new UnsupportedOperationException("Unmodifiable value");
        }
        
        /**
         * Get the raw value
         * 
         * @return The raw value which is the same as getValue() unless that is a NormalizedContentInterface
         */
        public Object getRawValue() {
            Object value = getValue();
            if (value instanceof NormalizedContentInterface) {
                value = ((NormalizedContentInterface) value).getEventFieldValue();
            }
            return value;
        }
        
        /**
         * Get the group for this value. This will only actually return a group if the value is a GroupedNormalizedContentInterface in which case it contains
         * the field name with its grouping info.
         * 
         * @return the group
         */
        public String getGroup() {
            Object value = getValue();
            if (value instanceof GroupedNormalizedContentInterface) {
                GroupedNormalizedContentInterface grouped = (GroupedNormalizedContentInterface) value;
                if (grouped.isGrouped()) {
                    return (grouped.getSubGroup() == null ? grouped.getGroup() : grouped.getSubGroup());
                }
            }
            return null;
        }
        
        /**
         * Determine if the raw value in this matches the raw value in another
         * 
         * @param other
         * @return true of equal
         */
        public boolean equalRawValue(FieldValue other) {
            Object left = getRawValue();
            Object right = other.getRawValue();
            if (left == null || right == null) {
                return left == right;
            } else {
                return left.toString().equals(right.toString());
            }
        }
        
    }
    
    /***************************************************************************
     * An unmodifiable field filter
     ***************************************************************************/
    class FieldFilter extends AbstractMapEntry implements Unmodifiable {
        public FieldFilter(List<String> keep, List<String> drop) {
            super(keep, drop);
        }
        
        public List<String> getKeepFields() {
            return (List<String>) getKey();
        }
        
        public List<String> getDropFields() {
            return (List<String>) getValue();
        }
        
        public Object setValue(Object value) {
            throw new UnsupportedOperationException("Unmodifiable value");
        }
    }
    
    /***************************************************************************
     * A parsed field configuration
     ***************************************************************************/
    class FieldConfiguration extends ArrayList<FieldFilter> implements List<FieldFilter> {
        
        public FieldConfiguration() {
            
        }
        
        public FieldConfiguration(String fieldStr, boolean fieldCountMustMatch) {
            load(fieldStr, fieldCountMustMatch);
        }
        
        /**
         * load a field configuration
         *
         * @param fieldsStr
         *            field configuration
         * @param fieldCountMustMatch
         *            true if the field counts on the left must match those on the right
         * @throws IllegalArgumentException
         *             if the configuration is invalid
         */
        public void load(String fieldsStr, boolean fieldCountMustMatch) throws IllegalArgumentException {
            if (StringUtils.isNotBlank(fieldsStr)) {
                for (String pair : StringUtils.split(fieldsStr, PAIR_DELIM)) {
                    if (!StringUtils.isBlank(pair)) {
                        String[] tokens = StringUtils.split(pair, VALUE_DELIM);
                        if (tokens.length == 2) {
                            List<String> left = parseFields(tokens[0]);
                            List<String> right = parseFields(tokens[1]);
                            if (fieldCountMustMatch && left.size() != right.size()) {
                                throw new IllegalArgumentException("Cannot compare different size lists: " + left + " vs " + right);
                            }
                            super.add(new FieldFilter(left, right));
                        } else {
                            throw new IllegalArgumentException("Expected a " + VALUE_DELIM + " delimited pair but received: " + pair
                                            + ", ignoring this config.");
                        }
                    }
                }
            }
        }
        
        /**
         * Parse a fields spec into a list of fields
         * 
         * @param fields
         *            fields delimited by '|'
         * @return a list of fields
         */
        private List<String> parseFields(String fields) {
            return Collections.unmodifiableList(Arrays.asList(StringUtils.split(fields, FIELD_DELIM)));
        }
        
    }
}
