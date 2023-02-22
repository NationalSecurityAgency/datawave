package datawave.ingest.data.config;

import org.apache.hadoop.conf.Configuration;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Helper for CSV data
 */
public class CSVHelper extends DataTypeHelperImpl {
    /**
     * Configuration parameter that specifies the separator for list of fields AND the data in the csv file. This configuration parameter supports multiple
     * datatypes, so a valid value would look like mydatatype.data.separator
     */
    public static final String DATA_SEP = ".data.separator";
    
    /**
     * Configuration parameter that should contain the list of fields in a csv file in the correct order. This configuration parameter supports multiple
     * datatypes, so a valid value would look like mydatatype.data.header
     */
    public static final String DATA_HEADER = ".data.header";
    
    /**
     * Configuration parameter that states whether we expect to find header fields or whether all field should be treated as 'extra' fields with name, value
     * pairs.
     */
    public static final String DATA_HEADER_ENABLED = ".data.header.enabled";
    
    /**
     * Parameter to specify that the csv file contains a header row and that we should skip it.
     */
    public static final String SKIP_CSV_HEADER_ROW = ".skip.csv.header.row";
    
    /**
     * Parameter to specify that the csv file contains fields past the basic header that should be processed as name=value pairs
     */
    public static final String PROCESS_EXTRA_FIELDS = ".data.process.extra.fields";
    
    /**
     * Parameter to explicitly specify a subset of fields that should be added to the event, all others are dropped.
     */
    public static final String FIELD_WHITELIST = ".data.field.whitelist";
    
    /**
     * Parameter to explicitly specify a subset of fields that should be removed from the event, all others are kept.
     */
    public static final String FIELD_BLACKLIST = ".data.field.blacklist";
    
    /**
     * Parameter to specify the fields that are multivalued (whitelist)
     */
    public static final String MULTI_VALUED_FIELDS = ".data.fields.multivalued";
    
    /**
     * Parameter to specify the fields that are not multivalued (blacklist)
     */
    public static final String MULTI_VALUED_FIELDS_BLACKLIST = ".data.fields.multivalued.blacklist";
    
    /**
     * Parameter to specify the separator for multivalued fields (default is ';');
     */
    public static final String MULTI_VALUED_SEPARATOR = ".data.multivalued.separator";
    
    /**
     * Parameter to specify the a threshold on the number of fields in a multi-valued field
     */
    public static final String MULTI_VALUED_THRESHOLD = ".data.multivalued.threshold";
    
    /**
     * Parameter to specify what to do when a field size or multi-valued field threshold is reached
     */
    public static final String MULTI_VALUED_THRESHOLD_ACTION = ".data.multivalued.threshold.action";
    
    /**
     * Parameter to specify the field to add to an event to denote fields that were truncated
     */
    public static final String MULTI_VALUED_TRUNCATE_FIELD = ".data.multivalued.truncate.threshold.field";
    
    /**
     * Parameter to specify the field to add to an event to denote fields that were dropped
     */
    public static final String MULTI_VALUED_DROP_FIELD = ".data.multivalued.drop.threshold.field";
    
    /**
     * Parameter to specify the threshold field replacement when the threshold action is to replace
     */
    public static final String MULTI_VALUED_THRESHOLD_FIELD_REPLACEMENT = ".data.multivalued.threshold.replacement";
    
    /**
     * Parameter to specify the threshold on the size of a field processed.
     */
    public static final String FIELD_SIZE_THRESHOLD = ".data.field.length.threshold";
    
    /**
     * Parameter to specify what to do when a field size or multi-valued field threshold is reached
     */
    public static final String THRESHOLD_ACTION = ".data.threshold.action";
    
    /**
     * Parameter to specify the field to add to an event to denote fields that were truncated
     */
    public static final String TRUNCATE_FIELD = ".data.truncate.threshold.field";
    
    /**
     * Parameter to specify the field to add to an event to denote fields that were dropped
     */
    public static final String DROP_FIELD = ".data.drop.threshold.field";
    
    /**
     * Parameter to specify the threshold field replacement when the threshold action is to replace
     */
    public static final String THRESHOLD_FIELD_REPLACEMENT = ".data.threshold.replacement";
    
    /** Partial configuration key for specifying CSV fields that a record must have. */
    public static final String REQUIRED_FIELDS = ".data.fields.required";
    
    /** Pattern used to prevent matching escaped multivalue field separators when splitting multivalued fields */
    public static final String BACKSLASH_ESCAPE_LOOKBEHIND_PATTERN = "(?<!\\\\)";
    
    public enum ThresholdAction {
        FAIL, DROP, REPLACE, TRUNCATE
    }
    
    private String[] header = null;
    private String separator = null;
    private boolean skipHeaderRow = false;
    private boolean processExtraFields = false;
    private Map<String,String> multiValuedFields = new HashMap<>();
    private Map<String,String> multiValuedFieldsBlacklist = new HashMap<>();
    private boolean hasMultiValuedFieldsBlacklist = false;
    private String multiValueSeparator = null;
    private int fieldSizeThreshold = Integer.MAX_VALUE;
    private int multiFieldSizeThreshold = Integer.MAX_VALUE;
    private ThresholdAction thresholdAction = ThresholdAction.FAIL;
    private String thresholdReplacement = "(too large)";
    private String truncateField = "TRUNCATED_FIELD";
    private String dropField = "DROPPED_FIELD";
    private ThresholdAction multiValuedThresholdAction = ThresholdAction.FAIL;
    private String multiValuedThresholdReplacement = "(too many)";
    private String multiValuedTruncateField = "TRUNCATED_MULTI_VALUED_FIELD";
    private String multiValuedDropField = "DROPPED_MULTI_VALUED_FIELD";
    private Set<String> fieldBlacklist = null;
    private Set<String> fieldWhitelist = null;
    
    /** The Set of field names that a record must have to be valid. */
    private Set<String> _requiredFields = null;
    
    /** Whether or not this CSV-based data format has required fields. */
    private boolean _hasReqFields;
    
    @Override
    public void setup(Configuration config) throws IllegalArgumentException {
        super.setup(config);
        
        boolean headerEnabled = config.getBoolean(this.getType().typeName() + DATA_HEADER_ENABLED, true);
        if (headerEnabled) {
            header = ConfigurationHelper.isNull(config, this.getType().typeName() + DATA_HEADER, String[].class);
        } else {
            header = new String[0];
        }
        
        separator = ConfigurationHelper.isNull(config, this.getType().typeName() + DATA_SEP, String.class);
        
        // Get the skip header row property
        this.skipHeaderRow = config.getBoolean(this.getType().typeName() + SKIP_CSV_HEADER_ROW, false);
        
        // Get the process extra fields property
        this.processExtraFields = config.getBoolean(this.getType().typeName() + PROCESS_EXTRA_FIELDS, false);
        
        // Get the whitelist of event fields to keep.
        Collection<String> cw = config.getStringCollection(this.getType().typeName() + FIELD_WHITELIST);
        if (cw != null && !cw.isEmpty()) {
            this.fieldWhitelist = new HashSet<>(cw);
        }
        
        // Get the blacklist of event fields to drop.
        Collection<String> cb = config.getStringCollection(this.getType().typeName() + FIELD_BLACKLIST);
        if (cb != null && !cb.isEmpty()) {
            this.fieldBlacklist = new HashSet<>(cw);
        }
        
        final Collection<String> reqFields = config.getStringCollection(getType().typeName() + REQUIRED_FIELDS);
        if (reqFields == null || reqFields.isEmpty()) {
            _hasReqFields = false;
        } else {
            this._requiredFields = new HashSet<>(reqFields);
            _hasReqFields = true;
        }
        
        if (!headerEnabled && !processExtraFields) {
            throw new IllegalArgumentException("Both " + this.getType().typeName() + DATA_HEADER_ENABLED + " or " + this.getType().typeName()
                            + PROCESS_EXTRA_FIELDS + " are " + "configured to 'false', either or both must be 'true'");
        }
        
        // Get the multi-valued fields blacklist configuration
        if (config.get(this.getType().typeName() + MULTI_VALUED_FIELDS_BLACKLIST) != null) {
            for (String field : config.getStrings(this.getType().typeName() + MULTI_VALUED_FIELDS_BLACKLIST, new String[0])) {
                int index = field.indexOf(':');
                if (index > 0) {
                    multiValuedFieldsBlacklist.put(field.substring(0, index), field.substring(index + 1));
                } else {
                    multiValuedFieldsBlacklist.put(field, field);
                }
            }
            hasMultiValuedFieldsBlacklist = true;
        }
        
        // Get the multi-valued fields configuration
        if (!hasMultiValuedFieldsBlacklist) {
            for (String field : config.getStrings(this.getType().typeName() + MULTI_VALUED_FIELDS, new String[0])) {
                int index = field.indexOf(':');
                if (index > 0) {
                    multiValuedFields.put(field.substring(0, index), field.substring(index + 1));
                } else {
                    multiValuedFields.put(field, field);
                }
            }
        }
        
        this.multiValueSeparator = config.get(this.getType().typeName() + MULTI_VALUED_SEPARATOR, ";");
        
        this.fieldSizeThreshold = config.getInt(this.getType().typeName() + FIELD_SIZE_THRESHOLD, this.fieldSizeThreshold);
        this.thresholdAction = ThresholdAction.valueOf(config.get(this.getType().typeName() + THRESHOLD_ACTION, this.thresholdAction.name()).toUpperCase());
        this.thresholdReplacement = config.get(this.getType().typeName() + THRESHOLD_FIELD_REPLACEMENT, this.thresholdReplacement);
        this.truncateField = config.get(this.getType().typeName() + TRUNCATE_FIELD, this.truncateField);
        this.dropField = config.get(this.getType().typeName() + DROP_FIELD, this.dropField);
        
        this.multiFieldSizeThreshold = config.getInt(this.getType().typeName() + MULTI_VALUED_THRESHOLD, this.multiFieldSizeThreshold);
        this.multiValuedThresholdAction = ThresholdAction.valueOf(config.get(this.getType().typeName() + MULTI_VALUED_THRESHOLD_ACTION,
                        this.multiValuedThresholdAction.name()).toUpperCase());
        this.multiValuedThresholdReplacement = config.get(this.getType().typeName() + MULTI_VALUED_THRESHOLD_FIELD_REPLACEMENT,
                        this.multiValuedThresholdReplacement);
        this.multiValuedTruncateField = config.get(this.getType().typeName() + MULTI_VALUED_TRUNCATE_FIELD, this.multiValuedTruncateField);
        this.multiValuedDropField = config.get(this.getType().typeName() + MULTI_VALUED_DROP_FIELD, this.multiValuedDropField);
    }
    
    /**
     * Whether or not the data format has required fields.
     * 
     * @return flag noting if the format has required fields
     */
    public boolean hasRequiredFields() {
        return _hasReqFields;
    }
    
    /**
     * Whether or not the data format has required fields.
     * 
     * @param fieldName
     *            the field name
     * @return flag if field is required or not
     */
    public boolean isFieldRequired(final String fieldName) {
        return _hasReqFields && _requiredFields.contains(fieldName);
    }
    
    /**
     * @return datatype specific field header
     */
    public String[] getHeader() {
        return header;
    }
    
    /**
     *
     * @return datatype specific field separator
     */
    public String getSeparator() {
        return separator;
    }
    
    public boolean skipHeaderRow() {
        return skipHeaderRow;
    }
    
    public boolean processExtraFields() {
        return processExtraFields;
    }
    
    public Map<String,String> getMultiValuedFields() {
        return multiValuedFields;
    }
    
    public Map<String,String> getMultiValuedFieldsBlacklist() {
        return multiValuedFieldsBlacklist;
    }
    
    public boolean usingMultiValuedFieldsBlacklist() {
        return hasMultiValuedFieldsBlacklist;
    }
    
    public boolean isMultiValuedField(String fieldName) {
        return hasMultiValuedFieldsBlacklist ? !multiValuedFieldsBlacklist.containsKey(fieldName) : multiValuedFields.containsKey(fieldName);
    }
    
    public String getMultiValueSeparator() {
        return multiValueSeparator;
    }
    
    /**
     *
     * @return a pattern based on the multivalueseparator value that will not match that value preceeded by a '\\' (backslash) character. Useful as an argument
     *         to the String.split(..) function or similar methods
     */
    public String getEscapeSafeMultiValueSeparatorPattern() {
        return BACKSLASH_ESCAPE_LOOKBEHIND_PATTERN + getMultiValueSeparator();
    }
    
    public int getMultiFieldSizeThreshold() {
        return multiFieldSizeThreshold;
    }
    
    public int getFieldSizeThreshold() {
        return fieldSizeThreshold;
    }
    
    public ThresholdAction getThresholdAction() {
        return thresholdAction;
    }
    
    public String getThresholdReplacement() {
        return thresholdReplacement;
    }
    
    public ThresholdAction getMultiValuedThresholdAction() {
        return multiValuedThresholdAction;
    }
    
    public String getMultiValuedThresholdReplacement() {
        return multiValuedThresholdReplacement;
    }
    
    public String getTruncateField() {
        return truncateField;
    }
    
    public String getDropField() {
        return dropField;
    }
    
    public String getMultiValuedTruncateField() {
        return multiValuedTruncateField;
    }
    
    public String getMultiValuedDropField() {
        return multiValuedDropField;
    }
    
    public Set<String> getFieldBlacklist() {
        return fieldBlacklist;
    }
    
    public Set<String> getFieldWhitelist() {
        return fieldWhitelist;
    }
    
    /**
     * Remove the escape characters from escaped multi value separators in field value
     * 
     * @param fieldValue
     *            the field value to clean
     * @return the cleaned field value
     */
    public String cleanEscapedMultivalueSeparators(String fieldValue) {
        // remove escaped multvalue separators.
        if (fieldValue.contains("\\" + getMultiValueSeparator())) {
            fieldValue = fieldValue.replaceAll("\\\\" + getMultiValueSeparator(), getMultiValueSeparator());
        }
        return fieldValue;
    }
}
