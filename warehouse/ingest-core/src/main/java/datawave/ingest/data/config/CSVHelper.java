package datawave.ingest.data.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

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
    public static final String FIELD_ALLOWLIST = ".data.field.allowlist";

    /**
     * Parameter to explicitly specify a subset of fields that should be removed from the event, all others are kept.
     */
    public static final String FIELD_DISALLOWLIST = ".data.field.disallowlist";

    /**
     * Parameter to specify the fields that are multivalued (allowlist)
     */
    public static final String MULTI_VALUED_FIELDS = ".data.fields.multivalued";

    /**
     * Parameter to specify the fields that are not multivalued (disallowlist)
     */
    public static final String MULTI_VALUED_FIELDS_DISALLOWLIST = ".data.fields.multivalued.disallowlist";

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

    /**
     * Partial configuration key for specifying CSV fields that a record must have.
     */
    public static final String REQUIRED_FIELDS = ".data.fields.required";

    /**
     * Pattern used to prevent matching escaped multivalue field separators when splitting multivalued fields
     */
    public static final String BACKSLASH_ESCAPE_LOOKBEHIND_PATTERN = "(?<!\\\\)";
    private static final String ESCAPE_SAFE_BACKSLASH_SEPARATOR_PATTERN = "(?<!\\\\)\\\\(?=(?:\\\\\\\\)*(?!\\\\))";

    public enum ThresholdAction {
        FAIL, DROP, REPLACE, TRUNCATE
    }

    private static final int DEFAULT_FIELD_SIZE_THRESHOLD = Integer.MAX_VALUE;
    private static final int DEFAULT_MULTI_FIELD_SIZE_THRESHOLD = Integer.MAX_VALUE;
    private static final ThresholdAction DEFAULT_THRESHOLD_ACTION = ThresholdAction.FAIL;
    private static final String DEFAULT_THRESHOLD_REPLACEMENT = "(too large)";
    private static final String DEFAULT_TRUNCATE_FIELD = "TRUNCATED_FIELD";
    private static final String DEFAULT_DROP_FIELD = "DROPPED_FIELD";
    private static final ThresholdAction DEFAULT_MULTI_VALUED_THRESHOLD_ACTION = ThresholdAction.FAIL;
    private static final String DEFAULT_MULTI_VALUED_THRESHOLD_REPLACEMENT = "(too many)";
    private static final String DEFAULT_MULTI_VALUED_TRUNCATE_FIELD = "TRUNCATED_MULTI_VALUED_FIELD";
    private static final String DEFAULT_MULTI_VALUED_DROP_FIELD = "DROPPED_MULTI_VALUED_FIELD";

    private String[] header = null;
    private String separator = null;
    private boolean skipHeaderRow = false;
    private boolean processExtraFields = false;
    private Map<String,String> multiValuedFields = new HashMap<>();
    private Map<String,String> multiValuedFieldsDisallowlist = new HashMap<>();
    private boolean hasMultiValuedFieldsDisallowlist = false;
    private String multiValueSeparator = null;
    private int fieldSizeThreshold = DEFAULT_FIELD_SIZE_THRESHOLD;
    private int multiFieldSizeThreshold = DEFAULT_MULTI_FIELD_SIZE_THRESHOLD;
    private ThresholdAction thresholdAction = DEFAULT_THRESHOLD_ACTION;
    private String thresholdReplacement = DEFAULT_THRESHOLD_REPLACEMENT;
    private String truncateField = DEFAULT_TRUNCATE_FIELD;
    private String dropField = DEFAULT_DROP_FIELD;
    private ThresholdAction multiValuedThresholdAction = DEFAULT_MULTI_VALUED_THRESHOLD_ACTION;
    private String multiValuedThresholdReplacement = DEFAULT_MULTI_VALUED_THRESHOLD_REPLACEMENT;
    private String multiValuedTruncateField = DEFAULT_MULTI_VALUED_TRUNCATE_FIELD;
    private String multiValuedDropField = DEFAULT_MULTI_VALUED_DROP_FIELD;
    private Set<String> fieldDisallowlist = null;
    private Set<String> fieldAllowlist = null;

    /**
     * The Set of field names that a record must have to be valid.
     */
    private Set<String> _requiredFields = null;

    /**
     * Whether or not this CSV-based data format has required fields.
     */
    private boolean _hasReqFields;

    @Override
    public void setup(Configuration config) throws IllegalArgumentException {
        resetSetupState();
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

        // Get the allowlist of event fields to keep.
        Collection<String> cw = config.getStringCollection(this.getType().typeName() + FIELD_ALLOWLIST);
        if (cw != null && !cw.isEmpty()) {
            this.fieldAllowlist = new HashSet<>(cw);
        }

        // Get the disallowlist of event fields to drop.
        Collection<String> cb = config.getStringCollection(this.getType().typeName() + FIELD_DISALLOWLIST);
        if (cb != null && !cb.isEmpty()) {
            this.fieldDisallowlist = new HashSet<>(cb);
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

        // Get the multi-valued fields disallowlist configuration
        if (config.get(this.getType().typeName() + MULTI_VALUED_FIELDS_DISALLOWLIST) != null) {
            for (String field : config.getStrings(this.getType().typeName() + MULTI_VALUED_FIELDS_DISALLOWLIST, new String[0])) {
                int index = field.indexOf(':');
                if (index > 0) {
                    multiValuedFieldsDisallowlist.put(field.substring(0, index), field.substring(index + 1));
                } else {
                    multiValuedFieldsDisallowlist.put(field, field);
                }
            }
            hasMultiValuedFieldsDisallowlist = true;
        }

        // Get the multi-valued fields configuration
        if (!hasMultiValuedFieldsDisallowlist) {
            for (String field : config.getStrings(this.getType().typeName() + MULTI_VALUED_FIELDS, new String[0])) {
                int index = field.indexOf(':');
                if (index > 0) {
                    multiValuedFields.put(field.substring(0, index), field.substring(index + 1));
                } else {
                    multiValuedFields.put(field, field);
                }
            }
        }

        this.multiValueSeparator = getNonEmptyString(config, this.getType().typeName() + MULTI_VALUED_SEPARATOR, ";");

        this.fieldSizeThreshold = getNonNegativeInt(config, this.getType().typeName() + FIELD_SIZE_THRESHOLD, this.fieldSizeThreshold);
        this.thresholdAction = getThresholdAction(config, this.getType().typeName() + THRESHOLD_ACTION, this.thresholdAction);
        this.thresholdReplacement = config.get(this.getType().typeName() + THRESHOLD_FIELD_REPLACEMENT, this.thresholdReplacement);
        this.truncateField = config.get(this.getType().typeName() + TRUNCATE_FIELD, this.truncateField);
        this.dropField = config.get(this.getType().typeName() + DROP_FIELD, this.dropField);

        this.multiFieldSizeThreshold = getNonNegativeInt(config, this.getType().typeName() + MULTI_VALUED_THRESHOLD, this.multiFieldSizeThreshold);
        this.multiValuedThresholdAction = getThresholdAction(config, this.getType().typeName() + MULTI_VALUED_THRESHOLD_ACTION, this.multiValuedThresholdAction);
        this.multiValuedThresholdReplacement = config.get(this.getType().typeName() + MULTI_VALUED_THRESHOLD_FIELD_REPLACEMENT,
                        this.multiValuedThresholdReplacement);
        this.multiValuedTruncateField = config.get(this.getType().typeName() + MULTI_VALUED_TRUNCATE_FIELD, this.multiValuedTruncateField);
        this.multiValuedDropField = config.get(this.getType().typeName() + MULTI_VALUED_DROP_FIELD, this.multiValuedDropField);
    }

    private static int getNonNegativeInt(Configuration config, String key, int defaultValue) {
        int value = config.getInt(key, defaultValue);
        if (value < 0) {
            throw new IllegalArgumentException(key + " must be non-negative: " + value);
        }
        return value;
    }

    private static String getNonEmptyString(Configuration config, String key, String defaultValue) {
        String value = config.get(key, defaultValue);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(key + " must be non-empty");
        }
        return value;
    }

    private static ThresholdAction getThresholdAction(Configuration config, String key, ThresholdAction defaultValue) {
        return ThresholdAction.valueOf(config.get(key, defaultValue.name()).toUpperCase(Locale.ROOT));
    }

    private void resetSetupState() {
        header = null;
        separator = null;
        skipHeaderRow = false;
        processExtraFields = false;
        multiValuedFields = new HashMap<>();
        multiValuedFieldsDisallowlist = new HashMap<>();
        hasMultiValuedFieldsDisallowlist = false;
        multiValueSeparator = null;
        fieldSizeThreshold = DEFAULT_FIELD_SIZE_THRESHOLD;
        multiFieldSizeThreshold = DEFAULT_MULTI_FIELD_SIZE_THRESHOLD;
        thresholdAction = DEFAULT_THRESHOLD_ACTION;
        thresholdReplacement = DEFAULT_THRESHOLD_REPLACEMENT;
        truncateField = DEFAULT_TRUNCATE_FIELD;
        dropField = DEFAULT_DROP_FIELD;
        multiValuedThresholdAction = DEFAULT_MULTI_VALUED_THRESHOLD_ACTION;
        multiValuedThresholdReplacement = DEFAULT_MULTI_VALUED_THRESHOLD_REPLACEMENT;
        multiValuedTruncateField = DEFAULT_MULTI_VALUED_TRUNCATE_FIELD;
        multiValuedDropField = DEFAULT_MULTI_VALUED_DROP_FIELD;
        fieldDisallowlist = null;
        fieldAllowlist = null;
        _requiredFields = null;
        _hasReqFields = false;
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

    public Map<String,String> getMultiValuedFieldsDisallowlist() {
        return multiValuedFieldsDisallowlist;
    }

    public boolean usingMultiValuedFieldsDisallowlist() {
        return hasMultiValuedFieldsDisallowlist;
    }

    public boolean isMultiValuedField(String fieldName) {
        return hasMultiValuedFieldsDisallowlist ? !multiValuedFieldsDisallowlist.containsKey(fieldName) : multiValuedFields.containsKey(fieldName);
    }

    public String getMultiValueSeparator() {
        return multiValueSeparator;
    }

    /**
     * @return a pattern based on the multivalueseparator value that will not match that value preceeded by a '\\' (backslash) character. Useful as an argument
     *         to the String.split(..) function or similar methods
     */
    public String getEscapeSafeMultiValueSeparatorPattern() {
        String separator = getMultiValueSeparator();
        if ("\\".equals(separator)) {
            // Split odd-length backslash runs; even-length runs are escaped separators.
            return ESCAPE_SAFE_BACKSLASH_SEPARATOR_PATTERN;
        }
        return BACKSLASH_ESCAPE_LOOKBEHIND_PATTERN + Pattern.quote(separator);
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

    public Set<String> getFieldDisallowlist() {
        return fieldDisallowlist;
    }

    public Set<String> getFieldAllowlist() {
        return fieldAllowlist;
    }

    /**
     * Remove the escape characters from escaped multi value separators in field value
     *
     * @param fieldValue
     *            the field value to clean
     * @return the cleaned field value
     */
    public String cleanEscapedMultivalueSeparators(String fieldValue) {
        // Remove escaped multivalue separators.
        return fieldValue.replace("\\" + getMultiValueSeparator(), getMultiValueSeparator());
    }
}
