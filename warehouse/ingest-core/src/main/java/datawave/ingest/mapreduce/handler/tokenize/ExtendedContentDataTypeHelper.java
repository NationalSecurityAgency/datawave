package datawave.ingest.mapreduce.handler.tokenize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.CSVHelper.ThresholdAction;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.metadata.id.MetadataIdParser;
import datawave.ingest.validation.EventValidator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class ExtendedContentDataTypeHelper extends DataTypeHelperImpl {
    
    public interface Properties {
        String REVERSE_INDEXING = ".reverse.indexing";
        
        String KEY_METADATA_PARSERS = ".id.metadata";
        
        String FIELD_METADATA_PARSERS = ".field.metadata";
        
        String FIELD_UUID_METADATA_PARSERS = ".field.metadata.uuid";
        
        String UUID_METADATA = ".uuids";
        
        String EXPECT_UUID = ".expect.uuid";
        
        String IGNORED_FIELDS = ".data.field.drop";
        
        String INCLUDE_CONTENT = ".include.full.content";
        
        String VALID_METADATA_KEY_REGEX = ".valid.metadata.key.regex";
        String DEFAULT_KEY_REGEX = "[A-Za-z0-9_-]{2,50}+";
        
        String METADATA_TOKENIZE_SKIP = ".tokenizer.skip.metadata.fields";
        
        String INHERITED_PAYLOAD_FIELDS = ".inherited.payload.fields";
        
        String USE_TOKEN_OFFSET_CACHE = ".use.token.offset.cache";
        /**
         * Comma-delimited list of fields in the header to use as security markings. This parameter supports multiple datatypes, for example
         * mydatatype.data.category.security.field.names. Field name N within this list must be paired with corresponding "domain" entry N within
         * {@link EVENT_SECURITY_MARKING_FIELD_DOMAINS}
         */
        String EVENT_SECURITY_MARKING_FIELD_NAMES = ".data.category.security.field.names";
        /**
         * Comma-delimited list of names to use as security marking domains. This parameter supports multiple datatypes, for example
         * mydatatype.data.category.security.field.domains. Domain N within this list must be paired with a corresponding field name entry N within
         * {@link EVENT_SECURITY_MARKING_FIELD_NAMES}
         */
        String EVENT_SECURITY_MARKING_FIELD_DOMAINS = ".data.category.security.field.domains";
        
        String EVENT_VALIDATORS = ".event.validators";
        
        String SESSION_METADATA_PROPAGATION_ENABLED = ".session.metadata.propagation.enabled";
        
        String SESSION_METADATA_PROPAGATION_BLACKLIST = ".session.metadata.propagation.blacklist";
        
        String SESSION_METADATA_PROPAGATION_WHITELIST = ".session.metadata.propagation.whitelist";
        /**
         * Parameter to specify the fields that are multivalued
         */
        String MULTI_VALUED_FIELDS = CSVHelper.MULTI_VALUED_FIELDS;
        /**
         * Parameter to specify the separator for multivalued fields (detault is ';');
         */
        String MULTI_VALUED_SEPARATOR = CSVHelper.MULTI_VALUED_SEPARATOR;
        /**
         * Parameter to specify the a threshold on the number of fields in a multi-valued field
         */
        String MULTI_VALUED_THRESHOLD = CSVHelper.MULTI_VALUED_THRESHOLD;
        /**
         * Parameter to specify what to do when a field size or multi-valued field threshold is reached
         */
        String MULTI_VALUED_THRESHOLD_ACTION = CSVHelper.MULTI_VALUED_THRESHOLD_ACTION;
        /**
         * Parameter to specify the threshold field replacement when the threshold action is to replace
         */
        String MULTI_VALUED_THRESHOLD_FIELD_REPLACEMENT = CSVHelper.MULTI_VALUED_THRESHOLD_FIELD_REPLACEMENT;
        /**
         * Parameter to specify the threshold on the size of a field processed.
         */
        String FIELD_SIZE_THRESHOLD = CSVHelper.FIELD_SIZE_THRESHOLD;
        /**
         * Parameter to specify what to do when a field size or multi-valued field threshold is reached
         */
        String THRESHOLD_ACTION = CSVHelper.THRESHOLD_ACTION;
        /**
         * Parameter to specify the threshold field replacement when the threshold action is to replace
         */
        String THRESHOLD_FIELD_REPLACEMENT = CSVHelper.THRESHOLD_FIELD_REPLACEMENT;
        /**
         * Parameter to specify the field to add to an event to denote fields that were truncated
         */
        String TRUNCATE_FIELD = CSVHelper.TRUNCATE_FIELD;
        /**
         * Parameter to specify the field to add to an event to denote fields that were dropped
         */
        String DROP_FIELD = CSVHelper.DROP_FIELD;
        /**
         * Parameter to specify the field to add to an event to denote fields that were truncated
         */
        String MULTI_VALUED_TRUNCATE_FIELD = CSVHelper.MULTI_VALUED_TRUNCATE_FIELD;
        /**
         * Parameter to specify the field to add to an event to denote fields that were dropped
         */
        String MULTI_VALUED_DROP_FIELD = CSVHelper.MULTI_VALUED_DROP_FIELD;
        
    }
    
    private boolean reverseIndexing = false;
    private List<MetadataIdParser> metadataKeyParsers = new ArrayList<>();
    private Multimap<String,MetadataIdParser> metadataFieldParsers = HashMultimap.create();
    private Multimap<String,MetadataIdParser> metadataFieldUuidParsers = HashMultimap.create();
    private Set<String> uuidMetadata = new HashSet<>();
    private boolean expectUuid = true;
    private String[] ignoredFields = new String[0];
    private boolean includeContent = true;
    private Pattern validMetadataKey = null;
    private Set<String> skipTokenizeMetadataFields = new HashSet<>();
    private Set<String> inheritedFields = new HashSet<>();
    // default is to use the token offset cache which reduces the number of
    // mutations (BulkIngestKeys) generated
    private boolean useTokenOffsetCache = true;
    private Map<String,String> eventSecurityMarkingFieldDomainMap = new HashMap<>();
    private List<EventValidator> validators = null;
    private boolean sessionMetadataPropagationEnabled = false;
    private Set<String> sessionMetadataPropagationBlacklist = new HashSet<>();
    private Set<String> sessionMetadataPropagationWhitelist = new HashSet<>();
    private Map<String,String> multiValuedFields = new HashMap<>();
    private String multiValueSeparator = null;
    private int multiFieldSizeThreshold = -1;
    private ThresholdAction multiValuedThresholdAction = ThresholdAction.FAIL;
    private String multiValuedThresholdReplacement = "(too many)";
    private int fieldSizeThreshold = -1;
    private ThresholdAction thresholdAction = ThresholdAction.FAIL;
    private String thresholdReplacement = "(too large)";
    private String truncateField = "TRUNCATED_FIELD";
    private String dropField = "DROPPED_FIELD";
    private String multiValuedTruncateField = "TRUNCATED_MULTI_VALUED_FIELD";
    private String multiValuedDropField = "DROPPED_MULTI_VALUED_FIELD";
    
    public ExtendedContentDataTypeHelper() {
        this("extcontent");
    }
    
    private String dataType;
    
    public ExtendedContentDataTypeHelper(String dataType) {
        this.dataType = dataType;
    }
    
    @Override
    public void setup(Configuration conf) throws IllegalArgumentException {
        super.setup(conf);
        reverseIndexing = conf.getBoolean(this.getType().typeName() + Properties.REVERSE_INDEXING, reverseIndexing);
        expectUuid = conf.getBoolean(this.getType().typeName() + Properties.EXPECT_UUID, expectUuid);
        includeContent = conf.getBoolean(this.getType().typeName() + Properties.INCLUDE_CONTENT, includeContent);
        validMetadataKey = Pattern.compile(conf.get(this.getType().typeName() + Properties.VALID_METADATA_KEY_REGEX, Properties.DEFAULT_KEY_REGEX));
        ignoredFields = conf.getStrings(this.getType().typeName() + Properties.IGNORED_FIELDS, ignoredFields);
        useTokenOffsetCache = conf.getBoolean(this.getType().typeName() + Properties.USE_TOKEN_OFFSET_CACHE, useTokenOffsetCache);
        fieldSizeThreshold = conf.getInt(this.getType().typeName() + Properties.FIELD_SIZE_THRESHOLD, fieldSizeThreshold);
        multiFieldSizeThreshold = conf.getInt(this.getType().typeName() + Properties.MULTI_VALUED_THRESHOLD, multiFieldSizeThreshold);
        thresholdAction = ThresholdAction.valueOf(conf.get(this.getType().typeName() + Properties.THRESHOLD_ACTION, thresholdAction.name()).toUpperCase());
        thresholdReplacement = conf.get(this.getType().typeName() + Properties.THRESHOLD_FIELD_REPLACEMENT, thresholdReplacement);
        truncateField = conf.get(this.getType().typeName() + Properties.TRUNCATE_FIELD, truncateField);
        dropField = conf.get(this.getType().typeName() + Properties.DROP_FIELD, dropField);
        multiValuedThresholdAction = ThresholdAction.valueOf(
                        conf.get(this.getType().typeName() + Properties.MULTI_VALUED_THRESHOLD_ACTION, multiValuedThresholdAction.name()).toUpperCase());
        multiValuedThresholdReplacement = conf.get(this.getType().typeName() + Properties.MULTI_VALUED_THRESHOLD_FIELD_REPLACEMENT,
                        multiValuedThresholdReplacement);
        multiValuedTruncateField = conf.get(this.getType().typeName() + Properties.MULTI_VALUED_TRUNCATE_FIELD, multiValuedTruncateField);
        multiValuedDropField = conf.get(this.getType().typeName() + Properties.MULTI_VALUED_DROP_FIELD, multiValuedDropField);
        validators = ConfigurationHelper.getInstances(conf, this.getType().typeName() + Properties.EVENT_VALIDATORS, EventValidator.class);
        for (EventValidator validator : validators) {
            validator.setup(getType(), conf);
        }
        
        String prefix = this.getType().typeName() + Properties.KEY_METADATA_PARSERS;
        Map<String,String> keyParsers = getValues(conf, prefix);
        for (String value : keyParsers.values()) {
            metadataKeyParsers.add(MetadataIdParser.createParser(value));
        }
        
        this.putMetadataParsers(conf, Properties.FIELD_METADATA_PARSERS, metadataFieldParsers);
        this.putMetadataParsers(conf, Properties.FIELD_UUID_METADATA_PARSERS, metadataFieldUuidParsers);
        
        uuidMetadata.addAll(conf.getStringCollection(this.getType().typeName() + Properties.UUID_METADATA));
        
        inheritedFields.addAll(conf.getStringCollection(this.getType().typeName() + Properties.INHERITED_PAYLOAD_FIELDS));
        
        // Get the multi-valued fields configuration
        for (String field : conf.getStrings(this.getType().typeName() + Properties.MULTI_VALUED_FIELDS, new String[0])) {
            int index = field.indexOf(':');
            if (index > 0) {
                multiValuedFields.put(field.substring(0, index), field.substring(index + 1));
            } else {
                multiValuedFields.put(field, field);
            }
        }
        this.multiValueSeparator = conf.get(this.getType().typeName() + Properties.MULTI_VALUED_SEPARATOR, ";");
        
        // Get the list of metadata fields we should not tokenize
        for (String field : conf.getStrings(this.getType().typeName() + Properties.METADATA_TOKENIZE_SKIP, new String[0])) {
            this.skipTokenizeMetadataFields.add(field.trim());
        }
        
        sessionMetadataPropagationEnabled = conf.getBoolean(this.getType().typeName() + Properties.SESSION_METADATA_PROPAGATION_ENABLED,
                        sessionMetadataPropagationEnabled);
        
        for (String field : conf.getStrings(this.getType().typeName() + Properties.SESSION_METADATA_PROPAGATION_BLACKLIST, new String[0])) {
            this.sessionMetadataPropagationBlacklist.add(field.trim());
        }
        
        for (String field : conf.getStrings(this.getType().typeName() + Properties.SESSION_METADATA_PROPAGATION_WHITELIST, new String[0])) {
            this.sessionMetadataPropagationWhitelist.add(field.trim());
        }
        
        // Get the list of security marking field names
        String[] eventSecurityMarkingFieldNames = conf.get(this.getType().typeName() + Properties.EVENT_SECURITY_MARKING_FIELD_NAMES, "").split(",");
        // Get the list of security marking field domains
        String[] eventSecurityMarkingFieldDomains = conf.get(this.getType().typeName() + Properties.EVENT_SECURITY_MARKING_FIELD_DOMAINS, "").split(",");
        
        if (eventSecurityMarkingFieldNames.length != eventSecurityMarkingFieldDomains.length) {
            throw new IllegalArgumentException("Both " + this.getType().typeName() + Properties.EVENT_SECURITY_MARKING_FIELD_NAMES + " and "
                            + this.getType().typeName() + Properties.EVENT_SECURITY_MARKING_FIELD_DOMAINS + " must contain the same number of values.");
        }
        
        for (int i = 0; i < eventSecurityMarkingFieldNames.length; i++) {
            eventSecurityMarkingFieldDomainMap.put(eventSecurityMarkingFieldNames[i], eventSecurityMarkingFieldDomains[i]);
        }
    }
    
    public boolean isSessionMetadataPropagationEnabled() {
        return sessionMetadataPropagationEnabled;
    }
    
    public boolean sessionMetadataShouldPropagate(String field, Collection<Object> value) {
        if (this.sessionMetadataPropagationEnabled) {
            if (!this.sessionMetadataPropagationBlacklist.isEmpty()) {
                return !this.sessionMetadataPropagationBlacklist.contains(field);
            } else if (!this.sessionMetadataPropagationWhitelist.isEmpty()) {
                return this.sessionMetadataPropagationWhitelist.contains(field);
            }
            
            return true;
        }
        return false;
    }
    
    /**
     * Populates the provided metadata parser map from the configuration. This allows different metadata parser maps to be populated based on different
     * configuration keys.
     * 
     * @param conf
     *            Configuration
     * @param keySuffix
     *            The configuration key suffix
     * @param metadataParserMap
     *            The metadata parser map to populate.
     */
    protected void putMetadataParsers(Configuration conf, String keySuffix, Multimap<String,MetadataIdParser> metadataParserMap) {
        String prefix;
        Map<String,String> keyParsers;
        prefix = this.getType().typeName() + keySuffix;
        keyParsers = getValues(conf, prefix);
        for (Map.Entry<String,String> entry : keyParsers.entrySet()) {
            String field = entry.getKey();
            if (field.length() <= (prefix.length() + 1)) {
                throw new IllegalArgumentException("Field metadata parser key is invalid: missing fieldname: " + field);
            }
            field = field.substring(prefix.length() + 1);
            int index = field.indexOf('.');
            if (index >= 0) {
                field = field.substring(0, index);
            }
            if (field.isEmpty()) {
                throw new IllegalArgumentException("Field metadata parser key is invalid: missing fieldname: " + field);
            }
            metadataParserMap.put(field, MetadataIdParser.createParser(entry.getValue()));
        }
    }
    
    protected Map<String,String> getValues(Configuration conf, String prefix) {
        Map<String,String> values = new HashMap<>();
        for (Map.Entry<String,String> entry : conf) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                // if the property is longer than the prefix, then ensure the
                // next character is a '.'
                if (key.length() == prefix.length() || key.charAt(prefix.length()) == '.') {
                    values.put(key, entry.getValue());
                }
            }
        }
        return values;
    }
    
    public boolean isReverseIndexing() {
        return reverseIndexing;
    }
    
    public boolean useTokenOffsetCache() {
        return useTokenOffsetCache;
    }
    
    public List<MetadataIdParser> getMetadataKeyParsers() {
        return metadataKeyParsers;
    }
    
    public Multimap<String,MetadataIdParser> getMetadataFieldUuidParsers() {
        return metadataFieldUuidParsers;
    }
    
    public Multimap<String,MetadataIdParser> getMetadataFieldParsers() {
        return metadataFieldParsers;
    }
    
    public Set<String> getUuids() {
        return uuidMetadata;
    }
    
    public Set<String> getInheritedPayloadFields() {
        return inheritedFields;
    }
    
    public boolean expectUuid() {
        return expectUuid;
    }
    
    public boolean includeContent() {
        return includeContent;
    }
    
    public boolean isValidMetadataKey(String key) {
        return validMetadataKey.matcher(key).matches();
    }
    
    public Map<String,String> getMultiValuedFields() {
        return multiValuedFields;
    }
    
    public String getMultiValueSeparator() {
        return multiValueSeparator;
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
    
    public Set<String> getSkipTokenizeMetadataFields() {
        return skipTokenizeMetadataFields;
    }
    
    /**
     * Since this helper may be used in the RecordReader during ingest, all configs will be loaded which means that the data.name value is actually random. We
     * need to override getType to return the correct one.
     * 
     * @return the data type
     */
    @Override
    public Type getType() {
        return TypeRegistry.getType(dataType);
    }
    
    /**
     * Lowercase MD5,SHA1,SHA256 but do *not* remove any whitespace
     * 
     * @param fieldName
     *            the field name
     * @param fieldValue
     *            the field value
     * @return a string of the field value
     */
    public String clean(String fieldName, String fieldValue) {
        if (StringUtils.isEmpty(fieldValue)) {
            return null;
        }
        if (fieldName.equalsIgnoreCase("md5") || fieldName.equalsIgnoreCase("sha1") || fieldName.equalsIgnoreCase("sha256"))
            return fieldValue.toLowerCase();
        return fieldValue;
    }
    
    public String[] getIgnoredFields() {
        return ignoredFields;
    }
    
    public List<EventValidator> getValidators() {
        return validators;
    }
    
    public Map<String,String> getSecurityMarkingFieldDomainMap() {
        return eventSecurityMarkingFieldDomainMap;
    }
}
