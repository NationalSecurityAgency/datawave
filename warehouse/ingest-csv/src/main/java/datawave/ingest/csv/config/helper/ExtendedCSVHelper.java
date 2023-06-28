package datawave.ingest.csv.config.helper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.config.CSVHelper;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.metadata.id.MetadataIdParser;
import datawave.ingest.validation.EventValidator;

public class ExtendedCSVHelper extends CSVHelper {

    public interface Properties {
        /**
         * Parameter to specify the name of the field that contains the event id. This parameter supports multiple datatypes, for example
         * mydatatype.data.category.id.field
         */
        String EVENT_ID_FIELD_NAME = ".data.category.id.field";
        /**
         * Parameter to specify that the incoming id should be downcased.
         */
        String EVENT_ID_FIELD_DOWNCASE = ".data.category.id.field.downcase";
        /**
         * Comma-delimited list of fields in the header to use as security markings. This parameter supports multiple datatypes, for example
         * mydatatype.data.category.security.field.names. Field name N within this list must be paired with corresponding "domain" entry N within
         * EVENT_SECURITY_MARKING_FIELD_DOMAINS
         */
        String EVENT_SECURITY_MARKING_FIELD_NAMES = ".data.category.security.field.names";
        /**
         * Comma-delimited list of names to use as security marking domains. This parameter supports multiple datatypes, for example
         * mydatatype.data.category.security.field.domains. Domain N within this list must be paired with a corresponding field name entry N within
         * EVENT_SECURITY_MARKING_FIELD_NAMES
         */
        String EVENT_SECURITY_MARKING_FIELD_DOMAINS = ".data.category.security.field.domains";
        /**
         * Property prefix that is used to specify the parsers to use on the event id. This property must specify the datatype at the beginning and the field
         * name at the end. This property supports multiple datatypes, valid values would look like: maydatatype.data.id.parser.EVENT_DATE.1 ,
         * maydatatype.data.id.parser.EVENT_DATE.2, etc.
         */
        String ID_PARSERS_PREFIX = ".data.id.parser.";
        /**
         * Comma-delimited list of fully qualified class names to use to validate an event
         */
        String EVENT_VALIDATORS = ".event.validators";
        /**
         * Comma-delimited list of ignored fields
         */
        String IGNORED_FIELDS = ".data.field.drop";

        String EVENT_DATA_TYPE_FIELD_NAME = ".data.type.field.name";
        String DATA_TYPE_KEYS = ".event.data.type.keys";
        String DATA_TYPE_VALUES = ".event.data.type.values";
    }

    /**
     * Default data type field name
     */
    protected static final String EVENT_DATA_TYPE_PARM = "DATA_TYPE";

    private boolean eventIdDowncase = false;
    private Map<String,String> eventSecurityMarkingFieldDomainMap = new HashMap<>();
    private List<EventValidator> validators = null;
    private String[] ignoredFields = new String[0];
    private String eventDataTypeFieldName = null;
    private Map<String,String> eventDataTypeMap = null;
    private String eventIdFieldName = null;
    private Multimap<String,MetadataIdParser> parsers = HashMultimap.create();

    @Override
    public void setup(Configuration config) throws IllegalArgumentException {
        super.setup(config);

        // If the event id is specified, then we will get the visibilities from it.
        this.eventIdFieldName = config.get(this.getType().typeName() + Properties.EVENT_ID_FIELD_NAME);

        // Should we downcase the id we receive.
        this.eventIdDowncase = config.getBoolean(this.getType().typeName() + Properties.EVENT_ID_FIELD_DOWNCASE, eventIdDowncase);

        // Get the list of security marking field names
        String[] eventSecurityMarkingFieldNames = config.get(this.getType().typeName() + Properties.EVENT_SECURITY_MARKING_FIELD_NAMES, "").split(",");
        // Get the list of security marking field domains
        String[] eventSecurityMarkingFieldDomains = config.get(this.getType().typeName() + Properties.EVENT_SECURITY_MARKING_FIELD_DOMAINS, "").split(",");

        if (eventSecurityMarkingFieldNames.length != eventSecurityMarkingFieldDomains.length) {
            throw new IllegalArgumentException("Both " + this.getType().typeName() + Properties.EVENT_SECURITY_MARKING_FIELD_NAMES + " and "
                            + this.getType().typeName() + Properties.EVENT_SECURITY_MARKING_FIELD_DOMAINS + " must contain the same number of values.");
        }

        for (int i = 0; i < eventSecurityMarkingFieldNames.length; i++) {
            eventSecurityMarkingFieldDomainMap.put(eventSecurityMarkingFieldNames[i], eventSecurityMarkingFieldDomains[i]);
        }

        this.ignoredFields = config.getStrings(this.getType().typeName() + Properties.IGNORED_FIELDS, ignoredFields);

        // Get the list of id parsers from the configuration
        addIdParsers(config, Pattern.compile(this.getType().typeName() + "\\.data\\.id\\.parser\\.(.*)\\..*"));

        validators = ConfigurationHelper.getInstances(config, this.getType().typeName() + Properties.EVENT_VALIDATORS, EventValidator.class);
        for (EventValidator validator : validators) {
            validator.setup(getType(), config);
        }

        this.eventDataTypeFieldName = config.get(this.getType().typeName() + Properties.EVENT_DATA_TYPE_FIELD_NAME, EVENT_DATA_TYPE_PARM);

        String[] eventDataTypeKeys = config.get(this.getType().typeName() + Properties.DATA_TYPE_KEYS, "").split(",");
        String[] eventDataTypeValues = config.get(this.getType().typeName() + Properties.DATA_TYPE_VALUES, "").split(",");

        this.eventDataTypeMap = new HashMap<>();

        if (eventDataTypeKeys.length != eventDataTypeValues.length) {
            throw new IllegalArgumentException("Both " + this.getType().typeName() + Properties.DATA_TYPE_KEYS + " and " + this.getType().typeName()
                            + Properties.DATA_TYPE_VALUES + " must contain the same number of values.");
        }

        for (int i = 0; i < eventDataTypeKeys.length; i++)
            this.eventDataTypeMap.put(eventDataTypeKeys[i].trim(), eventDataTypeValues[i].trim());
    }

    /**
     * @param config
     *            a configuration
     * @param prefixPattern
     *            prefix pattern to match
     */
    protected void addIdParsers(Configuration config, Pattern prefixPattern) {
        for (Map.Entry<String,String> entry : config) {
            Matcher m = prefixPattern.matcher(entry.getKey());
            if (m.matches()) {
                String fieldName = m.group(1);
                parsers.put(fieldName, MetadataIdParser.createParser(entry.getValue()));
            }
        }
    }

    public String getEventIdFieldName() {
        return eventIdFieldName;
    }

    public boolean getEventIdDowncase() {
        return eventIdDowncase;
    }

    protected void setEventIdFieldName(String fieldName) {
        eventIdFieldName = fieldName;
    }

    protected void setEventIdDowncase(boolean idDowncase) {
        eventIdDowncase = idDowncase;
    }

    public Multimap<String,MetadataIdParser> getParsers() {
        return parsers;
    }

    public static String expandFieldValue(String fieldValue) {
        // We replace new lines and carriage returns with \\~n~
        if (fieldValue.contains("\\~n~")) {
            fieldValue = fieldValue.replaceAll("\\\\~n~", "\n");
        }
        // We escape quotation marks
        if (fieldValue.contains("\"\"")) {
            fieldValue = fieldValue.replaceAll("\"\"", "\"");
        }

        return fieldValue;
    }

    public String[] getIgnoredFields() {
        return ignoredFields;
    }

    public List<EventValidator> getValidators() {
        return validators;
    }

    /**
     * Lowercase MD5,SHA1,SHA256 but do *not* remove any whitespace as CSVHelper does.
     *
     * @param fieldName
     *            the name of the field to clean
     * @param fieldValue
     *            the value to clean
     * @return the cleaned field, null if the field value is empty.
     */
    @Override
    public String clean(String fieldName, String fieldValue) {
        if (StringUtils.isEmpty(fieldValue)) {
            return null;
        }

        if (fieldName.equalsIgnoreCase("md5") || fieldName.equalsIgnoreCase("sha1") || fieldName.equalsIgnoreCase("sha256")) {
            fieldValue = fieldValue.toLowerCase();
        }

        return fieldValue;
    }

    public Map<String,String> getSecurityMarkingFieldDomainMap() {
        return Collections.unmodifiableMap(eventSecurityMarkingFieldDomainMap);
    }

    public String getEventDataTypeFieldName() {
        return eventDataTypeFieldName;
    }
}
