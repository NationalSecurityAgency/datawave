package datawave.query.function;

import static datawave.query.Constants.FIELD_INDEX_PREFIX;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import datawave.data.hash.UID;
import datawave.data.type.NumberType;
import datawave.data.type.util.NumericalEncoder;
import datawave.query.attributes.Attribute;
import datawave.query.iterator.SourcedOptions;
import datawave.query.attributes.Document;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

/**
 * This filter validates events in the shard table based on their association with one or more configured data types and number-normalized "version" fields
 * (named NORMALIZED_VERSION, by default). One of its more straightforward use cases is to answer the question, "Is this the most current version of an event?"
 * In other words, it determines whether or not a given Key is associated with a higher-level event's most current version, if applicable. As an abstract class,
 * this filter can be extended and used by Predicate or Function implementations, such as the {@link NormalizedVersionPredicate}.
 * <p>
 * So what is a "version"? A version is simply a meaningful position of a child event under some parent event in the shard table. Instead of treating child
 * events located at a "versioned" level as equals, each child is regarded with some kind of precedence with respect to its siblings. Version 3 typically has a
 * higher order of precedence, for example, than versions 1 and 2. In many cases, a versioned event and its descendants may have entirely different timestamps
 * and visibilities than other versioned events, having been inserted under the parent days, weeks, or even months later than previously inserted columns. Among
 * other things, they may stand to get purged at different times under different conditions than sibling events.
 * <p>
 * The {@link AbstractVersionFilter} helps to discriminate between normal and versioned child events by being injected into the framework as one of the
 * "filterClassNames" specified by a ShardQueryLogic bean. Once instantiated and properly configured, each call to the <i>apply(..)</i> method checks the Key
 * for a column family value prefixed with a matching data type. If a match is not made, the method ignores and quickly returns the original Key. If a match is
 * made, however, the same column family is checked for a "versionized" UID pattern appended to the data type, such as "datatype\x00kir5i4.tf9ozi.-ji6i29.1",
 * which is an example of the default case that assumes versions are simply the first-level children of a given data type's top-level events.
 * <p>
 * More complex and deeper level UID patterns can be configured, but any pattern is expected to correlate with a number-normalized field in the field-index (fi)
 * section of the shard. For each matching UID pattern, the filter will attempt to scan the shard in order to fully validate the Key. The default and most basic
 * form of validation is to ensure no greater versions are found, but other types of validation may be added in the future, such as ranges of versions specified
 * by namespace-registered JEXL functions.
 * <p>
 * The minimum "filter options" configuration must include at least one data type defined by a <i>normalized.version.filter.types</i> key, or multiple data
 * types separated by commas. A specialized UID regex pattern and/or field-index (fi) fieldname may also be specified for one or more of the data types, as in
 * the following example (notice that only datatype2 is configured with a specially defined UID pattern and fieldname):
 * 
 * <pre>
 * {@code
 *    <util:map id="NormalizedVersionFilterConfiguration">
 *       <entry key="normalized.version.filter.types" value="datatype1, datatype2"/>
 *       <entry key="normalized.version.filter.uid.pattern.datatype2" value="(\-?+[a-z0-9]*\.\-?+[a-z0-9]*\.\-?+[a-z0-9]*)((\+[a-z0-9]*)?+)(\.\d++)(\.\d++)"/>
 *       <entry key="normalized.version.filter.fieldname.datatype2" value="MY_CUSTOM_VERSION_FIELD"/>
 *    </util:map>
 * }
 * </pre>
 * <p>
 * The filter may also be configured with a JSON-formatted collection or array of one or more data types, plus optional mappings to specialized UID version
 * patterns and/or field names. If any one of the items in the JSON-deserialized configuration is simply a data type, it will be mapped to the default UID reqex
 * pattern and fi fieldname. Either way, any JSON-based configuration must be provided from a filter option identified by the
 * <i>normalized.version.filter.json</i> key. The following example shows how Spring might be used to automatically convert a list of mixed-type mappings into
 * JSON for injection as a single string value into a query logic bean:
 * 
 * <pre>
 * {@code
 *    <bean id="jsonifiedFilterOptions" class="org.springframework.beans.factory.config.MethodInvokingFactoryBean">
 *     <property name="targetObject">
 *       <bean class="com.google.gson.Gson"/>
 *     </property>
 *     <property name="targetMethod" value="toJson"/>
 *     <property name="arguments">
 *       <list value-type="java.lang.Object">
 *         <value>datatype1</value>
 *         <map key-type="java.lang.String" value-type="java.lang.String">
 *           <entry key="normalized.version.filter.datatype" value="datatype2"/>
 *           <entry key="normalized.version.filter.uid.pattern" value="(\-?+[a-z0-9]*\.\-?+[a-z0-9]*\.\-?+[a-z0-9]*)((\+[a-z0-9]*)?+)(\.\d++)(\.\d++)"/>
 *           <entry key="normalized.version.filter.fieldname" value="SOME_CUSTOM_VERSION_FIELD"/>
 *         </map>
 *       </list>  
 *     </property>
 *   </bean>
 * }
 * </pre>
 * 
 * <b>Note:</b> A working JSON example is provided in the application context XML file provided for the unit test of this class.
 */
public abstract class AbstractVersionFilter<A> {
    
    /**
     * The QueryOptions lookup key for patterns used to identify which data types and UIDs with which to filter events based on an associated "normalized
     * version" field. Such a field is expected to
     * <p>
     * The DEFAULT_VERSION_PATTERN, for example, ...
     * 
     */
    public static final String NORMALIZED_VERSION_FILTER_ROOT_KEY = "normalized.version.filter";
    
    private static final String DATATYPES_DELIMITER = ",";
    
    /**
     * The default field name defining an event's version, which is assumed to have a fi entry containing a number-normalized value
     */
    public static final String DEFAULT_VERSION_FIELDNAME = "NORMALIZED_VERSION";
    
    /**
     * The default regular expression used to identify the highest level that defines a "versionized" UID in a tree of sharded events. The default expression
     * would match the following example as the 9th version of some top-level event: "kir5i4.tf9ozi.-ji6i29.9".
     */
    public static final String DEFAULT_UID_VERSION_PATTERN = "(\\-?+[a-z0-9]*\\.\\-?+[a-z0-9]*\\.\\-?+[a-z0-9]*)((\\+[a-z0-9]*)?+)(\\.\\d++)";
    
    private static final Text DOCUMENT_COLUMN = new Text("d");
    private static final Text FI_COLUMN = new Text("fi");
    private static final byte[] FI_COLUMN_BYTES = FI_COLUMN.getBytes();
    
    public static final String KEY_JSON = NORMALIZED_VERSION_FILTER_ROOT_KEY + ".json";
    public static final String KEY_DATA_TYPE = NORMALIZED_VERSION_FILTER_ROOT_KEY + ".datatype";
    public static final String KEY_DATA_TYPES = NORMALIZED_VERSION_FILTER_ROOT_KEY + ".datatypes";
    public static final String KEY_UID_VERSION_PATTERN = NORMALIZED_VERSION_FILTER_ROOT_KEY + ".uid.pattern";
    public static final String KEY_VERSION_FIELDNAME = NORMALIZED_VERSION_FILTER_ROOT_KEY + ".fieldname";
    
    private static final Logger LOG = Logger.getLogger(AbstractVersionFilter.class);
    private static final NumberType NUMBER_NORMALIZER = new VersionNumberType();
    private static final String NUMBER_NORMALIZER_MAX_VALUE = NUMBER_NORMALIZER.normalize(Integer.toString(Integer.MAX_VALUE));
    private static final Text TF_COLUMN = new Text("tf");
    
    private static boolean USE_PARENT_DENORMALIZER = true;
    
    private Map<String,String> dataTypesToFieldNames = Collections.emptyMap(); // Used in cases where multiple versioned data types are defined
    private Map<String,String> dataTypesToPatterns = Collections.emptyMap(); // Used in cases where multiple versioned data types are defined
    private String dataType; // Optimizes the apply(..) method in cases where only one data type is defined
    private IteratorEnvironment environment;
    private String fieldName; // Optimizes the apply(..) method in cases where only one fieldname is defined
    private boolean isConfigured; // Optimizes the apply(..) method in cases of improper or incomplete configuration
    private SortedKeyValueIterator<Key,Value> originalSource;
    private String pattern; // Optimizes the apply(..) method in cases where only one pattern is defined
    private SortedKeyValueIterator<Key,Value> source;
    
    /**
     * Validates the applied input. If valid, the input is returned unaltered. If not valid, the returned value will be a null value or modified form of the
     * input, as applicable.
     * 
     * @param input
     * @param forceNullIfInvalid
     * @return
     */
    @SuppressWarnings("unchecked")
    protected A apply(final A input, boolean forceNullIfInvalid) {
        // Assume nothing will be filtered
        A to = input;
        
        // Check for valid configuration and input before proceeding
        if (isConfigured && (null != input)) {
            // Declare the key and document so they can be assigned only once for sake of efficiency, whether null or not
            final Key key;
            final Document document;
            
            // Get the column family and the index of the datatype/uid delimiter, if any.
            if (input instanceof Entry<?,?>) {
                Object object = ((Entry<Object,?>) input).getKey();
                if (object instanceof Key) {
                    key = (Key) object;
                    object = ((Entry<Object,?>) input).getValue();
                    if (object instanceof Document) {
                        document = (Document) object;
                    } else {
                        document = null;
                    }
                } else {
                    key = null;
                    document = null;
                }
            } else if (input instanceof Key) {
                key = (Key) input;
                document = null;
            } else {
                key = null;
                document = null;
            }
            
            // At least some kind of key would probably exist, but check for it anyway
            // before proceeding
            if (null != key) {
                // Extract the column family and check for a prefix delimited by a null character, avoiding
                // document (d) and term frequency (tf) columns.
                //
                // Note: This sort of character comparison and iteration was lifted from the DataTypeFilter,
                // presumably as a more efficient means than expensive java.lang.String operations.
                final Text cfText = key.getColumnFamily();
                byte[] dataTypeUidBytes = cfText.getBytes();
                int nullIndex = -1;
                if (!DOCUMENT_COLUMN.equals(cfText) && !TF_COLUMN.equals(cfText)) {
                    for (int i = 0; i < dataTypeUidBytes.length - 1; i++) {
                        if (dataTypeUidBytes[i] == '\0') {
                            nullIndex = i;
                            break;
                        }
                    }
                }
                
                // If the index looks good at this point, check for a qualifying data type and UID pattern
                if (nullIndex > 0) {
                    // Declare the data type to be assigned only once for sake of efficiency
                    final String dataType;
                    
                    // Extract the data type from the fi column's column qualifier
                    //
                    // Note: Again, this sort of character comparison and iteration was lifted from the DataTypeFilter,
                    // presumably as a more efficient means than expensive java.lang.String operations.
                    if (WritableComparator.compareBytes(dataTypeUidBytes, 0, nullIndex, FI_COLUMN_BYTES, 0, FI_COLUMN_BYTES.length) == 0) {
                        // Look for the data type and Uid portions of the column qualifier
                        final Text cqText = key.getColumnQualifier();
                        byte[] cqBytes = cqText.getBytes();
                        int fiUidNullIndex = -1;
                        int fiDataTypeNullIndex = -1;
                        for (int i = cqBytes.length - 1; i >= 0; i--) {
                            if (cqBytes[i] == '\0') {
                                if (fiUidNullIndex < 0) {
                                    fiUidNullIndex = i;
                                } else {
                                    fiDataTypeNullIndex = i;
                                    break;
                                }
                            }
                        }
                        
                        // Indices found, so reassign the byte array and null index based on the fi column family
                        if ((fiDataTypeNullIndex >= 0) && (fiUidNullIndex > fiDataTypeNullIndex)) {
                            dataType = new String(cqBytes, fiDataTypeNullIndex + 1, (fiUidNullIndex - fiDataTypeNullIndex) - 1);
                            dataTypeUidBytes = cqBytes;
                            nullIndex = fiUidNullIndex;
                        }
                        // Unexpected condition, so just quietly make the data type unusable
                        else {
                            dataType = StringUtils.EMPTY;
                        }
                    }
                    // Grab non-field index bytes (those expected from a standard shard event column)
                    else {
                        dataType = new String(dataTypeUidBytes, 0, nullIndex);
                    }
                    
                    // The data type's UID regex pattern, if applicable
                    final String pattern;
                    boolean isMultiMapping;
                    if ((null != this.dataType) && dataType.equals(this.dataType)) {
                        pattern = this.pattern;
                        isMultiMapping = false;
                    } else {
                        pattern = dataTypesToPatterns.get(dataType);
                        isMultiMapping = true;
                    }
                    
                    // If a qualifying pattern exists, extract and validate the UID. An invalid UID should return
                    // return null if specified
                    if (null != pattern) {
                        final String uid = new String(dataTypeUidBytes, nullIndex + 1, (dataTypeUidBytes.length - nullIndex) - 1);
                        if (!this.validate(dataType, uid, pattern, key, document, isMultiMapping)) {
                            if (forceNullIfInvalid || (null == document)) {
                                to = null;
                            }
                        }
                    }
                }
            }
        }
        
        return to;
    }
    
    private Map<String,Map<String,String>> buildMappedConfigs(final String dataTypes, final Map<String,String> options) {
        Map<String,Map<String,String>> mappedConfigs = Collections.emptyMap();
        for (final String dataType : dataTypes.split(DATATYPES_DELIMITER)) {
            // Fill in default values, if applicable, and add the config
            final String trimmedLowerCase = dataType.trim().toLowerCase();
            if ((null != trimmedLowerCase) && !trimmedLowerCase.isEmpty()) {
                // Create the map
                final Map<String,String> dataTypeConfig = new HashMap<>();
                if (mappedConfigs.isEmpty()) {
                    mappedConfigs = new HashMap<>();
                }
                
                // Fill in the default version pattern
                String key = KEY_UID_VERSION_PATTERN + '.' + trimmedLowerCase;
                String value = options.get(key);
                if ((null == value) || value.trim().isEmpty()) {
                    dataTypeConfig.put(KEY_UID_VERSION_PATTERN, DEFAULT_UID_VERSION_PATTERN);
                } else {
                    dataTypeConfig.put(KEY_UID_VERSION_PATTERN, value);
                }
                
                // Fill in the default file name
                key = KEY_VERSION_FIELDNAME + '.' + trimmedLowerCase;
                value = options.get(key);
                if ((null == value) || value.trim().isEmpty()) {
                    dataTypeConfig.put(KEY_VERSION_FIELDNAME, DEFAULT_VERSION_FIELDNAME);
                } else {
                    dataTypeConfig.put(KEY_VERSION_FIELDNAME, value);
                }
                
                // Add the config
                mappedConfigs.put(trimmedLowerCase, dataTypeConfig);
            } else {
                final String message = "Invalid " + AbstractVersionFilter.class.getSimpleName() + " config for data type: " + trimmedLowerCase;
                final IllegalArgumentException e = new IllegalArgumentException(message);
                LOG.error("Could not initialize config for " + AbstractVersionFilter.class.getSimpleName(), e);
            }
        }
        
        return mappedConfigs;
    }
    
    /*
     * Build the datatype-to-config mappings, if any, from the value passed from the QueryOptions
     * 
     * @param jsonFormattedConfigs The [presumably] json-formatted filter configuration
     * 
     * @return datatype-to-config mappings
     */
    @SuppressWarnings("unchecked")
    private Map<String,Map<String,String>> buildJsonFormattedConfigs(final String jsonFormattedConfigs) {
        Map<String,Map<String,String>> mappedConfigs = Collections.emptyMap();
        
        try {
            // Extract and deserialize the specified JSON string. Such configs may be structured
            // within a list or a single map. See the javadoc and unit tests for more details.
            final Gson gson = new Gson();
            Collection<Object> unmappedConfigs = Collections.emptyList();
            try {
                unmappedConfigs = gson.fromJson(jsonFormattedConfigs, Collection.class);
            } catch (final Exception e) {
                final Object singleConfig = gson.fromJson(jsonFormattedConfigs, Map.class);
                unmappedConfigs = new ArrayList<>(1);
                unmappedConfigs.add(singleConfig);
            }
            
            // Map the configs to data types. Right now it's just mapping UID version patterns
            // and fi fieldnames. Future mappings may include parameters for more complex version
            // validation.
            if (null != unmappedConfigs && !unmappedConfigs.isEmpty()) {
                mappedConfigs = new HashMap<>(unmappedConfigs.size());
                for (final Object object : unmappedConfigs) {
                    // Extract the data type and overall config
                    final String dataType;
                    final Map<String,String> unmappedConfig;
                    if (object instanceof Map<?,?>) {
                        unmappedConfig = (Map<String,String>) object;
                        dataType = unmappedConfig.get(KEY_DATA_TYPE);
                    } else if (object instanceof CharSequence) {
                        dataType = object.toString();
                        unmappedConfig = new HashMap<>(3);
                        unmappedConfig.put(KEY_DATA_TYPE, dataType);
                    } else {
                        dataType = null;
                        unmappedConfig = Collections.emptyMap();
                    }
                    
                    // Trim and force the data type to lowercase
                    final String trimmedLowerCase;
                    if (null != dataType) {
                        trimmedLowerCase = dataType.trim().toLowerCase();
                    } else {
                        trimmedLowerCase = null;
                    }
                    
                    // Fill in default values, if applicable, and add the config
                    if ((null != trimmedLowerCase) && !trimmedLowerCase.isEmpty()) {
                        // Fill in the default version pattern
                        String value = unmappedConfig.get(KEY_UID_VERSION_PATTERN);
                        if ((null == value) || value.trim().isEmpty()) {
                            unmappedConfig.put(KEY_UID_VERSION_PATTERN, DEFAULT_UID_VERSION_PATTERN);
                        }
                        
                        // Fill in the default file name
                        value = unmappedConfig.get(KEY_VERSION_FIELDNAME);
                        if ((null == value) || value.trim().isEmpty()) {
                            unmappedConfig.put(KEY_VERSION_FIELDNAME, DEFAULT_VERSION_FIELDNAME);
                        }
                        
                        // Add the config
                        mappedConfigs.put(trimmedLowerCase, unmappedConfig);
                    } else {
                        final String message = "Invalid " + AbstractVersionFilter.class.getSimpleName() + " config: " + unmappedConfig;
                        final IllegalArgumentException e = new IllegalArgumentException(message);
                        LOG.error("Could not initialize config for " + AbstractVersionFilter.class.getSimpleName(), e);
                    }
                }
            }
        } catch (final Exception e) {
            LOG.error("Could not initialize configs for " + AbstractVersionFilter.class.getSimpleName(), e);
        }
        
        return mappedConfigs;
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void configure(final Map<String,String> options) {
        // Generate the config mappings and source iterator, if applicable
        final SortedKeyValueIterator<Key,Value> source;
        final IteratorEnvironment env;
        final Map<String,Map<String,String>> configs;
        if (options instanceof SourcedOptions) {
            // Generate the datatype->config mappings
            final String dataTypes;
            final String json;
            if (null != (dataTypes = options.get(KEY_DATA_TYPES))) {
                configs = buildMappedConfigs(dataTypes, options);
            } else if (null != (json = options.get(KEY_JSON))) {
                configs = buildJsonFormattedConfigs(json);
            } else {
                configs = Collections.emptyMap();
            }
            
            // Generate the source iterator if any configs are defined
            if (!configs.isEmpty()) {
                final SourcedOptions<String,String> sourced = (SourcedOptions) options;
                source = sourced.getSource();
                env = sourced.getEnvironment();
            } else {
                source = null;
                env = null;
            }
        } else {
            source = null;
            env = null;
            configs = Collections.emptyMap();
        }
        
        // Create a new source based on the one provided through the options, and
        // populate the datatype mappings
        if (null != source) {
            try {
                // Prepare the source for expected initialization
                originalSource = source;
                environment = env;
                
                // Handle multiple data type mappings
                if (configs.size() > 1) {
                    dataTypesToPatterns = new HashMap<>();
                    dataTypesToFieldNames = new HashMap<>();
                    for (final Entry<String,Map<String,String>> entry : configs.entrySet()) {
                        final String dataType = entry.getKey();
                        final Map<String,String> config = entry.getValue();
                        dataTypesToPatterns.put(dataType, config.get(KEY_UID_VERSION_PATTERN));
                        dataTypesToFieldNames.put(dataType, config.get(KEY_VERSION_FIELDNAME));
                    }
                }
                // Handle single data type mapping
                else if (configs.size() == 1) {
                    final Entry<String,Map<String,String>> entry = configs.entrySet().iterator().next();
                    dataType = entry.getKey();
                    final Map<String,String> config = entry.getValue();
                    fieldName = config.get(KEY_VERSION_FIELDNAME);
                    pattern = config.get(KEY_UID_VERSION_PATTERN);
                }
                
                // Set the configured flag
                isConfigured = (null != dataType) || !dataTypesToPatterns.isEmpty();
            } catch (final Exception e) {
                LOG.error("Could not initialize configs for " + AbstractVersionFilter.class.getSimpleName(), e);
            }
        }
    }
    
    protected Map<String,String> getMappedFieldNames() {
        final Map<String,String> map;
        if (null != dataType) {
            map = Collections.singletonMap(dataType, fieldName);
        } else {
            map = this.dataTypesToFieldNames;
        }
        return map;
    }
    
    protected Map<String,String> getMappedUidPatterns() {
        final Map<String,String> map;
        if (null != dataType) {
            map = Collections.singletonMap(dataType, pattern);
        } else {
            map = dataTypesToPatterns;
        }
        return map;
    }
    
    protected SortedKeyValueIterator<Key,Value> getOriginalSource() {
        return originalSource;
    }
    
    protected SortedKeyValueIterator<Key,Value> getSource() {
        return source;
    }
    
    private Range newValidationRange(final Text row, final String dataType, final String uid, final boolean isMultiMapping) {
        // Create a normalized version, which must include normalized values for numeric parent levels
        String baseUid = null;
        StringBuilder normalizedVersion = null;
        int normalizedStubLength = -1;
        try {
            final UID parsed = UID.parse(uid);
            baseUid = parsed.getBaseUid();
            final String extra = parsed.getExtra();
            if (!StringUtils.isBlank(extra)) {
                // Working backwards through the "extra" section, normalize the trailing
                // numerical parts while trying to minimize expensive string operations,
                // such as splitting
                final StringBuilder unnormalizedVersion = new StringBuilder(extra);
                int beginningOfNum = -1;
                int endOfNum = -1;
                int originalLength = unnormalizedVersion.length();
                boolean normalized = false;
                int normalizedValueLength = -1;
                for (int i = originalLength; i >= 0; i--) {
                    // Analyze digits and decimals to determine the boundaries of each
                    // trailing numerical part of the UID
                    char character;
                    if (i > 0) {
                        character = unnormalizedVersion.charAt(i - 1);
                    } else {
                        character = '\2';
                    }
                    if ((character >= '0') && (character <= '9')) {
                        if (endOfNum < 0) {
                            endOfNum = i;
                        }
                    } else if ((character == '.') || (character == '\2')) {
                        if ((beginningOfNum < 0) && (endOfNum >= 0)) {
                            beginningOfNum = i;
                        }
                    } else {
                        break; // Stop looping once a non-qualifying character is found
                    }
                    
                    // Once the boundaries of a numerical part have been determined,
                    // extract, convert, increment (if applicable), and normalize the number
                    if ((beginningOfNum >= 0) && (endOfNum >= 0)) {
                        // Extract the numerical
                        final String chunk = unnormalizedVersion.substring(beginningOfNum, endOfNum);
                        
                        // Convert
                        int number = Integer.parseInt(chunk);
                        
                        // Increment if it's the trailing number
                        if (endOfNum == originalLength) {
                            number++;
                        }
                        
                        // Normalize
                        final String normalizedNumber = NUMBER_NORMALIZER.normalize(Integer.toString(number));
                        
                        // Replace in the original character sequence
                        unnormalizedVersion.replace(beginningOfNum, endOfNum, normalizedNumber);
                        beginningOfNum = -1;
                        endOfNum = -1;
                        
                        // Measure the length of the primary normalized value. We'll need this to compute
                        // an accurate range.
                        if (!normalized) {
                            normalizedValueLength = normalizedNumber.length();
                            normalized = true;
                        }
                    }
                }
                
                // Assign the normalized version and the index of its "stub", if any
                if (normalized) {
                    normalizedVersion = unnormalizedVersion;
                    normalizedStubLength = normalizedVersion.length() - normalizedValueLength;
                }
            }
        } catch (final Exception e) {
            LOG.debug("Could not extract normalized version for " + row + " " + dataType + "\\x00" + uid, e);
        }
        
        // Assign the range
        final Range range;
        if ((null != baseUid) && (null != normalizedVersion)) {
            // Get the field name to find in the fi section
            final String fieldName = (isMultiMapping) ? this.dataTypesToFieldNames.get(dataType) : this.fieldName;
            
            // Create the range, if applicable
            if (null != fieldName) {
                final String normalizedStub = (normalizedStubLength > 0) ? normalizedVersion.substring(0, normalizedStubLength) : StringUtils.EMPTY;
                final Key start = new Key(row, new Text(FIELD_INDEX_PREFIX + fieldName), new Text(normalizedVersion.toString() + '\0' + dataType + '\0'
                                + baseUid));
                final Key end = new Key(row, new Text(FIELD_INDEX_PREFIX + fieldName), new Text(normalizedStub + NUMBER_NORMALIZER_MAX_VALUE + '\0' + dataType
                                + '\0' + baseUid));
                range = new Range(start, true, end, false);
            } else {
                range = null;
            }
        } else {
            range = null;
        }
        
        return range;
    }
    
    /*
     * Validate the UID based on the specified pattern. An invalid UID may mean different things in different contexts, such as
     * "No, this Key does not belong to the most current version." but should always prevent the unmodified input from being returned.
     * 
     * @param uid
     * 
     * @param pattern
     * 
     * @param key
     * 
     * @param document
     * 
     * @param isMultiMapping indicates whether multiple data types are mapped, which helps make pattern lookup a little more efficient
     * 
     * @return
     */
    private boolean validate(final String dataType, final String uid, final String pattern, final Key key, final Document document, boolean isMultiMapping) {
        boolean isValid = true;
        if (!uid.isEmpty()) {
            final String[] split = uid.split(pattern);
            try {
                final Text row = key.getRow();
                if (split.length == 2) {
                    final String matchedUid = uid.substring(0, (uid.length() - split[1].length()));
                    isValid = this.validate(row, dataType, matchedUid, isMultiMapping);
                } else if (uid.matches(pattern)) {
                    isValid = this.validate(row, dataType, uid, isMultiMapping);
                }
                
                if (!isValid) {
                    if (null != document) {
                        if (null != document) {
                            final Set<Entry<String,Attribute<? extends Comparable<?>>>> entries = new HashSet<>(document.entrySet());
                            for (final Entry<String,Attribute<? extends Comparable<?>>> entry : entries) {
                                document.removeAll(entry.getKey());
                            }
                        }
                    }
                }
            } catch (final Exception e) {
                LOG.error("Could not validate normalized version for " + key, e);
            }
        }
        
        return isValid;
    }
    
    private boolean validate(final Text row, final String dataType, final String uid, final boolean isMultiMapping) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Validate uid " + uid + " for data type " + dataType);
        }
        
        // Create the range, if applicable
        final Range range;
        if ((null != row) && (row.getLength() > 0)) {
            range = newValidationRange(row, dataType, uid, isMultiMapping);
        } else {
            range = null;
        }
        
        // If the range is defined, seek to find out if any higher/later version exist. Assume the best case by assigning
        // the default return value as true.
        boolean isValid = true;
        try {
            if (null != range) {
                // Copy and initialize the source. Waiting until now instead of during configure(..) can potentially help to
                // prevent open, yet unused file handles.
                if (null == this.source) {
                    source = originalSource.deepCopy(environment);
                }
                
                // Perform the initial seek
                final Set<ByteSequence> emptyCfs = Collections.emptySet();
                source.seek(range, emptyCfs, false);
                
                // If a column exists, it is a higher version of some sort. It may be an invalid value, or more likely belong to a
                // version entry for some other UID, so further inspection is required.
                if (source.hasTop()) {
                    isValid = validate(range);
                }
            }
        } catch (final IterationInterruptedException e) {
            // Re-throw iteration interrupted as-is since this is an expected event from
            // a client going away. Re-throwing as-is will let the
            // tserver catch and ignore it as intended.
            throw e;
        } catch (final Exception e) {
            throw new IOException("Unable to validate uid " + uid + " for data type " + dataType, e);
        }
        
        return isValid;
    }
    
    private boolean validate(Range range) throws Exception {
        // Initialize the loop control variable
        boolean hasTop = true;
        
        // Establish the base UID
        final Key startKey = range.getStartKey();
        String seekCq = startKey.getColumnQualifier().toString();
        final String baseUid = seekCq.substring(seekCq.lastIndexOf('\0'), seekCq.length());
        final String dataTypeAndBaseUid = seekCq.substring(seekCq.indexOf('\0'), seekCq.length());
        
        // Keep seeking as long as we hit invalid versions or versions belonging to different base UIDs
        Text text = new Text();
        while (hasTop) {
            // Log it
            if (LOG.isDebugEnabled()) {
                LOG.debug("Validating version based on range start key " + range.getStartKey().toStringNoTime());
            }
            
            // Get the key of the current hit
            final Key topKey = source.getTopKey();
            topKey.getColumnQualifier(text);
            String topCq = text.toString();
            
            // Denormalize and increment the version of the current hit, falling back to the
            // current range if necessary
            String normalizedVersion = null;
            for (int i = 0; i < 2; i++) {
                try {
                    // Use the hit's column qualifier as a first choice. Otherwise, fall back to the range.
                    final String cq = (i == 0) ? topCq : range.getStartKey().getColumnQualifier().toString();
                    final String existingVersion = cq.substring(0, cq.indexOf('\0'));
                    
                    // Denormalize and increment the version from the hit in preparation for a new seek
                    int number = NUMBER_NORMALIZER.denormalize(existingVersion).intValue();
                    normalizedVersion = NUMBER_NORMALIZER.normalize(Integer.toString(++number));
                    
                    // Return immediately if we've hit on a valid, later version of the desired UID
                    if ((i == 0) && topCq.contains(baseUid)) {
                        return false;
                    }
                    
                    // Otherwise, the normalizedVersion is valid
                    break;
                } catch (Exception e) {
                    final String message = "Unable to validate version for range " + startKey.toStringNoTime()
                                    + " due to an invalid field index (fi) column qualifier " + topKey.toStringNoTime();
                    LOG.warn(message, e);
                }
            }
            
            // Return immediately if a normalized version could not be generated
            if (null == normalizedVersion) {
                return false;
            }
            
            // Create a new range for seeking
            seekCq = normalizedVersion + '\0' + dataTypeAndBaseUid;
            final Text row = startKey.getRow();
            final Text cf = startKey.getColumnFamily();
            final Key start = new Key(row, cf, new Text(seekCq));
            final Key end = range.getEndKey();
            range = new Range(start, true, end, false);
            
            // Seek to the next version, if any, ideally belonging to the desired UID
            final Set<ByteSequence> emptyCfs = Collections.emptySet();
            source.seek(range, emptyCfs, false);
            
            // If a column exists, it is a higher version, which invalidates the entry
            hasTop = source.hasTop();
        }
        
        return true;
    }
    
    private static class VersionNumberType extends NumberType {
        private static final long serialVersionUID = -2923525565982408880L;
        
        @Override
        public BigDecimal denormalize(final String in) {
            if (USE_PARENT_DENORMALIZER) {
                try {
                    return super.denormalize(in);
                } catch (final Exception e) {
                    final String message = "Unable to denormalize version " + in + " using parent " + NumberType.class.getSimpleName()
                                    + ". Falling back to the " + NumericalEncoder.class.getSimpleName() + '.';
                    LOG.warn(message, e);
                    
                    if (e instanceof NumberFormatException) {
                        USE_PARENT_DENORMALIZER = false;
                    }
                }
            }
            
            return NumericalEncoder.decode(in);
        }
    }
}
