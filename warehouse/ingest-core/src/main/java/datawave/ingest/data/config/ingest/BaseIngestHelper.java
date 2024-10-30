package datawave.ingest.data.config.ingest;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.data.normalizer.NormalizationException;
import datawave.data.type.NoOpType;
import datawave.data.type.OneToManyNormalizerType;
import datawave.ingest.config.IngestConfiguration;
import datawave.ingest.config.IngestConfigurationFactory;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.ingest.data.config.FieldConfigHelper;
import datawave.ingest.data.config.MarkingsHelper;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.XMLFieldConfigHelper;
import datawave.util.StringUtils;

/**
 * Specialization of the Helper type that validates the configuration for Ingest purposes. These helper classes also have the logic to parse the field names and
 * fields values from the datatypes that they represent.
 */
public abstract class BaseIngestHelper extends AbstractIngestHelper implements CompositeIngest, VirtualIngest {

    /**
     * Configuration parameter to specify that data should be marked for delete on ingest.
     */
    public static final String INGEST_MODE_DELETE = "ingest.mode.delete";

    /**
     * Configuration parameter to specify the fields that should be indexed. This parameter supports multiple datatypes, so a valid value would be something
     * like {@code <type>.data.category.index}.
     */
    public static final String INDEX_FIELDS = ".data.category.index";

    /**
     * Configuration parameter to specify which fields should NOT be indexed, implying that all other event fields should be indexed. This parameter supports
     * multiple datatypes, so a valid value would be something like {@code <type>.data.category.index.disallowlist}.
     */
    public static final String DISALLOWLIST_INDEX_FIELDS = ".data.category.index.disallowlist";

    /**
     * Configuration parameter to specify the fields that should be indexed in reverse This parameter supports multiple datatypes, so a valid value would be
     * something like {@code <type>.data.category.index}.
     */
    public static final String REVERSE_INDEX_FIELDS = ".data.category.index.reverse";

    /**
     * Configuration parameter to specify which fields should NOT be reverse indexed, implying that all other event fields should be reverse indexed. This
     * parameter supports multiple datatypes, so a valid value would be something like {@code <type>.data.category.index.reverse.disallowlist}.
     */
    public static final String DISALLOWLIST_REVERSE_INDEX_FIELDS = ".data.category.index.reverse.disallowlist";

    /**
     * Configuration parameter to specify the name of the normalizer that should be used for this datatype. This parameter supports multiple datatypes, so valid
     * values would be something like {@code mydatatype.data.default.type.class}
     */
    public static final String DEFAULT_TYPE = ".data.default.type.class";

    public static final String NORMALIZED_FIELDS = ".data.category.normalized";

    /**
     * Configuration parameter to specify the name of the type that should be used for a particular field for this datatype. This parameter supports multiple
     * datatypes and fields, so a valid value would be something like {@code product.productid.data.field.type.class}
     */
    public static final String FIELD_TYPE = ".data.field.type.class";

    /**
     * Configuration parameter to specify whether to use type regex hierarchy. If true, this will use the datatype associated with the "most precise" regex. If
     * there are multiple matching regular expressions of the same precision, the last one read will be used. If false, all matching datatypes will be used.
     */
    public static final String USE_MOST_PRECISE_FIELD_TYPE_REGEX = "use.most.precise.field.type.regex";

    /**
     * Configuration parameter to specify the precedence of types to be used for this datatype. The last type found to handle a field will be the one used.
     * Field specific types will override this list. This parameter supports multiple datatypes, so valid values would be something like
     * {@code mydatatype.data.type.class.list}. The instance for each type configuration is the index into this list starting with "0" which can be used in
     * property names when multiple of the same type is specified.
     */
    public static final String TYPE_LIST = ".data.type.class.list";

    /**
     * Configuration parameter to specify the names of the fields that should be indexed and NOT persisted with the event fields. This parameter supports
     * multiple datatypes , valid value would be something like "mydatatype.data.category.index.only";
     */
    public static final String INDEX_ONLY_FIELDS = ".data.category.index.only";

    /**
     * When data is getting stored in CF or CQ as Text objects should malformed UTF8 input be silently replaced (true) OR should it fail (false)?
     */
    public static final String REPLACE_MALFORMED_CHAR = ".data.replace.malformed.utf8";

    /**
     * Allows per datatype defined shard table field exclusions. The value is a comma separated list of field names to exclude from storing in the shard table.
     */
    public static final String SHARD_FIELD_EXCLUSIONS = ".data.shard.field.exclusions";

    /**
     * Configuration to specify field that records names of fields that failed normalization
     */
    public static final String FAILED_NORMALIZATION_FIELD = ".data.normalization.failure.field";

    /**
     * Configuration to catch any exception created by a normalizer and remove the field. Default is to not catch any exceptions.
     */
    public static final String DEFAULT_FAILED_NORMALIZATION_POLICY = ".data.default.normalization.failure.policy";

    /**
     * Configuration to denote what to do when a specified field fails to normalize. This will override the overall "data.normalizer.failed.fields.drop" setting
     * for the specified field.
     */
    public static final String FIELD_FAILED_NORMALIZATION_POLICY = ".data.field.normalization.failure.policy";

    public static final String FIELD_CONFIG_FILE = ".data.category.field.config.file";

    private static final Logger log = ThreadConfigurableLogger.getLogger(BaseIngestHelper.class);

    private Multimap<String,datawave.data.type.Type<?>> typeFieldMap = null;
    private Multimap<String,datawave.data.type.Type<?>> typePatternMap = null;
    private TreeMultimap<Matcher,datawave.data.type.Type<?>> typeCompiledPatternMap = null;
    protected Set<String> indexOnlyFields = Sets.newHashSet();

    protected Set<String> indexedFields = Sets.newHashSet();
    protected Map<String,Pattern> indexedPatterns = Maps.newHashMap();
    protected Set<String> unindexedFields = Sets.newHashSet();

    protected Set<String> reverseIndexedFields = Sets.newHashSet();
    protected Map<String,Pattern> reverseIndexedPatterns = Maps.newHashMap();
    protected Set<String> reverseUnindexedFields = Sets.newHashSet();

    // for all the atoms that are normalized, but not indexed
    protected Set<String> normalizedFields = Sets.newHashSet();
    protected Set<String> unNormalizedFields = Sets.newHashSet();
    protected Map<String,Pattern> normalizedPatterns = Maps.newHashMap();

    protected Set<String> allIndexFields = Sets.newTreeSet(); // the indexed
    // fields across
    // all types
    protected Set<String> allReverseIndexFields = Sets.newTreeSet(); // the
    // indexed
    // fields
    // across
    // all
    // types

    private CompositeIngest compositeIngest;
    private VirtualIngest virtualIngest;

    protected boolean useMostPreciseFieldTypeRegex = false;

    public enum FailurePolicy {
        DROP, LEAVE, FAIL
    }

    protected FailurePolicy defaultFailedFieldPolicy = FailurePolicy.FAIL;
    protected Map<String,FailurePolicy> failedFieldPolicy = null;
    protected Map<String,FailurePolicy> failedFieldPatternPolicy = null;
    protected Map<Matcher,FailurePolicy> failedFieldCompiledPatternPolicy = null;
    protected String failedNormalizationField = "FAILED_NORMALIZATION_FIELD";

    protected MarkingsHelper markingsHelper = null;

    protected FieldConfigHelper fieldConfigHelper = null;

    /**
     * This matcher is used to create a deterministic ordering of regular expressions
     */
    public static class MatcherComparator implements Comparator<Matcher> {

        @Override
        public int compare(Matcher o1, Matcher o2) {
            String o1str = o1.pattern().pattern();
            int o1len = o1str.length();
            String o2str = o2.pattern().pattern();
            int o2len = o2str.length();
            if (o1len > o2len) {
                return -1;
            } else if (o1len == o2len) {
                return o2str.compareTo(o1str);
            } else {
                return 1;
            }
        }
    }

    @Override
    public void setup(Configuration config) {
        super.setup(config);

        this.failedFieldPolicy = Maps.newHashMap();
        this.failedFieldPatternPolicy = Maps.newHashMap();

        this.typeFieldMap = HashMultimap.create();
        this.typeFieldMap.put(null, new NoOpType());
        this.typePatternMap = HashMultimap.create();
        this.typeCompiledPatternMap = null;

        this.useMostPreciseFieldTypeRegex = config.getBoolean(USE_MOST_PRECISE_FIELD_TYPE_REGEX, false);

        this.getVirtualIngest().setup(config);

        if (this.compositeIngest == null)
            this.compositeIngest = new CompositeFieldIngestHelper(this.getType());
        this.getCompositeIngest().setup(config);

        IngestConfiguration ingestConfiguration = IngestConfigurationFactory.getIngestConfiguration();
        markingsHelper = ingestConfiguration.getMarkingsHelper(config, getType());

        this.normalizedFields.addAll(config.getTrimmedStringCollection(this.getType().typeName() + NORMALIZED_FIELDS));
        this.moveToPatternMap(this.normalizedFields, this.normalizedPatterns);

        deleteMode = config.getBoolean(INGEST_MODE_DELETE, false);
        replaceMalformedUTF8 = config.getBoolean(this.getType().typeName() + REPLACE_MALFORMED_CHAR, false);

        defaultFailedFieldPolicy = FailurePolicy
                        .valueOf(config.get(this.getType().typeName() + DEFAULT_FAILED_NORMALIZATION_POLICY, defaultFailedFieldPolicy.name()));
        failedNormalizationField = config.get(this.getType().typeName() + FAILED_NORMALIZATION_FIELD, failedNormalizationField);

        // Ensure that we have only an allowlist or a disallowedlist of fields to
        // index
        if (config.get(this.getType().typeName() + DISALLOWLIST_INDEX_FIELDS) != null && config.get(this.getType().typeName() + INDEX_FIELDS) != null) {
            throw new RuntimeException("Configuration contains Disallowlist and Allowlist for indexed fields, " + "it specifies both.  Type: "
                            + this.getType().typeName() + ", parameters: " + config.get(this.getType().typeName() + DISALLOWLIST_INDEX_FIELDS) + " and "
                            + config.get(this.getType().typeName() + INDEX_FIELDS));
        }

        String configProperty = null;

        // Load the field helper, which takes precedence over the individual field configurations
        final String fieldConfigFile = config.get(this.getType().typeName() + FIELD_CONFIG_FILE);
        if (fieldConfigFile != null) {
            if (log.isDebugEnabled()) {
                log.debug("Field config file " + fieldConfigFile + " specified for: " + this.getType().typeName() + FIELD_CONFIG_FILE);
            }
            this.fieldConfigHelper = XMLFieldConfigHelper.load(fieldConfigFile, this);
        }

        // Process the indexed fields
        if (config.get(this.getType().typeName() + DISALLOWLIST_INDEX_FIELDS) != null) {
            if (log.isDebugEnabled()) {
                log.debug("Disallowlist specified for: " + this.getType().typeName() + DISALLOWLIST_INDEX_FIELDS);
            }
            super.setHasIndexDisallowlist(true);
            configProperty = DISALLOWLIST_INDEX_FIELDS;
        } else if (config.get(this.getType().typeName() + INDEX_FIELDS) != null) {
            log.debug("IndexedFields specified.");
            super.setHasIndexDisallowlist(false);
            configProperty = INDEX_FIELDS;
        }

        // Load the proper list of fields to (not) index
        if (fieldConfigHelper != null) {
            log.info("Using field config helper for " + this.getType().typeName());
        } else if (configProperty == null) {
            log.warn("No index fields or disallowlist fields specified, not generating index fields for " + this.getType().typeName());
        } else {
            this.indexedFields = Sets.newHashSet();
            Collection<String> indexedStrings = config.getStringCollection(this.getType().typeName() + configProperty);
            if (null != indexedStrings && !indexedStrings.isEmpty()) {
                for (String indexedString : indexedStrings) {
                    this.indexedFields.add(indexedString.trim());
                }
                this.moveToPatternMap(this.indexedFields, this.indexedPatterns);
            } else {
                log.warn(this.getType().typeName() + configProperty + " not specified.");
            }
        }

        // Ensure that we have only an allowlist or a disallowlist of fields to
        // reverse index
        if (config.get(this.getType().typeName() + DISALLOWLIST_REVERSE_INDEX_FIELDS) != null
                        && config.get(this.getType().typeName() + REVERSE_INDEX_FIELDS) != null) {
            throw new RuntimeException("Configuration contains Disallowlist and Allowlist for indexed fields, it specifies both.  Type: "
                            + this.getType().typeName() + ", parameters: " + config.get(this.getType().typeName() + DISALLOWLIST_REVERSE_INDEX_FIELDS) + "  "
                            + config.get(this.getType().typeName() + REVERSE_INDEX_FIELDS));
        }

        configProperty = null;

        // Process the reverse index fields
        if (config.get(this.getType().typeName() + DISALLOWLIST_REVERSE_INDEX_FIELDS) != null) {
            if (log.isDebugEnabled()) {
                log.debug("Disallowlist specified for: " + this.getType().typeName() + DISALLOWLIST_REVERSE_INDEX_FIELDS);
            }

            this.setHasReverseIndexDisallowlist(true);

            configProperty = DISALLOWLIST_REVERSE_INDEX_FIELDS;
        } else if (config.get(this.getType().typeName() + REVERSE_INDEX_FIELDS) != null) {
            if (log.isDebugEnabled()) {
                log.debug("Reverse Index specified.for: " + this.getType().typeName() + REVERSE_INDEX_FIELDS);
            }
            this.setHasReverseIndexDisallowlist(false);
            configProperty = REVERSE_INDEX_FIELDS;
        }

        // Load the proper list of fields to (not) reverse index
        if (configProperty == null) {
            log.warn("No reverse index fields or disallowlist reverse index fields specified, not generating reverse index fields for "
                            + this.getType().typeName());
        } else {
            reverseIndexedFields = Sets.newHashSet();
            Collection<String> reverseIndexedStrings = config.getStringCollection(this.getType().typeName() + configProperty);
            if (null != reverseIndexedStrings && !reverseIndexedStrings.isEmpty()) {
                for (String reverseIndexedString : reverseIndexedStrings) {
                    reverseIndexedFields.add(reverseIndexedString.trim());
                }
                this.moveToPatternMap(this.reverseIndexedFields, this.reverseIndexedPatterns);
            } else {
                log.warn(this.getType().typeName() + configProperty + " not specified");
            }

        }

        // gather the list of all indexed fields across all types
        // this list is only used for generating warnings if we are not indexing
        // something that
        // somebody else is
        for (Type type : TypeRegistry.getTypes()) {
            Collection<String> indexedStrings = config.getStringCollection(type.typeName() + INDEX_FIELDS);
            if (null != indexedStrings && !indexedStrings.isEmpty()) {
                for (String indexedString : indexedStrings) {
                    String indexedTrimmedString = indexedString.trim();
                    allIndexFields.add(indexedTrimmedString);
                }
            }
            Collection<String> reverseIndexedStrings = config.getStringCollection(type.typeName() + REVERSE_INDEX_FIELDS);
            if (null != reverseIndexedStrings && !reverseIndexedStrings.isEmpty()) {
                for (String reverseIndexedString : reverseIndexedStrings) {
                    String reverseIndexedTrimmedString = reverseIndexedString.trim();
                    allReverseIndexFields.add(reverseIndexedTrimmedString);
                }
            }
        }

        for (Entry<String,String> property : config) {

            // Make sure we are only processing normalizers for this type
            if (!property.getKey().startsWith(this.getType().typeName() + '.')) {
                continue;
            }

            String fieldName = null;

            String key = property.getKey();
            if (key.endsWith(DEFAULT_TYPE) || key.endsWith(FIELD_TYPE)) {
                if (key.endsWith(FIELD_TYPE)) {
                    if ((fieldName = getFieldType(key, FIELD_TYPE)) == null) {
                        continue;
                    }
                }

                String typeClasses = property.getValue();

                updateDatawaveTypes(fieldName, typeClasses);

            } else if (property.getKey().endsWith(FIELD_FAILED_NORMALIZATION_POLICY)) {
                if ((fieldName = getFieldName(property.getKey(), FIELD_FAILED_NORMALIZATION_POLICY)) == null) {
                    continue;
                }

                FailurePolicy policy;
                try {
                    policy = FailurePolicy.valueOf(property.getValue());
                } catch (Exception e) {
                    log.error("Unable to parse field normalization failure policy: " + property.getValue(), e);
                    throw new IllegalArgumentException("Unable to parse field normalization failure policy: " + property.getValue(), e);
                }
                if (fieldName.indexOf('*') >= 0) {
                    failedFieldPatternPolicy.put(fieldName, policy);
                } else {
                    failedFieldPolicy.put(fieldName, policy);
                }
            }
        }

        // Support for excluding specific fields from being inserted into the
        // Shard table
        // This is useful if virtual fields are used heavily, but you don't want
        // these fields inserted
        // into the shard table. For instance, if many virtual fields are used
        // in the edge table
        //
        // Note: this pruning occurs after all the field names are aliased or
        // normalized
        shardExclusions.clear();
        String exclusionsList = config.get(this.getType().typeName() + SHARD_FIELD_EXCLUSIONS);
        if (exclusionsList != null) {
            String[] exclusions = StringUtils.split(exclusionsList, ',');
            if (exclusions != null && exclusions.length > 0) {
                for (String exclusionFieldName : exclusions) {

                    String fieldName = exclusionFieldName.trim();

                    if (!fieldName.isEmpty()) {

                        shardExclusions.add(fieldName);
                    } else {

                        // TODO: Possibly add warning to indicated a potentially
                        // questionable configuration file...
                    }
                }
            }
        }

        String indexOnlyFieldList = config.get(this.getType().typeName() + INDEX_ONLY_FIELDS);
        if (null != indexOnlyFieldList) {
            for (String s : indexOnlyFieldList.split(",")) {

                String fieldName = s.trim();

                if (!fieldName.isEmpty()) {

                    this.indexOnlyFields.add(fieldName);
                } else {

                    // TODO: Possibly add warning to indicated a potentially
                    // questionable configuration file...
                }
            }
        }
    }

    private void moveToPatternMap(Set<String> in, Map<String,Pattern> out) {
        for (Iterator<String> itr = in.iterator(); itr.hasNext();) {
            String str = itr.next();
            if (str.indexOf('*') != -1) {
                itr.remove();
                Pattern pattern = Pattern.compile(str.replace("*", ".*"));
                out.put(str, pattern);
            }
        }
    }

    /**
     * lazy instantiation
     *
     * @return a {@link CompositeIngest}
     */
    public CompositeIngest getCompositeIngest() {
        return this.compositeIngest;
    }

    /**
     * lazy instantiation
     *
     * @return a {@link VirtualIngest}
     */
    public VirtualIngest getVirtualIngest() {
        if (this.virtualIngest == null) {
            this.virtualIngest = new VirtualFieldIngestHelper(this.getType());
        }
        return this.virtualIngest;
    }

    public Set<String> getIndexOnlyFields() {
        return indexOnlyFields;
    }

    public Set<String> getIndexedFields() {
        return indexedFields;
    }

    public Set<String> getReverseIndexedFields() {
        return reverseIndexedFields;
    }

    public Set<String> getNormalizedFields() {
        return normalizedFields;
    }

    public boolean isAliasedIndexField(String fieldName) {
        return (null != aliaser.getIndexAliases(fieldName));
    }

    public HashSet<String> getAliasesForIndexedField(String fieldName) {
        return aliaser.getIndexAliases(fieldName);
    }

    /**
     * Get a field name from a property name given the pattern, or null if no field name is found
     *
     * @param property
     *            a property that contains a field name
     * @param propertyPattern
     *            a pattern used to extract a field name from a property
     * @return the field name extracted from the property name
     */
    protected String getFieldName(String property, String propertyPattern) {
        return getFieldName(this.getType(), property, propertyPattern);
    }

    public static String getFieldName(Type dataType, String property, String propertyPattern) {
        // if this type already has a '.', then we have a malformed property
        // name
        if (dataType.typeName().indexOf('.') >= 0) {
            log.error(propertyPattern + " property malformed: " + property);
            throw new IllegalArgumentException(propertyPattern + " property malformed: " + property);
        }

        String fieldName = property.substring(dataType.typeName().length() + 1, property.length() - propertyPattern.length());

        if (fieldName.isEmpty()) {
            fieldName = null;
        } else {
            fieldName = fieldName.toUpperCase();
        }

        return fieldName;
    }

    protected String getFieldType(String property, String propertyPattern) {
        return getFieldType(this.getType(), property, propertyPattern);
    }

    public static String getFieldType(Type dataType, String property, String propertyPattern) {
        // if this type already has a '.', then we have a malformed property
        // name
        if (dataType.typeName().indexOf('.') >= 0) {
            log.error(propertyPattern + " property malformed: " + property);
            throw new IllegalArgumentException(propertyPattern + " property malformed: " + property);
        }

        String fieldName = property.substring(dataType.typeName().length() + 1, property.length() - propertyPattern.length());

        if (fieldName.isEmpty()) {
            fieldName = null;
        } else {
            fieldName = fieldName.toUpperCase();
        }

        return fieldName;
    }

    @Override
    public void setEmbeddedHelper(DataTypeHelperImpl embeddedHelper) {
        this.embeddedHelper = embeddedHelper;
        // When the embedded helper is set, check to see if it is an instance
        // of MaskedFieldHelper. If so, then normalize the values
        if (embeddedHelper instanceof MaskedFieldHelper) {
            mfHelper = (MaskedFieldHelper) embeddedHelper;
        }
    }

    public MarkingsHelper getMarkingsHelper() {
        return markingsHelper;
    }

    @Override
    public boolean isIndexOnlyField(String fieldName) {
        if (fieldConfigHelper != null) {
            return fieldConfigHelper.isIndexOnlyField(fieldName);
        }
        return indexOnlyFields.contains(fieldName);
    }

    @Override
    public void addIndexOnlyField(String fieldName) {
        indexOnlyFields.add(fieldName);
    }

    @Override
    public void addShardExclusionField(String fieldName) {
        shardExclusions.add(fieldName);
    }

    @Override
    public boolean isOverloadedCompositeField(String fieldName) {
        return CompositeIngest.isOverloadedCompositeField(getCompositeFieldDefinitions(), fieldName);
    }

    @Override
    public boolean isCompositeField(String fieldName) {
        return this.compositeIngest.isCompositeField(fieldName);
    }

    @Override
    public boolean isDataTypeField(String fieldName) {
        return this.typeFieldMap.containsKey(fieldName);

    }

    private void compilePatterns() {
        TreeMultimap<Matcher,datawave.data.type.Type<?>> patterns = TreeMultimap.create(new MatcherComparator(),
                        (o1, o2) -> o1.toString().compareTo(o2.toString()));
        if (typePatternMap != null) {
            for (String pattern : typePatternMap.keySet()) {
                patterns.putAll(compileFieldNamePattern(pattern), typePatternMap.get(pattern));
            }
        }
        typeCompiledPatternMap = patterns;
    }

    public static Matcher compileFieldNamePattern(String fieldNamePattern) {
        return Pattern.compile(fieldNamePattern.replace("*", ".*")).matcher("");
    }

    @Override
    public List<datawave.data.type.Type<?>> getDataTypes(String fieldName) {

        final String typeFieldName = fieldName.toUpperCase();

        LinkedList<datawave.data.type.Type<?>> types = new LinkedList<>(typeFieldMap.get(typeFieldName));

        if (types.isEmpty()) {
            if (typeCompiledPatternMap == null) {
                compilePatterns();
            }

            if (useMostPreciseFieldTypeRegex) {
                Matcher bestMatch = getBestMatch(typeCompiledPatternMap.keySet(), fieldName);
                if (null != bestMatch) {
                    Collection<datawave.data.type.Type<?>> bestMatchTypes = typeCompiledPatternMap.get(bestMatch);
                    types.addAll(bestMatchTypes);
                    typeFieldMap.putAll(fieldName, bestMatchTypes);
                }
            } else {
                for (Matcher patternMatcher : typeCompiledPatternMap.keySet()) {
                    if (patternMatcher.reset(fieldName).matches()) {
                        Collection<datawave.data.type.Type<?>> matchTypes = typeCompiledPatternMap.get(patternMatcher);
                        types.addAll(matchTypes);
                        typeFieldMap.putAll(fieldName, matchTypes);
                    }
                }
            }
        }

        // if no types were defined or matched via regex, use the default
        if (types.isEmpty()) {
            types.addAll(typeFieldMap.get(null));
        }

        return types;
    }

    public static Matcher getBestMatch(Set<Matcher> patterns, String fieldName) {
        Matcher bestMatch = null;
        for (Matcher patternMatcher : patterns) {
            if (bestMatch != null && patternMatcher.pattern().pattern().length() < bestMatch.pattern().pattern().length()) {
                break;
            }
            if (patternMatcher.reset(fieldName).matches()) {
                if (bestMatch != null) {
                    log.warn("Multiple regular expression patterns with the same length exist for matching field " + fieldName
                                    + ". The pattern that sorts lexicographically last will be used. Please verify your configurations.");
                    break;
                } else {
                    bestMatch = patternMatcher;
                }
            }
        }
        return bestMatch;
    }

    /**
     * This is a helper routine that will return a normalized field value using the configured normalizer
     *
     * @param fieldValue
     *            the field value
     * @return the normalized field values
     */
    @Override
    protected Set<String> normalizeFieldValue(String fieldName, String fieldValue) throws NormalizationException {
        Collection<datawave.data.type.Type<?>> dataTypes = getDataTypes(fieldName);
        HashSet<String> values = new HashSet<>(dataTypes.size());
        for (datawave.data.type.Type<?> dataType : dataTypes) {
            values.add(dataType.normalize(fieldValue));
        }
        return values;
    }

    /**
     * This is a helper routine that will create a normalized field out of a name and value pair
     *
     * @param field
     *            the field
     * @param value
     *            the value
     * @return The normalized field and value
     */
    protected Set<NormalizedContentInterface> normalize(String field, String value) {
        return normalize(new NormalizedFieldAndValue(field, value));
    }

    protected NormalizedContentInterface normalize(NormalizedContentInterface normalizedContent, datawave.data.type.Type<?> datawaveType) {
        // copy it
        NormalizedContentInterface copy = new NormalizedFieldAndValue(normalizedContent);
        try {
            copy.setIndexedFieldValue(datawaveType.normalize(copy.getIndexedFieldValue()));
        } catch (Exception ex) {
            copy.setError(ex);
        }
        return copy;
    }

    protected List<NormalizedContentInterface> normalize(NormalizedContentInterface normalizedContent,
                    datawave.data.type.OneToManyNormalizerType<?> datawaveType) {
        List<NormalizedContentInterface> list = Lists.newArrayList();
        // copy it
        NormalizedContentInterface copy = new NormalizedFieldAndValue(normalizedContent);
        for (String one : datawaveType.normalizeToMany(copy.getIndexedFieldValue())) {
            try {
                copy.setIndexedFieldValue(one);
                list.add(copy);
                copy = new NormalizedFieldAndValue(normalizedContent);
            } catch (Exception ex) {
                copy.setError(ex);
            }
        }
        return list;
    }

    public HashSet<NormalizedContentInterface> normalizeFieldValue(String fieldName, NormalizedContentInterface normalizedContent, boolean indexOnly) {
        Collection<datawave.data.type.Type<?>> dataTypes = getDataTypes(fieldName);
        HashSet<NormalizedContentInterface> values = new HashSet<>(dataTypes.size());
        for (datawave.data.type.Type<?> dataType : dataTypes) {
            NormalizedContentInterface value = normalizeFieldValue(normalizedContent, dataType);
            if (indexOnly) {
                value.setEventFieldValue(null);
            }
            values.add(value);
            if (log.isDebugEnabled()) {
                log.debug("added normalized field " + value + " to values set.");
            }
        }
        return values;
    }

    protected NormalizedContentInterface normalizeFieldValue(NormalizedContentInterface normalizedContent, datawave.data.type.Type<?> datawaveType) {
        // copy it
        NormalizedContentInterface copy = new NormalizedFieldAndValue(normalizedContent);
        try {
            copy.setEventFieldValue(datawaveType.normalize(copy.getIndexedFieldValue()));
            copy.setIndexedFieldValue(datawaveType.normalize(copy.getIndexedFieldValue()));
        } catch (Exception ex) {
            copy.setError(ex);
        }
        return copy;
    }

    /**
     * This is a helper routine that will create and normalize a field out of a base normalized field.
     *
     * @param normalizedContent
     *            a {@link NormalizedContentInterface}
     * @return the normalized field
     */
    protected Set<NormalizedContentInterface> normalize(NormalizedContentInterface normalizedContent) {
        String eventFieldName = normalizedContent.getEventFieldName();
        if (log.isDebugEnabled()) {
            log.debug("event field name is " + eventFieldName + " in " + normalizedContent);
        }
        String indexedFieldName = normalizedContent.getIndexedFieldName();
        if (log.isDebugEnabled()) {
            log.debug("indexed field name is " + indexedFieldName + " in " + normalizedContent);
        }

        // if it is indexed, set the index part,
        if (this.isIndexedField(eventFieldName) || this.isIndexedField(indexedFieldName)) {
            if (log.isDebugEnabled()) {
                log.debug("eventFieldName=" + eventFieldName + ", indexedFieldName =" + indexedFieldName + " we have an indexed field here "
                                + normalizedContent);
            }
            Collection<datawave.data.type.Type<?>> dataTypes = getDataTypes(normalizedContent.getIndexedFieldName());
            HashSet<NormalizedContentInterface> values = new HashSet<>(dataTypes.size());
            for (datawave.data.type.Type<?> dataType : dataTypes) {
                if (dataType instanceof OneToManyNormalizerType) {
                    values.addAll(normalize(normalizedContent, (OneToManyNormalizerType) dataType));
                } else {
                    values.add(normalize(normalizedContent, dataType));
                }
                if (log.isDebugEnabled()) {
                    log.debug("added normalized field " + normalizedContent + " to values " + values);
                }
            }
            return values;
        }
        // if it is normalized, set the field value part and the (unused)
        // indexed field value part
        if (this.isNormalizedField(eventFieldName) || this.isNormalizedField(indexedFieldName)) {
            if (log.isDebugEnabled()) {
                log.debug("eventFieldName=" + eventFieldName + ", indexedFieldName =" + indexedFieldName + " we have a normalized field here "
                                + normalizedContent);
            }
            Collection<datawave.data.type.Type<?>> dataTypes = getDataTypes(normalizedContent.getIndexedFieldName());
            HashSet<NormalizedContentInterface> values = new HashSet<>(dataTypes.size());
            for (datawave.data.type.Type<?> dataType : dataTypes) {
                values.add(normalizeFieldValue(normalizedContent, dataType));
                if (log.isDebugEnabled()) {
                    log.debug("added normalized field " + normalizedContent + " to values " + values);
                }
            }
            return values;
        } else {
            // gets the default normalizer, if present
            if (log.isDebugEnabled()) {
                log.debug("not a normalized field: " + indexedFieldName + " nor " + eventFieldName);
            }
            Collection<datawave.data.type.Type<?>> dataTypes = getDataTypes(normalizedContent.getIndexedFieldName());
            HashSet<NormalizedContentInterface> values = new HashSet<>(dataTypes.size());
            for (datawave.data.type.Type<?> dataType : dataTypes) {
                values.add(normalize(normalizedContent, dataType));
                if (log.isDebugEnabled()) {
                    log.debug("added normalized field " + normalizedContent + " to values " + values);
                }
            }
            return values;
        }
    }

    @Override
    public boolean isNormalizedField(String fieldName) {
        if (this.normalizedFields.contains(fieldName)) {
            return true;
        } else if (this.unNormalizedFields.contains(fieldName)) {
            return false;
        } else if (this.normalizedPatterns.isEmpty()) { // avoids filling unNormalizedFields if not necessary
            return false;
        } else {
            for (Pattern pattern : this.normalizedPatterns.values()) {
                if (pattern.matcher(fieldName).matches()) {
                    this.normalizedFields.add(fieldName);
                    return true;
                }
            }
            this.unNormalizedFields.add(fieldName);
            return false;
        }
    }

    @Override
    public boolean isShardExcluded(String fieldname) {
        if (fieldConfigHelper != null) {
            return !fieldConfigHelper.isStoredField(fieldname);
        }
        return super.isShardExcluded(fieldname);
    }

    @Override
    public boolean isIndexedField(String fieldName) {
        if (fieldConfigHelper != null) {
            return fieldConfigHelper.isIndexedField(fieldName);
        }
        return this.hasIndexDisallowlist() ? !isIndexed(fieldName) : isIndexed(fieldName);
    }

    private boolean isIndexed(String fieldName) {
        if (fieldConfigHelper != null && fieldConfigHelper.isIndexedField(fieldName)) {
            return true;
        } else if (this.indexedFields.contains(fieldName)) {
            return true;
        } else if (this.unindexedFields.contains(fieldName)) {
            return false;
        } else if (this.indexedPatterns.isEmpty()) { // avoids filling unindexedFields if not necessary
            return false;
        } else {
            for (Pattern pattern : this.indexedPatterns.values()) {
                if (pattern.matcher(fieldName).matches()) {
                    this.indexedFields.add(fieldName); // update so we don't need to match the next time we see it
                    return true;
                }
            }
            this.unindexedFields.add(fieldName);
            return false;
        }
    }

    @Override
    public boolean isReverseIndexedField(String fieldName) {
        if (fieldConfigHelper != null) {
            return fieldConfigHelper.isReverseIndexedField(fieldName);
        }

        return super.hasReverseIndexDisallowlist() ? !this.isReverseIndexed(fieldName) : this.isReverseIndexed(fieldName);
    }

    private boolean isReverseIndexed(String fieldName) {
        if (fieldConfigHelper != null && fieldConfigHelper.isReverseIndexedField(fieldName)) {
            return true;
        } else if (this.reverseIndexedFields.contains(fieldName)) {
            return true;
        } else if (this.reverseUnindexedFields.contains(fieldName)) {
            return false;
        } else if (this.reverseIndexedPatterns.isEmpty()) { // avoids filling reverseUnindexedFields if not necessary
            return false;
        } else {
            for (Pattern pattern : this.reverseIndexedPatterns.values()) {
                if (pattern.matcher(fieldName).matches()) {
                    this.reverseIndexedFields.add(fieldName); // update so we don't need to match the next time we see it
                    return true;
                }
            }
            this.reverseUnindexedFields.add(fieldName);
            return false;
        }
    }

    /**
     * This is a helper routine that will create the normalized forms of a value given a set of fields
     *
     * @param fields
     *            A map of the original field name to the original value
     */
    public Multimap<String,NormalizedContentInterface> normalize(Multimap<String,String> fields) {
        Multimap<String,NormalizedContentInterface> results = HashMultimap.create();

        for (Entry<String,String> e : fields.entries()) {
            if (e.getValue() != null) {
                applyNormalizationAndAddToResults(results, new NormalizedFieldAndValue(e.getKey(), e.getValue()));
            } else
                log.warn(this.getType().typeName() + " has key " + e.getKey() + " with a null value.");
        }
        return results;
    }

    /**
     * This is a helper routine that will create the normalized forms of a value given a set of fields
     *
     * @param fields
     *            A map of the original field name to a field
     */
    public Multimap<String,NormalizedContentInterface> normalizeMap(Multimap<String,NormalizedContentInterface> fields) {
        Multimap<String,NormalizedContentInterface> results = HashMultimap.create();

        for (Entry<String,NormalizedContentInterface> e : fields.entries()) {
            if (e.getValue() != null) {
                applyNormalizationAndAddToResults(results, e.getValue());
            } else
                log.warn(this.getType().typeName() + " has key " + e.getKey() + " with a null value.");
        }
        return results;
    }

    /**
     * Apply the normalization and add the results to the map. Failure policy handling: LEAVE: The non-normalized form is put in the map with a cleared out
     * error, and a failedNormalizationField is added to the map with the field name as the value DROP: Only a failedNormalizationField is added to the map with
     * the field name as the value FAIL: The non-normalized form is put in the map with the error set allowing the caller to fail out the event with appropriate
     * error handling
     *
     * @param results
     *            a MultiMap of {@link NormalizedContentInterface}
     * @param nArg
     *            current data to be normalized
     */
    protected void applyNormalizationAndAddToResults(Multimap<String,NormalizedContentInterface> results, NormalizedContentInterface nArg) {
        // If an alias exists, then we want to use the alias as the key in the
        // map.
        // We don't want to use the normalized form of the alias.
        Set<NormalizedContentInterface> ns = normalizeAndAlias(nArg);
        for (NormalizedContentInterface n : ns) {
            if (n != null) {
                if (n.getError() != null) {
                    switch (getFailurePolicy(n.getIndexedFieldName())) {
                        case LEAVE:
                            // for the leave policy, clear out the exception but add
                            // a failed normalization field
                            n.setError(null);
                            results.put(n.getIndexedFieldName(), n);
                            results.put(failedNormalizationField, new NormalizedFieldAndValue(failedNormalizationField, n.getIndexedFieldName()));
                            break;
                        case DROP:
                            // for the drop policy, clear out the exception,
                            // clear out the indexed field value and add
                            // a failed normalization field
                            n.setError(null);
                            n.setIndexedFieldValue(null);
                            results.put(n.getIndexedFieldName(), n);
                            results.put(failedNormalizationField, new NormalizedFieldAndValue(failedNormalizationField, n.getIndexedFieldName()));
                            break;
                        case FAIL:
                            // for the fail policy, leave the exception and let the
                            // caller (EventMapper) fail the event
                            results.put(n.getIndexedFieldName(), n);
                    }
                } else {
                    results.put(n.getIndexedFieldName(), n);
                }
            }
        }

    }

    /**
     * Normalize and alias a field
     *
     * @param nArg
     *            a field value pair encapsulated within a {@link NormalizedContentInterface}
     * @return the normalized field, or the non-normalized field with an appropriate error set
     */
    protected Set<NormalizedContentInterface> normalizeAndAlias(NormalizedContentInterface nArg) {
        Set<NormalizedContentInterface> ns;
        try {
            ns = normalize(aliaser.normalizeAndAlias(nArg));
        } catch (Exception e) {
            if (log.isTraceEnabled()) {
                log.trace(this.getType().typeName() + ": Field failed normalization: " + nArg, e);
            }
            nArg.setError(e);
            return Collections.singleton(nArg);
        }

        for (NormalizedContentInterface n : ns) {
            if (n != null) {
                markingsHelper.markField(n);
            }
        }
        return ns;
    }

    private void compilePolicyPatterns() {
        Map<Matcher,FailurePolicy> patterns = new HashMap<>();
        if (failedFieldPatternPolicy != null) {
            for (String pattern : failedFieldPatternPolicy.keySet()) {
                patterns.put(Pattern.compile(pattern.replace("*", ".*")).matcher(""), failedFieldPatternPolicy.get(pattern));
            }
        }
        failedFieldCompiledPatternPolicy = patterns;
    }

    protected FailurePolicy getFailurePolicy(String fieldName) {
        FailurePolicy policy = failedFieldPolicy.get(fieldName);
        if (policy == null) {
            if (failedFieldCompiledPatternPolicy == null)
                compilePolicyPatterns();
            if (!failedFieldCompiledPatternPolicy.isEmpty()) {
                for (Matcher patternMatcher : failedFieldCompiledPatternPolicy.keySet()) {

                    if (patternMatcher.reset(fieldName).matches()) {
                        policy = failedFieldCompiledPatternPolicy.get(patternMatcher);
                        break;
                    }
                }
            }
            if (policy == null) {
                policy = defaultFailedFieldPolicy;
            }
        }
        return policy;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.ingest.IngestHelperInterface#addIndexedField (java.lang.String)
     */
    @Override
    public void addIndexedField(String fieldName) {
        this.indexedFields.add(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.ingest.IngestHelperInterface# addReverseIndexedField(java.lang.String)
     */
    @Override
    public void addReverseIndexedField(String fieldName) {
        this.reverseIndexedFields.add(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.ingest.IngestHelperInterface# addNormalizedField(java.lang.String)
     */
    @Override
    public void addNormalizedField(String fieldName) {
        this.normalizedFields.add(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.ingest.IngestHelperInterface# shouldHaveBeenIndexed(java.lang.String)
     */
    @Override
    public boolean shouldHaveBeenIndexed(String fieldName) {
        return this.allIndexFields.contains(fieldName);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.config.ingest.IngestHelperInterface# shouldHaveBeenReverseIndexed(java.lang.String)
     */
    @Override
    public boolean shouldHaveBeenReverseIndexed(String fieldName) {
        return this.allReverseIndexFields.contains(fieldName);
    }

    public boolean verify() {
        boolean retVal = true;
        // first verify the index fields
        if (this.indexedFields == null) {
            retVal = false;
            log.error(this.getType().typeName() + ": index set has been set to null.");
        } else if (this.indexedFields.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug(this.getType().typeName() + ": no fields have been set to index.");
            }
        } else {
            upperCaseSetEntries(this.indexedFields, this.getType().typeName() + ": index fields");
        }
        // next verify the index fields
        if (this.reverseIndexedFields == null) {
            retVal = false;
            log.error(this.getType().typeName() + ": reverse index set has been set to null.");
        } else if (this.reverseIndexedFields.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug(this.getType().typeName() + ": no fields have been set to reverse index.");
            }
        } else {
            upperCaseSetEntries(this.reverseIndexedFields, this.getType().typeName() + ": reverse index fields");
        }
        return retVal;
    }

    @Override
    public Multimap<String,String> getCompositeFieldDefinitions() {
        return getCompositeIngest().getCompositeFieldDefinitions();
    }

    @Override
    public Map<String,String> getCompositeFieldSeparators() {
        return getCompositeIngest().getCompositeFieldSeparators();
    }

    @Override
    public void setCompositeFieldDefinitions(Multimap<String,String> compositeFieldDefinitions) {
        getCompositeIngest().setCompositeFieldDefinitions(compositeFieldDefinitions);
    }

    @Override
    public String getDefaultVirtualFieldSeparator() {
        return getVirtualIngest().getDefaultVirtualFieldSeparator();
    }

    @Override
    public void setDefaultVirtualFieldSeparator(String sep) {
        getVirtualIngest().setDefaultVirtualFieldSeparator(sep);
    }

    @Override
    public Multimap<String,NormalizedContentInterface> getCompositeFields(Multimap<String,NormalizedContentInterface> fields) {
        return getCompositeIngest().getCompositeFields(fields);
    }

    @Override
    public boolean isVirtualIndexedField(String fieldName) {
        return getVirtualIngest().isVirtualIndexedField(fieldName);
    }

    @Override
    public Map<String,String[]> getVirtualNameAndIndex(String virtualFieldName) {
        return getVirtualIngest().getVirtualNameAndIndex(virtualFieldName);
    }

    @Override
    public Map<String,String[]> getVirtualFieldDefinitions() {
        return getVirtualIngest().getVirtualFieldDefinitions();
    }

    @Override
    public void setVirtualFieldDefinitions(Map<String,String[]> virtualFieldDefinitions) {
        getVirtualIngest().setVirtualFieldDefinitions(virtualFieldDefinitions);
    }

    @Override
    public Multimap<String,NormalizedContentInterface> getVirtualFields(Multimap<String,NormalizedContentInterface> values) {
        return normalizeMap(getVirtualIngest().getVirtualFields(values));
    }

    /**
     * This method allows updating the typeFieldMap from a derived helper.
     *
     * @param fieldName
     *            the field name
     * @param typeClasses
     *            a comma-delimited string of {@link Type} classes
     */
    public void updateDatawaveTypes(String fieldName, String typeClasses) {

        // updates multimap typeFieldMap
        if (log.isDebugEnabled()) {
            log.debug("[" + fieldName + "] Type classes: " + typeClasses);
        }

        for (String typeClass : Splitter.on(',').split(typeClasses)) {

            datawave.data.type.Type<?> datawaveType = datawave.data.type.Type.Factory.createType(typeClass);
            // Add the type to the map, the null key is the default key
            if (fieldName == null) {
                typeFieldMap.removeAll(null);
                if (log.isDebugEnabled()) {
                    log.debug("Setting " + datawaveType + " as default type.");
                }
                typeFieldMap.put(null, datawaveType);
            } else if (fieldName.indexOf('*') >= 0 || fieldName.indexOf('+') >= 0) { // We need a more conclusive test for regex
                typePatternMap.put(fieldName, datawaveType);
            } else {
                typeFieldMap.put(fieldName, datawaveType);
            }
            if (log.isDebugEnabled()) {
                log.debug("Registered a " + typeClass + " for type[" + this.getType().typeName() + "], field[" + fieldName + "]");
            }
        }
    }
}
