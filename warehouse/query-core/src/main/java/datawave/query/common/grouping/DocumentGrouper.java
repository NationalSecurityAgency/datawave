package datawave.query.common.grouping;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;;
import datawave.data.type.Type;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Provides functionality needed to group documents and aggregate field values within identified groups (regardless if done server or client-side).
 */
public class DocumentGrouper {
    
    private static final Logger log = getLogger(DocumentGrouper.class);
    
    public static final String GROUP_COUNT = "COUNT";
    public static final String FIELD_SUM_SUFFIX = "_SUM";
    public static final String FIELD_MAX_SUFFIX = "_MAX";
    public static final String FIELD_MIN_SUFFIX = "_MIN";
    public static final String FIELD_AVERAGE_NUMERATOR_SUFFIX = "_AVERAGE_NUMERATOR";
    public static final String FIELD_AVERAGE_DIVISOR_SUFFIX = "_AVERAGE_DIVISOR";
    public static final String FIELD_AVERAGE_SUFFIX = "_AVERAGE";
    public static final String FIELD_COUNT_SUFFIX = "_COUNT";
    
    /**
     * Returns a column visibility that results from the combination of all given visibilities using the given {@link MarkingFunctions}.
     * @param visibilities the visibilities to combine
     * @param markingFunctions the marking functions to combine the visibilities with
     * @param failOnError if true and the visibilities cannot be combined, an {@link IllegalArgumentException} will be thrown. If false and the visibilities
     *                    cannot be combined, it will be logged and a new, blank {@link ColumnVisibility} will be returned.
     * @return the combined column visibility
     */
    public static ColumnVisibility combine(Collection<ColumnVisibility> visibilities, MarkingFunctions markingFunctions, boolean failOnError) {
        try {
            return markingFunctions.combine(visibilities);
        } catch (MarkingFunctions.Exception e) {
            if (failOnError) {
                throw new IllegalArgumentException("Unable to combine visibilities: " + visibilities, e);
            } else {
                log.warn("Unable to combine visibilities from {}", visibilities);
            }
        }
        return new ColumnVisibility();
    }
    
    /**
     * Groups and aggregates fields from the entries in the given document and merges the new group information into the given {@link Groups} instance.
     * @param entry the document entry
     * @param groupFields the fields to group
     * @param aggregateFieldsFactory the aggregate fields factory to use
     * @param groups the {@link Groups} instance to merge newly found groups into
     */
    public static void group(Map.Entry<Key, Document> entry, Set<String> groupFields, AggregatedFields.Factory aggregateFieldsFactory, Groups groups) {
        group(entry, groupFields, aggregateFieldsFactory, groups, null);
    }
    
    /**
     * Groups and aggregates fields from the entries in the given document and merges the new group information into the given {@link Groups} instance. Each
     * field will be mapped using the given model mapping.
     *
     * @param entry                  the document entry
     * @param groupFields            the fields to group
     * @param aggregateFieldsFactory the aggregate fields factory to use
     * @param groups                 the {@link Groups} instance to merge newly found groups into
     * @param reverseModelMappings   the model mappings to apply to any identified fields
     */
    public static void group(Map.Entry<Key,Document> entry, Set<String> groupFields, AggregatedFields.Factory aggregateFieldsFactory, Groups groups,
                    Map<String,String> reverseModelMappings) {
        DocumentGrouper grouper = new DocumentGrouper(entry, groupFields, aggregateFieldsFactory, reverseModelMappings, groups);
        grouper.group();
    }
    
    private final Key documentKey;
    private final Document document;
    private final Set<String> groupFields;
    private final Map<String,String> reverseModelMappings = new HashMap<>();
    private final AggregatedFields.Factory aggregateFieldsFactory;
    
    /**
     * Map of field names (with grouping context) to their {@link GroupingAttribute} counterpart.
     */
    private final Map<String,GroupingAttribute<?>> fieldToGroupingAttribute = Maps.newHashMap();
    
    /**
     * Map of field names to the field names with grouping contexts, in sorted order.
     */
    private final Multimap<String,String> fieldToFieldWithContextMap = TreeMultimap.create();
    
    /**
     * Map of field names (with grouping context) to the priority queue containing entries to be matched in groups.
     */
    private final SortedMap<String,PriorityQueue<String>> fieldToPriorityQueue = new TreeMap<>();
    
    private final Set<String> expandedGroupFields = new LinkedHashSet<>();
    
    /**
     * Map of field names to their corresponding {@link Field} to be aggregated.
     */
    private final Multimap<String,Field> aggregatedFieldMap = ArrayListMultimap.create();
    
    /**
     * Map of grouping attributes to fields used to determine how to identify which fields should be aggregated for a given group.
     */
    private final Multimap<GroupingAttribute<?>,Field> groupAttributesToFields = HashMultimap.create();
    
    /**
     * The groups.
     */
    private final Groups groups;
    
    private DocumentGrouper(Map.Entry<Key, Document> documentEntry, Set<String> groupFields, AggregatedFields.Factory aggregateFieldsFactory, Map<String, String> reverseModelMappings, Groups groups) {
        this.documentKey = documentEntry.getKey();
        this.document = documentEntry.getValue();
        this.groupFields = groupFields;
        this.aggregateFieldsFactory = aggregateFieldsFactory;
        if (reverseModelMappings != null) {
            this.reverseModelMappings.putAll(reverseModelMappings);
        }
        this.groups = groups;
    }
    
    /**
     * Group entries from the current document and aggregates fields if specified.
     */
    private void group() {
        log.trace("apply to {} {}", documentKey, document);
        // If the document contains entries that indicate grouping has already been performed, we are seeing a document that was generated by
        // GroupingIterator.flatten(). No further grouping can occur. Extract the grouping information from the document and merge them into the current groups.
        if(groupsAlreadyCounted()) {
            putPreviousGroupingInGroups();
        } else { // Otherwise, the document contains entries that have not yet been grouped and counted.
            // Index the document entries.
            indexDocumentEntries();
            // Group the document entries.
            groupEntries();
            // Aggregate fields only if there were aggregation fields specified and if any entries for aggregation were found.
            if(aggregateFieldsFactory.hasFieldsToAggregate() && !aggregatedFieldMap.isEmpty()) {
                aggregateEntries();
            }
        }
    }
    
    /**
     * Return whether the document contains entries representing a flattened set of group counts generated by {@link datawave.query.iterator.GroupingIterator}.
     * @return true if the document contains flattened group counts, or false otherwise.
     */
    private boolean groupsAlreadyCounted() {
        return document.getDictionary().keySet().stream().anyMatch(key -> key.startsWith(GROUP_COUNT));
    }
    
    /**
     * Extract grouping information from the current document and add them to the current groups. Each field will be remapped if a reverse-model mapping was
     * supplied.
     */
    @SuppressWarnings("unchecked")
    private void putPreviousGroupingInGroups() {
        // Parse a field from each entry and store them in instanceToFields. The instance indicates which grouping, count, and aggregated values go together.
        Multimap<String,Field> instanceToFields = HashMultimap.create();
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : document.entrySet()) {
            Field field = parseField(entry);
            instanceToFields.put(field.getInstance(), field);
        }
        // For each distinct grouping, parse and write the grouping information to the current groups.
        for (String instance : instanceToFields.keySet()) {
            // The distinct grouping.
            Set<GroupingAttribute<?>> groupingAttributes = new HashSet<>();
            // The aggregated values.
            AggregatedFields aggregatedFields = new AggregatedFields();
            // The total times the grouping was seen.
            int count = 0;
            for (Field field : instanceToFields.get(instance)) {
                // We found the group count.
                if (field.getFieldBase().equals(GROUP_COUNT)) {
                    TypeAttribute<BigDecimal> attribute = (TypeAttribute<BigDecimal>) field.getAttribute();
                    count = attribute.getType().getDelegate().intValue();
                    // We found the sum of an aggregated field.
                } else if (field.getFieldBase().endsWith(FIELD_SUM_SUFFIX)) {
                    TypeAttribute<BigDecimal> attribute = (TypeAttribute<BigDecimal>) field.getAttribute();
                    String fieldName = getMappedFieldName(removeSuffix(field.getFieldBase(), FIELD_SUM_SUFFIX));
                    aggregatedFields.mergeAggregator(SumAggregator.of(fieldName, attribute));
                    // We found the numerator of the average of an aggregated field.
                } else if (field.getFieldBase().endsWith(FIELD_AVERAGE_NUMERATOR_SUFFIX)) {
                    String unmappedFieldName = removeSuffix(field.getFieldBase(), FIELD_AVERAGE_NUMERATOR_SUFFIX);
                    String fieldName = getMappedFieldName(removeSuffix(field.getFieldBase(), FIELD_AVERAGE_NUMERATOR_SUFFIX));
                    // It's possible that the divisor will be stored under a previously unmapped field name. For example, the field ETA from
                    // ETA_AVERAGE_NUMERATOR.1 could be mapped to AG here. Use the original field name (e.g. ETA) to ensure we find the
                    // corresponding divisor (e.g. ETA_AVERAGE_DIVISOR.1) for the numerator.
                    String divisorField = unmappedFieldName + FIELD_AVERAGE_DIVISOR_SUFFIX + "." + field.getInstance();
                    TypeAttribute<BigDecimal> divisorAttribute = (TypeAttribute<BigDecimal>)document.get(divisorField);
                    TypeAttribute<BigDecimal> numeratorAttribute = (TypeAttribute<BigDecimal>) field.getAttribute();
                    aggregatedFields.mergeAggregator(AverageAggregator.of(fieldName, numeratorAttribute, divisorAttribute));
                    // We found the count of an aggregated field.
                } else if (field.getFieldBase().endsWith(FIELD_COUNT_SUFFIX)) {
                    TypeAttribute<BigDecimal> attribute = (TypeAttribute<BigDecimal>) field.getAttribute();
                    String fieldName = getMappedFieldName(removeSuffix(field.getFieldBase(), FIELD_COUNT_SUFFIX));
                    aggregatedFields.mergeAggregator(CountAggregator.of(fieldName, attribute));
                    // We found the min of an aggregated field.
                } else if (field.getFieldBase().endsWith(FIELD_MIN_SUFFIX)) {
                    String fieldName = getMappedFieldName(removeSuffix(field.getFieldBase(), FIELD_MIN_SUFFIX));
                    aggregatedFields.mergeAggregator(MinAggregator.of(fieldName, field.getAttribute()));
                    // We found the max of an aggregated field.
                } else if (field.getFieldBase().endsWith(FIELD_MAX_SUFFIX)) {
                    String fieldName = getMappedFieldName(removeSuffix(field.getFieldBase(), FIELD_MAX_SUFFIX));
                    aggregatedFields.mergeAggregator(MaxAggregator.of(fieldName, field.getAttribute()));
                    // We found a field that is part of the grouping.
                } else if (!field.getFieldBase().endsWith(FIELD_AVERAGE_DIVISOR_SUFFIX)) {
                    Attribute<?> attribute = field.getAttribute();
                    GroupingAttribute<?> newAttribute = new GroupingAttribute<>((Type<?>) attribute.getData(), new Key(field.getFieldBase()), true);
                    newAttribute.setColumnVisibility(attribute.getColumnVisibility());
                    groupingAttributes.add(newAttribute);
                }
            }
            // Create a new group and merge it into the existing groups.
            Group group = new Group(groupingAttributes, count);
            group.setAggregatedFields(aggregatedFields);
            group.addDocumentVisibility(document.getColumnVisibility());
            groups.mergeOrPutGroup(group);
        }
    }
    
    /**
     * Return a substring of the given str without the given suffix.
     * @param str the string
     * @param suffix the suffix
     * @return the string without the suffix
     */
    private String removeSuffix(String str, String suffix) {
        int suffixLength = suffix.length();
        return str.substring(0, str.length() - suffixLength);
    }
    
    /**
     * Iterates the document entries to identify and retain field entries for grouping and aggregation (if specified).
     */
    private void indexDocumentEntries() {
        // Examine each entry in the document.
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : document.entrySet()) {
            // Parse the field and retain the relevant field name information and its corresponding attribute value.
            Field field = parseField(entry);
            Attribute<?> attribute = field.getAttribute();
    
            // We've found a field that may be part of a grouping.
            if (groupFields.contains(field.getFieldBase())) {
                expandedGroupFields.add(field.getFieldAndGroup());
                // We've found a multi-value entry.
                if (attribute.getData() instanceof Collection<?>) {
                    // Sort and create grouping attributes for each value.
                    SortedSetMultimap<Type<?>,GroupingAttribute<?>> attrSortedMap = TreeMultimap.create();
                    for (Object typeAttribute : ((Collection<?>) attribute.getData())) {
                        Type<?> type = ((TypeAttribute<?>) typeAttribute).getType();
                        GroupingAttribute<?> newAttribute = new GroupingAttribute<>(type, new Key(field.getFieldAndGroup()), true);
                        newAttribute.setColumnVisibility(attribute.getColumnVisibility());
                        attrSortedMap.put(type, newAttribute);
                    }
    
                    log.trace("Found multiple values for grouping field {} : {} ", field.getFullField(), attrSortedMap);
    
                    // Generate an artificial incrementing grouping context for the field for each value that it has. It is assumed that the field does not have
                    // a grouping context already (e.g. RECORD instead of RECORD.1).
                    int i = 0;
                    for (Map.Entry<Type<?>,GroupingAttribute<?>> sortedEntry : attrSortedMap.entries()) {
                        String fieldNameWithContext = field.getFullField() + "." + i++;
                        fieldToGroupingAttribute.put(fieldNameWithContext, sortedEntry.getValue());
                        groupAttributesToFields.put(sortedEntry.getValue(), field);
                        fieldToFieldWithContextMap.put(field.getFieldAndGroup(), fieldNameWithContext);
                        addFieldToQueue(field.getFieldAndGroup(), fieldNameWithContext);
                    }
                } else {
                    // Handle single-valued entries.
                    GroupingAttribute<?> newAttribute = new GroupingAttribute<>((Type<?>) attribute.getData(), new Key(field.getFieldAndGroup()), true);
                    newAttribute.setColumnVisibility(attribute.getColumnVisibility());
                    fieldToGroupingAttribute.put(field.getFullField(), newAttribute);
                    groupAttributesToFields.put(newAttribute, field);
                    fieldToFieldWithContextMap.put(field.getFieldAndGroup(), field.getFullField());
                    addFieldToQueue(field.getFieldAndGroup(), field.getFullField());
                    log.trace("Found single value for grouping field {} : {}", field.getFullField(), newAttribute);
                }
            }
            // We've found a field that may need to be aggregated.
            if (aggregateFieldsFactory.isFieldToAggregate(field.getFieldBase())) {
                aggregatedFieldMap.put(field.getFieldBase(), field);
            }
        }
    }
    
    /**
     * Add the given value to priority queue for the given key, or create a new priority queue if one does not exist.
     * @param key the key
     * @param value the value
     */
    private void addFieldToQueue(String key, String value) {
        if (fieldToPriorityQueue.containsKey(key)) {
            fieldToPriorityQueue.get(key).add(value);
        } else {
            PriorityQueue<String> queue = new PriorityQueue<>();
            queue.add(value);
            fieldToPriorityQueue.put(key, queue);
        }
    }
    
    /**
     * Identify groupings that contain at least one value from each designated group-by field.
     */
    private void groupEntries() {
        int maxValueSize = 0;
        for (PriorityQueue<String> queue : fieldToPriorityQueue.values()) {
            maxValueSize = Math.max(maxValueSize, queue.size());
        }
        // Grab an attribute for each group-by field and create a grouping.
        for (int i = 0; i < maxValueSize; i++) {
            Set<GroupingAttribute<?>> attributes = new HashSet<>();
            for (String groupField : expandedGroupFields) {
                if (fieldToPriorityQueue.containsKey(groupField)) {
                    PriorityQueue<String> queue = fieldToPriorityQueue.get(groupField);
                    if (!queue.isEmpty()) {
                        String field = queue.poll();
                        attributes.add(fieldToGroupingAttribute.get(field));
                    } else {
                        // If the queue is empty, there are no more intersections available from this field.
                        break;
                    }
                } else {
                    // If there is no matching priority queue, we cannot generate anymore intersections for this group field.
                    break;
                }
            }
            
            // Only count groupings that have a value from each specified group field.
            if (attributes.size() == expandedGroupFields.size()) {
                Group group = groups.getGroup(attributes);
                // Create a group for the grouping if one does not already exist.
                if (group == null) {
                    group = new Group(attributes);
                    groups.putGroup(group);
                    group.setAggregatedFields(aggregateFieldsFactory.newInstance());
                }
                // Add the visibilities of each attribute in the grouping for combination later, and increment the count for how many times this distinct
                // grouping was seen.
                group.addAttributeVisibilities(attributes);
                group.incrementCount();
                group.addDocumentVisibility(document.getColumnVisibility());
            }
        }
    }
    
    /**
     * Aggregate any specified aggregation fields to the groupings that they should be included with.
     */
    private void aggregateEntries() {
        // Aggregate fields for each group.
        for (Group group : groups.getGroups()) {
            // Determine the matching patterns to use to identify fields that should be aggregated for the current group.
            Set<Field> groupFields = group.getAttributes().stream().map(groupAttributesToFields::get).flatMap(Collection::stream).collect(Collectors.toSet());
            FieldMatcher fieldMatcher = getFieldMatcher(groupFields);
            AggregatedFields aggregatedFields = group.getAggregatedFields();
            // For each field to aggregate, find the entries that should be included in the current group and aggregate them.
            for (String aggregationTargetField : aggregatedFields.getFieldsToAggregate()) {
                if (aggregatedFieldMap.containsKey(aggregationTargetField)) {
                    List<Attribute<?>> matches = fieldMatcher.getMatches(aggregatedFieldMap.get(aggregationTargetField)).stream().map(Field::getAttribute)
                                    .collect(Collectors.toList());
                    aggregatedFields.aggregateAll(aggregationTargetField, matches);
                }
            }
        }
    }
    
    /**
     * Return a {@link FieldMatcher} that will filter field entries to those that are a match.
     * @param fields the group fields to generate a field matcher for
     * @return a field matcher
     */
    private FieldMatcher getFieldMatcher(Set<Field> fields) {
        FieldMatcher fieldMatcher = new FieldMatcher();
        for (Field field : fields) {
            // If a group field is seen with no field group or field instance, e.g. RECORD and not RECORD.FOO.1 or RECORD.1, then all entries of target
            // aggregation fields should be aggregated for the grouping that the group field is in, and no further refinement on which fields to match needs to
            // be done.
            if (!field.hasGroup() && !field.hasInstance()) {
                fieldMatcher.matchAll();
                return fieldMatcher;
            } else if (!field.hasGroup()) {
                // If a group field is seen with an instance only, e.g. RECORD.1, match against any target aggregation fields that have the same instance, e.g.
                // AGE.1.
                fieldMatcher.matchInstance(field.getInstance());
            } else {
                // If a group field is seen with a group and instance, e.g. RECORD.FOO.1, match against any target aggregation fields that have the same group
                // and instance, AGE.FOO.1.
                fieldMatcher.matchGroupAndInstance(field.getGroup(), field.getInstance());
            }
        }
        return fieldMatcher;
    }
    
    /**
     * Parses the relevant information from the given entry and returns a {@link Field} that contains the field name, group, instance, and the value. It is
     * assumed that the entry's key will have the format {@code <NAME>}, {@code <NAME>.<INSTANCE>} or {@code <NAME>.<GROUP>...<INSTANCE>}.
     *
     * @param entry
     *            the document entry
     * @return the field entry.
     */
    private Field parseField(Map.Entry<String,Attribute<?>> entry) {
        String field = entry.getKey();
        String name = field;
        String group = null;
        String instance = null;
    
        int firstPeriod = field.indexOf('.');
        // If the field name contains at least one period, the field's format is either <NAME>.<INSTANCE> or <NAME>.<GROUP>...<INSTANCE>
        if (firstPeriod != -1) {
            // The field name is everything before the first period.
            name = field.substring(0, firstPeriod);
    
            int secondPeriod = field.indexOf(".", firstPeriod + 1);
            // If a second period is present, we know that field's format is <NAME>.<GROUP>...<INSTANCE>
            if (secondPeriod != -1) {
                // Parse the group from the substring directly following the name.
                group = field.substring(firstPeriod + 1, secondPeriod);
                // Parse the instance from the substring after the last period.
                instance = field.substring(field.lastIndexOf(".") + 1);
            } else {
                // If there is no second period present, the field's format is <NAME>.<INSTANCE>.
                instance = field.substring(firstPeriod + 1);
            }
        }
        
        // Map the field name.
        name = getMappedFieldName(name);
        
        return new Field(name, group, instance, entry.getValue());
    }
    
    /**
     * Get the corresponding model mapping for the field. If model mappings have not been provided, the original field will be returned.
     *
     * @param field
     *            the field to map
     * @return the mapped field
     */
    private String getMappedFieldName(String field) {
        return reverseModelMappings.getOrDefault(field, field);
    }
    
    /**
     * Represents an entry from a document with a field name broken down into its name, group, and instance, and the entry's attribute.
     */
    static class Field {
        
        private static final Joiner joiner = Joiner.on(".").skipNulls();
    
        private final String fieldBase;
        private final String group;
        private final String instance;
        private final Attribute<?> attribute;
        
        private final String fullField;
        private final String fieldAndGroup;
    
        public Field(String fieldBase, String group, String instance, Attribute<?> attribute) {
            this.fullField = joiner.join(fieldBase, group, instance);
            this.fieldAndGroup = joiner.join(fieldBase, group);
            this.fieldBase = fieldBase;
            this.group = group;
            this.instance = instance;
            this.attribute = attribute;
        }
    
        /**
         * Return the full field with the field base, group (if present), and instance (if present) in the format {@code <BASE>.<GROUP>.<INSTANCE>}.
         * @return the full field name.
         */
        public String getFullField() {
            return fullField;
        }
        
        public String getFieldAndGroup() {
            return fieldAndGroup;
        }
    
        /**
         * Return the field base.
         * @return the field base
         */
        public String getFieldBase() {
            return fieldBase;
        }
    
        /**
         * Return whether this field has a group as part of its name.
         * @return true if this field has a group, or false otherwise
         */
        public boolean hasGroup() {
            return group != null;
        }
    
        /**
         * Return the field's group, or null if the field does not have a group.
         * @return the group
         */
        public String getGroup() {
            return group;
        }
    
        /**
         * Return the field's instance, or null if the field does not have an instance.
         * @return the instance
         */
        public String getInstance() {
            return instance;
        }
    
        /**
         * Return whether this field has an instance as part of its name.
         * @return true if this field has an instance, or false otherwise
         */
        public boolean hasInstance() {
            return instance != null;
        }
    
        /**
         * Return this field's attribute
         * @return the attribute
         */
        public Attribute<?> getAttribute() {
            return attribute;
        }
    
        @Override
        public String toString() {
            return new ToStringBuilder(this).append("fieldName", fieldBase).append("group", group).append("instance", instance)
                            .append("attribute", attribute).toString();
        }
    }
    
    /**
     * Represents a collection of filters that should be used to identify field entries that are valid aggregation targets for a particular grouping.
     */
    private static class FieldMatcher {
    
        private final List<Predicate<Field>> filters = new ArrayList<>();
        
        public void matchAll() {
            filters.clear();
            filters.add(field -> true);
        }
    
        public void matchInstance(String instance) {
            filters.add(field -> field.getInstance().equals(instance));
        }
    
        public void matchGroupAndInstance(String group, String instance) {
            filters.add(field -> field.getGroup().equals(group) && field.getInstance().equals(instance));
        }
        
        public boolean matches(Field field) {
            return filters.stream().anyMatch(predicate -> predicate.test(field));
        }
    
        public List<Field> getMatches(Collection<Field> fields) {
            return fields.stream().filter(this::matches).collect(Collectors.toList());
        }
    }
}
