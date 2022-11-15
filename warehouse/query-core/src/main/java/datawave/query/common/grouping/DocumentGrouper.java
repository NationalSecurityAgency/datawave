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
import java.util.Comparator;
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
 * Provides functionality commonly needed to group documents (regardless if done server or client side).
 *
 * This class and its methods aren't static so that we don't run into concurrency issues, although all required state should be passed into the individual
 * methods and not kept in this class. Calling classes could extend this class to inherit the methods, but the state still shouldn't be inherited because not
 * all callers will be able to easily extend this class if they already/need to extend other parents.
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
    
    private static final Comparator<Field> FIELD_COMPARATOR = Comparator.comparing(Field::getFieldName).thenComparing(Field::getGroup)
                    .thenComparing(Field::getInstance);
    
    
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
    
    public static void getGroups(Map.Entry<Key, Document> entry, Set<String> groupFields, AggregatedFields.Factory aggregateFieldsFactory, Groups groups) {
        getGroups(entry, groupFields, aggregateFieldsFactory, null, groups);
    }
    
    public static void getGroups(Map.Entry<Key,Document> entry, Set<String> groupFields, AggregatedFields.Factory aggregateFieldsFactory, Map<String,String> reverseModelMappings, Groups groups) {
        DocumentGrouper grouper = new DocumentGrouper(entry, groupFields, aggregateFieldsFactory, reverseModelMappings, groups);
        grouper.group();
    }
    
    private final Key documentKey;
    private final Document document;
    private final Set<String> groupFields;
    private final Map<String,String> reverseModelMappings = new HashMap<>();
    private final AggregatedFields.Factory aggregateFieldsFactory;
    
    // Mapping of field name (with grouping context) to value attribute
    private final Map<String,GroupingAttribute<?>> fieldToGroupingAttributeMap = Maps.newHashMap();
    
    // Mapping of fields to their instance.
    private final Multimap<String,String> fieldToFieldWithContextMap = TreeMultimap.create();
    private final SortedMap<String,PriorityQueue<String>> fieldToPriorityQueue = new TreeMap<>();
    private final Set<String> expandedGroupFields = new LinkedHashSet<>();
    private final Multimap<String,Field> aggregatedFieldMap = ArrayListMultimap.create();
    private final Multimap<GroupingAttribute<?>,Field> groupAttributesToFields = HashMultimap.create();
    
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
    
    public void group() {
        log.trace("apply to {} {}", documentKey, document);
        // If the document contains entries representing group counts, it was generated via GroupingIterator.flatten(). We do not need to recount the grouping
        // entries. We can simply put the group counts from the document directly into the counting map.
        if(groupsAlreadyCounted()) {
            putPrevCountsInCountingMap();
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
    
    @SuppressWarnings("unchecked")
    private void putPrevCountsInCountingMap() {
        Multimap<String,Field> instanceToFields = HashMultimap.create();
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : document.entrySet()) {
            Field field = parseField(entry);
            instanceToFields.put(field.getInstance(), field);
        }
        for (String instance : instanceToFields.keySet()) {
            Set<GroupingAttribute<?>> groupingAttributes = new HashSet<>();
            AggregatedFields aggregatedFields = new AggregatedFields();
            int count = 0;
            for (Field field : instanceToFields.get(instance)) {
                // We found the group count.
                if (field.getFieldName().equals(GROUP_COUNT)) {
                    TypeAttribute<BigDecimal> attribute = (TypeAttribute<BigDecimal>) field.getAttribute();
                    count = attribute.getType().getDelegate().intValue();
                    // We found the sum of an aggregated field.
                } else if (field.getFieldName().endsWith(FIELD_SUM_SUFFIX)) {
                    TypeAttribute<BigDecimal> attribute = (TypeAttribute<BigDecimal>) field.getAttribute();
                    String fieldName = getMappedFieldName(removeSuffix(field.getFieldName(), FIELD_SUM_SUFFIX));
                    aggregatedFields.mergeAggregator(SumAggregator.of(fieldName, attribute));
                    // We found the numerator of the average of an aggregated field.
                } else if (field.getFieldName().endsWith(FIELD_AVERAGE_NUMERATOR_SUFFIX)) {
                    String unmappedFieldName = removeSuffix(field.getFieldName(), FIELD_AVERAGE_NUMERATOR_SUFFIX);
                    String fieldName = getMappedFieldName(removeSuffix(field.getFieldName(), FIELD_AVERAGE_NUMERATOR_SUFFIX));
                    // It's possible that the divisor will be stored under a previously unmapped field name. Use the original field name to ensure we find the
                    // corresponding divisor for the numerator.
                    String divisorField = unmappedFieldName + FIELD_AVERAGE_DIVISOR_SUFFIX + "." + field.getInstance();
                    TypeAttribute<BigDecimal> divisorAttribute = (TypeAttribute<BigDecimal>)document.get(divisorField);
                    TypeAttribute<BigDecimal> numeratorAttribute = (TypeAttribute<BigDecimal>) field.getAttribute();
                    aggregatedFields.mergeAggregator(AverageAggregator.of(fieldName, numeratorAttribute, divisorAttribute));
                } else if (field.getFieldName().endsWith(FIELD_COUNT_SUFFIX)) {
                    TypeAttribute<BigDecimal> attribute = (TypeAttribute<BigDecimal>) field.getAttribute();
                    String fieldName = getMappedFieldName(removeSuffix(field.getFieldName(), FIELD_COUNT_SUFFIX));
                    aggregatedFields.mergeAggregator(CountAggregator.of(fieldName, attribute));
                    // We found the min of an aggregated field.
                } else if (field.getFieldName().endsWith(FIELD_MIN_SUFFIX)) {
                    String fieldName = getMappedFieldName(removeSuffix(field.getFieldName(), FIELD_MIN_SUFFIX));
                    aggregatedFields.mergeAggregator(MinAggregator.of(fieldName, field.getAttribute()));
                    // We found the max of an aggregated field.
                } else if (field.getFieldName().endsWith(FIELD_MAX_SUFFIX)) {
                    String fieldName = getMappedFieldName(removeSuffix(field.getFieldName(), FIELD_MAX_SUFFIX));
                    aggregatedFields.mergeAggregator(MaxAggregator.of(fieldName, field.getAttribute()));
                    // We found a field that is part of the group.
                } else if (!field.getFieldName().endsWith(FIELD_AVERAGE_DIVISOR_SUFFIX)) {
                    Attribute<?> attribute = field.getAttribute();
                    GroupingAttribute<?> newAttribute = new GroupingAttribute<>((Type<?>) attribute.getData(), new Key(field.getFieldName()), true);
                    newAttribute.setColumnVisibility(attribute.getColumnVisibility());
                    groupingAttributes.add(newAttribute);
                }
            }
            Group group = new Group(groupingAttributes, count);
            group.setAggregatedFields(aggregatedFields);
            group.addDocumentVisibility(document.getColumnVisibility());
            groups.mergeOrPutGroup(group);
        }
    }
    
    private String removeSuffix(String str, String suffix) {
        int suffixLength = suffix.length();
        return str.substring(0, str.length() - suffixLength);
    }
    
    private void indexDocumentEntries() {
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : document.entrySet()) {
            Field field = parseField(entry);
            Attribute<?> attribute = field.getAttribute();
    
            if (groupFields.contains(field.getFieldName())) {
                expandedGroupFields.add(field.getFieldAndGroup());
                
                if (attribute.getData() instanceof Collection<?>) {
                    // This handles multivalued entries that do not have grouping context
                    // Create GroupingTypeAttribute and put in ordered map ordered on the attribute type
                    SortedSetMultimap<Type<?>,GroupingAttribute<?>> attrSortedMap = TreeMultimap.create();
                    for (Object typeAttribute : ((Collection<?>) attribute.getData())) {
                        Type<?> type = ((TypeAttribute<?>) typeAttribute).getType();
                        GroupingAttribute<?> newAttribute = new GroupingAttribute<>(type, new Key(field.getFieldAndGroup()), true);
                        newAttribute.setColumnVisibility(attribute.getColumnVisibility());
                        attrSortedMap.put(type, newAttribute);
                    }
                    
                    log.trace("Found multiple values for grouping field {} : {} ", field.getFullField(), attrSortedMap);
                    
                    // Add GroupingTypeAttribute to fieldMap with a grouping context that is based on ordered attribute type
                    int i = 0;
                    for (Map.Entry<Type<?>,GroupingAttribute<?>> sortedEntry : attrSortedMap.entries()) {
                        String fieldNameWithContext = field.getFullField() + "." + i++;
                        fieldToGroupingAttributeMap.put(fieldNameWithContext, sortedEntry.getValue());
                        groupAttributesToFields.put(sortedEntry.getValue(), field);
                        fieldToFieldWithContextMap.put(field.getFieldAndGroup(), fieldNameWithContext);
                        addFieldToQueue(field.getFieldAndGroup(), fieldNameWithContext);
                    }
                } else {
                    // Handle single-valued entries.
                    GroupingAttribute<?> newAttribute = new GroupingAttribute<>((Type<?>) attribute.getData(), new Key(field.getFieldAndGroup()), true);
                    newAttribute.setColumnVisibility(attribute.getColumnVisibility());
                    fieldToGroupingAttributeMap.put(field.getFullField(), newAttribute);
                    groupAttributesToFields.put(newAttribute, field);
                    fieldToFieldWithContextMap.put(field.getFieldAndGroup(), field.getFullField());
                    addFieldToQueue(field.getFieldAndGroup(), field.getFullField());
                    log.trace("Found single value for grouping field {} : {}", field.getFullField(), newAttribute);
                }
            } else if (aggregateFieldsFactory.isFieldToAggregate(field.getFieldName())) {
                aggregatedFieldMap.put(field.getFieldName(), field);
            }
        }
        log.trace("fieldToPriorityQueue: {}", fieldToPriorityQueue);
        log.trace("fieldMap: {}", fieldToGroupingAttributeMap);
    }
    
    private void addFieldToQueue(String key, String queueValue) {
        if (fieldToPriorityQueue.containsKey(key)) {
            fieldToPriorityQueue.get(key).add(queueValue);
        } else {
            PriorityQueue<String> queue = new PriorityQueue<>();
            queue.add(queueValue);
            fieldToPriorityQueue.put(key, queue);
        }
    }
    
    private void groupEntries() {
        int maxValueSize = 0;
        for (PriorityQueue<String> queue : fieldToPriorityQueue.values()) {
            maxValueSize = Math.max(maxValueSize, queue.size());
        }
        for (int i = 0; i < maxValueSize; i++) {
            Set<GroupingAttribute<?>> attributes = new HashSet<>();
            for (String groupField : expandedGroupFields) {
                if (fieldToPriorityQueue.containsKey(groupField)) {
                    PriorityQueue<String> queue = fieldToPriorityQueue.get(groupField);
                    if (!queue.isEmpty()) {
                        String field = queue.poll();
                        attributes.add(fieldToGroupingAttributeMap.get(field));
                    } else {
                        // If the queue is empty, there are no more intersections available from this field.
                        break;
                    }
                } else {
                    // If there is matching priority queue, we cannot generate anymore intersections for this group field.
                    break;
                }
            }
            
            // Only count groupings that have a value from each specified group field.
            if (attributes.size() == expandedGroupFields.size()) {
                Group group;
                if (groups.containsGroup(attributes)) {
                    group = groups.getGroup(attributes);
                } else {
                    group = new Group(attributes);
                    groups.putGroup(group);
                    group.setAggregatedFields(aggregateFieldsFactory.newInstance());
                }
                group.addAttributeVisibilities(attributes);
                group.addDocumentVisibility(document.getColumnVisibility());
                group.incrementCount();
            }
        }
    }
    
    private void aggregateEntries() {
        for (Group group : groups.getGroups()) {
            Set<Field> groupFields = group.getAttributes().stream().map(groupAttributesToFields::get).flatMap(Collection::stream).collect(Collectors.toSet());
            FieldMatcher fieldMatcher = getFieldMatcher(groupFields);
            AggregatedFields aggregatedFields = group.getAggregatedFields();
            for (String aggregationTargetField : aggregatedFields.getFieldsToAggregate()) {
                List<Attribute<?>> matches = fieldMatcher.getMatches(aggregatedFieldMap.get(aggregationTargetField)).stream().map(Field::getAttribute)
                                .collect(Collectors.toList());
                aggregatedFields.aggregateAll(aggregationTargetField, matches);
            }
            group.setAggregatedFields(aggregatedFields);
        }
    }
    
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
                fieldMatcher.matchInstance(field.getInstance());
            } else {
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
    
    static class Field {
        
        private static final Joiner joiner = Joiner.on(".").skipNulls();
        
        private final String fullField;
        private final String groupAndInstance;
        private final String fieldAndGroup;
        private final String fieldName;
        private final String group;
        private final String instance;
        private final Attribute<?> attribute;
    
        public Field(String fieldName, String group, String instance, Attribute<?> attribute) {
            this.fullField = joiner.join(fieldName, group, instance);
            this.groupAndInstance = joiner.join(group, instance);
            this.fieldAndGroup = joiner.join(fieldName, group);
            this.fieldName = fieldName;
            this.group = group;
            this.instance = instance;
            this.attribute = attribute;
        }
        
        public String getFullField() {
            return fullField;
        }
        
        /**
         * Returns the group and instance (if present) of this field entry. The string may be one of the following:
         * <ul>
         * <li>An empty string if this field entry has a null group and a null instance.</li>
         * <li>The instance alone if this field entry has a null group.</li>
         * <li>The group and instance in the format {@code <GROUP>.<INSTANCE>}.</li>
         * </ul>
         *
         * @return the group and instance
         */
        public String getGroupAndInstance() {
            return groupAndInstance;
        }
        
        public String getFieldAndGroup() {
            return fieldAndGroup;
        }
        
        public String getFieldName() {
            return fieldName;
        }
        
        public boolean hasGroup() {
            return group != null;
        }
        
        public String getGroup() {
            return group;
        }
        
        public String getInstance() {
            return instance;
        }
        
        public boolean hasInstance() {
            return instance != null;
        }
        
        public Attribute<?> getAttribute() {
            return attribute;
        }
    
        @Override
        public String toString() {
            return new ToStringBuilder(this).append("fieldName", fieldName).append("group", group).append("instance", instance)
                            .append("attribute", attribute).toString();
        }
    }
    
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
