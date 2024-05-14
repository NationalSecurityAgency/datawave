package datawave.query.common.grouping;

import static org.slf4j.LoggerFactory.getLogger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.javatuples.Pair;
import org.slf4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;

/**
 * <P>
 * This class provides the primary functionality needed to group documents and aggregate field values within identified groups (regardless if done server or
 * client-side).
 * </P>
 * <H2>Grouping</H2>
 * <P>
 * Grouping fields across documents will result in groupings of distinct value groupings for each specified field to group, as well as the total number of times
 * each particular grouping combination was seen. Fields to group by can be specified by the following options:
 * <ul>
 * <li>The LUCENE function {@code #GROUPBY()}.</li>
 * <li>The JEXL function {@code f:groupby()}.</li>
 * <li>The query parameter {@code group.fields}.</li>
 * </ul>
 * Groupings may be of any size that encompass none, some, or all of the target group fields. If a document has no entries for any of the target group fields,
 * it will be grouped as part of an 'empty' grouping, and all target aggregation entries will be aggregated to the empty grouping. The count for 'empty' groups
 * will be the same as the number of documents seen without any group-by fields. Values are grouped together based on the format of each document entry's key,
 * which may have one of the following formats:
 * <ul>
 * <li>{@code <FIELD>}</li>
 * <li>{@code <FIELD>.<INSTANCE>}</li>
 * <li>{@code <FIELD>.<CONTEXT>...<INSTANCE>}</li>
 * </ul>
 * Values of fields with the same context and instance are considered direct one-to-one grouping matches, and will be placed within the same groupings. Direct
 * matches cannot be determined for values of fields that do not have a context, and as such they will be combined with each possible grouping, effectively a
 * cartesian product. Direct matches are prioritized and found first before indirect matches are combined with them. <\P>
 * <H2>Aggregation</H2>
 * <P>
 * Once all valid groupings have been identified and counted, aggregation can be performed on the values of any specified fields for each grouping. The
 * aggregation fields can differ from the group-by fields. The following aggregation operations are supported:
 * </P>
 * <P>
 * </P>
 * <strong>SUM</strong>: Sum up all the values for specified fields across groupings. This operation is limited to fields with numerical values. Fields may be
 * specified via:
 * <ul>
 * <li>The LUCENE function {@code #SUM()}.</li>
 * <li>The JEXL function {@code f:sum()}.</li>
 * <li>The query parameter {@code sum.fields}.</li>
 * </ul>
 * <strong>MAX</strong>: Find the max values for specified fields across groupings. Fields may be specified via:
 * <ul>
 * <li>The LUCENE function {@code #MAX()}.</li>
 * <li>The JEXL function {@code f:max()}.</li>
 * <li>The query parameter {@code max.fields}.</li>
 * </ul>
 * <strong>MIN</strong>: Find the min values for specified fields across groupings. Fields may be specified via:
 * <ul>
 * <li>The LUCENE function {@code #MIN()}.</li>
 * <li>The JEXL function {@code f:min()}.</li>
 * <li>The query parameter {@code min.fields}.</li>
 * </ul>
 * <strong>COUNT</strong>: Count the number of times values were seen for specified fields across groupings. Fields may be specified via:
 * <ul>
 * <li>The LUCENE function {@code #COUNT()}.</li>
 * <li>The JEXL function {@code f:count()}.</li>
 * <li>The query parameter {@code count.fields}.</li>
 * </ul>
 * <strong>AVERAGE</strong>: Find the average of all values for specified fields across groupings. This operation is limited to fields with numerical values.
 * Fields may be specified via:
 * <ul>
 * <li>The LUCENE function {@code #AVERAGE()}.</li>
 * <li>The JEXL function {@code f:average()}.</li>
 * <li>The query parameter {@code average.fields}.</li>
 * </ul>
 * <H2>Model Mapping Notes
 * <H2>
 * <P>
 * It is possible to supply a mapping model mappings derived from the query model. If supplied, the field names of entries will be mapped to their respective
 * root model mapping (if one exists) before they are grouped or aggregated. It is important to note that it is possible for multiple fields in a document to be
 * mapped to the same root model mapping. In this case, if multiple fields with the same value in a document are mapped to the same root model mapping, they
 * will be considered equivalent datum points, and only one instance of the field and value will be used when grouping and aggregating in order to prevent
 * duplicate counts. As an example, given a document with the following three entries ({@code field -> value}):
 * <ul>
 * <li>{@code AGE -> 23}</li>
 * <li>{@code ETA -> 23}</li>
 * <li>{@code ETA -> 10}</li>
 * </ul>
 * And the following model mapping:
 * <ul>
 * <li>{@code AGE -> AG}</li>
 * <li>{@code ETA -> AG}</li>
 * </ul>
 * </P>
 * Then, after applying model mapping we would have two instances of {@code AG -> 23} and one instance of {@code AG -> 10}. Only one instance of the
 * {@code AG -> 23} field-value pairing will be counted towards grouping and aggregation along with {@code AG -> 10}, and the remaining duplicate of
 * {@code AG -> 23} will be disregarded.
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
     * Groups and aggregates fields from the entries in the given document and merges the new group information into the given {@link Groups} instance.
     *
     * @param entry
     *            the document entry
     * @param groupFields
     *            the fields to group and aggregate
     * @param groups
     *            the {@link Groups} instance to merge newly found groups into
     */
    public static void group(Map.Entry<Key,Document> entry, GroupFields groupFields, Groups groups) {
        DocumentGrouper documentGrouper = new DocumentGrouper(entry, groupFields, groups);
        documentGrouper.group();
    }

    private final Key documentKey;
    private final Document document;
    private final Set<String> groupFields;
    private final Map<String,String> reverseModelMappings;
    private final FieldAggregator.Factory fieldAggregatorFactory;

    private final Groups groups;
    private final Groups currentGroups = new Groups();
    private final FieldIndex groupFieldsIndex = new FieldIndex(false); // Do not allow null attributes when indexing fields to group by.
    private final FieldIndex aggregateFieldsIndex = new FieldIndex(true); // Allow null attributes when indexing fields to aggregate.
    private final Multimap<Pair<String,String>,Grouping> groupingContextAndInstancesSeenForGroups = HashMultimap.create();
    private final int maxGroupSize;

    private DocumentGrouper(Map.Entry<Key,Document> documentEntry, GroupFields groupFields, Groups groups) {
        this.documentKey = documentEntry.getKey();
        this.document = documentEntry.getValue();
        this.groupFields = groupFields.getGroupByFields();
        this.fieldAggregatorFactory = groupFields.getFieldAggregatorFactory();
        this.reverseModelMappings = groupFields.getReverseModelMap();
        this.groups = groups;
        this.maxGroupSize = this.groupFields.size();
    }

    /**
     * Identify valid groups in the given document and aggregate relevant events to those groups.
     */
    private void group() {
        log.trace("apply to {} {}", documentKey, document);
        // If the document contains entries that indicate grouping has already been performed, we are seeing a document that was generated by
        // GroupingIterator.flatten(). No further grouping can occur. Extract the grouping information from the document and merge them into the current groups.
        if (isDocumentAlreadyGrouped()) {
            extractGroupsFromDocument();
        } else { // Otherwise, the document contains entries that have not yet been grouped and counted.
            // Index the document entries.
            indexDocumentEntries();
            // Group the document entries.
            groupEntries();
            // Aggregate fields only if there were aggregation fields specified and if any entries for aggregation were found.
            if (fieldAggregatorFactory.hasFieldsToAggregate() && !aggregateFieldsIndex.isEmpty()) {
                aggregateEntries();
            }

            // Merge the groups and aggregations we found in this particular group-by operation into the groups passed by the user. The separation is required
            // to ensure that any grouping and aggregation done in this session was applied only to the current document.
            this.groups.mergeAll(currentGroups);
        }
    }

    /**
     * Return whether the document contains entries representing a flattened set of group counts generated by {@link datawave.query.iterator.GroupingIterator}.
     *
     * @return true if the document contains flattened group counts, or false otherwise.
     */
    private boolean isDocumentAlreadyGrouped() {
        return document.getDictionary().keySet().stream().anyMatch(key -> key.startsWith(GROUP_COUNT));
    }

    /**
     * Extract grouping information from the current document and add them to the current groups. Each field will be remapped if a reverse-model mapping was
     * supplied.
     */
    @SuppressWarnings("unchecked")
    private void extractGroupsFromDocument() {
        // Parse a field from each entry and store them in instanceToFields. The id indicates which grouping, count, and aggregated values go together.
        Multimap<String,Field> idToFields = HashMultimap.create();
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : document.entrySet()) {
            Field field = parseField(entry);
            idToFields.put(field.getInstance(), field);
        }
        // For each distinct grouping, parse and write the grouping information to the current groups.
        for (String instance : idToFields.keySet()) {
            // The distinct grouping.
            Grouping grouping = new Grouping();
            // The aggregated values.
            FieldAggregator fieldAggregator = new FieldAggregator();
            // The total times the grouping was seen.
            int count = 0;
            for (Field field : idToFields.get(instance)) {
                // We found the group count.
                if (field.getBase().equals(GROUP_COUNT)) {
                    TypeAttribute<BigDecimal> attribute = (TypeAttribute<BigDecimal>) field.getAttribute();
                    count = attribute.getType().getDelegate().intValue();
                    // We found the sum of an aggregated field.
                } else if (field.getBase().endsWith(FIELD_SUM_SUFFIX)) {
                    TypeAttribute<BigDecimal> attribute = (TypeAttribute<BigDecimal>) field.getAttribute();
                    String fieldName = removeSuffix(field.getBase(), FIELD_SUM_SUFFIX);
                    fieldAggregator.mergeAggregator(SumAggregator.of(fieldName, attribute));
                    // We found the numerator of the average of an aggregated field.
                } else if (field.getBase().endsWith(FIELD_AVERAGE_NUMERATOR_SUFFIX)) {
                    String unmappedFieldName = removeSuffix(field.getBase(), FIELD_AVERAGE_NUMERATOR_SUFFIX);
                    String fieldName = removeSuffix(field.getBase(), FIELD_AVERAGE_NUMERATOR_SUFFIX);
                    // It's possible that the divisor will be stored under a previously unmapped field name. For example, the field ETA from
                    // ETA_AVERAGE_NUMERATOR.1 could be mapped to AG here. Use the original field name (e.g. ETA) to ensure we find the
                    // corresponding divisor (e.g. ETA_AVERAGE_DIVISOR.1) for the numerator.
                    String divisorField = unmappedFieldName + FIELD_AVERAGE_DIVISOR_SUFFIX + "." + field.getInstance();
                    TypeAttribute<BigDecimal> divisorAttribute = (TypeAttribute<BigDecimal>) document.get(divisorField);
                    TypeAttribute<BigDecimal> numeratorAttribute = (TypeAttribute<BigDecimal>) field.getAttribute();
                    fieldAggregator.mergeAggregator(AverageAggregator.of(fieldName, numeratorAttribute, divisorAttribute));
                    // We found the count of an aggregated field.
                } else if (field.getBase().endsWith(FIELD_COUNT_SUFFIX)) {
                    TypeAttribute<BigDecimal> attribute = (TypeAttribute<BigDecimal>) field.getAttribute();
                    String fieldName = removeSuffix(field.getBase(), FIELD_COUNT_SUFFIX);
                    fieldAggregator.mergeAggregator(CountAggregator.of(fieldName, attribute));
                    // We found the min of an aggregated field.
                } else if (field.getBase().endsWith(FIELD_MIN_SUFFIX)) {
                    String fieldName = removeSuffix(field.getBase(), FIELD_MIN_SUFFIX);
                    fieldAggregator.mergeAggregator(MinAggregator.of(fieldName, field.getAttribute()));
                    // We found the max of an aggregated field.
                } else if (field.getBase().endsWith(FIELD_MAX_SUFFIX)) {
                    String fieldName = removeSuffix(field.getBase(), FIELD_MAX_SUFFIX);
                    fieldAggregator.mergeAggregator(MaxAggregator.of(fieldName, field.getAttribute()));
                    // We found a field that is part of the grouping.
                } else if (!field.getBase().endsWith(FIELD_AVERAGE_DIVISOR_SUFFIX)) {
                    Attribute<?> attribute = field.getAttribute();
                    GroupingAttribute<?> newAttribute = new GroupingAttribute<>((Type<?>) attribute.getData(), new Key(field.getBase()), true);
                    newAttribute.setColumnVisibility(attribute.getColumnVisibility());
                    grouping.add(newAttribute);
                }
            }
            // Create a new group and merge it into the existing groups.
            Group group = new Group(grouping, count);
            group.setFieldAggregator(fieldAggregator);
            group.addDocumentVisibility(document.getColumnVisibility());
            groups.mergeOrPutGroup(group);
        }
    }

    /**
     * Return a substring of the given str without the given suffix.
     *
     * @param str
     *            the string
     * @param suffix
     *            the suffix
     * @return the string without the suffix
     */
    private String removeSuffix(String str, String suffix) {
        int suffixLength = suffix.length();
        return str.substring(0, str.length() - suffixLength);
    }

    /**
     * Identify which events in the document are targets for grouping and/or aggregation, and index them.
     */
    private void indexDocumentEntries() {
        for (Map.Entry<String,Attribute<?>> entry : document.entrySet()) {
            Field field = parseField(entry);
            // The current field is a target for grouping.
            if (groupFields.contains(field.getBase())) {
                groupFieldsIndex.index(field);
            }
            // The current field is a target for aggregation.
            if (fieldAggregatorFactory.isFieldToAggregate(field.getBase())) {
                aggregateFieldsIndex.index(field);
            }
        }
    }

    /**
     * Identify valid groupings consisting of target group pairs and create/update their corresponding {@link Group} in {@link #currentGroups}.
     */
    private void groupEntries() {
        // If we found any entries for target group fields, identify all valid groupings.
        if (groupEntriesFound()) {
            // The groupings combinations that we find. Each combination may only have one Field from a particular target group field, e.g. if doing
            // #GROUP_BY(AGE,GENDER), a combination set will have at most one AGE field and one GENDER field.
            List<Set<Field>> groupings = new ArrayList<>();

            // If we only have one target grouping field, we do not need to find any group combinations. All events for the given target group field should be
            // tracked as individual groupings.
            if (maxGroupSize == 1) {
                groupFieldsIndex.fields.values().stream().map(Collections::singleton).forEach(groupings::add);
            } else {
                // If we have any group field events with grouping contexts and instances, e.g. GENDER.FOO.1, it's possible that we will find direct matches to
                // other group field events with the same grouping context and instance (a direct match). These should be found first for efficiency purposes.
                if (groupFieldsIndex.hasFieldsWithPossibleDirectMatch()) {
                    groupings = getGroupingsWithDirectMatches();
                }
                // If we have any group field events that do not have a grouping context and instance, e.g. GENDER.1 or GENDER, then each one of those events
                // should
                // be combined with each existing group combination, effectively creating cartesian products.
                if (groupFieldsIndex.hasFieldsWithoutDirectMatch()) {
                    groupings = getGroupingsWithoutDirectMatches(groupings);
                }
            }

            // Track each identified grouping.
            groupings.forEach(this::trackGroup);
        } else {
            // If no entries were found for any of the target group fields, create a single 'empty' group that will represent this document in the final
            // grouping results.
            trackGroup(Grouping.emptyGrouping());
        }
    }

    /**
     * Identify grouping combinations that are direct matches to each other based on the grouping context and instance of the field events. If we do not find
     * any direct match at all for a specified target group field, then all events for the group field will be combined.
     *
     * @return the direct match combinations
     */
    private List<Set<Field>> getGroupingsWithDirectMatches() {
        List<Set<Field>> groupings = new ArrayList<>();
        Set<String> fieldsWithGroupingContextAndInstance = groupFieldsIndex.getFieldsWithPossibleDirectMatch();
        // If we only saw one field with a grouping context and instance, return a list of singletons with each field event. We cannot create any combinations
        // at this time.
        if (fieldsWithGroupingContextAndInstance.size() == 1) {
            Collection<Field> fields = groupFieldsIndex.getFields(fieldsWithGroupingContextAndInstance.iterator().next());
            fields.stream().map(Collections::singleton).forEach(groupings::add);
        } else {
            // If we have more than one target field with a grouping context and instance, determine the correct groupings based off matching the grouping
            // context and instance where possible with direct 1-to-1 matches, i.e. AGE.FOO.1 is a direct match to GENDER.FOO.1.
            Multimap<Pair<String,String>,Field> groupingContextAndInstanceToField = HashMultimap.create();
            for (String fieldName : fieldsWithGroupingContextAndInstance) {
                Collection<Field> fields = groupFieldsIndex.getFields(fieldName);
                for (Field field : fields) {
                    groupingContextAndInstanceToField.put(Pair.with(field.getGroupingContext(), field.getInstance()), field);
                }
            }

            // Sort the entries by the number of direct matches seen for each grouping context-instance pair.
            // using secondary key string comparison because Tuple does not handle comparing null values
            SortedSet<Map.Entry<Pair<String,String>,Collection<Field>>> directMatchesSortedByPrevalence = new TreeSet<>(
                            Comparator.comparingInt((Map.Entry<Pair<String,String>,Collection<Field>> left) -> left.getValue().size()).reversed()
                                            .thenComparing(e -> String.valueOf(e.getKey())));
            directMatchesSortedByPrevalence.addAll(groupingContextAndInstanceToField.asMap().entrySet());

            // Map of group target field names to the grouping combinations found for them.
            Multimap<SortedSet<String>,Set<Field>> fieldsToGroupings = ArrayListMultimap.create();
            // Tracks the largest size seen for any combination of direct matches for target group fields.
            Map<String,Integer> fieldToLargestGroupingSize = new HashMap<>();

            for (Map.Entry<Pair<String,String>,Collection<Field>> entry : directMatchesSortedByPrevalence) {
                Collection<Field> fields = entry.getValue();
                SortedSet<String> groupingFields = new TreeSet<>();
                boolean keep = false;
                for (Field field : fields) {
                    groupingFields.add(field.getBase());
                    // If we have seen this field before associated with another grouping context and instance, only keep this grouping if it is the same size
                    // as the largest grouping we've seen for the field.
                    if (fieldToLargestGroupingSize.containsKey(field.getBase())) {
                        if (fields.size() == fieldToLargestGroupingSize.get(field.getBase())) {
                            keep = true;
                        }
                    } else {
                        // If this is the first time we are seeing this field, then we have found the largest batch size for the grouping that this field is in.
                        // Automatically keep this grouping.
                        fieldToLargestGroupingSize.put(field.getBase(), fields.size());
                        keep = true;
                    }
                }
                if (keep) {
                    fieldsToGroupings.put(groupingFields, Sets.newHashSet(fields));
                }
            }

            // Now that we've found the largest direct match combinations for each target group field, we need to effectively create cartesian products between
            // each combination. For instance, given the following grouping combinations resulting from #GROUP_BY(AGE,GENDER,RECORD_ID,RECORD_TEXT,BUILDING):
            //
            // {AGE,GENDER} => [{"20", "MALE"},{"10", "FEMALE"}]
            // {RECORD_ID,RECORD_TEXT} => [{"123", "Summary"}]
            // {BUILDING} => [{West},{East}]
            //
            // We want to generate the following combinations:
            // {"20","MALE","123","Summary","West"}
            // {"20","MALE","123","Summary","East"}
            // {"10","FEMALE","123","Summary","West"}
            // {"10","FEMALE","123","Summary","East"}
            for (SortedSet<String> fields : fieldsToGroupings.keySet()) {
                Collection<Set<Field>> currentGroupings = fieldsToGroupings.get(fields);
                if (groupings.isEmpty()) {
                    groupings.addAll(currentGroupings);
                } else {
                    List<Set<Field>> newGroupings = new ArrayList<>();
                    for (Set<Field> oldGrouping : groupings) {
                        for (Set<Field> currentGrouping : currentGroupings) {
                            Set<Field> newGrouping = new HashSet<>(oldGrouping);
                            newGrouping.addAll(currentGrouping);
                            newGroupings.add(newGrouping);
                        }
                    }
                    groupings = newGroupings;
                }
            }
        }
        return groupings;
    }

    /**
     * Combine each field event for target group fields that do not have both a grouping context and instance to any previously found grouping combinations.
     *
     * @param prevGroupings
     *            the combinations that have been found thus far
     * @return the updated grouping combinations
     */
    private List<Set<Field>> getGroupingsWithoutDirectMatches(List<Set<Field>> prevGroupings) {
        List<Set<Field>> groupings = new ArrayList<>(prevGroupings);
        for (String fieldName : groupFieldsIndex.getFieldsWithoutDirectMatch()) {
            Collection<Field> fields = groupFieldsIndex.getFields(fieldName);
            // If there are no previous grouping combinations, add each field event as a singular combination.
            if (groupings.isEmpty()) {
                for (Field field : fields) {
                    groupings.add(Sets.newHashSet(field));
                }
            } else {
                // Effectively create cartesian products of each previously seen grouping combination and each field event for the current target event field.
                // For instance, if we have the previous combination [{"20","MALE"},{"10","FEMALE"}] and the field events {"A","B","C"}, we want to generate
                // the following combinations:
                //
                // {"20","MALE", "A"}
                // {"20","MALE", "B"}
                // {"20","MALE", "C"}
                // {"10","FEMALE", "A"}
                // {"10","FEMALE", "B"}
                // {"10","FEMALE", "C"}
                List<Set<Field>> newGroupings = new ArrayList<>();
                for (Set<Field> oldGrouping : groupings) {
                    for (Field field : fields) {
                        Set<Field> newGrouping = new HashSet<>(oldGrouping);
                        newGrouping.add(field);
                        newGroupings.add(newGrouping);
                    }
                }
                groupings = newGroupings;
            }
        }
        return groupings;
    }

    /**
     * Track the groups identified by the given field event combinations.
     *
     * @param groupedFields
     *            the group combination
     */
    private void trackGroup(Collection<Field> groupedFields) {
        // The grouping context-instance pairs seen for all grouping keys generated in this method.
        Set<Pair<String,String>> groupingContextAndInstances = new HashSet<>();
        // The set of 'keys' that are used to identify individual distinct groupings.
        List<Grouping> groupings = new ArrayList<>();
        // It is possible for a field event in a grouping combination to have a multi-value attribute. If this occurs, we must once again create cartesian
        // products between all the values of the attribute of each field.
        for (Field field : groupedFields) {
            // Track the grouping context-instance pair. This is required for us to be able to find direct matches later when aggregating.
            if (field.hasGroupingContext() && field.hasInstance()) {
                groupingContextAndInstances.add(Pair.with(field.getGroupingContext(), field.getInstance()));
            }
            // If we have no grouping keys yet, create keys consisting of each value of the current field.
            if (groupings.isEmpty()) {
                for (Attribute<?> attribute : field.getAttributes()) {
                    GroupingAttribute<?> copy = createCopyWithKey(attribute, field.getBase());
                    groupings.add(new Grouping(copy));
                }
            } else {
                // Otherwise, create the cartesian product between the current field's value and each existing key.
                List<Grouping> newGroupings = new ArrayList<>();
                for (Attribute<?> attribute : field.getAttributes()) {
                    GroupingAttribute<?> copy = createCopyWithKey(attribute, field.getBase());
                    for (Grouping grouping : groupings) {
                        Grouping groupingCopy = new Grouping(grouping);
                        groupingCopy.add(copy);
                        newGroupings.add(groupingCopy);
                    }
                }
                groupings = newGroupings;
            }
        }

        // Track which grouping context-instance pairs we have seen for each grouping key.
        for (Pair<String,String> groupingContextAndInstance : groupingContextAndInstances) {
            this.groupingContextAndInstancesSeenForGroups.putAll(groupingContextAndInstance, groupings);
        }

        // Now we can create/update groups in currentGroups for each grouping key.
        groupings.forEach(this::trackGroup);
    }

    /**
     * Create/update the group for the given grouping.
     *
     * @param grouping
     *            the grouping to track
     */
    private void trackGroup(Grouping grouping) {
        // Get the group.
        Group group = currentGroups.getGroup(grouping);
        // Create a group for the grouping if one does not already exist.
        if (group == null) {
            group = new Group(grouping);
            group.setFieldAggregator(fieldAggregatorFactory.newInstance());
            currentGroups.putGroup(group);
        }
        // Add the visibilities of each attribute in the grouping for combination later, and increment the count for how many times this distinct
        // grouping was seen.
        group.addAttributeVisibilities(grouping);
        group.incrementCount();
        group.addDocumentVisibility(document.getColumnVisibility());
    }

    private GroupingAttribute<?> createCopyWithKey(Attribute<?> attribute, String key) {
        Type<?> type = ((TypeAttribute<?>) attribute).getType();
        GroupingAttribute<?> newAttribute = new GroupingAttribute<>(type, new Key(key), true);
        newAttribute.setColumnVisibility(attribute.getColumnVisibility());
        return newAttribute;
    }

    /**
     * Aggregate all qualifying events that are from target aggregation fields.
     */
    private void aggregateEntries() {
        // Groupings were found in the document. Aggregate entries according to their association based on each entry's grouping context and instance.
        if (groupEntriesFound()) {
            // If we have any target events for aggregation that have a grouping context and instance, e.g. AGE.FOO.1, attempt to find groups that have matching
            // grouping context and instance pairs, and aggregate the events into those groups only. If we do not find any direct match at all for a specified
            // aggregation field, then all events for the aggregation field will be aggregated into each group.
            if (aggregateFieldsIndex.hasFieldsWithPossibleDirectMatch()) {
                // Attempt to find a direct match for the current aggregation target field.
                for (String fieldName : aggregateFieldsIndex.fieldToFieldsByGroupingContextAndInstance.keySet()) {
                    Multimap<Pair<String,String>,Field> groupingContextAndInstanceToFields = aggregateFieldsIndex.fieldToFieldsByGroupingContextAndInstance
                                    .get(fieldName);
                    Set<Pair<String,String>> aggregatePairs = groupingContextAndInstanceToFields.keySet();
                    Set<Pair<String,String>> groupPairs = this.groupingContextAndInstancesSeenForGroups.keySet();
                    // A group and an aggregation event is considered to be a direct match if and only if the group contains any event that has the same
                    // grouping context and instance as the aggregation event.
                    Set<Pair<String,String>> directMatches = Sets.intersection(aggregatePairs, groupPairs);
                    // If we have any direct matches, then only aggregate the direct matches into the groups where we saw a direct match.
                    if (!directMatches.isEmpty()) {
                        for (Pair<String,String> directMatch : directMatches) {
                            for (Grouping grouping : this.groupingContextAndInstancesSeenForGroups.get(directMatch)) {
                                Group group = currentGroups.getGroup(grouping);
                                Collection<Field> fields = groupingContextAndInstanceToFields.get(directMatch);
                                group.aggregateAll(fields);
                            }
                        }
                    } else {
                        // Otherwise, aggregate all events for this field into all groups.
                        Collection<Field> fields = aggregateFieldsIndex.getFields(fieldName);
                        currentGroups.aggregateToAllGroups(fields);
                    }
                }
            }
            // If there are any target aggregation events that do not have a grouping context, e.g. AGE or AGE.1, then all target aggregation events should be
            // aggregated into all groups.
            if (aggregateFieldsIndex.hasFieldsWithoutDirectMatch()) {
                for (String fieldName : aggregateFieldsIndex.fieldsWithoutDirectMatch) {
                    Collection<Field> fields = aggregateFieldsIndex.getFields(fieldName);
                    currentGroups.aggregateToAllGroups(fields);
                }
            }
        } else {
            // No groupings were found in the document. In this case, we will consider this document to contain a placeholder 'empty' grouping, and aggregate
            // all aggregation entries to the empty grouping.
            Group group = currentGroups.getGroup(Grouping.emptyGrouping());
            // Aggregate all aggregate entries to the grouping.
            Multimap<String,Field> fields = aggregateFieldsIndex.fields;
            for (String field : fields.keySet()) {
                group.aggregateAll(field, fields.get(field));
            }
        }
    }

    private boolean groupEntriesFound() {
        return !groupFieldsIndex.isEmpty();
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
        String groupingContext = null;
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
                groupingContext = field.substring(firstPeriod + 1, secondPeriod);
                // Parse the instance from the substring after the last period.
                instance = field.substring(field.lastIndexOf(".") + 1);
            } else {
                // If there is no second period present, the field's format is <NAME>.<INSTANCE>.
                instance = field.substring(firstPeriod + 1);
            }
        }

        // Map the field name to the root model name. This ensures that even if we're grouping fields that can be seen with different model names, e.g. AG, ETA,
        // and AGE, that the same root name will be used across the board to ensure that they're treated as from the same target group/aggregation field.
        name = getMappedFieldName(name);

        return new Field(name, groupingContext, instance, entry.getValue());
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
     * This class maintains useful indexes that will be used for determining direct and non-direct matches when grouping and aggregating.
     */
    private static class FieldIndex {

        // Map of field names to their entries.
        private final ArrayListMultimap<String,Field> fields = ArrayListMultimap.create();
        // The set of fields with possible direct matches.
        private final Set<String> fieldsWithPossibleDirectMatch = new HashSet<>();
        // The set of fields with no direct matches.
        private final Set<String> fieldsWithoutDirectMatch = new HashSet<>();
        // Map of field names to Multimaps of grouping contexts to entries.
        private final Map<String,Multimap<Pair<String,String>,Field>> fieldToFieldsByGroupingContextAndInstance = new HashMap<>();
        // Whether to accept entries that have null attributes for indexing.
        private final boolean allowNullAttributes;
        // Map of fields to values that have already been indexed. This is used to prevent duplicates being counted when multiple fields in a record with the
        // same value are mapped to the same model mapping when model mapping is supplied.
        private final Multimap<String,Object> alreadyIndexed = HashMultimap.create();

        private FieldIndex(boolean allowNullAttributes) {
            this.allowNullAttributes = allowNullAttributes;
        }

        /**
         * Index the given {@link Field}. If {@link #allowNullAttributes} is set to false and the given field has a null attribute, it will not be indexed.
         *
         * @param field
         *            the field to index
         */
        public void index(Field field) {
            // Check if we can index this field.
            if (field.getAttribute() != null || allowNullAttributes) {
                // Indexed all values for the field that have not yet been indexed.
                // @formatter:off
                expandAttributes(field).stream()
                                .filter(this::isUnindexed)
                                .forEach(this::indexField);
                // @formatter:on
            }
        }

        /**
         * Returns a set of fields expanded from the attributes of the given field. It is possible for a {@link Field} to have an attribute that is an
         * {@link Attributes}. In this case, the set will consist of copies of the given field, where the attribute of each copy is extracted from the original
         * {@link Attributes}. Otherwise, the set will consist solely of the given field.
         */
        private Set<Field> expandAttributes(Field field) {
            Attribute<?> attribute = field.getAttribute();
            if (attribute instanceof Attributes) {
                // If the attribute is collection of attributes, return a set of fields for each attribute contained within.
                Set<Field> fields = new HashSet<>();
                for (Attribute<?> subAttribute : ((Attributes) attribute).getAttributes()) {
                    fields.add(new Field(field.getBase(), field.getGroupingContext(), field.getInstance(), subAttribute));
                }
                return fields;
            } else {
                // Otherwise return the original field.
                return Collections.singleton(field);
            }
        }

        /**
         * Return whether the given field has been indexed. A field is considered indexed if we have already seen a field with the given field's base name,
         * grouping context, instance, and value.
         */
        private boolean isUnindexed(Field field) {
            // Create the index id.
            String id = getIndexId(field);
            Attribute<?> attribute = field.getAttribute();
            // Determine if we have already seen the given index id with the field's attribute (null or otherwise).
            if (attribute != null) {
                return !alreadyIndexed.containsEntry(id, attribute.getData());
            } else {
                return !alreadyIndexed.containsEntry(id, null);
            }
        }

        /**
         * Index the given field.
         */
        private void indexField(Field field) {
            fields.put(field.getBase(), field);
            // If the field has a grouping context and instance, it's possible that it may have a direct match. Index the field and its grouping
            // context-instance pair.
            if (field.hasGroupingContext() && field.hasInstance()) {
                fieldsWithPossibleDirectMatch.add(field.getBase());
                Multimap<Pair<String,String>,Field> groupingContextAndInstanceToField = fieldToFieldsByGroupingContextAndInstance.get(field.getBase());
                if (groupingContextAndInstanceToField == null) {
                    groupingContextAndInstanceToField = HashMultimap.create();
                    fieldToFieldsByGroupingContextAndInstance.put(field.getBase(), groupingContextAndInstanceToField);
                }
                groupingContextAndInstanceToField.put(Pair.with(field.getGroupingContext(), field.getInstance()), field);
            } else {
                // Otherwise, the field will have no direct matches.
                fieldsWithoutDirectMatch.add(field.getBase());
            }

            // Mark the field as indexed.
            markIndexed(field);
        }

        /**
         * Mark the given field as indexed.
         */
        private void markIndexed(Field field) {
            String id = getIndexId(field);
            Attribute<?> attribute = field.getAttribute();
            if (attribute != null) {
                alreadyIndexed.put(id, attribute.getData());
            } else {
                alreadyIndexed.put(id, null);
            }
        }

        /**
         * Return the index id for the given field, which will consist of the field's base name, grouping context (if present), and instance (if present).
         */
        private String getIndexId(Field field) {
            return Stream.of(field.getBase(), field.getGroupingContext(), field.getInstance()).filter(Objects::nonNull).collect(Collectors.joining(""));
        }

        /**
         * Return a list of all {@link Field} instances indexed with the given base field name.
         */
        public List<Field> getFields(String field) {
            return fields.get(field);
        }

        /**
         * Return the set of fields with possible direct matches.
         */
        public Set<String> getFieldsWithPossibleDirectMatch() {
            return fieldsWithPossibleDirectMatch;
        }

        /**
         * Return whether any fields with a possible direct match are present.
         */
        public boolean hasFieldsWithPossibleDirectMatch() {
            return !fieldsWithPossibleDirectMatch.isEmpty();
        }

        /**
         * Return whether any fields without a direct match are present.
         */
        public boolean hasFieldsWithoutDirectMatch() {
            return !fieldsWithoutDirectMatch.isEmpty();
        }

        /**
         * Return the set of fields without a direct match.
         */
        public Set<String> getFieldsWithoutDirectMatch() {
            return fieldsWithoutDirectMatch;
        }

        /**
         * Return true if no fields have been indexed in this {@link FieldIndex}, or false otherwise.
         */
        public boolean isEmpty() {
            return fields.isEmpty();
        }
    }
}
