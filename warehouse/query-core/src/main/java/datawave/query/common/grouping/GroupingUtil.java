package datawave.query.common.grouping;

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
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Provides functionality commonly needed to group documents (regardless if done server or client side).
 *
 * This class and its methods aren't static so that we don't run into concurrency issues, although all required state should be passed into the individual
 * methods and not kept in this class. Calling classes could extend this class to inherit the methods, but the state still shouldn't be inherited because not
 * all callers will be able to easily extend this class if they already/need to extend other parents.
 */
public class GroupingUtil {
    
    private static final Logger log = getLogger(GroupingUtil.class);
    
    public ColumnVisibility combine(Collection<ColumnVisibility> in, MarkingFunctions markingFunctions) {
        try {
            ColumnVisibility columnVisibility = markingFunctions.combine(in);
            log.trace("combined {} into {}", in, columnVisibility);
            return columnVisibility;
        } catch (MarkingFunctions.Exception e) {
            log.warn("unable to combine visibilities from {}", in);
        }
        return new ColumnVisibility();
    }
    
    /**
     * This method mutates the countingMap argument that is passed into it. The caller may either anticipate that (and hopefully make a comment when this method
     * is called that it is expecting the countingMap to be mutated) or the caller can reset the instance of countingMap by calling getCountingMap on the
     * GroupInfo object (clearer, but relies more on garbage collection)
     *
     * @param entry
     * @param groupFieldsSet
     * @param countingMap
     * @return
     */
    public GroupingInfo getGroupingInfo(Map.Entry<Key,Document> entry, Set<String> groupFieldsSet, GroupCountingHashMap countingMap) {
        return getGroupingInfo(entry, groupFieldsSet, countingMap, null);
    }
    
    public GroupingInfo getGroupingInfo(Map.Entry<Key,Document> entry, Set<String> groupFieldsSet, GroupCountingHashMap countingMap,
                    Map<String,String> reverseModelMapping) {
        log.trace("apply to {}", entry);
        
        // mapping of field name (with grouping context) to value attribute
        Map<String,GroupingTypeAttribute<?>> fieldMap = Maps.newHashMap();
        
        // holds the aggregated column visibilities for each grouped event
        Multimap<Collection<GroupingTypeAttribute<?>>,ColumnVisibility> fieldVisibilities = HashMultimap.create();
        
        if (entry != null) {
            Set<String> expandedGroupFieldsList = new LinkedHashSet<>();
            Map<String,Attribute<? extends Comparable<?>>> dictionary = entry.getValue().getDictionary();
            Map<String,Integer> countKeyMap = new HashMap<>();
            dictionary.keySet().stream().filter(key -> key.startsWith("COUNT")).filter(countKey -> entry.getValue().getDictionary().containsKey(countKey))
                            .forEach(countKey -> {
                                TypeAttribute<?> countTypeAttribute = ((TypeAttribute<?>) entry.getValue().getDictionary().get(countKey));
                                int count = ((BigDecimal) countTypeAttribute.getType().getDelegate()).intValue();
                                countKeyMap.put(countKey, count);
                            });
            Set<String> groupingContextSet = new HashSet<>();
            TreeMultimap<String,String> fieldToFieldWithContextMap = (TreeMultimap<String, String>) getFieldToFieldWithGroupingContextMap(entry.getValue(), expandedGroupFieldsList, fieldMap,
                            groupFieldsSet, groupingContextSet, reverseModelMapping);
            log.trace("got a new fieldToFieldWithContextMap: {}", fieldToFieldWithContextMap);
            int longest = longestValueList(fieldToFieldWithContextMap);
            for (String currentGroupingContext : groupingContextSet) {
                Collection<GroupingUtil.GroupingTypeAttribute<?>> fieldCollection = new HashSet<>();
                for (String fieldListItem : expandedGroupFieldsList) {
                    log.trace("fieldListItem: {}", fieldListItem);
                    NavigableSet<String> gtNames = fieldToFieldWithContextMap.get(fieldListItem);
                    if (gtNames == null || gtNames.isEmpty()) {
                        log.trace("gtNames: {}", gtNames);
                        log.trace("fieldToFieldWithContextMap: {} did not contain: {}", fieldToFieldWithContextMap, fieldListItem);
                    } else {
                        String nameWithGrouping = fieldListItem + "." + currentGroupingContext;
                        final String gtName = gtNames.contains(nameWithGrouping) ? nameWithGrouping : null;
                        if (gtName == null || gtName.isEmpty()) {
                            continue;
                        }
                        if (!fieldListItem.equals(gtName)) {
                            fieldToFieldWithContextMap.remove(fieldListItem, gtName);
                        }
                        log.trace("fieldToFieldWithContextMap now: {}", fieldToFieldWithContextMap);
                        log.trace("gtName: {}", gtName);
                        fieldCollection.add(fieldMap.get(gtName));
                    }
                }
                
                if (fieldCollection.size() == expandedGroupFieldsList.size()) {
                    
                    // get the count out of the countKeyMap
                    Integer count = countKeyMap.get("COUNT." + currentGroupingContext);
                    if (count == null)
                        count = 1;
                    // see above comment about the COUNT field
                    log.trace("adding {} of {} to counting map", count, fieldCollection);
                    IntStream.range(0, count).forEach(j -> countingMap.add(fieldCollection));
                    fieldVisibilities.put(fieldCollection, entry.getValue().getColumnVisibility());
                    log.trace("put {} to {} into fieldVisibilities {}", fieldCollection, entry.getValue().getColumnVisibility(), fieldVisibilities);
                } else {
                    log.trace("fieldList.size() != this.expandedGroupFieldsList.size()");
                    log.trace("fieldList: {}", fieldCollection);
                    log.trace("expandedGroupFieldsList: {}", expandedGroupFieldsList);
                }
            }
            
            log.trace("countingMap: {}", countingMap);
        }
        
        return new GroupingInfo(countingMap, fieldVisibilities);
    }
    
    protected Multimap<String,String> getFieldToFieldWithGroupingContextMap(Document d, Set<String> expandedGroupFieldsList,
                    Map<String,GroupingTypeAttribute<?>> fieldMap, Set<String> groupFieldsSet, Set<String> contextSet, Map<String,String> reverseModelMapping) {
        
        Multimap<String,String> fieldToFieldWithContextMap = TreeMultimap.create();
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : d.entrySet()) {
            Attribute<?> field = entry.getValue();
            log.trace("field is {}", field);

            String fieldName = entry.getKey();
            String shortName = fieldName;
            String shorterName = shortName;
            boolean containsContext = false;

            if (shortName.indexOf('.') != -1) {
                int lastDotIndex = shortName.lastIndexOf('.');
                shortName = shortName.substring(0, lastDotIndex);
                containsContext = true;
                contextSet.add(fieldName.substring(++lastDotIndex));
            }

            if (shorterName.indexOf('.') != -1) {
                shorterName = shorterName.substring(0, shorterName.indexOf('.'));
            }
            log.trace("fieldName: {}, shortName: {}", fieldName, shortName);

            if (reverseModelMapping != null) {
                String finalName = reverseModelMapping.get(shorterName);
                if (finalName != null) {
                    shortName = finalName + shortName.substring(shorterName.length());
                    fieldName = finalName + fieldName.substring(shorterName.length());
                    shorterName = finalName;
                }
            }
            if (groupFieldsSet.contains(shorterName)) {
                expandedGroupFieldsList.add(shortName);
                log.trace("{} contains {}", groupFieldsSet, shorterName);
                
                if (field.getData() instanceof Collection<?>) {
                    // This handles multivalued entries that do not have grouping context.
                    // Also handles multivalued entries from separate entries, but the field names contain
                    //  the same context number
                    // Create GroupingTypeAttribute and put in ordered map ordered on the attribute type
                    SortedSetMultimap<Type<?>,GroupingTypeAttribute<?>> attrSortedMap = TreeMultimap.create();
                    for (Object typeAttribute : ((Collection<?>) field.getData())) {
                        Type<?> type = ((TypeAttribute<?>) typeAttribute).getType();
                        GroupingTypeAttribute<?> created = new GroupingTypeAttribute<>(type, new Key(shortName), true);
                        created.setColumnVisibility(field.getColumnVisibility());
                        attrSortedMap.put(type, created);
                    }
                    
                    // Add GroupingTypeAttribute to fieldMap with a grouping context that is based on ordered attribute type
                    int i = 0;
                    for (Map.Entry<Type<?>,GroupingTypeAttribute<?>> sortedEntry : attrSortedMap.entries()) {
                        String fieldNameWithContext = null;
                        String context = convertGroupingContext(i++);
                        if (containsContext) {
                            // multivalued entries from separate entries, but the field names contain the same context number
                            fieldNameWithContext = shortName + "." + context;
                        } else {
                            // multivalued entries that do not have grouping context
                            fieldNameWithContext = fieldName + "." + context;
                        }

                        fieldMap.put(fieldNameWithContext, sortedEntry.getValue());
                        fieldToFieldWithContextMap.put(shortName, fieldNameWithContext);
                        contextSet.add(context);
                    }
                } else {
                    GroupingTypeAttribute<?> created = new GroupingTypeAttribute<>((Type) field.getData(), new Key(shortName), true);
                    created.setColumnVisibility(field.getColumnVisibility());
                    String fieldNameWithContext = containsContext ? fieldName : fieldName + ".0";
                    contextSet.add("0");
                    fieldMap.put(fieldNameWithContext, created);
                    fieldToFieldWithContextMap.put(shortName, fieldNameWithContext);
                }
            } else {
                log.trace("{} does not contain {}", groupFieldsSet, shorterName);
            }
        }
        log.trace("fieldMap: {}", fieldMap);
        log.trace("fields: {}", d.entrySet());
        log.trace("fieldToFieldWithGroupingContextMap: {}", fieldToFieldWithContextMap);
        log.trace("expandedGroupFieldsList: {}", expandedGroupFieldsList);
        return fieldToFieldWithContextMap;
    }
    
    private static int longestValueList(Multimap<String,String> in) {
        int max = 0;
        for (Collection<String> valueCollection : in.asMap().values()) {
            max = Math.max(max, valueCollection.size());
        }
        return max;
    }

    private String convertGroupingContext(int contextGrouping) {
        return Integer.toHexString(contextGrouping).toUpperCase();
    }

    /**
     * Provides a clear way to return multiple things related to grouping that are generated from one method.
     */
    public static class GroupingInfo {
        
        private final GroupCountingHashMap countingMap;
        
        private final Multimap<Collection<GroupingTypeAttribute<?>>,ColumnVisibility> fieldVisibilities;
        
        GroupingInfo(GroupCountingHashMap countingMap, Multimap<Collection<GroupingTypeAttribute<?>>,ColumnVisibility> fieldVisibilities) {
            this.countingMap = countingMap;
            this.fieldVisibilities = fieldVisibilities;
        }
        
        public GroupCountingHashMap getCountsMap() {
            return countingMap;
        }
        
        public Multimap<Collection<GroupingTypeAttribute<?>>,ColumnVisibility> getFieldVisibilities() {
            return fieldVisibilities;
        }
    }
    
    public static class GroupCountingHashMap extends HashMap<Collection<GroupingTypeAttribute<?>>,Integer> {
        
        private static final Logger log = getLogger(GroupCountingHashMap.class);
        
        private MarkingFunctions markingFunctions;
        
        public GroupCountingHashMap(MarkingFunctions markingFunctions) {
            this.markingFunctions = markingFunctions;
        }
        
        public int add(Collection<GroupingTypeAttribute<?>> in) {
            int count = 0;
            if (super.containsKey(in)) {
                count = super.get(in);
                // aggregate the visibilities
                combine(this.keySet(), in);
            }
            count++;
            super.put(in, count);
            return count;
        }
        
        private void combine(Set<Collection<GroupingTypeAttribute<?>>> existingMapKeys, Collection<? extends Attribute<?>> incomingAttributes) {
            
            // for each Attribute in the incomingAttributes, find the existing map key attribute that matches its data.
            // combine the column visibilities of the incoming attribute and the existing one, and set
            // the column visibility of the EXISTING map key to the new value.
            // Note that the hashCode and equals methods for the GroupingTypeAttribute will ignore the metadata (which contains the column visibility)
            incomingAttributes.forEach(incomingAttribute -> {
                existingMapKeys.stream()
                                .flatMap(Collection::stream)
                                // if the existing and incoming attributes are equal (other than the metadata), the incoming attribute's visibility will be
                                // considered for merging into the existing attribute unless the column visibilities are already equal
                                .filter(existingAttribute -> existingAttribute.getData().equals(incomingAttribute.getData())
                                                && !existingAttribute.getColumnVisibility().equals(incomingAttribute.getColumnVisibility()))
                                .forEach(existingAttribute -> existingAttribute.setColumnVisibility(combine(Arrays.asList(
                                                existingAttribute.getColumnVisibility(), incomingAttribute.getColumnVisibility()))));
            });
        }
        
        private ColumnVisibility combine(Collection<ColumnVisibility> in) {
            try {
                ColumnVisibility columnVisibility = markingFunctions.combine(in);
                log.trace("combined {} into {}", in, columnVisibility);
                return columnVisibility;
            } catch (MarkingFunctions.Exception e) {
                log.warn("was unable to combine visibilities from {}", in);
            }
            return new ColumnVisibility();
        }
        
    }
    
    public static class GroupingTypeAttribute<T extends Comparable<T>> extends TypeAttribute<T> {
        
        public GroupingTypeAttribute(Type type, Key key, boolean toKeep) {
            super(type, key, toKeep);
        }
        
        @Override
        public boolean equals(Object o) {
            if (null == o) {
                return false;
            }
            
            if (o instanceof TypeAttribute) {
                TypeAttribute other = (TypeAttribute) o;
                return this.getType().equals(other.getType()) && (0 == this.compareMetadataRow(other));
            }
            return false;
        }
        
        private int compareMetadataRow(Attribute<T> other) {
            if (this.isMetadataSet() != other.isMetadataSet()) {
                if (this.isMetadataSet()) {
                    return 1;
                } else {
                    return -1;
                }
            } else if (this.isMetadataSet()) {
                return this.metadata.compareRow(other.getMetadata().getRow());
            } else {
                return 0;
            }
        }
        
        @Override
        public int hashCode() {
            HashCodeBuilder hcb = new HashCodeBuilder(2099, 2129);
            hcb.append(getType().getDelegateAsString());
            return hcb.toHashCode();
        }
    }
    
}
