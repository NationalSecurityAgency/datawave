package datawave.query.transformer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.model.QueryModel;
import datawave.query.tables.ShardQueryLogic;
import datawave.webservice.query.Query;
import datawave.webservice.query.logic.BaseQueryLogic;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.springframework.util.Assert;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * GroupingTransform mimics GROUP BY with a COUNT in SQL. For the given fields, this transform will group into unique combinations of values and assign a count
 * to each combination. It is possible that values in a specific group may hold different column visibilities. Because the multiple fields are aggregated into
 * one, it is necessary to combine the column visibilities for the fields and remark the grouped fields. Additionally, the overall document visibility must be
 * computed.
 *
 * Because the tserver may tear down and start a new iterator at any time after a next() call, there can be no saved state in this class. For that reason, each
 * next call on the tserver will flatten the aggregated data into a single Entry&gt;Key,Document&lt; to return to the web server. The web server will then
 * aggregate these documents by count.
 */
public class GroupingTransform extends DocumentTransform.DefaultDocumentTransform {
    
    private static final Logger log = getLogger(GroupingTransform.class);
    
    /**
     * the fields (user provided) to group by
     */
    private Set<String> groupFieldsSet;
    
    /**
     * mapping of field name (with grouping context) to value attribute
     */
    private Map<String,GroupingTypeAttribute<?>> fieldMap = Maps.newHashMap();
    
    /**
     * holds the aggregated column visibilities for each grouped event
     */
    private Multimap<Collection<GroupingTypeAttribute<?>>,ColumnVisibility> fieldVisibilities = HashMultimap.create();
    
    /**
     * A map of TypeAttribute collection keys to integer counts This map uses a special key type that ignores the metadata (with visibilities) in its hashCode
     * and equals methods
     */
    private GroupCountingHashMap countingMap;
    
    /**
     * list of documents to return, created from the countingMap
     */
    private final LinkedList<Document> documents = new LinkedList<>();
    
    /**
     * mapping used to combine field names that map to different model names
     */
    private Map<String,String> reverseModelMapping = null;
    
    /**
     * list of keys that have been read, in order to keep track of where we left off when a new iterator is created
     */
    private List<Key> keys = new ArrayList<>();
    
    /**
     * flatten or not. true on the tserver, false on the webserver
     */
    private boolean flatten;
    
    /**
     * tserver calls this CTOR with flatten = true. Called by QueryIterator::seek
     * 
     * @param logic
     * @param groupFieldsSet
     * @param flatten
     */
    public GroupingTransform(BaseQueryLogic<Entry<Key,Value>> logic, Collection<String> groupFieldsSet, boolean flatten) {
        this(logic, groupFieldsSet);
        this.flatten = flatten;
    }
    
    /**
     * web server calls this CTOR (flatten defaults to false) called by ShardQueryLogic::getTransformer(Query)
     * 
     * @param logic
     * @param groupFieldsSet
     */
    public GroupingTransform(BaseQueryLogic<Entry<Key,Value>> logic, Collection<String> groupFieldsSet) {
        this.groupFieldsSet = deconstruct(groupFieldsSet);
        if (logic != null) {
            QueryModel model = ((ShardQueryLogic) logic).getQueryModel();
            if (model != null) {
                reverseModelMapping = model.getReverseQueryMapping();
            }
        }
        log.trace("groupFieldsSet: {}", this.groupFieldsSet);
    }
    
    private Set<String> deconstruct(Collection<String> fields) {
        return fields.stream().map(field -> JexlASTHelper.deconstructIdentifier(field)).collect(Collectors.toSet());
    }
    
    @Override
    public void initialize(Query settings, MarkingFunctions markingFunctions) {
        super.initialize(settings, markingFunctions);
        this.countingMap = new GroupCountingHashMap(markingFunctions);
    }
    
    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> keyDocumentEntry) {
        log.trace("apply to {}", keyDocumentEntry);
        
        if (keyDocumentEntry != null) {
            getListKeyCounts(keyDocumentEntry);
        }
        return null;
    }
    
    private String getFieldName(Attribute<?> attr) {
        return attr.getMetadata().getRow().toString();
    }
    
    private Key createAttrKey(String fieldName) {
        return new Key(fieldName);
    }
    
    /**
     * Aggregate items from the incoming iterator and supply them via the flush method
     * 
     * @param in
     *            an iterator source
     * @return the flushed value that is an aggregation from the source iterator
     */
    public Iterator<Entry<Key,Document>> getGroupingIterator(final Iterator<Entry<Key,Document>> in, int groupFieldsBatchSize, YieldCallback<Key> yieldCallback) {
        
        return new Iterator<Entry<Key,Document>>() {
            
            Entry<Key,Document> next;
            
            @Override
            public boolean hasNext() {
                for (int i = 0; i < groupFieldsBatchSize; i++) {
                    if (in.hasNext()) {
                        GroupingTransform.this.apply(in.next());
                    } else if (yieldCallback != null && yieldCallback.hasYielded()) {
                        log.trace("hasNext is false because yield was called");
                        if (countingMap != null && !countingMap.isEmpty()) {
                            // reset the yield and use its key in the flattened document prepared below
                            keys.add(yieldCallback.getPositionAndReset());
                        }
                        break;
                    } else {
                        // in.hasNext() was false and there was no yield
                        break;
                    }
                }
                // if there is nothing in the countingMap, next will be null (so method returns false)
                next = GroupingTransform.this.flush();
                return next != null;
            }
            
            @Override
            public Entry<Key,Document> next() {
                if (flatten) {
                    return new AbstractMap.SimpleEntry<>(next.getKey(), next.getValue());
                } else {
                    // web server will aggregate the visibilities from the Attributes
                    // and set the Document's visibility to that aggregation
                    Document documentToReturn = next.getValue();
                    // combine the column visibilities of all attributes in order to compute the overall document column visibility
                    documentToReturn.setColumnVisibility(combine(documentToReturn.getAttributes().stream().filter(attr -> attr != null)
                                    .map(attr -> attr.getColumnVisibility()).collect(Collectors.toSet())));
                    
                    return new AbstractMap.SimpleEntry<>(next.getKey(), documentToReturn);
                }
            }
        };
    }
    
    @Override
    public Entry<Key,Document> flush() {
        
        if (!countingMap.isEmpty()) {
            
            log.trace("flush will use the countingMap: {}", countingMap);
            
            for (Collection<GroupingTypeAttribute<?>> entry : countingMap.keySet()) {
                log.trace("from countingMap, got entry: {}", entry);
                ColumnVisibility columnVisibility = null;
                try {
                    columnVisibility = toColumnVisibility(fieldVisibilities.get(entry));
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to merge column visibilities: " + fieldVisibilities.get(entry), e);
                }
                // grab a key from those saved during getListKeyCounts
                Assert.notEmpty(keys, "no available keys for grouping results");
                // use the last (most recent) key so a new iterator will know where to start
                Key docKey = keys.get(keys.size() - 1);
                Document d = new Document(docKey, true);
                d.setColumnVisibility(columnVisibility);
                
                entry.forEach(base -> d.put(getFieldName(base), base));
                NumberType type = new NumberType();
                type.setDelegate(new BigDecimal(countingMap.get(entry)));
                TypeAttribute<BigDecimal> attr = new TypeAttribute<>(type, new Key("count"), true);
                d.put("COUNT", attr);
                documents.add(d);
            }
            if (flatten) {
                // flatten to just one document on the tserver.
                flatten(documents);
            }
        }
        if (!documents.isEmpty()) {
            log.trace("{} will flush first of {} documents: {}", this.hashCode(), documents.size(), documents);
            Document d = documents.pop();
            Key key;
            if (keys.size() > 0 && flatten) {
                // use the last (most recent) key so a new iterator will know where to start
                key = keys.get(keys.size() - 1);
            } else {
                key = d.getMetadata();
            }
            Entry<Key,Document> entry = Maps.immutableEntry(key, d);
            log.trace("flushing out {}", entry);
            countingMap.clear();
            return entry;
        }
        return null;
    }
    
    /**
     * <pre>
     * flush used the countingMap:
     * [[MALE, 16],
     * [MALE, 20],
     * [40, MALE],
     * [40, MALE],
     * [MALE, 22] x 2,
     * [FEMALE, 18],
     * [MALE, 24],
     * [20, MALE],
     * [30, MALE],
     * [FEMALE, 18],
     * [34, MALE]]
     * 
     * to create documents list: [
     * {AGE=16, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=1, ETA=20, GENERE=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=1, ETA=40, GENERE=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=40, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=2, ETA=22, GENERE=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=18, COUNT=1, GENDER=FEMALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=1, ETA=24, GENERE=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=20, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=30, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=1, ETA=18, GENERE=FEMALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=34, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false]
     * 
     * which is then flattened to just one document with the fields and counts correlated with a grouping context suffix:
     * 
     * {
     * AGE.0=16, GENDER.0=MALE, COUNT.0=1,
     * ETA.1=20, GENERE.1=MALE, COUNT.1=1,
     * ETA.2=40, GENERE.2=MALE, COUNT.2=1,
     * AGE.3=40, GENDER.3=MALE, COUNT.3=1,
     * ETA.4=22, GENERE.4=MALE, COUNT.4=2,
     * AGE.5=18, GENDER.5=FEMALE, COUNT.5=1,
     * ETA.6=24, GENERE.6=MALE, COUNT.6=1,
     * AGE.7=20, GENDER.7=MALE, COUNT.7=1,
     * AGE.8=30, GENDER.8=MALE, COUNT.8=1,
     * ETA.9=18, GENERE.9=FEMALE, COUNT.9=1,
     * AGE.A=34, GENDER.A=MALE, COUNT.A=1,
     * }
     * </pre>
     *
     * The Attributes, which have had their visibilities merged, are copied into normal TypeAttributes for serialization to the webserver.
     *
     * @param documents
     */
    private void flatten(List<Document> documents) {
        log.trace("flatten {}", documents);
        Document theDocument = new Document(documents.get(documents.size() - 1).getMetadata(), true);
        int context = 0;
        Set<ColumnVisibility> visibilities = new HashSet<>();
        for (Document document : documents) {
            log.trace("document: {}", document);
            for (Entry<String,Attribute<? extends Comparable<?>>> entry : document.entrySet()) {
                String name = entry.getKey();
                visibilities.add(entry.getValue().getColumnVisibility());
                
                Attribute<? extends Comparable<?>> attribute = entry.getValue();
                attribute.setColumnVisibility(entry.getValue().getColumnVisibility());
                // call copy() on the GroupingTypeAttribute to get a plain TypeAttribute
                // instead of a GroupingTypeAttribute that is package protected and won't serialize
                theDocument.put(name + "." + Integer.toHexString(context).toUpperCase(), (TypeAttribute) attribute.copy(), true, false);
            }
            context++;
        }
        ColumnVisibility combinedVisibility = combine(visibilities);
        log.trace("combined visibilities: {} to {}", visibilities, combinedVisibility);
        theDocument.setColumnVisibility(combinedVisibility);
        documents.clear();
        log.trace("flattened document: {}", theDocument);
        documents.add(theDocument);
    }
    
    private Multimap<String,String> getFieldToFieldWithGroupingContextMap(Document d, Set<String> expandedGroupFieldsList) {
        
        Multimap<String,String> fieldToFieldWithContextMap = TreeMultimap.create();
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : d.entrySet()) {
            Attribute<?> field = entry.getValue();
            log.trace("field is {}", field);
            String fieldName = entry.getKey();
            String shortName = fieldName;
            String shorterName = shortName;
            if (shortName.indexOf('.') != -1)
                shortName = shortName.substring(0, shortName.lastIndexOf('.'));
            if (shorterName.indexOf('.') != -1)
                shorterName = shorterName.substring(0, shorterName.indexOf('.'));
            log.trace("fieldName: {}, shortName: {}", fieldName, shortName);
            if (reverseModelMapping != null) {
                String finalName = reverseModelMapping.get(shorterName);
                if (finalName != null) {
                    shortName = finalName + shortName.substring(shorterName.length());
                    fieldName = finalName + fieldName.substring(shorterName.length());
                    shorterName = finalName;
                }
            }
            if (this.groupFieldsSet.contains(shorterName)) {
                expandedGroupFieldsList.add(shortName);
                log.trace("{} contains {}", this.groupFieldsSet, shorterName);
                GroupingTypeAttribute<?> created = makeGroupingTypeAttribute(shortName, field.getData());
                created.setColumnVisibility(field.getColumnVisibility());
                fieldMap.put(fieldName, created);
                fieldToFieldWithContextMap.put(shortName, fieldName);
            } else {
                log.trace("{} does not contain {}", this.groupFieldsSet, shorterName);
            }
        }
        log.trace("fieldMap: {}", fieldMap);
        log.trace("fields: {}", d.entrySet());
        log.trace("fieldToFieldWithGroupingContextMap: {}", fieldToFieldWithContextMap);
        log.trace("expandedGroupFieldsList: {}", expandedGroupFieldsList);
        return fieldToFieldWithContextMap;
    }
    
    private GroupingTypeAttribute<?> makeGroupingTypeAttribute(String fieldName, Object fieldValue) {
        log.trace("{} creating attribute with {} and {}", flatten ? "tserver" : "webserver", fieldName, fieldValue);
        return new GroupingTypeAttribute<>((Type) fieldValue, createAttrKey(fieldName), true);
    }
    
    private int longestValueList(Multimap<String,String> in) {
        int max = 0;
        for (Collection<String> valueCollection : in.asMap().values()) {
            max = Math.max(max, valueCollection.size());
        }
        return max;
    }
    
    private void getListKeyCounts(Entry<Key,Document> entry) {
        
        log.trace("{} get list key counts for: {}", flatten ? "t" : "web" + "server", entry);
        keys.add(entry.getKey());
        
        Set<String> expandedGroupFieldsList = new LinkedHashSet<>();
        // if the incoming Documents have been aggregated on the tserver, they will have a COUNT field.
        // use the value in the COUNT field as a loop max when the fields are put into the countingMap
        // During the flush operation, a new COUNT field will be created based on the number of unique
        // field sets in the countingMap
        Map<String,Attribute<? extends Comparable<?>>> dictionary = entry.getValue().getDictionary();
        Map<String,Integer> countKeyMap = new HashMap<>();
        dictionary.keySet().stream().filter(key -> key.startsWith("COUNT")).filter(countKey -> entry.getValue().getDictionary().containsKey(countKey))
                        .forEach(countKey -> {
                            TypeAttribute countTypeAttribute = ((TypeAttribute) entry.getValue().getDictionary().get(countKey));
                            int count = ((BigDecimal) countTypeAttribute.getType().getDelegate()).intValue();
                            countKeyMap.put(countKey, count);
                        });
        
        Multimap<String,String> fieldToFieldWithContextMap = this.getFieldToFieldWithGroupingContextMap(entry.getValue(), expandedGroupFieldsList);
        log.trace("got a new fieldToFieldWithContextMap: {}", fieldToFieldWithContextMap);
        int longest = this.longestValueList(fieldToFieldWithContextMap);
        for (int i = 0; i < longest; i++) {
            Collection<GroupingTypeAttribute<?>> fieldCollection = new HashSet<>();
            String currentGroupingContext = "";
            for (String fieldListItem : expandedGroupFieldsList) {
                log.trace("fieldListItem: {}", fieldListItem);
                Collection<String> gtNames = fieldToFieldWithContextMap.get(fieldListItem);
                if (gtNames == null || gtNames.isEmpty()) {
                    log.trace("gtNames: {}", gtNames);
                    log.trace("fieldToFieldWithContextMap: {} did not contain: {}", fieldToFieldWithContextMap, fieldListItem);
                } else {
                    String gtName = gtNames.iterator().next();
                    int idx = gtName.indexOf('.');
                    if (idx != -1) {
                        currentGroupingContext = gtName.substring(idx + 1);
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
                log.trace("{} adding {} of {} to counting map", flatten ? "tserver" : "webserver", count, fieldCollection);
                IntStream.range(0, count).forEach(j -> countingMap.add(fieldCollection));
                fieldVisibilities.put(fieldCollection, getColumnVisibility(entry));
                log.trace("put {} to {} into fieldVisibilities {}", fieldCollection, getColumnVisibility(entry), fieldVisibilities);
            } else {
                log.trace("fieldList.size() != this.expandedGroupFieldsList.size()");
                log.trace("fieldList: {}", fieldCollection);
                log.trace("expandedGroupFieldsList: {}", expandedGroupFieldsList);
            }
        }
        log.trace("countingMap: {}", countingMap);
    }
    
    private ColumnVisibility combine(Collection<ColumnVisibility> in) {
        try {
            ColumnVisibility columnVisibility = markingFunctions.combine(in);
            log.trace("combined {} into {}", in, columnVisibility);
            return columnVisibility;
        } catch (MarkingFunctions.Exception e) {
            log.warn("unable to combine visibilities from {}", in);
        }
        return new ColumnVisibility();
    }
    
    private ColumnVisibility getColumnVisibility(Entry<Key,Document> e) {
        return e.getValue().getColumnVisibility();
    }
    
    private ColumnVisibility toColumnVisibility(Collection<ColumnVisibility> visibilities) throws Exception {
        return combine(visibilities);
    }
    
    static class GroupCountingHashMap extends HashMap<Collection<GroupingTypeAttribute<?>>,Integer> {
        
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
    
    /**
     * ignores the metadata for comparison.
     * 
     * @param <T>
     */
    static class GroupingTypeAttribute<T extends Comparable<T>> extends TypeAttribute<T> {
        
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
