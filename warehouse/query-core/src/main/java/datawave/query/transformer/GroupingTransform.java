package datawave.query.transformer;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultimap;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.model.QueryModel;
import datawave.query.tables.ShardQueryLogic;
import datawave.webservice.query.logic.BaseQueryLogic;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
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

public class GroupingTransform extends DocumentTransform.DefaultDocumentTransform {
    
    private static final Logger log = Logger.getLogger(GroupingTransform.class);
    
    private Set<String> groupFieldsSet;
    private Map<String,Attribute<?>> fieldMap = Maps.newHashMap();
    private Multimap<Collection<Attribute<?>>,String> fieldDatatypes = HashMultimap.create();
    private Multimap<Collection<Attribute<?>>,ColumnVisibility> fieldVisibilities = HashMultimap.create();
    private Multiset<Collection<Attribute<?>>> multiset = HashMultiset.create();
    private LinkedList<Document> documents = null;
    private Map<String,String> reverseModelMapping = null;
    
    private List<Key> keys = new ArrayList<>();
    
    /**
     * flatten or not. true on the tserver, false on the webserver
     */
    private boolean flatten;
    
    /**
     * tserver calls this CTOR with flatten = true called by QueryIterator::seek
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
        this.groupFieldsSet = new HashSet<>(groupFieldsSet);
        if (logic != null) {
            QueryModel model = ((ShardQueryLogic) logic).getQueryModel();
            if (model != null) {
                reverseModelMapping = model.getReverseQueryMapping();
            }
        }
        if (log.isTraceEnabled())
            log.trace("groupFieldsSet:" + this.groupFieldsSet);
    }
    
    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> keyDocumentEntry) {
        if (log.isTraceEnabled()) {
            log.trace("apply to " + keyDocumentEntry);
        }
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
    public Iterator<Entry<Key,Document>> getGroupingIterator(final Iterator<Entry<Key,Document>> in, int max) {
        
        return new Iterator<Entry<Key,Document>>() {
            
            Entry<Key,Document> next;
            
            Key lastKey;
            
            @Override
            public boolean hasNext() {
                for (int i = 0; i < max && in.hasNext(); i++) {
                    Entry<Key,Document> lastEntry = in.next();
                    // save the last key so I will return it with the flushed Entry<Key,Document>
                    lastKey = lastEntry.getKey();
                    GroupingTransform.this.apply(lastEntry);
                }
                next = GroupingTransform.this.flush();
                return next != null;
            }
            
            @Override
            public Entry<Key,Document> next() {
                if (log.isTraceEnabled()) {
                    log.trace("next has key:" + next.getKey() + " but I will use key:" + lastKey);
                }
                Entry<Key,Document> nextEntryWithLastKey = new AbstractMap.SimpleEntry<>(lastKey, next.getValue());
                return nextEntryWithLastKey;
            }
        };
        
    }
    
    @Override
    public Entry<Key,Document> flush() {
        if (documents == null) {
            documents = new LinkedList<>();
        }
        if (!multiset.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace("flush will use the multiset:" + multiset);
            }
            for (Collection<Attribute<?>> entry : multiset.elementSet()) {
                try {
                    toColumnVisibility(fieldVisibilities.get(entry));
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to merge column visibilities: " + fieldVisibilities.get(entry), e);
                }
                // grab a key from those saved during getListKeyCounts
                Assert.notEmpty(keys, "no available keys for grouping results");
                Key docKey = keys.get(keys.size() - 1);
                Document d = new Document(docKey, true);
                
                for (Attribute base : entry) {
                    d.put(getFieldName(base), base);
                }
                NumberType type = new NumberType();
                type.setDelegate(new BigDecimal(multiset.count(entry)));
                TypeAttribute<BigDecimal> attr = new TypeAttribute<>(type, new Key("count"), true);
                d.put("COUNT", attr);
                documents.add(d);
            }
            if (flatten) {
                // flatten to just one document
                flatten(documents);
            }
        }
        if (!documents.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace(this.hashCode() + " will flush first of " + documents.size() + " documents:" + documents);
            }
            Document d = documents.pop();
            Key key;
            if (keys.size() > 0) {
                key = keys.get(keys.size() - 1);
            } else {
                key = d.getMetadata();
            }
            Entry<Key,Document> entry = Maps.immutableEntry(key, d);
            if (log.isTraceEnabled()) {
                log.trace("flushing out " + entry);
            }
            multiset.clear();
            return entry;
        }
        return null;
    }
    
    // @formatter:off
    /**
     * flush used the multiset:
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
     *
     *
     * @param documents
     */
    // @formatter:on
    private void flatten(List<Document> documents) {
        if (log.isTraceEnabled()) {
            log.trace("flatten documents:" + documents);
        }
        Document theDocument = new Document(documents.get(0).getMetadata(), true);
        int context = 0;
        for (Document document : documents) {
            log.info("document:" + document);
            for (Entry<String,Attribute<? extends Comparable<?>>> entry : document.entrySet()) {
                String name = entry.getKey();
                Attribute<? extends Comparable<?>> attribute = entry.getValue();
                theDocument.put(name + "." + Integer.toHexString(context).toUpperCase(), attribute, true, false);
            }
            context++;
        }
        documents.clear();
        if (log.isTraceEnabled()) {
            log.trace("flattened document:" + theDocument);
        }
        documents.add(theDocument);
    }
    
    private Multimap<String,String> getFieldToFieldWithGroupingContextMap(Document d, Set<String> expandedGroupFieldsList) {
        Multimap<String,String> fieldToFieldWithContextMap = TreeMultimap.create();
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : d.entrySet()) {
            Attribute<?> field = entry.getValue();
            if (log.isTraceEnabled())
                log.trace("field is " + field);
            String fieldName = entry.getKey();
            String shortName = fieldName;
            String shorterName = shortName;
            if (shortName.indexOf('.') != -1)
                shortName = shortName.substring(0, shortName.lastIndexOf('.'));
            if (shorterName.indexOf('.') != -1)
                shorterName = shorterName.substring(0, shorterName.indexOf('.'));
            if (log.isTraceEnabled())
                log.trace("fieldName:" + fieldName + ", shortName:" + shortName);
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
                if (log.isTraceEnabled())
                    log.trace(this.groupFieldsSet + " contains " + shorterName);
                Attribute<?> created = makeAttribute(shortName, field.getData());
                fieldMap.put(fieldName, created);
                fieldToFieldWithContextMap.put(shortName, fieldName);
            } else {
                if (log.isTraceEnabled())
                    log.trace(this.groupFieldsSet + " does not contain " + shorterName);
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("fields:" + d.entrySet());
            log.trace("fieldToFieldWithGroupingContextMap:" + fieldToFieldWithContextMap);
            log.trace("expandedGroupFieldsList:" + expandedGroupFieldsList);
        }
        return fieldToFieldWithContextMap;
    }
    
    private Attribute<?> makeAttribute(String fieldName, Object fieldValue) {
        return new TypeAttribute((Type) fieldValue, createAttrKey(fieldName), true);
    }
    
    private int longestValueList(Multimap<String,String> in) {
        int max = 0;
        for (Collection<String> valueCollection : in.asMap().values()) {
            max = Math.max(max, valueCollection.size());
        }
        return max;
    }
    
    private void getListKeyCounts(Entry<Key,Document> entry) {
        
        if (log.isTraceEnabled()) {
            log.trace("get list key counts for:" + entry);
        }
        keys.add(entry.getKey());
        
        Set<String> expandedGroupFieldsList = new LinkedHashSet<>();
        // if the incoming Documents have been aggregated on the tserver, they will have a COUNT field.
        // use the value in the COUNT field as a loop max when the fields are put into the multiset
        // During the flush operation, a new COUNT field will be created based on the number of unique
        // field sets in the multiset
        Map<String,Attribute<? extends Comparable<?>>> dictionary = entry.getValue().getDictionary();
        Set<String> countKeys = dictionary.keySet().stream().filter(s -> s.startsWith("COUNT")).collect(Collectors.toSet());
        Map<String,Integer> countKeyMap = new HashMap<>();
        for (String countKey : countKeys) {
            if (entry.getValue().getDictionary().containsKey(countKey)) {
                TypeAttribute countTypeAttribute = ((TypeAttribute) entry.getValue().getDictionary().get(countKey));
                int count = ((BigDecimal) countTypeAttribute.getType().getDelegate()).intValue();
                countKeyMap.put(countKey, count);
            }
        }
        
        Multimap<String,String> fieldToFieldWithContextMap = this.getFieldToFieldWithGroupingContextMap(entry.getValue(), expandedGroupFieldsList);
        if (log.isTraceEnabled())
            log.trace("got a new fieldToFieldWithContextMap:" + fieldToFieldWithContextMap);
        int longest = this.longestValueList(fieldToFieldWithContextMap);
        for (int i = 0; i < longest; i++) {
            Collection<Attribute<?>> fieldCollection = new HashSet<>();
            String currentGroupingContext = "";
            for (String fieldListItem : expandedGroupFieldsList) {
                if (log.isTraceEnabled())
                    log.trace("fieldListItem:" + fieldListItem);
                Collection<String> gtNames = fieldToFieldWithContextMap.get(fieldListItem);
                if (gtNames == null || gtNames.isEmpty()) {
                    if (log.isTraceEnabled()) {
                        log.trace("gtNames:" + gtNames);
                        log.trace("fieldToFieldWithContextMap:" + fieldToFieldWithContextMap + " did not contain " + fieldListItem);
                    }
                } else {
                    String gtName = gtNames.iterator().next();
                    int idx = gtName.indexOf('.');
                    if (idx != -1) {
                        currentGroupingContext = gtName.substring(idx + 1);
                    }
                    if (!fieldListItem.equals(gtName)) {
                        fieldToFieldWithContextMap.remove(fieldListItem, gtName);
                    }
                    if (log.isTraceEnabled()) {
                        log.trace("fieldToFieldWithContextMap now:" + fieldToFieldWithContextMap);
                        log.trace("gtName:" + gtName);
                    }
                    fieldCollection.add(this.fieldMap.get(gtName));
                }
            }
            if (fieldCollection.size() == expandedGroupFieldsList.size()) {
                
                // get the count out of the countKeyMap
                Integer count = countKeyMap.get("COUNT." + currentGroupingContext);
                if (count == null)
                    count = 1;
                // see above comment about the COUNT field
                for (int j = 0; j < count; j++) {
                    multiset.add(fieldCollection);
                }
                fieldDatatypes.put(fieldCollection, getDataType(entry));
                fieldVisibilities.put(fieldCollection, getColumnVisibility(entry));
                if (log.isTraceEnabled())
                    log.trace("added fieldList to the map:" + fieldCollection);
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("fieldList.size() != this.expandedGroupFieldsList.size()");
                    log.trace("fieldList:" + fieldCollection);
                    log.trace("expandedGroupFieldsList:" + expandedGroupFieldsList);
                }
            }
        }
        if (log.isTraceEnabled())
            log.trace("map:" + multiset);
    }
    
    private String getDataType(Entry<Key,Document> e) {
        String colf = e.getKey().getColumnFamily().toString();
        return colf.substring(0, colf.indexOf('\0'));
    }
    
    private ColumnVisibility getColumnVisibility(Entry<Key,Document> e) {
        return e.getValue().getColumnVisibility();
    }
    
    private String toString(Collection<String> strings) {
        return Joiner.on(',').join(strings);
    }
    
    private ColumnVisibility toColumnVisibility(Collection<ColumnVisibility> visibilities) throws Exception {
        return markingFunctions.combine(visibilities);
    }
}
