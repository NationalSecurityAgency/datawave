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

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class GroupingTransform extends DocumentTransform.DefaultDocumentTransform {
    
    private static final Logger log = Logger.getLogger(GroupingTransform.class);
    
    private Set<String> groupFieldsSet;
    private Map<String,Attribute<?>> fieldMap = Maps.newHashMap();
    private Multimap<Collection<Attribute<?>>,String> fieldDatatypes = HashMultimap.create();
    private Multimap<Collection<Attribute<?>>,ColumnVisibility> fieldVisibilities = HashMultimap.create();
    private Multiset<Collection<Attribute<?>>> multiset = HashMultiset.create();
    private LinkedList<Document> documents = null;
    private Map<String,String> reverseModelMapping = null;
    
    private boolean pending;
    
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
            
            @Override
            public boolean hasNext() {
                for (int i = 0; i < max && in.hasNext(); i++) {
                    GroupingTransform.this.apply(in.next());
                }
                next = GroupingTransform.this.flush();
                return next != null;
            }
            
            @Override
            public Entry<Key,Document> next() {
                return next;
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
                ColumnVisibility vis;
                try {
                    vis = toColumnVisibility(fieldVisibilities.get(entry));
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to merge column visibilities: " + fieldVisibilities.get(entry), e);
                }
                Key docKey = new Key("grouping", toString(fieldDatatypes.get(entry)) + '\u0000', "", vis, -1);
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
        }
        if (!documents.isEmpty()) {
            Document d = documents.pop();
            Entry<Key,Document> entry = Maps.immutableEntry(d.getMetadata(), d);
            if (log.isTraceEnabled()) {
                log.trace("flushing out " + entry);
            }
            multiset.clear();
            return entry;
        }
        return null;
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
    
    private void getListKeyCounts(Entry<Key,Document> e) {
        
        if (log.isTraceEnabled()) {
            log.trace("get list key counts for:" + e);
        }
        int count = 1;
        Set<String> expandedGroupFieldsList = new LinkedHashSet<>();
        // if the incoming Documents have been aggregated on the tserver, they will have a COUNT field.
        // use the value in the COUNT field as a loop max when the fields are put into the multiset
        // During the flush operation, a new COUNT field will be created based on the number of unique
        // field sets in the multiset
        if (e.getValue().getDictionary().containsKey("COUNT")) {
            TypeAttribute countTypeAttribute = ((TypeAttribute) e.getValue().getDictionary().get("COUNT"));
            count = ((BigDecimal) countTypeAttribute.getType().getDelegate()).intValue();
        }
        Multimap<String,String> fieldToFieldWithContextMap = this.getFieldToFieldWithGroupingContextMap(e.getValue(), expandedGroupFieldsList);
        if (log.isTraceEnabled())
            log.trace("got a new fieldToFieldWithContextMap:" + fieldToFieldWithContextMap);
        int longest = this.longestValueList(fieldToFieldWithContextMap);
        for (int i = 0; i < longest; i++) {
            Collection<Attribute<?>> fieldCollection = new HashSet<>();
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
                // see above comment about the COUNT field
                for (int j = 0; j < count; j++) {
                    multiset.add(fieldCollection);
                }
                fieldDatatypes.put(fieldCollection, getDataType(e));
                fieldVisibilities.put(fieldCollection, getColumnVisibility(e));
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
