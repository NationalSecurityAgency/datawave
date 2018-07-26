package datawave.query.transformer;

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultimap;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.model.QueryModel;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.tables.ShardQueryLogic;
import datawave.webservice.query.logic.BaseQueryLogic;
import java.util.HashSet;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Collection;
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
    
    public GroupingTransform(BaseQueryLogic<Entry<Key,Value>> logic, Collection<String> groupFieldsSet) {
        this.groupFieldsSet = new HashSet<>(groupFieldsSet);
        QueryModel model = ((ShardQueryLogic) logic).getQueryModel();
        if (model != null) {
            reverseModelMapping = model.getReverseQueryMapping();
        }
        if (log.isTraceEnabled())
            log.trace("groupFieldsSet:" + this.groupFieldsSet);
    }
    
    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> keyDocumentEntry) {
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
    
    @Override
    public Entry<Key,Document> flush() {
        if (documents == null) {
            Map<String,String> markings = Maps.newHashMap();
            documents = new LinkedList<>();
            for (Collection<Attribute<?>> entry : multiset.elementSet()) {
                ColumnVisibility vis = null;
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
            return Maps.immutableEntry(d.getMetadata(), d);
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
        
        Set<String> expandedGroupFieldsList = Sets.newLinkedHashSet();
        Multimap<String,String> fieldToFieldWithContextMap = this.getFieldToFieldWithGroupingContextMap(e.getValue(), expandedGroupFieldsList);
        if (log.isTraceEnabled())
            log.trace("got a new fieldToFieldWithContextMap:" + fieldToFieldWithContextMap);
        int longest = this.longestValueList(fieldToFieldWithContextMap);
        for (int i = 0; i < longest; i++) {
            Collection<Attribute<?>> fieldCollection = Sets.newHashSet();
            for (String fieldListItem : expandedGroupFieldsList) {
                if (log.isTraceEnabled())
                    log.trace("fieldListItem:" + fieldListItem);
                Collection<String> gtNames = fieldToFieldWithContextMap.get(fieldListItem);
                if (gtNames == null || gtNames.isEmpty()) {
                    if (log.isTraceEnabled()) {
                        log.trace("gtNames:" + gtNames);
                        log.trace("fieldToFieldWithContextMap:" + fieldToFieldWithContextMap + " did not contain " + fieldListItem);
                    }
                    continue;
                } else {
                    String gtName = gtNames.iterator().next();
                    if (fieldListItem.equals(gtName) == false) {
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
                multiset.add(fieldCollection);
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
