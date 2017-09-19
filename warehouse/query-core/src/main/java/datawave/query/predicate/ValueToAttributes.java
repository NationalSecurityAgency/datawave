package datawave.query.predicate;

import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.Map.Entry;

import com.google.common.collect.*;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;
import datawave.query.attributes.Attribute;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.TypeAttribute;
import datawave.query.util.Composite;
import datawave.query.util.CompositeMetadata;
import datawave.query.util.TypeMetadata;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Function;

/**
 * 
 * Unused. It was written to put Composite Fields into fetched documents. It is preserved in case we change the way composite fields are managed
 * 
 * 
 */
public class ValueToAttributes implements Function<Entry<Key,String>,Iterable<Entry<String,Attribute<? extends Comparable<?>>>>> {
    private static final Logger log = Logger.getLogger(ValueToAttributes.class);
    
    private final Text holder = new Text(), cvHolder = new Text();
    
    private AttributeFactory attrFactory;
    
    private Map<String,Multimap<String,String>> compositeToFieldMap;
    private MarkingFunctions markingFunctions;
    private Multimap<String,Attribute<?>> compositeAttributes = ArrayListMultimap.create();
    
    private EventDataQueryFilter attrFilter;
    
    private LRUMap cvCache = new LRUMap(256);
    
    public ValueToAttributes(CompositeMetadata compositeMetadata, TypeMetadata typeMetadata, EventDataQueryFilter attrFilter, MarkingFunctions markingFunctions) {
        this.attrFactory = new AttributeFactory(typeMetadata);
        this.markingFunctions = markingFunctions;
        this.compositeToFieldMap = compositeMetadata.getCompositeToFieldMap();
        this.attrFilter = attrFilter;
    }
    
    @Override
    public Iterable<Entry<String,Attribute<? extends Comparable<?>>>> apply(Entry<Key,String> from) {
        String origFieldName = from.getValue();
        String modifiedFieldName = JexlASTHelper.deconstructIdentifier(origFieldName, false);
        String groupingContext = JexlASTHelper.getGroupingContext(origFieldName);
        
        Key key = from.getKey();
        String ingestDatatype = this.getDatatypeFromKey(key);
        Multimap<String,String> mm = this.compositeToFieldMap.get(ingestDatatype);
        // mm looks something like this: {MAKE_COLOR=[MAKE,COLOR], COLOR_WHEELS=[COLOR,WHEELS]}
        if (mm != null) {
            Multimap<String,String> inverted = Multimaps.invertFrom(mm, ArrayListMultimap.<String,String> create());
            // inverted, it looks like this: {MAKE=[MAKE_COLOR],WHEELS=[COLOR_WHEELS],COLOR=[MAKE_COLOR,COLOR_WHEELS]}
            if (inverted.containsKey(modifiedFieldName)) {
                for (String composite : inverted.get(modifiedFieldName)) {
                    List<Attribute<?>> list = (List<Attribute<?>>) this.compositeAttributes.get(getCompositeAttributeKey(composite, groupingContext));
                    List<String> components = (List<String>) this.compositeToFieldMap.get(ingestDatatype).get(composite);
                    int idx = components.indexOf(modifiedFieldName);
                    if (idx > list.size()) {
                        idx = list.size();
                    }
                    list.add(idx, getFieldValue(modifiedFieldName, key));
                    if (log.isDebugEnabled()) {
                        log.debug("added to " + list + " in " + this.compositeAttributes);
                    }
                }
            }
        }
        List<Entry<String,Attribute<? extends Comparable<?>>>> list = new ArrayList<>();
        list.add(Maps.<String,Attribute<? extends Comparable<?>>> immutableEntry(origFieldName, getFieldValue(modifiedFieldName, from.getKey())));
        Set<String> goners = Sets.newHashSet();
        
        for (String composite : this.compositeAttributes.keySet()) {
            String compositeFieldMapKey = JexlASTHelper.deconstructIdentifier(composite, false);
            if (this.compositeToFieldMap.get(ingestDatatype).get(compositeFieldMapKey).size() == this.compositeAttributes.get(composite).size()) {
                try {
                    Attribute<? extends Comparable<?>> combined = this.joinAttributes(composite, this.compositeAttributes.get(composite));
                    list.add(Maps.<String,Attribute<? extends Comparable<?>>> immutableEntry(composite, combined));
                    log.debug("will be removing " + composite + " from compositeAttributes:" + this.compositeAttributes);
                    goners.add(composite);
                } catch (Exception e) {
                    log.debug("could not join attributes:", e);
                }
            }
        }
        for (String goner : goners) {
            this.compositeAttributes.removeAll(goner);
        }
        return list;
    }
    
    public String getCompositeAttributeKey(String composite, String grouping) {
        return composite + JexlASTHelper.GROUPING_CHARACTER_SEPARATOR + grouping;
    }
    
    public Attribute<?> getFieldValue(String fieldName, Key k) {
        k.getColumnQualifier(holder);
        int index = holder.find(Constants.NULL);
        
        if (0 > index) {
            throw new IllegalArgumentException("Could not find null-byte contained in columnqualifier for key: " + k);
        }
        
        try {
            String data = Text.decode(holder.getBytes(), index + 1, (holder.getLength() - (index + 1)));
            
            ColumnVisibility cv = getCV(k);
            
            Attribute<?> attr = this.attrFactory.create(fieldName, data, k, (attrFilter == null || attrFilter.keep(k)));
            if (attrFilter != null) {
                attr.setToKeep(attrFilter.keep(k));
            }
            
            if (log.isTraceEnabled()) {
                log.trace("Created " + attr.getClass().getName() + " for " + fieldName);
            }
            
            return attr;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
    public Attribute<?> joinAttributes(String compositeName, Collection<Attribute<?>> in) throws Exception {
        Collection<ColumnVisibility> columnVisibilities = Sets.newHashSet();
        String[] dataList = new String[0];
        long timestamp = 0;
        boolean toKeep = false;
        // use the metadata from the first attribute being merged
        Attribute<?> attribute = in.iterator().next();
        Key metadata = attribute.getMetadata();
        if (metadata == null) {
            attribute.getColumnVisibility();
            metadata = attribute.getMetadata();
        }
        for (Attribute<?> attr : in) {
            if (attr.isToKeep()) {
                toKeep = true;
            }
            if (attr instanceof Attributes) {
                Attributes attrs = (Attributes) attr;
                int newAttributeCount = attrs.size();
                int originalDataListSize = dataList.length;
                if (newAttributeCount > 0) {
                    if (dataList.length == 0) {
                        dataList = new String[newAttributeCount];
                        int i = 0;
                        for (Attribute<?> a : attrs.getAttributes()) {
                            dataList[i++] = attributeValue(a) + Composite.END_SEPARATOR;
                        }
                    } else {
                        // resize the dataList array - dataList.length and newAttributeCount are each > 0
                        String[] resizedDataList = new String[dataList.length * newAttributeCount];
                        int count = resizedDataList.length / dataList.length;
                        for (int i = 0; i < count; i++) {
                            System.arraycopy(dataList, 0, resizedDataList, i * dataList.length, dataList.length);
                        }
                        dataList = resizedDataList;
                        
                        int attributeIndex = 0;
                        List<Attribute<?>> attributeList = Lists.newArrayList(attrs.getAttributes());
                        for (int i = 0; i < dataList.length; i++) {
                            attributeIndex = i / originalDataListSize;
                            
                            Attribute<?> a = attributeList.get(attributeIndex);
                            if (dataList[i] == null) {
                                dataList[i] = attributeValue(a) + Composite.END_SEPARATOR;
                            } else if (dataList[i].length() > 0) {
                                dataList[i] += Composite.START_SEPARATOR + attributeValue(a) + Composite.END_SEPARATOR;
                            } else {
                                dataList[i] += attributeValue(a) + Composite.END_SEPARATOR;
                            }
                            timestamp = Math.max(timestamp, attr.getTimestamp());
                            columnVisibilities.add(attr.getColumnVisibility());
                            
                        }
                    }
                }
            } else {
                
                if (log.isTraceEnabled()) {
                    log.trace("Join for data list size: " + dataList.length);
                }
                if (dataList.length == 0) {
                    dataList = new String[] {""};
                }
                if (dataList.length == 0) {
                    dataList = new String[] {""};
                }
                
                for (int i = 0; i < dataList.length; i++) { // append to everything in the list
                    if (dataList[i].length() > 0) {
                        dataList[i] += Composite.START_SEPARATOR + attributeValue(attr) + Composite.END_SEPARATOR;
                    } else {
                        dataList[i] += attributeValue(attr);
                    }
                    
                }
                timestamp = Math.max(timestamp, attr.getTimestamp());
                columnVisibilities.add(attr.getColumnVisibility());
            }
        }
        log.debug("dataList is " + Arrays.toString(dataList));
        ColumnVisibility combinedColumnVisibility = this.markingFunctions.combine(columnVisibilities);
        metadata = new Key(metadata.getRow(), metadata.getColumnFamily(), new Text(), combinedColumnVisibility, timestamp);
        if (dataList.length == 1) {
            return this.attrFactory.create(compositeName, dataList[0], metadata, toKeep);
        } else {
            Attributes attributes = new Attributes(toKeep);
            for (String d : dataList) {
                attributes.add(this.attrFactory.create(compositeName, d, metadata, toKeep));
            }
            return attributes;
        }
    }
    
    private String attributeValue(Attribute attr) {
        if (attr instanceof TypeAttribute) {
            return ((TypeAttribute) attr).getType().getNormalizedValue();
        } else {
            new Exception().printStackTrace(System.err);
            return String.valueOf(attr.getData());
        }
    }
    
    private ColumnVisibility getCV(Key k) {
        Text expr = k.getColumnVisibility(cvHolder);
        ColumnVisibility vis = (ColumnVisibility) cvCache.get(expr);
        if (vis == null) {
            // the column visibility needs to take ownership of the expression
            vis = new ColumnVisibility(new Text(expr));
            cvCache.put(expr, vis);
        }
        return vis;
    }
    
    protected String getDatatypeFromKey(Key key) {
        String colf = key.getColumnFamily().toString();
        int indexOfNull = colf.indexOf("\0");
        return colf.substring(0, indexOfNull);
    }
    
}
