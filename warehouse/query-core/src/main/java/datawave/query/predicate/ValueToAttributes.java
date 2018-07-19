package datawave.query.predicate;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import datawave.data.type.OneToManyNormalizerType;
import datawave.data.type.Type;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.TypeAttribute;
import datawave.query.composite.CompositeMetadata;
import datawave.query.composite.CompositeUtils;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
        this.compositeToFieldMap = compositeMetadata.getCompositeFieldMapByType();
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
                    boolean isOverloadedComposite = CompositeIngest.isOverloadedCompositeField(
                                    this.compositeToFieldMap.get(ingestDatatype).get(compositeFieldMapKey), compositeFieldMapKey);
                    Attribute<? extends Comparable<?>> combined = this
                                    .joinAttributes(composite, this.compositeAttributes.get(composite), isOverloadedComposite);
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
        if (grouping == null || grouping.length() == 0) {
            return composite;
        }
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
    
    public Attribute<?> joinAttributes(String compositeName, Collection<Attribute<?>> in, boolean isOverloadedComposite) throws Exception {
        Collection<ColumnVisibility> columnVisibilities = Sets.newHashSet();
        List<String> dataList = new ArrayList<>();
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
            if (attr.isToKeep() && !isOverloadedComposite) {
                toKeep = true;
            }
            if (attr instanceof Attributes) {
                Attributes attrs = (Attributes) attr;
                int newAttributeCount = attrs.size();
                int originalDataListSize = dataList.size();
                if (newAttributeCount > 0) {
                    if (dataList.size() == 0) {
                        for (String value : attributeValues(attrs))
                            dataList.add(value);
                    } else {
                        for (int i = 0; i < originalDataListSize; i++) {
                            String base = dataList.remove(0);
                            if (base == null) {
                                for (String value : attributeValues(attrs))
                                    dataList.add(value);
                            } else if (base.length() > 0) {
                                for (String value : attributeValues(attrs))
                                    dataList.add(base + CompositeUtils.SEPARATOR + value);
                            } else {
                                for (String value : attributeValues(attrs))
                                    dataList.add(base + value);
                            }
                            timestamp = Math.max(timestamp, attrs.getTimestamp());
                            columnVisibilities.add(attrs.getColumnVisibility());
                        }
                    }
                }
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("Join for data list size: " + dataList.size());
                }
                if (dataList.size() == 0) {
                    for (String value : attributeValues(attr))
                        dataList.add(value);
                } else {
                    int originalDataListSize = dataList.size();
                    for (int i = 0; i < originalDataListSize; i++) { // append to everything in the list
                        String base = dataList.remove(0);
                        if (base.length() > 0) {
                            for (String value : attributeValues(attr))
                                dataList.add(base + CompositeUtils.SEPARATOR + value);
                        } else {
                            for (String value : attributeValues(attr))
                                dataList.add(base + value);
                        }
                    }
                }
                timestamp = Math.max(timestamp, attr.getTimestamp());
                columnVisibilities.add(attr.getColumnVisibility());
            }
        }
        log.debug("dataList is " + dataList.toString());
        ColumnVisibility combinedColumnVisibility = this.markingFunctions.combine(columnVisibilities);
        metadata = new Key(metadata.getRow(), metadata.getColumnFamily(), new Text(), combinedColumnVisibility, timestamp);
        if (dataList.size() == 1) {
            return this.attrFactory.create(compositeName, dataList.get(0), metadata, toKeep);
        } else {
            Attributes attributes = new Attributes(toKeep);
            for (String d : dataList) {
                attributes.add(this.attrFactory.create(compositeName, d, metadata, toKeep));
            }
            return attributes;
        }
    }
    
    private List<String> attributeValues(Attributes attrs) {
        List<String> attributeValues = new ArrayList<>();
        for (Attribute attr : attrs.getAttributes()) {
            List<String> attrValues = attributeValues(attr);
            if (attrValues != null && !attrValues.isEmpty())
                attributeValues.addAll(attrValues);
        }
        return attributeValues;
    }
    
    private List<String> attributeValues(Attribute attr) {
        if (attr instanceof TypeAttribute) {
            Type type = ((TypeAttribute) attr).getType();
            return (type instanceof OneToManyNormalizerType) ? ((OneToManyNormalizerType) type).getNormalizedValues() : Arrays
                            .asList(type.getNormalizedValue());
        } else {
            new Exception().printStackTrace(System.err);
            return Arrays.asList(String.valueOf(attr.getData()));
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
