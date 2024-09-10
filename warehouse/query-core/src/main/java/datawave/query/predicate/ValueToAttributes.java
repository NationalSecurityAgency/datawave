package datawave.query.predicate;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

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
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.TypeMetadata;

/**
 *
 * Unused. It was written to put Composite Fields into fetched documents. It is preserved in case we change the way composite fields are managed
 *
 *
 */
public class ValueToAttributes implements Function<Entry<Key,String>,Iterable<Entry<String,Attribute<? extends Comparable<?>>>>> {
    private static final Logger log = Logger.getLogger(ValueToAttributes.class);

    private final Text holder = new Text();

    private AttributeFactory attrFactory;

    private Map<String,Multimap<String,String>> compositeToFieldMap;
    private Map<String,Map<String,String>> compositeFieldSeparatorsByType;
    private MarkingFunctions markingFunctions;
    private Multimap<String,Attribute<?>> componentFieldToValues = ArrayListMultimap.create();

    private EventDataQueryFilter attrFilter;

    // Whether the value is from the index
    private final boolean fromIndex;

    public ValueToAttributes(CompositeMetadata compositeMetadata, TypeMetadata typeMetadata, EventDataQueryFilter attrFilter, MarkingFunctions markingFunctions,
                    boolean fromIndex) {
        this.attrFactory = new AttributeFactory(typeMetadata);
        this.markingFunctions = markingFunctions;
        this.attrFilter = attrFilter;
        if (compositeMetadata != null) {
            this.compositeToFieldMap = compositeMetadata.getCompositeFieldMapByType();
            this.compositeFieldSeparatorsByType = compositeMetadata.getCompositeFieldSeparatorsByType();
        }
        this.fromIndex = fromIndex;
    }

    @Override
    public Iterable<Entry<String,Attribute<? extends Comparable<?>>>> apply(Entry<Key,String> from) {
        String origFieldName = from.getValue();
        String modifiedFieldName = JexlASTHelper.deconstructIdentifier(origFieldName, false);
        Key key = from.getKey();

        Attribute<?> curAttr = getFieldValue(modifiedFieldName, key);

        // by default, we will return the attribute for the given entry
        List<Entry<String,Attribute<? extends Comparable<?>>>> list = new ArrayList<>();
        list.add(Maps.immutableEntry(origFieldName, curAttr));

        // check to see if we can create any composite attributes using this entry
        String ingestDatatype = this.getDatatypeFromKey(key);
        Multimap<String,String> compToFieldMap = (this.compositeToFieldMap != null) ? this.compositeToFieldMap.get(ingestDatatype) : null;
        if (compToFieldMap != null && !compToFieldMap.isEmpty()) {
            Multimap<String,String> inverted = Multimaps.invertFrom(compToFieldMap, ArrayListMultimap.create());
            // check to see if this entry can be used to create a composite
            if (inverted.containsKey(modifiedFieldName)) {
                // save a list of composites that could be built from this component
                List<String> compsToBuild = new ArrayList<>(inverted.get(modifiedFieldName));

                // add this component to the list of composite components
                componentFieldToValues.put(modifiedFieldName, curAttr);

                // try to build each of the composites with this component
                for (String composite : compsToBuild) {
                    List<Collection<Attribute<?>>> componentAttributes = new ArrayList<>();
                    Collection<String> components = compToFieldMap.get(composite);
                    for (String component : components) {
                        if (component.equals(modifiedFieldName)) {
                            // add the attribute from this entry
                            componentAttributes.add(Arrays.asList(curAttr));
                        } else if (componentFieldToValues.containsKey(component)) {
                            // add the attributes for this component
                            componentAttributes.add(componentFieldToValues.get(component));
                        } else {
                            // stop, we can't build this composite, as we're missing a component
                            break;
                        }
                    }

                    // if we have attributes for each component, we'll build composites
                    if (componentAttributes.size() == components.size()) {
                        list.addAll(buildCompositesFromComponents(
                                        CompositeIngest.isOverloadedCompositeField(this.compositeToFieldMap.get(ingestDatatype), composite), composite,
                                        componentAttributes, this.compositeFieldSeparatorsByType.get(ingestDatatype).get(composite)));
                    }
                }
            }
        }
        return list;
    }

    private List<Entry<String,Attribute<? extends Comparable<?>>>> buildCompositesFromComponents(boolean isOverloadedComposite, String compositeField,
                    List<Collection<Attribute<?>>> componentAttributes, String separator) {
        return buildCompositesFromComponents(isOverloadedComposite, compositeField, null, componentAttributes, separator);
    }

    private List<Entry<String,Attribute<? extends Comparable<?>>>> buildCompositesFromComponents(boolean isOverloadedComposite, String compositeField,
                    Collection<Attribute<?>> currentAttributes, List<Collection<Attribute<?>>> componentAttributes, String separator) {
        if (componentAttributes == null) {
            // finally create the composite from what we have
            try {
                return Arrays.asList(Maps.immutableEntry(compositeField, joinAttributes(compositeField, currentAttributes, isOverloadedComposite, separator)));
            } catch (Exception e) {
                log.debug("could not join attributes:", e);
            }
        } else {
            List<Entry<String,Attribute<? extends Comparable<?>>>> compositeAttributes = new ArrayList<>();
            Collection<Attribute<?>> compAttrs = componentAttributes.get(0);
            for (Attribute<?> compAttr : compAttrs) {
                List<Attribute<?>> attrs = (currentAttributes != null) ? new ArrayList<>(currentAttributes) : new ArrayList<>();
                attrs.add(compAttr);
                compositeAttributes.addAll(buildCompositesFromComponents(isOverloadedComposite, compositeField, attrs,
                                (componentAttributes.size() > 1) ? componentAttributes.subList(1, componentAttributes.size()) : null, separator));
            }
            return compositeAttributes;
        }
        return new ArrayList<>();
    }

    public String getCompositeAttributeKey(String composite, String grouping) {
        if (grouping == null || grouping.isEmpty()) {
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

            Attribute<?> attr = this.attrFactory.create(fieldName, data, k, (attrFilter == null || attrFilter.keep(k)));
            if (attrFilter != null) {
                attr.setToKeep(attrFilter.keep(k));
            }
            attr.setFromIndex(fromIndex);

            if (log.isTraceEnabled()) {
                log.trace("Created " + attr.getClass().getName() + " for " + fieldName);
            }

            return attr;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public Attribute<?> joinAttributes(String compositeName, Collection<Attribute<?>> in, boolean isOverloadedComposite, String separator) throws Exception {
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
                    if (dataList.isEmpty()) {
                        dataList.addAll(attributeValues(attrs));
                    } else {
                        for (int i = 0; i < originalDataListSize; i++) {
                            String base = dataList.remove(0);
                            if (base == null) {
                                dataList.addAll(attributeValues(attrs));
                            } else if (!base.isEmpty()) {
                                for (String value : attributeValues(attrs))
                                    dataList.add(base + separator + value);
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
                if (dataList.isEmpty()) {
                    dataList.addAll(attributeValues(attr));
                } else {
                    int originalDataListSize = dataList.size();
                    for (int i = 0; i < originalDataListSize; i++) { // append to everything in the list
                        String base = dataList.remove(0);
                        if (!base.isEmpty()) {
                            for (String value : attributeValues(attr))
                                dataList.add(base + separator + value);
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
        if (log.isDebugEnabled()) {
            log.debug("dataList is " + dataList);
        }
        ColumnVisibility combinedColumnVisibility = this.markingFunctions.combine(columnVisibilities);
        metadata = new Key(metadata.getRow(), metadata.getColumnFamily(), new Text(), combinedColumnVisibility, timestamp);
        if (dataList.size() == 1) {
            return this.attrFactory.create(compositeName, dataList.get(0), metadata, toKeep, true);
        } else {
            Attributes attributes = new Attributes(toKeep);
            for (String d : dataList) {
                attributes.add(this.attrFactory.create(compositeName, d, metadata, toKeep, true));
            }
            return attributes;
        }
    }

    private List<String> attributeValues(Attributes attrs) {
        List<String> attributeValues = new ArrayList<>();
        for (Attribute<?> attr : attrs.getAttributes()) {
            List<String> attrValues = attributeValues(attr);
            if (attrValues != null && !attrValues.isEmpty())
                attributeValues.addAll(attrValues);
        }
        return attributeValues;
    }

    private List<String> attributeValues(Attribute<?> attr) {
        if (attr instanceof TypeAttribute) {
            Type<?> type = ((TypeAttribute<?>) attr).getType();
            return (type instanceof OneToManyNormalizerType) ? ((OneToManyNormalizerType<?>) type).getNormalizedValues()
                            : Arrays.asList(type.getNormalizedValue());
        } else {
            new Exception().printStackTrace(System.err);
            return Arrays.asList(String.valueOf(attr.getData()));
        }
    }

    protected String getDatatypeFromKey(Key key) {
        String cf = key.getColumnFamily().toString();
        int indexOfNull = cf.indexOf('\u0000');
        return cf.substring(0, indexOfNull);
    }

}
