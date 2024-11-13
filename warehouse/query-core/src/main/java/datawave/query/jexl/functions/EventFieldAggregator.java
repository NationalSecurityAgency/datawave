package datawave.query.jexl.functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import datawave.data.type.Type;
import datawave.data.type.TypeFactory;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.util.Tuple2;
import datawave.query.util.TypeMetadata;

public class EventFieldAggregator extends IdentityAggregator {

    private final TypeMetadata typeMetadata;
    private final String defaultTypeClass;

    private int typeCacheSize = -1;
    private int typeCacheTimeoutMinutes = -1;
    private TypeFactory typeFactory;

    public EventFieldAggregator(String field, EventDataQueryFilter filter, int maxNextCount, TypeMetadata typeMetadata, String defaultTypeClass) {
        super(Collections.singleton(field), filter, maxNextCount);

        this.typeMetadata = typeMetadata;
        this.defaultTypeClass = defaultTypeClass;
    }

    @Override
    protected List<Tuple2<String,String>> parserFieldNameValue(Key topKey) {
        String cq = topKey.getColumnQualifier().toString();
        int nullIndex1 = cq.indexOf('\u0000');
        String field = cq.substring(0, nullIndex1);
        String value = cq.substring(nullIndex1 + 1);

        int dataTypeEnd = -1;
        ByteSequence cf = topKey.getColumnFamilyData();
        for (int i = 0; i < cf.length(); i++) {
            if (cf.byteAt(i) == '\u0000') {
                dataTypeEnd = i;
                break;
            }
        }

        if (dataTypeEnd <= 0) {
            throw new RuntimeException("malformed key, cannot parse data type from event");
        }

        String dataType = cf.subSequence(0, dataTypeEnd).toString();

        Set<String> normalizedValues = getNormalizedValues(dataType, field, value);
        List<Tuple2<String,String>> fieldValuePairs = new ArrayList<>(normalizedValues.size());
        for (String normalizedValue : normalizedValues) {
            fieldValuePairs.add(new Tuple2<>(field, normalizedValue));
        }

        return fieldValuePairs;
    }

    @Override
    protected ByteSequence parseFieldNameValue(ByteSequence cf, ByteSequence cq) {
        return cq;
    }

    @Override
    protected ByteSequence getPointerData(Key key) {
        return key.getColumnFamilyData();
    }

    @Override
    protected ByteSequence parsePointer(ByteSequence columnFamily) {
        return columnFamily;
    }

    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Document doc, AttributeFactory attrs) throws IOException {
        Document d = new Document();
        Key result = super.apply(itr, d, attrs);

        // for each thing in the doc, mark it as to-keep false because it will ultimately come from the document aggregation, otherwise there will be duplicates
        for (Attribute<?> attr : d.getDictionary().values()) {
            attr.setToKeep(false);
        }

        doc.putAll(d, false);

        return result;
    }

    private Set<String> getNormalizedValues(String dataType, String fieldName, String fieldValue) {
        Set<String> normalizedValues = new HashSet<>();

        // fetch all Types for the field/dataType combination, make a modifiable copy
        Collection<String> typeClasses = new HashSet<>(typeMetadata.getTypeMetadata(fieldName, dataType));

        // if its not found add the default
        if (typeClasses.size() == 0) {
            typeClasses.add(defaultTypeClass);
        }

        // transform the key for each type and add it to the normalized set
        for (String typeClass : typeClasses) {
            Type<?> type = getTypeFactory().createType(typeClass);
            String normalizedValue = type.normalize(fieldValue);

            normalizedValues.add(normalizedValue);
        }

        return normalizedValues;
    }

    /**
     * Get the TypeFactory. If no TypeFactory exists one will be created. Configs for cache size and timeout may be configured.
     *
     * @return the TypeFactory
     */
    private TypeFactory getTypeFactory() {
        if (typeFactory == null) {
            if (typeCacheSize != -1 && typeCacheTimeoutMinutes != -1) {
                typeFactory = new TypeFactory(typeCacheSize, typeCacheTimeoutMinutes);
            } else {
                typeFactory = new TypeFactory();
            }
        }
        return typeFactory;
    }

    /**
     * Set the cache size for the TypeFactory
     *
     * @param typeCacheSize
     *            the cache size
     */
    public void setTypeCacheSize(int typeCacheSize) {
        this.typeCacheSize = typeCacheSize;
    }

    /**
     * Set the timeout for the TypeFactory
     *
     * @param typeCacheTimeoutMinutes
     *            the timeout
     */
    public void setTypeCacheTimeoutMinutes(int typeCacheTimeoutMinutes) {
        this.typeCacheTimeoutMinutes = typeCacheTimeoutMinutes;
    }
}
