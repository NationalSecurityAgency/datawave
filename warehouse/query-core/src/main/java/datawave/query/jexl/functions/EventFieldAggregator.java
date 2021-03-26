package datawave.query.jexl.functions;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.util.Tuple2;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class EventFieldAggregator extends IdentityAggregator {
    // speedy cache loading for types, duplicated from AttributeFactory with caching of types rather than classes
    protected static final LoadingCache<String,Type<?>> typeCache = CacheBuilder.newBuilder().maximumSize(128).expireAfterAccess(1, TimeUnit.HOURS)
                    .build(new CacheLoader<String,Type<?>>() {
                        @Override
                        public Type<?> load(String clazz) throws Exception {
                            Class<?> c = Class.forName(clazz);
                            return (Type<?>) c.newInstance();
                        }
                    });
    
    private TypeMetadata typeMetadata;
    private String defaultTypeClass;
    
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
            try {
                Type<?> type = typeCache.get(typeClass);
                String normalizedValue = type.normalize(fieldValue);
                
                normalizedValues.add(normalizedValue);
            } catch (ExecutionException e) {
                throw new RuntimeException("cannot instantiate class '" + typeClass + "'", e);
            }
        }
        
        return normalizedValues;
    }
}
