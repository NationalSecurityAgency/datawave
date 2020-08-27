package datawave.query.iterator.logic;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.Tuple1;
import datawave.query.util.Tuple2;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Iterator expects Event Key/Value pairs and will filter to only matching field and apply normalizations. This will result in one Key generated per normalizer
 * applied to a field
 */
public class EventFieldNormalizingIterator implements SortedKeyValueIterator<Key,Value> {
    // speedy cache loading for types, duplicated from AttributeFactory with caching of types rather than classes
    protected static LoadingCache<String,Type<?>> typeCache = CacheBuilder.newBuilder().maximumSize(128).expireAfterAccess(1, TimeUnit.HOURS)
                    .build(new CacheLoader<String,Type<?>>() {
                        @Override
                        public Type<?> load(String clazz) throws Exception {
                            Class<?> c = Class.forName(clazz);
                            return (Type<?>) c.newInstance();
                        }
                    });
    
    private final String field;
    private final TypeMetadata typeMetadata;
    
    private SortedKeyValueIterator<Key,Value> delegate;
    private IteratorEnvironment environment;
    
    // sorted cache for normalized key/value pairs sorted by Key
    private SortedSet<Tuple2<Key,Value>> sorted = new TreeSet<>(Comparator.comparing(Tuple1::first));
    private String defaultTypeClass = NoOpType.class.getName();
    
    private boolean initialized = false;
    
    public EventFieldNormalizingIterator(String field, SortedKeyValueIterator<Key,Value> delegate, TypeMetadata typeMetadata, String defaultTypeClass) {
        this.field = field;
        this.delegate = delegate;
        this.typeMetadata = typeMetadata;
        this.defaultTypeClass = defaultTypeClass;
    }
    
    public EventFieldNormalizingIterator(EventFieldNormalizingIterator clone) {
        field = clone.field;
        typeMetadata = clone.typeMetadata;
        delegate = clone.delegate.deepCopy(clone.environment);
        environment = clone.environment;
        
        // sorted should not need to be copied since the new object must be seeked
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        delegate.init(source, options, env);
        environment = env;
    }
    
    @Override
    public boolean hasTop() {
        if (!initialized) {
            throw new IllegalStateException("must call seek before hasTop()");
        }
        
        // if the sorted set has more than one element in it there is always more
        if (sorted.size() > 0) {
            return true;
        } else {
            fillSorted();
            
            return sorted.size() > 0;
        }
    }
    
    @Override
    public void next() throws IOException {
        if (!initialized) {
            throw new IllegalStateException("must call seek before hasTop()");
        }
        
        // make sure we are exhausted
        if (sorted.size() == 0) {
            fillSorted();
        }
        
        if (sorted.size() == 0) {
            throw new NoSuchElementException("called next after exhausting all elements");
        }
        
        sorted.remove(sorted.first());
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // clear whatever is cached
        sorted.clear();
        
        delegate.seek(range, columnFamilies, inclusive);
        
        initialized = true;
        
        // populate sorted
        fillSorted();
    }
    
    @Override
    public Key getTopKey() {
        if (!initialized) {
            throw new IllegalStateException("must call seek before hasTop()");
        }
        
        return sorted.first().first();
    }
    
    @Override
    public Value getTopValue() {
        if (!initialized) {
            throw new IllegalStateException("must call seek before hasTop()");
        }
        
        return sorted.first().second();
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new EventFieldNormalizingIterator(this);
    }
    
    private void fillSorted() {
        Key baseKey = null;
        String dataType = null;
        
        // need to fill look ahead and fill the sorted set before answering. In order to guarantee ordering must grab all matching Key's for a given
        // cf/field, inserting the modified keys into sorted
        while (delegate.hasTop()) {
            // grab the first key for comparisons to know when its safe to stop
            if (baseKey == null) {
                baseKey = delegate.getTopKey();
            }
            
            Key current = delegate.getTopKey();
            
            // if this key has a different row/cf than the baseKey we have looked ahead far enough to guarantee sorted order.
            if (current.compareTo(baseKey, PartialKey.ROW_COLFAM) != 0) {
                // if sorted contains more than 1 key we have looked ahead far enough to find more keys and can stop, otherwise reset the baseKey and type
                // and continue to look
                if (sorted.size() > 0) {
                    break;
                } else {
                    // since there are no keys yet keep looking, clear assumptions about key and reset stop condition
                    baseKey = null;
                    dataType = null;
                }
            }
            
            // test if current matches the target field
            int fieldNameEnd = -1;
            ByteSequence cq = current.getColumnQualifierData();
            for (int i = 0; i < cq.getBackingArray().length; i++) {
                if (cq.byteAt(i) == '\u0000') {
                    fieldNameEnd = i;
                    break;
                }
            }
            
            String fieldName = null;
            // there are sometimes strange keys, make sure this one is well formed
            if (fieldNameEnd > 0) {
                fieldName = cq.subSequence(0, fieldNameEnd).toString();
                
                // remove any grouping context
                fieldName = JexlASTHelper.removeGroupingContext(fieldName);
            }
            
            // this matches the target field, normalize and add to sorted
            if (fieldName != null && field.equals(fieldName)) {
                // first get the data type from the key if we don't have it already
                if (dataType == null) {
                    int dataTypeEnd = -1;
                    ByteSequence cf = current.getColumnFamilyData();
                    for (int i = 0; i < cf.getBackingArray().length; i++) {
                        if (cf.byteAt(i) == '\u0000') {
                            dataTypeEnd = i;
                        }
                    }
                    
                    if (dataTypeEnd <= 0) {
                        throw new RuntimeException("malformed key, cannot parse data type from event");
                    }
                    
                    dataType = cf.subSequence(0, dataTypeEnd).toString();
                }
                
                // parse the value from the cq which should be fieldNameEnd + 1 to the end
                String fieldValue = cq.subSequence(fieldNameEnd + 1, cq.length()).toString();
                
                // fetch all Types for the field/dataType combination
                Collection<String> typeClasses = typeMetadata.getTypeMetadata(fieldName, dataType);
                
                // if its not found add the default
                if (typeClasses.size() == 0) {
                    typeClasses.add(defaultTypeClass);
                }
                
                // transform the key for each type and add it to sorted
                for (String typeClass : typeClasses) {
                    try {
                        Type<?> type = typeCache.get(typeClass);
                        String normalizedValue = type.normalize(fieldValue);
                        
                        // construct a new Key with the normalized value and put it into sorted
                        sorted.add(new Tuple2<>(new Key(current.getRow(), current.getColumnFamily(), new Text(fieldName + '\u0000' + normalizedValue)),
                                        delegate.getTopValue()));
                    } catch (ExecutionException e) {
                        throw new RuntimeException("cannot instantiate class '" + typeClass + "'", e);
                    }
                }
            }
            
            try {
                delegate.next();
            } catch (IOException e) {
                throw new RuntimeException("Could not get next from delegate");
            }
        }
    }
}
