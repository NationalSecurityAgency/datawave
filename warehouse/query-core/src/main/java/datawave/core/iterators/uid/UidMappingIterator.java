package datawave.core.iterators.uid;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import datawave.query.QueryParameters;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

/**
 * <p>
 * An iterator that will map uids in table entries per a configured UidMapper.
 * </p>
 * 
 * 
 * @see datawave.core.iterators.uid.UidMapper
 * 
 */
public abstract class UidMappingIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    
    public static final String UID_MAPPER = QueryParameters.QUERY_CONTEXT_UID_MAPPER;
    public static final byte[] EMPTY_BYTES = new byte[0];
    
    protected Key topKey = null;
    protected Value topValue = null;
    protected SortedKeyValueIterator<Key,Value> source = null;
    protected UidMapper uidMapper = null;
    
    public UidMappingIterator() {}
    
    public UidMappingIterator(UidMappingIterator iter, IteratorEnvironment env) {
        this.source = (iter.source == null ? null : iter.source.deepCopy(env));
        this.uidMapper = iter.uidMapper;
        this.topKey = (iter.topKey == null ? null : new Key(iter.topKey));
        this.topValue = (iter.topValue == null ? null : new Value(iter.topValue));
    }
    
    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(UID_MAPPER, "UidMapper class");
        return new IteratorOptions(getClass().getSimpleName(), "Enriches the data passing through the iterator", options, null);
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (options.containsKey(UidMappingIterator.UID_MAPPER)) {
            Class<?> uidMapperClass;
            try {
                uidMapperClass = Class.forName(options.get(UidMappingIterator.UID_MAPPER));
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Cannot find class for " + UidMappingIterator.UID_MAPPER + " option: "
                                + options.get(UidMappingIterator.UID_MAPPER), e);
            }
            if (!UidMapper.class.isAssignableFrom(uidMapperClass)) {
                throw new IllegalArgumentException(UidMappingIterator.UID_MAPPER + " option does not implement " + UidMapper.class + ": " + uidMapperClass);
            }
            try {
                this.uidMapper = (UidMapper) (uidMapperClass.getDeclaredConstructor().newInstance());
            } catch (InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                throw new IllegalArgumentException("Cannot instantiate class for " + UidMappingIterator.UID_MAPPER + " option: " + uidMapperClass, e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Cannot access constructor for " + UidMappingIterator.UID_MAPPER + " option: " + uidMapperClass, e);
            }
            return true;
        }
        
        return false;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (!validateOptions(options)) {
            throw new IOException("Invalid options");
        }
        
        this.topKey = null;
        this.topValue = null;
        this.source = source;
    }
    
    @Override
    public boolean hasTop() {
        return this.source != null && this.topKey != null;
    }
    
    @Override
    public void next() throws IOException {
        if (this.source == null) {
            return;
        }
        
        // next the source
        this.source.next();
        
        // and find the top
        findTop();
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (this.source == null) {
            return;
        }
        
        // remap the range etc. if needed to encompass all of the keys that will map into this range
        SeekParams params = mapSeek(range, columnFamilies, inclusive);
        
        // seek the source
        this.source.seek(params.range, params.columnFamilies, params.inclusive);
        
        // and find the top
        findTop();
    }
    
    protected void findTop() throws IOException {
        if (this.source.hasTop()) {
            KeyValue keyValue = mapUid(new KeyValue(this.source.getTopKey(), this.source.getTopValue().get()), false, false, false, false);
            this.topKey = keyValue.getKey();
            this.topValue = keyValue.getValue();
        } else {
            this.topKey = null;
            this.topValue = null;
        }
    }
    
    @Override
    public Key getTopKey() {
        return this.topKey;
    }
    
    @Override
    public Value getTopValue() {
        return this.topValue;
    }
    
    /**
     * Map the uid in the supplied key and value.
     * 
     * @param keyValue
     *            The key and value to map. Could be null.
     * @param startKey
     *            true if this is the startKey in a range
     * @param startKeyInclusive
     *            true if the start key was inclusive
     * @param endKey
     *            true if this is the endKey in a range
     * @param endKeyInclusive
     *            true if the end key was inclusive
     * @return the keyValue with the uid mapped appropriately
     */
    protected KeyValue mapUid(KeyValue keyValue, boolean startKey, boolean startKeyInclusive, boolean endKey, boolean endKeyInclusive) {
        return keyValue;
    }
    
    /**
     * Map the uid in the supplied key.
     * 
     * @param key
     *            The key to map. Could be null.
     * @param startKey
     *            true if this is the startKey in a range
     * @param startKeyInclusive
     *            true if the start key was inclusive
     * @param endKey
     *            true if this is the endKey in a range
     * @param endKeyInclusive
     *            true if the end key was inclusive
     * @return the key with the uid mapped appropriately
     */
    protected Key mapUid(Key key, boolean startKey, boolean startKeyInclusive, boolean endKey, boolean endKeyInclusive) {
        return mapUid(new KeyValue(key, EMPTY_BYTES), startKey, startKeyInclusive, endKey, endKeyInclusive).getKey();
    }
    
    /**
     * Map the seek parameters so that they include all uids that map to the original range
     * 
     * @param range
     *            the range provided
     * @return the modified seek parameters
     */
    protected Range mapRange(Range range) {
        if (range != null) {
            range = new Range(mapUid(range.getStartKey(), true, range.isStartKeyInclusive(), false, false), range.isStartKeyInclusive(), mapUid(
                            range.getEndKey(), false, false, true, range.isEndKeyInclusive()), range.isEndKeyInclusive());
        }
        return range;
    }
    
    /**
     * Map the column families so that includes all uids that map to the original column family filter
     * 
     * @param range
     *            the range provided
     * @param columnFamilies
     *            the column families
     * @param inclusive
     *            inclusive flag
     * @return the new set of column families
     */
    protected Collection<ByteSequence> mapColumnFamilies(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) {
        if (columnFamilies != null && !columnFamilies.isEmpty()) {
            List<ByteSequence> newCfs = new ArrayList<>();
            for (ByteSequence cf : columnFamilies) {
                Key key = new Key((range == null ? new Text() : range.getStartKey().getRow()), new Text(cf.toArray()));
                key = mapUid(key, false, false, false, false);
                newCfs.add(key.getColumnFamilyData());
            }
            columnFamilies = newCfs;
        }
        return columnFamilies;
    }
    
    /**
     * Map the seek parameters so that they include all uids that map to the original range. Uses mapRange and mapColumnFamilies
     * 
     * @param range
     *            the range
     * @param columnFamilies
     *            families
     * @param inclusive
     *            column families filter
     * @return the modified seek parameters
     */
    protected SeekParams mapSeek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) {
        SeekParams params = new SeekParams();
        params.range = mapRange(range);
        params.columnFamilies = mapColumnFamilies(range, columnFamilies, inclusive);
        params.inclusive = inclusive;
        return params;
    }
    
    /**
     * The class returned by mapSeek
     * 
     * 
     * 
     */
    protected static class SeekParams {
        public Range range;
        public Collection<ByteSequence> columnFamilies;
        public boolean inclusive;
        
        public boolean contains(SeekParams params) {
            if (inclusive == params.inclusive) {
                if (new HashSet<>(columnFamilies).equals(new HashSet<>(params.columnFamilies))) {
                    if ((range.getStartKey() == null && params.range.getStartKey() == null)
                                    || (range.isStartKeyInclusive() && params.range.isStartKeyInclusive() && range.getStartKey() != null
                                                    && params.range.getStartKey() != null && range.getStartKey().compareTo(params.range.getStartKey()) <= 0)) {
                        if ((range.getEndKey() == null && params.range.getEndKey() == null)
                                        || (range.getEndKey() != null && params.range.getEndKey() != null && range.getEndKey().compareTo(
                                                        params.range.getEndKey()) >= 0)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }
    
    @Override
    public abstract SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env);
}
