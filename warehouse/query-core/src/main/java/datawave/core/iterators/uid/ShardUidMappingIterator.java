package datawave.core.iterators.uid;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import datawave.util.StringUtils;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

/**
 * <p>
 * An iterator that will map uids in shard table entries per a configured UidMapper.
 * </p>
 * <p>
 * This iterator will cache all of the entries for the same base UID to ensure that the resulting series is sorted correctly per the contract of an
 * SortedKeyValueIterator.
 * </p>
 * 
 * 
 * @see datawave.core.iterators.uid.UidMapper
 * 
 */
public class ShardUidMappingIterator extends UidMappingIterator {
    
    // a filter for column families
    protected Collection<ByteSequence> columnFamilies = null;
    protected boolean inclusive = false;
    
    protected Key cacheBaseUidKey = null;
    protected SortedMap<Key,ByteArrayOutputStream> cache = new TreeMap<>();
    protected Key cacheTopKey;
    protected Value cacheTopValue;
    protected SeekParams lastSeekParams;
    
    public ShardUidMappingIterator() {}
    
    public ShardUidMappingIterator(ShardUidMappingIterator iter, IteratorEnvironment env) {
        super(iter, env);
        this.columnFamilies = (iter.columnFamilies == null ? null : new ArrayList<>(iter.columnFamilies));
        this.inclusive = iter.inclusive;
        this.cacheBaseUidKey = (iter.cacheBaseUidKey == null ? null : new Key(iter.cacheBaseUidKey));
        for (Map.Entry<Key,ByteArrayOutputStream> entry : iter.cache.entrySet()) {
            this.cache.put(entry.getKey(), entry.getValue());
        }
        this.cacheTopKey = (iter.cacheTopKey == null ? null : new Key(iter.cacheTopKey));
        this.cacheTopValue = (iter.cacheTopValue == null ? null : new Value(iter.cacheTopValue.get()));
        this.lastSeekParams = null;
        if (iter.lastSeekParams != null) {
            this.lastSeekParams = new SeekParams();
            this.lastSeekParams.range = new Range(iter.lastSeekParams.range);
            this.lastSeekParams.inclusive = iter.lastSeekParams.inclusive;
            if (iter.lastSeekParams.columnFamilies != null) {
                this.lastSeekParams.columnFamilies = new ArrayList<>(iter.lastSeekParams.columnFamilies);
            }
        }
    }
    
    /**
     * Map the uid in the supplied key and value. The formats expected are for the shard table only.
     * 
     * @param keyValue
     *            the key value
     * @param startKey
     *            true if this is the startKey in a range
     * @param startKeyInclusive
     *            true if the start key was inclusive
     * @param endKey
     *            true if this is the endKey in a range
     * @param endKeyInclusive
     *            true if the end key was inclusive
     * @return the key with the uid mapped appropriately. The original UID if any is placed in the value.
     */
    @Override
    protected KeyValue mapUid(KeyValue keyValue, boolean startKey, boolean startKeyInclusive, boolean endKey, boolean endKeyInclusive) {
        if (keyValue == null || keyValue.getKey() == null) {
            return keyValue;
        }
        // pull the column family
        String cf = keyValue.getKey().getColumnFamily().toString();
        int index = cf.indexOf('\0');
        if (index > 0) {
            if (cf.startsWith("fi\0")) {
                keyValue = replaceEventUidInCQ(keyValue, 2, startKey, startKeyInclusive, endKey, endKeyInclusive);
            } else { // assume DataType\0UID column family
                keyValue = replaceEventUidInCF(keyValue, 1, startKey, startKeyInclusive, endKey, endKeyInclusive);
            }
        } else if (cf.equals("d")) {
            keyValue = replaceEventUidInCQ(keyValue, 1, startKey, startKeyInclusive, endKey, endKeyInclusive);
        } else if (cf.equals("tf")) {
            keyValue = replaceEventUidInCQ(keyValue, 1, startKey, startKeyInclusive, endKey, endKeyInclusive);
        }
        return keyValue;
    }
    
    /**
     * Get the top key
     *
     * @return the top key
     */
    @Override
    public Key getTopKey() {
        return this.cacheTopKey;
    }
    
    /**
     * Get the top value
     *
     * @return the top value
     */
    @Override
    public Value getTopValue() {
        return this.cacheTopValue;
    }
    
    /**
     * Do we have a top key and value?
     *
     * @return boolean on whether the top is currently held
     */
    @Override
    public boolean hasTop() {
        return this.source != null && this.cacheTopKey != null;
    }
    
    /**
     * Seek to a range and setup the next top key and value
     *
     * @param range
     *            the range
     * @param columnFamilies
     *            the column families
     * @param inclusive
     *            inclusive flag
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (this.source == null) {
            return;
        }
        
        // remap the range etc. if needed to encompass all of the keys that will map into this range
        SeekParams params = mapSeek(range, columnFamilies, inclusive);
        
        // seek the source
        this.source.seek(params.range, params.columnFamilies, params.inclusive);
        
        // if the mapped range is contained by the current mapped range
        // and the start key has the same baseUid,
        // and we have not already passed by the start key
        // then simply seek within the current cache
        boolean cacheSeeked = false;
        if (cacheBaseUidKey != null && ((cacheTopKey != null) || !cache.isEmpty()) && (lastSeekParams != null) && (range.getStartKey() != null)) {
            if (cacheTopKey == null) {
                findCacheTop();
            }
            if (range.beforeStartKey(cacheTopKey) && getBaseUidKey(range.getStartKey()).equals(cacheBaseUidKey) && lastSeekParams.contains(params)) {
                cacheSeek(range);
                cacheSeeked = true;
            }
        }
        
        // else clear the cache and reload
        if (!cacheSeeked) {
            cache.clear();
            
            // recache for this base uid
            findTop();
            if (super.topKey != null) {
                cacheKeys(getBaseUidKey(super.topKey));
            }
            
            // and get the first item off of the cache in the range specified
            cacheSeek(range);
        }
        
        // and remember what we did
        lastSeekParams = params;
    }
    
    /**
     * Find the next top key and value
     *
     * @throws IOException
     *             for issues with read/write
     */
    @Override
    public void next() throws IOException {
        // seek to the next item in the cache...but if empty and the source had more data
        if (!findCacheTop() && super.topKey != null) {
            
            // then cache the keys
            cacheKeys(getBaseUidKey(super.topKey));
            
            // and get the first item off of the cache
            findCacheTop();
        }
    }
    
    /**
     * @param range
     *            the range
     * @param columnFamilies
     *            the column families
     * @param inclusive
     *            inclusive flag
     * @see datawave.core.iterators.uid.UidMappingIterator#mapSeek(org.apache.accumulo.core.data.Range, java.util.Collection, boolean)
     */
    @Override
    protected SeekParams mapSeek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) {
        this.columnFamilies = null;
        
        SeekParams params = new SeekParams();
        params.range = mapRange(range);
        params.columnFamilies = columnFamilies;
        params.inclusive = inclusive;
        
        // if the column families contain UIDs, then save them off and use them as a post filter
        if (columnFamilies != null && !columnFamilies.isEmpty()) {
            boolean containsUid = false;
            for (ByteSequence cfBytes : columnFamilies) {
                String cf = cfBytes.toString();
                int index = cf.indexOf('\0');
                if (index > 0) {
                    if (!cf.startsWith("fi\0")) {
                        containsUid = true;
                        break;
                    }
                }
            }
            if (containsUid) {
                this.columnFamilies = columnFamilies;
                this.inclusive = inclusive;
                params.columnFamilies = Collections.emptyList();
                params.inclusive = false;
            }
        }
        
        return params;
    }
    
    /**
     * Copy this iterator in a deep kind of way
     *
     * @param env
     *            The iterator environment
     * @return an iterator of key/values
     */
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new ShardUidMappingIterator(this, env);
    }
    
    /**
     * Find the top for the underlying source iterator. This does not find the top for this iterator (@see findCacheTop).
     *
     * @throws IOException
     *             for issues with read/write
     */
    @Override
    protected void findTop() throws IOException {
        // get the next top
        super.findTop();
        // and loop while the top is not contained by the filter
        while (super.topKey != null && !containedByFilter(super.topKey)) {
            this.source.next();
            super.findTop();
        }
    }
    
    // Helper methods
    private KeyValue replaceEventUidInCQ(KeyValue keyValue, int partIndex, boolean startKey, boolean startKeyInclusive, boolean endKey,
                    boolean endKeyInclusive) {
        Key key = keyValue.getKey();
        String[] replacement = replaceUid(key.getColumnQualifier().toString(), partIndex, startKey, startKeyInclusive, endKey, endKeyInclusive);
        // if no change in the uid, then return the original key
        if (replacement == null) {
            return keyValue;
        }
        
        Key newKey = new Key(key.getRow(), key.getColumnFamily(), new Text(replacement[CQ_INDEX]), key.getColumnVisibility(), key.getTimestamp());
        return new KeyValue(newKey, replacement[ORG_UID_INDEX].getBytes());
    }
    
    private KeyValue replaceEventUidInCF(KeyValue keyValue, int partIndex, boolean startKey, boolean startKeyInclusive, boolean endKey,
                    boolean endKeyInclusive) {
        Key key = keyValue.getKey();
        String[] replacement = replaceUid(key.getColumnFamily().toString(), partIndex, startKey, startKeyInclusive, endKey, endKeyInclusive);
        // if no change in the uid, then return the original key
        if (replacement == null) {
            return keyValue;
        }
        
        Key newKey = new Key(key.getRow(), new Text(replacement[CQ_INDEX]), key.getColumnQualifier(), key.getColumnVisibility(), key.getTimestamp());
        return new KeyValue(newKey, replacement[ORG_UID_INDEX].getBytes());
    }
    
    private static final int ORG_UID_INDEX = 0;
    private static final int NEW_UID_INDEX = 1;
    private static final int CQ_INDEX = 2;
    
    /**
     * Replace the uid in the specified "part" of the value (delimited by '\0' characters)
     *
     * @param value
     *            the value
     * @param partIndex
     *            the "part" index
     * @param startKey
     *            true if this is the startKey in a range
     * @param startKeyInclusive
     *            true if the start key was inclusive
     * @param endKey
     *            true if this is the endKey in a range
     * @param endKeyInclusive
     *            true if the end key was inclusive
     * @return The value with the uid replaced. Null if no changes required
     */
    private String[] replaceUid(String value, int partIndex, boolean startKey, boolean startKeyInclusive, boolean endKey, boolean endKeyInclusive) {
        String[] parts = StringUtils.split(value, '\0', true);
        if (parts.length <= partIndex) {
            return null;
        }
        String extra = null;
        // if the part we are replacing is the last part, then pull off any invalid UID characters (non printables)
        // in case this is coming from a start or end key of a range
        if (partIndex == parts.length - 1) {
            String part = parts[partIndex];
            int index = part.length() - 1;
            while (index >= 0 && (part.charAt(index) > '~' || part.charAt(index) <= ' ')) {
                index--;
            }
            if (index < part.length() - 1) {
                extra = part.substring(index + 1);
                parts[partIndex] = part.substring(0, index + 1);
            }
        }
        String orgUid = parts[partIndex];
        String newUid = null;
        if (startKey) {
            // if we had extra characters, or not startKeyInclusive, then we do not want an inclusive start key
            newUid = uidMapper.getStartKeyUidMapping(orgUid, (extra == null && startKeyInclusive));
        } else if (endKey) {
            // if we had extra characters, or not endKeyInclusive, then we do not want an inclusive end key
            newUid = uidMapper.getEndKeyUidMapping(orgUid, (extra == null && endKeyInclusive));
        } else {
            newUid = uidMapper.getUidMapping(orgUid);
        }
        if (newUid == null) {
            return null;
        }
        parts[partIndex] = newUid;
        StringBuilder buffer = new StringBuilder();
        String separator = "";
        for (int i = 0; i < parts.length; i++) {
            buffer.append(separator).append(parts[i]);
            separator = "\0";
        }
        String[] replacement = new String[3];
        replacement[ORG_UID_INDEX] = orgUid;
        replacement[NEW_UID_INDEX] = newUid;
        replacement[CQ_INDEX] = buffer.toString();
        return replacement;
    }
    
    /**
     * Is this key contained by our column family filter
     * 
     * @param key
     *            the key
     * @return true/false if this key is contained by the column family filter
     */
    protected boolean containedByFilter(Key key) {
        boolean contained = true;
        if (this.columnFamilies != null) {
            contained = (this.inclusive ? this.columnFamilies.contains(key.getColumnFamilyData()) : !this.columnFamilies.contains(key.getColumnFamilyData()));
        }
        return contained;
    }
    
    /**
     * Get the base uid key for a key. This returns a key that contains only the shardid, datatype, and base UID. Used to determine how much we cache.
     * 
     * @param key
     *            the key
     * @return the base uid key
     */
    protected Key getBaseUidKey(Key key) {
        // pull the column family
        String cf = key.getColumnFamily().toString();
        int index = cf.indexOf('\0');
        if (index > 0) {
            if (cf.startsWith("fi\0")) {
                String cq = key.getColumnQualifier().toString();
                index = cq.indexOf('\0');
                if (index < 0) {
                    return new Key(key.getRow(), key.getColumnQualifier());
                } else {
                    return new Key(key.getRow(), new Text(cq.substring(index + 1)));
                }
            } else { // assume DataType\0UID column family
                return new Key(key.getRow(), key.getColumnFamily());
            }
        } else { // assume DataType\0UID\)... column qualifier
            String cq = key.getColumnQualifier().toString();
            index = cq.indexOf('\0');
            if (index < 0) {
                return new Key(key.getRow());
            }
            int index2 = cq.indexOf('\0', index + 1);
            if (index2 < 0) {
                return new Key(key.getRow(), key.getColumnQualifier());
            } else {
                return new Key(key.getRow(), new Text(cq.substring(0, index2)));
            }
        }
    }
    
    /**
     * Cache keys that map to the same base uid key
     * 
     * @param baseUidKey
     *            the base uid key
     * @throws IOException
     *             if there is an issue with read/write
     */
    protected void cacheKeys(Key baseUidKey) throws IOException {
        this.cacheBaseUidKey = baseUidKey;
        
        // now cache data until we run out or move to a new base uid
        while (super.topKey != null && baseUidKey.equals(getBaseUidKey(super.topKey), PartialKey.ROW_COLFAM)) {
            cacheAdd(super.topKey, super.topValue);
            super.next();
        }
    }
    
    /**
     * Find the next item from the cache
     * 
     * @return the cache top
     */
    protected boolean findCacheTop() {
        if (!cache.isEmpty()) {
            this.cacheTopKey = cache.firstKey();
            this.cacheTopValue = new Value(cache.remove(this.cacheTopKey).toByteArray());
            return true;
        } else {
            this.cacheTopKey = null;
            this.cacheTopValue = null;
            return false;
        }
    }
    
    /**
     * Seek within the cache to a specified range
     * 
     * @param range
     *            the range
     * @return true if we have a new top key and value
     */
    protected boolean cacheSeek(Range range) {
        if (cacheTopKey == null) {
            findCacheTop();
        }
        while (cacheTopKey != null && range.beforeStartKey(cacheTopKey)) {
            findCacheTop();
        }
        // if we passed the end of the range, the put the last cache key back
        if (cacheTopKey != null && range.afterEndKey(cacheTopKey)) {
            cacheAdd(cacheTopKey, cacheTopValue);
            cacheTopKey = null;
            cacheTopValue = null;
        }
        return (cacheTopKey != null);
    }
    
    /**
     * Add an entry in the cache, appending values if needed
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     */
    protected void cacheAdd(Key key, Value value) {
        if (cache.containsKey(key)) {
            ByteArrayOutputStream orgValue = cache.get(key);
            orgValue.write(0);
            orgValue.write(value.get(), 0, value.getSize());
        } else {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            if (value != null && value.getSize() > 0) {
                bytes.write(value.get(), 0, value.getSize());
            }
            cache.put(new Key(key), bytes);
        }
    }
    
}
