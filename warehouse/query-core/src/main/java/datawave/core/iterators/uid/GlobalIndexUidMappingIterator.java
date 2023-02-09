package datawave.core.iterators.uid;

import datawave.ingest.protobuf.Uid;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * <p>
 * An iterator that will map uids in shard index and reverse index table entries per a configured UidMapper.
 * </p>
 * 
 * 
 * @see datawave.core.iterators.uid.UidMapper
 * 
 */
public class GlobalIndexUidMappingIterator extends UidMappingIterator {
    
    public GlobalIndexUidMappingIterator() {}
    
    public GlobalIndexUidMappingIterator(GlobalIndexUidMappingIterator iter, IteratorEnvironment env) {
        super(iter, env);
    }
    
    /**
     * Map the uid in the supplied value. The formats expected are for the shard table only.
     * 
     * @param keyValue
     *            the key value
     * @param startKey
     *            the start key flag
     * @param endKey
     *            the end key flag
     * @param endKeyInclusive
     *            the end key inclusive flag
     * @param startKeyInclusive
     *            the start key inclusive flag
     * @return the value with the uid mapped appropriately
     */
    @Override
    protected KeyValue mapUid(KeyValue keyValue, boolean startKey, boolean startKeyInclusive, boolean endKey, boolean endKeyInclusive) {
        if (keyValue != null && keyValue.getValue() != null && keyValue.getValue().getSize() > 0) {
            try {
                Uid.List.Builder uidList = Uid.List.parseFrom(keyValue.getValue().get()).toBuilder();
                boolean changed = false;
                for (int i = 0; i < uidList.getUIDList().size(); i++) {
                    String uid = uidList.getUID(i);
                    String newUid = null;
                    if (startKey) {
                        // if we had extra characters, or not startKeyInclusive, then we do not want an inclusive start key
                        newUid = uidMapper.getStartKeyUidMapping(uid, startKeyInclusive);
                    } else if (endKey) {
                        // if we had extra characters, or not endKeyInclusive, then we do not want an inclusive end key
                        newUid = uidMapper.getEndKeyUidMapping(uid, endKeyInclusive);
                    } else {
                        newUid = uidMapper.getUidMapping(uid);
                    }
                    if (newUid != null) {
                        uidList.setUID(i, newUid);
                        changed = true;
                    }
                }
                if (changed) {
                    keyValue = new KeyValue(keyValue.getKey(), uidList.build().toByteArray());
                }
            } catch (InvalidProtocolBufferException e) {
                // return the value as is
            }
        }
        return keyValue;
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new GlobalIndexUidMappingIterator(this, env);
    }
}
