package datawave.ingest.table.aggregator;

import java.util.HashSet;
import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;

/**
 * Implementation of an Aggregator that aggregates objects of the type Uid.List. This is an optimization for the shardIndex and shardReverseIndex, where the
 * list of UIDs for events will be maintained in the global index for low cardinality terms.
 * 
 * 
 * 
 */
public class GlobalIndexUidAggregator extends PropogatingCombiner {
    private static final Logger log = Logger.getLogger(GlobalIndexUidAggregator.class);
    private Uid.List.Builder builder = Uid.List.newBuilder();
    
    /**
     * Using a set instead of a list so that duplicate UIDs are filtered out of the list. This might happen in the case of rows with masked fields that share a
     * UID.
     */
    private HashSet<String> uids = new HashSet<>();
    
    public GlobalIndexUidAggregator(int max) {
        this.maxUids = max;
    }
    
    public GlobalIndexUidAggregator() {
        this.maxUids = MAX;
    }
    
    /**
     * List of UIDs to remove.
     */
    private HashSet<String> uidsToRemove = new HashSet<>();
    
    /**
     * List of UIDs to remove.
     */
    private HashSet<String> quarantinedIds = new HashSet<>();
    
    /**
     * List of UIDs to remove.
     */
    private HashSet<String> releasedUids = new HashSet<>();
    
    /**
     * flag for whether or not we have seen ignore
     */
    private boolean seenIgnore = false;
    
    /**
     * Maximum number of UIDs.
     */
    public static final int MAX = 20;
    
    /**
     * Maximum number of UIDs.
     */
    public int maxUids;
    
    /**
     * representative count.
     */
    private long count = 0;
    
    /**
     * temporary set for removals.
     */
    protected HashSet<String> tempSet;
    
    public Value aggregate() {
        
        // as a backup, we remove the intersection of the UID sets
        
        builder.setCOUNT(count);
        
        if (seenIgnore || count > maxUids) {
            builder.setIGNORE(true);
            builder.clearUID();
            // if we catch seenIgnore, then there is
            // no need to propogate removals.
            propogate = false;
        } else {
            builder.setIGNORE(false);
            
            uidsToRemove.removeAll(quarantinedIds);
            uidsToRemove.removeAll(releasedUids);
            quarantinedIds.removeAll(releasedUids);
            
            uids.removeAll(uidsToRemove);
            uids.removeAll(quarantinedIds);
            
            if (!releasedUids.isEmpty()) {
                if (log.isDebugEnabled())
                    log.debug("Adding released UIDS");
                uids.addAll(releasedUids);
            }
            
            builder.addAllUID(uids);
        }
        
        if (log.isDebugEnabled())
            log.debug("Propogating: " + propogate);
        
        // clear all removals
        builder.clearREMOVEDUID();
        
        if (propogate) {
            
            builder.addAllREMOVEDUID(uidsToRemove);
            builder.addAllQUARANTINEUID(quarantinedIds);
        }
        if (log.isDebugEnabled())
            log.debug("Building aggregate. Count is " + count + ", uids.size() is " + uids.size() + ". builder size is " + builder.getUIDList().size());
        return new Value(builder.build().toByteArray());
        
    }
    
    /**
     * We should closely examine the possible use cases to ensure that we have covered all scenarios.
     * 
     * Ingest: If we ingest, we would like to aggregate index entries with the same Key. This means that the reducer ( or combiner ) will combine UIDs for a
     * given index ( on a given shard ). In this case it is unlikey that we have any removals.
     * 
     * Deletes: We may have have removals at any point in the RFile read for a given tablet. We need to propogate the removals across compactions, until we have
     * a full major compaction.
     * 
     * If we reach the point where we are merging a UID protobuf, where ignore has been seen, then we do not continue with removals.
     */
    @Override
    public Value reduce(Key key, Iterator<Value> iter) {
        if (log.isTraceEnabled())
            log.trace("has next ? " + iter.hasNext());
        while (iter.hasNext()) {
            
            Value value = iter.next();
            
            // Collect the values, which are serialized Uid.List objects
            try {
                Uid.List v = Uid.List.parseFrom(value.get());
                
                long delta = v.getCOUNT();
                
                count += delta;
                /**
                 * Fail fast approach.
                 */
                if (v.getIGNORE()) {
                    seenIgnore = true;
                    if (log.isDebugEnabled())
                        log.debug("SeenIgnore is true. Skipping collections");
                }
                
                // if delta > 0, we are collecting the uid list
                // in the protobuf into our object's uid list.
                if (delta > 0) {
                    
                    for (String uid : v.getQUARANTINEUIDList()) {
                        
                        quarantinedIds.remove(uid);
                        releasedUids.add(uid);
                    }
                    
                    for (String uid : v.getUIDList()) {
                        
                        // check that a removal has not occurred
                        // if it has, we decrement the count, from above.
                        if (!uidsToRemove.contains(uid) && !quarantinedIds.contains(uid)) {
                            
                            // add the UID iff we are under our MAX
                            if (uids.size() < maxUids)
                                uids.add(uid);
                            
                        }
                        
                    }
                    
                    if (log.isDebugEnabled())
                        log.debug("Adding uids " + delta + " " + count);
                    
                    // if our delta is < 0, then we can remove, iff seenIgnore is false. If it is true, there is no need to proceed with removals
                } else if (delta < 0 && !seenIgnore) {
                    
                    // so that we can perform the decrement
                    for (String uid : v.getREMOVEDUIDList()) {
                        
                        uidsToRemove.add(uid);
                        
                        if (uids.contains(uid)) {
                            
                            uids.remove(uid);
                        }
                        
                    }
                    
                    quarantinedIds.addAll(v.getQUARANTINEUIDList());
                    
                    /**
                     * This is added for backwards compatability. The removal list was added to ensure that removals are propogated across compactions. In the
                     * case where compactions did not occur, and the indices are converted into the newer protobuff, we must use the UID list to maintain
                     * removals for deltas less than 0
                     */
                    for (String uid : v.getUIDList()) {
                        // add to uidsToRemove, and decrement count if the uid is in UIDS
                        uidsToRemove.add(uid);
                        if (uids.contains(uid)) {
                            uids.remove(uid);
                        }
                    }
                }
                
            } catch (InvalidProtocolBufferException e) {
                if (key.isDeleted()) {
                    log.warn("Value passed to aggregator for a delete key was not of type Uid.List");
                } else {
                    log.error("Value passed to aggregator was not of type Uid.List", e);
                }
            }
        }
        return aggregate();
    }
    
    public void reset() {
        if (log.isDebugEnabled())
            log.debug("Resetting GlobalIndexUidAggregator");
        count = 0;
        seenIgnore = false;
        builder = Uid.List.newBuilder();
        uids.clear();
        uidsToRemove.clear();
        releasedUids.clear();
        quarantinedIds.clear();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.ingest.table.aggregator.PropogatingAggregator#propogateKey()
     */
    @Override
    public boolean propogateKey() {
        
        /**
         * Changed logic so that if seenIgnore is true and count > MAX, we keep propogate the key
         */
        if ((seenIgnore && count > maxUids) || !quarantinedIds.isEmpty())
            return true;
        
        HashSet<String> uidsCopy = new HashSet<>(uids);
        uidsCopy.removeAll(uidsToRemove);
        
        if (log.isDebugEnabled()) {
            log.debug(count + " " + uids.size() + " " + uidsToRemove.size() + " " + uidsCopy.size() + " removing " + (count == 0 && uidsCopy.isEmpty()));
        }
        
        // if <= 0 and uids is empty, we can safely remove
        if (count <= 0 && uidsCopy.isEmpty())
            return false;
        else
            return true;
    }
    
}
