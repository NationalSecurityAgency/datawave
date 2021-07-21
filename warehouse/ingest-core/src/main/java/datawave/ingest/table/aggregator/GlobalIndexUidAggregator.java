package datawave.ingest.table.aggregator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of an Aggregator that aggregates objects of the type Uid.List. This is an optimization for the shardIndex and shardReverseIndex, where the
 * list of UIDs for events will be maintained in the global index for low cardinality terms.
 * 
 * 
 * 
 */
public class GlobalIndexUidAggregator extends PropogatingCombiner {
    private static final Logger log = LoggerFactory.getLogger(GlobalIndexUidAggregator.class);
    private static final String TIMESTAMPS_IGNORED = "timestampsIgnored";
    private Uid.List.Builder builder = Uid.List.newBuilder();
    
    /**
     * Using a set instead of a list so that duplicate UIDs are filtered out of the list. This might happen in the case of rows with masked fields that share a
     * UID.
     */
    private final HashSet<String> uids = new HashSet<>();
    
    /**
     * List of UIDs to remove.
     */
    private final HashSet<String> uidsToRemove = new HashSet<>();
    
    /**
     * List of UIDs to remove.
     */
    private final HashSet<String> quarantinedIds = new HashSet<>();
    
    /**
     * List of UIDs to remove.
     */
    private final HashSet<String> releasedUids = new HashSet<>();
    
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
     * Indicates whether timestamps are "ignored" where it is assumed all keys aggregated by this class share the same timestamp value.
     */
    private boolean timestampsIgnored = true;
    
    public GlobalIndexUidAggregator(int max) {
        this.maxUids = max;
    }
    
    public GlobalIndexUidAggregator() {
        this.maxUids = MAX;
    }
    
    public Value aggregate() {
        
        // as a backup, we remove the intersection of the UID sets
        
        builder.setCOUNT(count);
        
        if (seenIgnore || count > maxUids) {
            builder.setIGNORE(true);
            builder.clearUID();
        } else {
            builder.setIGNORE(false);
            
            uidsToRemove.removeAll(quarantinedIds);
            uidsToRemove.removeAll(releasedUids);
            quarantinedIds.removeAll(releasedUids);
            
            // If propogate is false, then we know this aggregate result
            // considers all possible values, so put the released UID values
            // back into the uid list. Otherwise, we can't do that because we
            // might have re-quarantined in a different value that we haven't
            // seen yet.
            if (!propogate)
                uids.addAll(releasedUids);
            uids.removeAll(uidsToRemove);
            uids.removeAll(quarantinedIds);
            
            builder.addAllUID(uids);
        }
        
        log.debug("Propogating: {}", propogate);
        
        // clear all removals, quarantined, and released lists
        builder.clearREMOVEDUID();
        builder.clearQUARANTINEUID();
        builder.clearRELEASEUID();
        
        if (propogate && !builder.getIGNORE()) {
            
            builder.addAllREMOVEDUID(uidsToRemove);
            builder.addAllQUARANTINEUID(quarantinedIds);
            builder.addAllRELEASEUID(releasedUids);
        }
        log.debug("Building aggregate. Count is {}, uids.size() is {}. builder size is {}", count, uids.size(), builder.getUIDList().size());
        return new Value(builder.build().toByteArray());
        
    }
    
    /**
     * We should closely examine the possible use cases to ensure that we have covered all scenarios.
     * 
     * Ingest: If we ingest, we would like to aggregate index entries with the same Key. This means that the reducer ( or combiner ) will combine UIDs for a
     * given index ( on a given shard ). In this case it is unlikely that we have any removals.
     * 
     * Deletes: We may have have removals at any point in the RFile read for a given tablet. We need to propagate the removals across compactions, until we have
     * a full major compaction.
     * 
     * If we reach the point where we are merging a UID protobuf, where ignore has been seen, then we do not continue with removals.
     */
    @Override
    public Value reduce(Key key, Iterator<Value> iter) {
        log.trace("has next ? {}", iter.hasNext());
        while (iter.hasNext()) {
            
            Value value = iter.next();
            
            // Collect the values, which are serialized Uid.List objects
            try {
                Uid.List v = Uid.List.parseFrom(value.get());
                
                long delta = v.getCOUNT();
                
                count += delta;
                /*
                 * Fail fast approach.
                 */
                if (v.getIGNORE()) {
                    seenIgnore = true;
                    log.debug("SeenIgnore is true. Skipping collections");
                }
                
                // Partial major compactions make it possible for any combination of delta (positive/negative)
                // and UIDs in the UID and REMOVEDUID list. For example, consider the case where uid1 and uid2
                // are both added at separate times, and later uid1 is removed. If all three of those operations
                // end up in different rfiles, then a partial major compaction could select the file with the add
                // of uid2 and the removal of uid1 for compacting. The resulting protocol buffer would have a
                // +1 for uid2 and a -1 for uid1, yielding a count of 0 and uid2 in the UID list and uid1 in the
                // REMOVEDUID list. If another uid, uid3, ended up with the same order of operations as uid1
                // then we could have a count of -1 (+1 for uid2, -1 for uid1, -1 for uid3) but still have uid2
                // in the UID list and uid1 and uid3 in the REMOVEDUID list.
                
                // Manage quarantined and released UIDs.
                if (delta > 0 && v.getQUARANTINEUIDCount() > 0 && v.getUIDCount() == 0 && v.getREMOVEDUIDCount() == 0 && v.getRELEASEUIDCount() == 0) {
                    // This is for backwards compatibility. Previously the QUARANTINEDUID list was used for both
                    // quarantining and releasing UIDs (negative count was quarantine and positive count was release).
                    // However, this method does not work when a protocol buffer gets combined via a partial major
                    // compaction. If we have only quarantined UIDs in the protocol buffer with a positive count, and
                    // nothing else, then handle that as a released UID(s).
                    for (String uid : v.getQUARANTINEUIDList()) {
                        if ((timestampsIgnored || !quarantinedIds.contains(uid)) && !uids.contains(uid))
                            releasedUids.add(uid);
                        if (timestampsIgnored)
                            quarantinedIds.remove(uid);
                    }
                } else {
                    // Add any quarantined UIDs to the internal list. If we're paying attention to timestamps, then we
                    // don't want to quarantine any uid that we've already seen (i.e., with a newer timestamp) that has
                    // been released.
                    for (String uid : v.getQUARANTINEUIDList()) {
                        if (timestampsIgnored || !releasedUids.contains(uid))
                            quarantinedIds.add(uid);
                    }
                    // Add any released UIDs to the internal list. If we're paying attention to timestamps, then we
                    // don't want to release any uid that we've already seen (i.e., with a newer timestamp) that has
                    // been quarantined. If we're not paying attention to timestamps, then releasing a UID always takes
                    // precedence over quarantining. In either case, if the UID is already in the uids list, then we
                    // don't need to track it as released.
                    for (String uid : v.getRELEASEUIDList()) {
                        if ((timestampsIgnored || !quarantinedIds.contains(uid)) && !uids.contains(uid))
                            releasedUids.add(uid);
                        if (timestampsIgnored)
                            quarantinedIds.remove(uid);
                    }
                }
                
                // Remove any UIDs in the REMOVEDUID list.
                for (String uid : v.getREMOVEDUIDList()) {
                    // Don't remove the UID if it's in the UID list since that means a newer key
                    // (larger timestamp value) added the UID and we don't want to undo that add.
                    // If timestampsIgnored is set, then we are presuming lots of collisions on
                    // timestamp and the order of incoming values is non-deterministic. In that case
                    // we give precedence to a removal over an add and mark this UID as removed even
                    // if it was in the UID list.
                    //
                    // If we're not propagating changes then don't bother adding UIDs to the
                    // REMOVEDUID list. This happens if either we're doing a full major compaction
                    // or we've seen the ignore flag (e.g., we exceeded the max UID count). In
                    // either of those cases, the output protocol buffer won't include any removed
                    // UIDs so there's no point to collecting them here.
                    if (propogate && (timestampsIgnored || (!uids.contains(uid) && !releasedUids.contains(uid))))
                        uidsToRemove.add(uid);
                    if (timestampsIgnored)
                        uids.remove(uid);
                }
                
                /*
                 * This is added for backwards compatibility. The removal list was added to ensure that removals are propagated across compactions. In the case
                 * where compactions did not occur, and the indices are converted into the newer protobuf, we must use the UID list to maintain removals for
                 * deltas less than 0
                 */
                if (delta < 0 && v.getREMOVEDUIDList().isEmpty()) {
                    for (String uid : v.getUIDList()) {
                        // add to uidsToRemove, and decrement count if the uid is in UIDS
                        uidsToRemove.add(uid);
                        uids.remove(uid);
                    }
                }
                
                // Add any UIDs in the add list, but only if we haven't seen an ignore (a PB that has exceeded the
                // max UID count, which means that the UID list will be cleared). If we've seen an ignore, then we
                // won't be outputting a UID list so there's no point in adding UIDs to the in-memory set.
                if (!seenIgnore) {
                    for (String uid : v.getUIDList()) {
                        
                        // check that a removal has not occurred
                        if (!uidsToRemove.contains(uid) && !quarantinedIds.contains(uid)) {
                            
                            // Add the UID iff we are under our MAX. If we reach the max,
                            // then treat it as though we've seen an ignore--don't try to
                            // add any more UIDs to the list (or removals from the REMOVEDUIDs
                            // list) since they won't be included when we aggregate anyway.
                            if (uids.size() < maxUids) {
                                uids.add(uid);
                                // We might have just added a UID that's in the released list. Remove it now so that
                                // we don't aggregate it into the outbound released UID list.
                                releasedUids.remove(uid);
                            } else {
                                seenIgnore = true;
                                break;
                            }
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
        log.debug("Resetting GlobalIndexUidAggregator");
        count = 0;
        seenIgnore = false;
        builder = Uid.List.newBuilder();
        uids.clear();
        uidsToRemove.clear();
        releasedUids.clear();
        quarantinedIds.clear();
    }
    
    @Override
    public boolean propogateKey() {
        
        // This method is called after reduce and then aggregate, so all of the work to combine has been done.
        // If the propogate flag is true, then this might have been a partial major compaction, scan, or minor
        // compaction and we want to keep all keys no matter what. When propogate is false, that means it's a full major
        // compaction and therefore we can be certain that the aggregated result has combined all possible values for a
        // given key. In that case, we only need to keep the resulting key/value pair if it has any UIDs (which means
        // either UIDs in the quarantine or uid lists, or a positive uid count).
        return propogate || !quarantinedIds.isEmpty() || !uids.isEmpty() || count > 0;
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        boolean valid = super.validateOptions(options);
        if (valid) {
            if (options.containsKey(TIMESTAMPS_IGNORED)) {
                timestampsIgnored = Boolean.parseBoolean(options.get(TIMESTAMPS_IGNORED));
            }
        }
        return valid;
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        GlobalIndexUidAggregator copy = (GlobalIndexUidAggregator) super.deepCopy(env);
        copy.timestampsIgnored = timestampsIgnored;
        copy.propogate = propogate;
        // Not copying other fields that are all cleared in the reset() method.
        return copy;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (options.containsKey(TIMESTAMPS_IGNORED)) {
            timestampsIgnored = Boolean.parseBoolean(options.get(TIMESTAMPS_IGNORED));
        }
    }
    
    public static void setTimestampsIgnoredOpt(IteratorSetting is, boolean timestampsIgnored) {
        is.addOption(TIMESTAMPS_IGNORED, Boolean.toString(timestampsIgnored));
    }
}
