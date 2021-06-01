package datawave.ingest.table.aggregator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import datawave.ingest.protobuf.Uid.List.Builder;
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
 */
public class GlobalIndexUidAggregator extends PropogatingCombiner {
    private static final Logger log = LoggerFactory.getLogger(GlobalIndexUidAggregator.class);
    private static final String TIMESTAMPS_IGNORED = "timestampsIgnored";
    
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
    
    /**
     * @return True if we saw a "count only" protobuf during the last reduce operation.
     */
    protected boolean isSeenIgnore() {
        return seenIgnore;
    }
    
    public Value aggregate() {
        
        Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(seenIgnore);
        if (seenIgnore) {
            // If we're over the max UID size, then the count is simply the sum of counts
            // as reported by the protocol buffers. If UIDs were duplicated, then this
            // count might include that info, but there's no way to know since we're not
            // tracking individual UIDs.
            builder.setCOUNT(count);
        } else {
            uids.removeAll(uidsToRemove);
            builder.addAllUID(uids);
            // If we're not over the max UID size, then the count is simply the number of
            // UIDs we have in memory. This will take care of de-duping any UIDs that were
            // added more than once. Note that we specifically do not account for any UIDs
            // in uidsToRemove (other than those that were removed from uids above). If we
            // saw removals for 10 UIDs, we'd have those 10 UIDs in the uidsToRemove list.
            // Then if the next value contains adds for those 10 UIDs, we would not add
            // them to the uids set since they're in uidsToRemove, but we would also NOT
            // remove them from uidsToRemove since we don't know whether or not we'll see
            // any of those UIDs again and don't want to discard the fact that they are
            // removed. In that case, we'd have a count of -10 after aggregation if we
            // subtracted uidsToRemove.size() even though the correct count would be 0.
            builder.setCOUNT(uids.size());
            
            // Only track REMOVEDUIDs if we're propagating, which means it's a minor or
            // partial major compaction and therefore the result of aggregation might not
            // include all possible values for a key. In that case, it's possible the adds
            // to which these removes apply are in a different file that wasn't involved in
            // this operation.
            if (propogate) {
                builder.addAllREMOVEDUID(uidsToRemove);
            }
        }
        
        log.trace("Building aggregate. propogate={}, count={}, uids.size()={}, uidsToRemove.size()={}, builder UIDCount={} REMOVEDUIDCount={}", propogate,
                        count, uids.size(), uidsToRemove.size(), builder.getUIDCount(), builder.getREMOVEDUIDCount());
        return new Value(builder.build().toByteArray());
    }
    
    /**
     * Combines UID lists. The {@link Uid.List} protocol buffer contains a count, ignore flag, and lists of UIDs and REMOVEDUIDs. The intent is that the list
     * can store up to a certain number of UIDs and after that, the lists are no longer tracked (the ignore flag will be set) and only counts are tracked.
     * REMOVEDUIDs are tracked to handle minor and partial major compactions where this reduce method won't necessarily see all possible values for a given key
     * (e.g., the UIDs that are being removed might be in a different RFile that isn't involved in the current compaction).
     *
     * Aggregation operates in one of two modes depending on whether or not timestamps are ignored. By default, timestamps are ignored since DataWave uses date
     * to the day as the timestamp in the global term index. When timestamps are ignored, we cannot infer anything about the order of values under aggregation.
     * Therefore, a decision must be made about how to handle removed UIDs vs added UIDs. In that case, removed UIDs take priority. This means that adding a
     * UID, then removing it, and adding it back again is not a supported operation. When timestamps are not ignored, then it is assumed that the timestamps are
     * set properly and values will be processed in order from newest to oldest. In that case, it is possible to add a UID, then remove it, and add it back
     * again.
     *
     * @param key
     *            the current key
     * @param iter
     *            an {@link Iterator} providing the UID lists to be aggregated
     * @return a new {@link Value} containing the aggregated result
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
                
                // For best performance, don't attempt to accumulate any individual UIDs (or removals)
                // if this PB has its ignore flag set or we've seen any other PB with the ignored flag set.
                if (v.getIGNORE() || seenIgnore) {
                    seenIgnore = true;
                    log.debug("SeenIgnore is true. Skipping collections");
                } else {
                    // Save a starting count in the event that we go over the max UID count while
                    // aggregating this protocol buffer. Once we cross the max UID threshold, then we'll
                    // only use the count reported by the protocol buffer. However, until that point
                    // we want to count UIDs in the internal set since that will take care of de-duping
                    // any duplicate UIDs which would be over counted if we just used the protocol buffer
                    // count.
                    long prevCount = uids.size();
                    
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
                        // REMOVEDUID list. This happens if either we're doing a scan or a full major
                        // compaction. In those cases, we're guaranteed that we'll see all of the values
                        // for a given key and therefore there's no need to include removed UIDs in the
                        // output protocol buffer since the remove have already been applied.
                        if (timestampsIgnored) {
                            uids.remove(uid);
                            if (propogate)
                                uidsToRemove.add(uid);
                        } else if (propogate && !uids.contains(uid)) {
                            uidsToRemove.add(uid);
                        }
                    }
                    
                    // Add UIDs from the UID list
                    for (String uid : v.getUIDList()) {
                        // Don't add a uid that's been removed. This is the same whether or
                        // not timestamps are ignored since if they are ignored, removals take
                        // priority and if they are not ignored, then this add is happening
                        // before a removal and therefore should not take place.
                        if (!uidsToRemove.contains(uid)) {
                            // Add the UID iff we are under our MAX. If we reach the max,
                            // then treat it as though we've seen an ignore--don't try to
                            // add any more UIDs to the list (or removals from the REMOVEDUIDs
                            // list) since they won't be included when we aggregate anyway.
                            if (uids.size() < maxUids) {
                                uids.add(uid);
                            } else {
                                // If aggregating this PB pushed us over the max UID limit,
                                // then ignore any work we've done integrating this PB so far
                                // and instead treat it as though its ignore flag had been set.
                                // Set the count to the number of collected UIDs before we went
                                // over the max, and then we'll add this PBs delta below.
                                seenIgnore = true;
                                count = prevCount;
                                uids.clear();
                                uidsToRemove.clear();
                                break;
                            }
                        }
                    }
                }
                
                // If we've seen an ignore, then we won't be outputting a UID list and therefore
                // need to just assume the count in the incoming protocol buffer is correct and
                // use it.
                if (seenIgnore) {
                    count += delta;
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
        uids.clear();
        uidsToRemove.clear();
    }
    
    @Override
    public boolean propogateKey() {
        
        // This method is called after reduce and then aggregate, so all of the work to combine has been done.
        // If the propogate flag is true, then this might have been a partial major compaction, scan, or minor
        // compaction and we want to keep all keys no matter what. When propogate is false, that means it's a scan or
        // full major compaction and therefore we can be certain that the aggregated result has combined all possible
        // values for a given key. In that case, we only need to keep the resulting key/value pair if it has any UIDs
        // (which means either UIDs in the uid list, or a positive uid count).
        return propogate || !uids.isEmpty() || count > 0;
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
