package datawave.query.index.lookup;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.ingest.protobuf.Uid;
import datawave.query.Constants;
import datawave.query.tld.TLD;
import datawave.query.util.Tuple3;
import datawave.query.util.Tuples;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CondensedUidIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    
    private static final Logger log = Logger.getLogger(CondensedUidIterator.class);
    
    public static final String IS_TLD = "index.lookup.condense.tld";
    
    public static final String SHARDS_TO_EVALUATE = "index.lookup.shards.evaluate";
    
    public static final String MAX_IDS = "index.lookup.shards.evaluate.max.ids";
    
    public static final String FAILURE_POLICY = "index.lookup.shards.fail.policy";
    
    protected boolean isTld = false;
    protected SortedKeyValueIterator<Key,Value> src;
    protected Key tk;
    protected CondensedIndexInfo tv;
    
    public static final String COLLAPSE_UIDS = "index.lookup.collapse";
    
    public static final String COMPRESS_MAPPING = "index.lookup.compress";
    
    protected boolean collapseUids = false;
    
    protected boolean compressResults = false;
    
    protected int shardsToEvaluate = Integer.MAX_VALUE, maxIds = Integer.MAX_VALUE;
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        src = source;
        if (null != options) {
            final String tldOpt = options.get(IS_TLD);
            if (null != tldOpt) {
                try {
                    isTld = Boolean.valueOf(tldOpt);
                } catch (Exception e) {
                    isTld = false;
                }
            }
            
            final String collapseOpt = options.get(COLLAPSE_UIDS);
            if (null != collapseOpt) {
                try {
                    collapseUids = Boolean.valueOf(collapseOpt);
                } catch (Exception e) {
                    collapseUids = false;
                }
            }
            
            final String compressOpt = options.get(COMPRESS_MAPPING);
            
            if (null != compressOpt) {
                try {
                    compressResults = Boolean.valueOf(compressOpt);
                } catch (Exception e) {
                    compressResults = false;
                }
            }
            
            final String shardsToEvaluateStr = options.get(SHARDS_TO_EVALUATE);
            final String shardsToEvaluateMaxStr = options.get(MAX_IDS);
            
            if (null != shardsToEvaluateStr && null != shardsToEvaluateMaxStr) {
                shardsToEvaluate = Integer.valueOf(shardsToEvaluateStr);
                maxIds = Integer.valueOf(shardsToEvaluateMaxStr);
            }
        }
    }
    
    @Override
    public boolean hasTop() {
        return tk != null;
    }
    
    @Override
    public void next() throws IOException {
        tk = null;
        
        if (src.hasTop()) {
            DescriptiveStatistics stats = new DescriptiveStatistics();
            final String day = getDay(src.getTopKey());
            Key reference = makeRootKey(src.getTopKey(), day + "_");
            Set<String> uids = Sets.newHashSet();
            long count = 0L;
            boolean ignore = false;
            boolean ignoreDay = false;
            
            if (collapseUids) {
                ignore = true;
            }
            
            int merged = 1;
            Set<String> ignored = Sets.newHashSet();
            Multimap<String,String> shardToUidList = HashMultimap.create();
            while (src.hasTop()) {
                
                Key nextTop = src.getTopKey();
                if (log.isTraceEnabled())
                    log.trace("nextTop is " + nextTop);
                String shard = getShard(nextTop);
                if (sameDay(day, shard)) {
                    if (!ignoreDay) {
                        Tuple3<Long,Boolean,List<String>> uidInfo = parseUids(nextTop, src.getTopValue());
                        long myCount = uidInfo.first();
                        count += myCount;
                        ignore |= uidInfo.second();
                        stats.addValue(myCount);
                        if (merged >= shardsToEvaluate && stats.getPercentile(50) > maxIds) {
                            // skip the day
                            if (log.isTraceEnabled())
                                log.trace("Breaking because " + merged + " " + shardsToEvaluate + " " + maxIds + " " + stats.getPercentile(50));
                            ignoreDay = true;
                            continue;
                        }
                        if (!ignore && !ignored.contains(shard)) {
                            uids = Sets.newHashSet();
                            for (String uid : uidInfo.third()) {
                                if (log.isTraceEnabled())
                                    log.trace("Adding uid " + uid.split("\u0000")[1] + " " + uid.toString() + " " + TLD.parseRootPointerFromId(uid) + " "
                                                    + TLD.parseRootPointerFromId(uid.toString()));
                                if (isTld) {
                                    uids.add(TLD.parseRootPointerFromId(uid));
                                } else {
                                    uids.add(uid);
                                }
                                
                            }
                            shardToUidList.putAll(shard, uids);
                        } else {
                            if (log.isTraceEnabled())
                                log.trace("Ignoring " + shard);
                            shardToUidList.removeAll(shard);
                            ignored.add(shard);
                        }
                        merged++;
                    }
                    
                    src.next();
                    
                } else {
                    if (log.isTraceEnabled())
                        log.trace("breaking");
                    break;
                }
            }
            tv = ignoreDay ? new CondensedIndexInfo(day, count) : new CondensedIndexInfo(day, shardToUidList, ignored);
            tk = reference;
        }
        
    }
    
    private boolean sameDay(String day, String shard) {
        return shard.startsWith(day);
    }
    
    private String getShard(Key nextTop) {
        ByteSequence cq = nextTop.getColumnQualifierData();
        return new String(cq.getBackingArray(), 0, lastNull(cq));
    }
    
    private static String getDay(Key reference) {
        ByteSequence cq = reference.getColumnQualifierData();
        if (cq.length() > 8 && cq.getBackingArray()[8] == '_') {
            return new String(cq.getBackingArray(), 0, 8);
        } else {
            ByteSequence strippedCq = cq;
            strippedCq = cq.subSequence(0, lastNull(cq));
            return new String(strippedCq.getBackingArray(), 0, strippedCq.length());
        }
    }
    
    /**
     * This iterator returns a top level key whose column qualifier includes only the shard. In that event, we must ensure we skip to the next sorted entry,
     * using skipkey
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        Range seekRange = range;
        if (!range.isStartKeyInclusive()) {
            seekRange = skipKey(range);
            if (seekRange == null) {
                // skipKey() resulted in a Key beyond the current end Key, reset to ensure hasNext() = false and return
                tk = null;
                return;
            }
        }
        
        src.seek(seekRange, columnFamilies, inclusive);
        
        next();
    }
    
    /**
     * Method that ensures if we have to skip the current key, we do so with the contract provided by the create UID iterator.
     * 
     * @param range
     *            the seek range
     * @return the new seek range or null if no range is possible
     */
    protected Range skipKey(Range range) {
        Key startKey = range.getStartKey();
        Key newKey = new Key(startKey.getRow(), startKey.getColumnFamily(), new Text(startKey.getColumnQualifier() + Constants.MAX_UNICODE_STRING));
        if (range.getEndKey().compareTo(newKey) <= 0) {
            return null;
        }
        
        return new Range(newKey, true, range.getEndKey(), range.isEndKeyInclusive());
    }
    
    @Override
    public Key getTopKey() {
        return tk;
    }
    
    @Override
    public Value getTopValue() {
        try {
            if (compressResults) {
                return new Value(this.getValue().toByteArray());
            } else {
                return new Value(WritableUtils.toByteArray(this.getValue()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        CondensedUidIterator itr = new CondensedUidIterator();
        itr.src = src.deepCopy(env);
        return itr;
    }
    
    public CondensedIndexInfo getValue() {
        return tv;
    }
    
    public static boolean sameShard(Key ref, Key test) {
        ByteSequence refCq = ref.getColumnQualifierData();
        ByteSequence testCq = test.getColumnQualifierData();
        if (testCq.length() < refCq.length()) {
            return false;
        }
        for (int i = 0; i < refCq.length(); ++i) {
            if (refCq.byteAt(i) != testCq.byteAt(i)) {
                return false;
            }
        }
        if (testCq.byteAt(refCq.length()) == 0x00) {
            return true;
        } else {
            return false;
        }
    }
    
    public static boolean sameDay(Key ref, Key test) {
        ByteSequence refCq = ref.getColumnQualifierData();
        ByteSequence testCq = test.getColumnQualifierData();
        if (testCq.length() < refCq.length()) {
            return false;
        }
        return WritableComparator.compareBytes(refCq.getBackingArray(), 0, 8, testCq.getBackingArray(), 0, 8) == 0;
        
    }
    
    public static Tuple3<Long,Boolean,List<String>> parseUids(Key k, Value v) throws IOException {
        final String dataType = parseDataType(k);
        Uid.List docIds = Uid.List.parseFrom(v.get());
        final boolean ignore = docIds.getIGNORE();
        List<String> uids = ignore || docIds.getUIDList() == null ? Collections.emptyList() : Lists.transform(docIds.getUIDList(),
                        s -> dataType + "\u0000" + s.trim());
        return Tuples.tuple(docIds.getCOUNT(), ignore, uids);
    }
    
    public static String parseDataType(Key k) {
        ByteSequence colq = k.getColumnQualifierData();
        return new String(colq.subSequence(lastNull(colq) + 1, colq.length()).toArray());
    }
    
    public static int lastNull(ByteSequence bs) {
        int pos = bs.length();
        while (--pos > 0 && bs.byteAt(pos) != 0x00)
            ;
        return pos;
    }
    
    /**
     * When skipKey is false, we produce the key based upon k, removing any data type
     * 
     * When skipKey is true, we will produce a key producing a skipkey from the root key. This will be helpful when we are being torn down.
     * 
     * @param k
     * @param day
     * @return
     */
    public static Key makeRootKey(Key k, String day) {
        ByteSequence cq = k.getColumnQualifierData();
        ByteSequence strippedCq = cq;
        strippedCq = new ArrayByteSequence(day);
        final ByteSequence row = k.getRowData(), cf = k.getColumnFamilyData(), cv = k.getColumnVisibilityData();
        return new Key(row.getBackingArray(), row.offset(), row.length(), cf.getBackingArray(), cf.offset(), cf.length(), strippedCq.getBackingArray(),
                        strippedCq.offset(), strippedCq.length(), cv.getBackingArray(), cv.offset(), cv.length(), k.getTimestamp());
    }
    
    /**
     * Implemented to allow this iterator to be used in the shell.
     */
    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptions("", "", Collections.emptyMap(), Collections.emptyList());
    }
    
    /**
     * Implemented to allow this iterator to be used in the shell.
     */
    @Override
    public boolean validateOptions(Map<String,String> options) {
        return true;
    }
}
