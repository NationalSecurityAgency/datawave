package datawave.query.index.lookup;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import datawave.ingest.protobuf.Uid;
import datawave.query.util.Tuple3;
import datawave.query.util.Tuples;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class CreateUidsIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    
    private static final Logger log = Logger.getLogger(CreateUidsIterator.class);
    
    public static final String COLLAPSE_UIDS = "index.lookup.collapse";
    
    protected boolean collapseUids = false;
    protected SortedKeyValueIterator<Key,Value> src;
    protected Key tk;
    protected IndexInfo tv;
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        src = source;
        if (null != options) {
            final String collapseOpt = options.get(COLLAPSE_UIDS);
            if (null != collapseOpt) {
                try {
                    collapseUids = Boolean.valueOf(collapseOpt);
                } catch (Exception e) {
                    collapseUids = false;
                }
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
            Key reference = makeRootKey(src.getTopKey());
            List<String> uids = Lists.newLinkedList();
            long count = 0L;
            boolean ignore = false;
            if (collapseUids) {
                ignore = true;
            }
            while (src.hasTop() && sameShard(reference, src.getTopKey())) {
                Key nextTop = src.getTopKey();
                Tuple3<Long,Boolean,List<String>> uidInfo = parseUids(nextTop, src.getTopValue());
                count += uidInfo.first();
                ignore |= uidInfo.second();
                if (!ignore)
                    for (String uid : uidInfo.third()) {
                        if (log.isTraceEnabled())
                            log.trace("Adding uid " + uid.split("\u0000")[1]);
                        uids.add(uid);
                    }
                src.next();
            }
            tv = ignore ? new IndexInfo(count) : new IndexInfo(uids);
            tk = reference;
            
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
        }
        
        src.seek(seekRange, columnFamilies, inclusive);
        
        next();
    }
    
    /**
     * Method that ensures if we have to skip the current key, we do so with the contract provided by the create UID iterator.
     * 
     * @param range
     */
    protected Range skipKey(Range range) {
        Key startKey = range.getStartKey();
        Key newKey = new Key(startKey.getRow(), startKey.getColumnFamily(), new Text(startKey.getColumnQualifier() + "\u0000\uffff"));
        return new Range(newKey, true, range.getEndKey(), range.isEndKeyInclusive());
        
    }
    
    @Override
    public Key getTopKey() {
        return tk;
    }
    
    @Override
    public Value getTopValue() {
        return new Value(WritableUtils.toByteArray(this.getValue()));
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        CreateUidsIterator itr = new CreateUidsIterator();
        itr.src = src.deepCopy(env);
        return itr;
    }
    
    public IndexInfo getValue() {
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
    
    public static Tuple3<Long,Boolean,List<String>> parseUids(Key k, Value v) throws IOException {
        final String dataType = parseDataType(k);
        Uid.List docIds = Uid.List.parseFrom(v.get());
        final boolean ignore = docIds.getIGNORE();
        List<String> uids = ignore || docIds.getUIDList() == null ? Collections.<String> emptyList() : Lists.transform(docIds.getUIDList(),
                        new Function<String,String>() {
                            public String apply(String s) {
                                return dataType + "\u0000" + s.trim();
                            }
                        });
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
     * @return
     */
    public static Key makeRootKey(Key k) {
        ByteSequence cq = k.getColumnQualifierData();
        ByteSequence strippedCq = cq;
        strippedCq = cq.subSequence(0, lastNull(cq));
        final ByteSequence row = k.getRowData(), cf = k.getColumnFamilyData(), cv = k.getColumnVisibilityData();
        return new Key(row.getBackingArray(), row.offset(), row.length(), cf.getBackingArray(), cf.offset(), cf.length(), strippedCq.getBackingArray(),
                        strippedCq.offset(), strippedCq.length(), cv.getBackingArray(), cv.offset(), cv.length(), k.getTimestamp());
    }
    
    /*
     * Te following methods were implemented to allow this iterator to be used in the shell.
     */
    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptions("", "", Collections.<String,String> emptyMap(), Collections.<String> emptyList());
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        return true;
    }
}
