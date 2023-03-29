package datawave.query.index.lookup;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.tables.RangeStreamScanner;
import datawave.query.util.Tuple2;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.Collections;
import java.util.Iterator;

/**
 * Basic implementation of an IndexStream for a single term.
 *
 * Note that certain delayed terms may create a ScannerStream without an underlying RangeStreamScanner.
 */
public class ScannerStream extends BaseIndexStream {
    
    private ScannerStream(RangeStreamScanner scanSession, EntryParser entryParser, StreamContext ctx, JexlNode currNode, IndexStream debugDelegate) {
        super(scanSession, entryParser, currNode, ctx, debugDelegate);
    }
    
    private ScannerStream(BaseIndexStream itr, StreamContext ctx, JexlNode currNode) {
        this(itr.rangeStreamScanner, itr.entryParser, ctx, currNode, null);
    }
    
    private ScannerStream(Iterator<Tuple2<String,IndexInfo>> iterator, StreamContext context, JexlNode node, IndexStream debugDelegate) {
        super(iterator, node, context, debugDelegate);
    }
    
    private ScannerStream(Iterator<Tuple2<String,IndexInfo>> itr, StreamContext ctx, JexlNode currNode) {
        this(itr, ctx, currNode, null);
    }
    
    public static ScannerStream unindexed(JexlNode currNode) {
        return new ScannerStream(Collections.emptyIterator(), StreamContext.UNINDEXED, currNode);
    }
    
    public static ScannerStream unindexed(JexlNode currNode, IndexStream debugDelegate) {
        return new ScannerStream(Collections.emptyIterator(), StreamContext.UNINDEXED, currNode, debugDelegate);
    }
    
    public static ScannerStream noData(JexlNode currNode) {
        return new ScannerStream(Collections.emptyIterator(), StreamContext.ABSENT, currNode);
    }
    
    public static ScannerStream noData(JexlNode currNode, IndexStream debugDelegate) {
        return new ScannerStream(Collections.emptyIterator(), StreamContext.ABSENT, currNode, debugDelegate);
    }
    
    public static ScannerStream withData(PeekingIterator<Tuple2<String,IndexInfo>> itr, JexlNode currNode) {
        return new ScannerStream(itr, StreamContext.PRESENT, currNode);
    }
    
    public static ScannerStream withData(Iterator<Tuple2<String,IndexInfo>> itr, JexlNode currNode) {
        return new ScannerStream(itr, StreamContext.PRESENT, currNode);
    }
    
    public static ScannerStream variable(Iterator<Tuple2<String,IndexInfo>> itr, JexlNode currNode) {
        return new ScannerStream(itr, StreamContext.VARIABLE, currNode);
    }
    
    public static ScannerStream variable(BaseIndexStream itr, JexlNode currNode) {
        return new ScannerStream(itr, StreamContext.VARIABLE, currNode);
    }
    
    // exceeded value threshold, so we can evaluate with data but may need special handling
    public static ScannerStream exceededValueThreshold(Iterator<Tuple2<String,IndexInfo>> itr, JexlNode currNode) {
        JexlNode resultNode = JexlNodeFactory.wrap(currNode);
        return new ScannerStream(itr, StreamContext.EXCEEDED_VALUE_THRESHOLD, resultNode);
    }
    
    public static ScannerStream delayedExpression(JexlNode currNode) {
        return new ScannerStream(Collections.emptyIterator(), StreamContext.DELAYED_FIELD, currNode);
    }
    
    public static ScannerStream unknownField(JexlNode currNode) {
        return new ScannerStream(Collections.emptyIterator(), StreamContext.UNKNOWN_FIELD, currNode);
    }
    
    public static ScannerStream unknownField(JexlNode currNode, IndexStream debugDelegate) {
        return new ScannerStream(Collections.emptyIterator(), StreamContext.UNKNOWN_FIELD, currNode, debugDelegate);
    }
    
    public static ScannerStream ignored(JexlNode currNode) {
        return new ScannerStream(Collections.emptyIterator(), StreamContext.IGNORED, currNode);
    }
    
    public static ScannerStream ignored(JexlNode currNode, IndexStream debugDelegate) {
        return new ScannerStream(Collections.emptyIterator(), StreamContext.IGNORED, currNode, debugDelegate);
    }
    
    // exceeded term threshold, so we cannot evaluate
    public static ScannerStream exceededTermThreshold(JexlNode currNode) {
        return new ScannerStream(Collections.emptyIterator(), StreamContext.EXCEEDED_TERM_THRESHOLD, currNode);
    }
    
    /**
     * Create a stream in the initialized state
     * 
     * @param itr
     *            an iterator
     * @param currNode
     *            the current node
     * @return a ScannerStream
     */
    public static ScannerStream initialized(Iterator<Tuple2<String,IndexInfo>> itr, JexlNode currNode) {
        return new ScannerStream(itr, StreamContext.INITIALIZED, currNode);
    }
    
    public static ScannerStream initialized(RangeStreamScanner scannerStream, EntryParser entryParser, JexlNode currNode) {
        return new ScannerStream(scannerStream, entryParser, StreamContext.INITIALIZED, currNode, null);
    }
    
    /**
     * Seek this ScannerStream to the specified shard.
     *
     * If no underlying RangeStreamScanner exists then the seek operation is delegated to {@link #seekByNext(String)}.
     *
     * @param seekShard
     *            the shard to seek to.
     * @return the next element great than or equal to the seek shard, or null if all elements were exhausted.
     */
    @Override
    public String seek(String seekShard) {
        if (rangeStreamScanner != null) {
            
            String seekedShard = rangeStreamScanner.seek(seekShard);
            if (seekedShard == null) {
                // If the underlying RangeStreamScanner returns null we are done.
                this.peekedElement = null;
                this.hasPeeked = false;
                this.backingIter = Iterators.emptyIterator();
                return null;
            } else {
                
                resetBackingIterator();
                
                if (hasNext()) {
                    Tuple2<String,IndexInfo> top = peek();
                    if (top != null) {
                        seekedShard = top.first();
                    }
                }
                return seekedShard;
            }
        } else {
            return seekByNext(seekShard);
        }
    }
    
    /**
     * In the case of {@link RangeStream#createIndexScanList} or a unit test, we need to be able to 'seek' by the backing iterator alone.
     *
     * @param seekShard
     *            the shard to seek to
     * @return the top shard after seeking
     */
    public String seekByNext(String seekShard) {
        
        String target = extractDayFromShard(seekShard);
        
        // First advance by day.
        Tuple2<String,IndexInfo> entry = null;
        while (hasNext()) {
            entry = peek();
            if (entry.first().compareTo(target) < 0) {
                // Continue advancing so long as the top shard sorts before the seekShard
                next();
            } else {
                // If we match or exceed the seekShard, breakout.
                break;
            }
        }
        
        // Then advance by shards within a day. The day will sort before day_shard
        if (entry != null && !ShardEquality.isDay(entry.first()) && entry.first().compareTo(seekShard) <= 0) {
            // Only drop in here if the top shard is not a day and is less than the seekShard.
            while (hasNext()) {
                entry = peek();
                if (entry.first().compareTo(seekShard) < 0) {
                    // If the top shard is less than the seekShard keep going.
                    next();
                } else {
                    // If we matched or exceed the seekShard, breakout.
                    break;
                }
            }
        }
        
        return hasNext() ? entry.first() : null;
    }
    
    public String extractDayFromShard(String shard) {
        int splitIndex = shard.indexOf('_');
        if (splitIndex > 0) {
            shard = shard.substring(0, splitIndex);
        }
        return shard;
    }
    
    public static IndexStream noOp(JexlNode node) {
        return new ScannerStream(Collections.emptyIterator(), StreamContext.NO_OP, node);
    }
}
