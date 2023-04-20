package datawave.query.index.lookup;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.tables.RangeStreamScanner;
import datawave.query.util.Tuple2;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.Iterator;

/**
 * Provides a core set of variables for the ScannerStream, Union, and Intersection.
 *
 * A reference to the underlying {@link datawave.query.tables.RangeStreamScanner} is required for seeking.
 *
 * Note that the BaseIndexStream does not implement the {@link IndexStream#seek(String)} method. Inheriting classes are responsible for determining the correct
 * implementation.
 */
public abstract class BaseIndexStream implements IndexStream {
    
    protected RangeStreamScanner rangeStreamScanner;
    
    protected EntryParser entryParser;
    
    protected JexlNode node;
    
    protected StreamContext context;
    
    protected IndexStream debugDelegate;
    
    // variables to support the PeekingIterator interface
    protected Iterator<Tuple2<String,IndexInfo>> backingIter;
    protected Tuple2<String,IndexInfo> peekedElement;
    protected boolean hasPeeked = false;
    
    /**
     * This constructor is used by BaseIndexStreams that have a backing range stream scanner. I.e., this will actually scan the global index
     *
     * @param rangeStreamScanner
     *            a range stream scanner
     * @param entryParser
     *            an entry parser
     * @param node
     *            the query node
     * @param context
     *            a stream context
     * @param debugDelegate
     *            a delegate used for debugging (not in use)
     */
    public BaseIndexStream(RangeStreamScanner rangeStreamScanner, EntryParser entryParser, JexlNode node, StreamContext context, IndexStream debugDelegate) {
        this.rangeStreamScanner = Preconditions.checkNotNull(rangeStreamScanner);
        this.entryParser = Preconditions.checkNotNull(entryParser);
        this.node = node;
        this.backingIter = Iterators.transform(this.rangeStreamScanner, this.entryParser);
        this.context = context;
        this.debugDelegate = debugDelegate;
    }
    
    /**
     * This constructor is for terms that do not have a range stream scanner.
     *
     * This is used by the SHARDS_AND_DAYS hint and terms that do not hit anything in the global index (delayed terms)
     *
     * @param iterator
     *            an iterator, usually empty
     * @param node
     *            the query node
     * @param context
     *            a stream context
     * @param debugDelegate
     *            delegate used for debugging (not in use)
     */
    public BaseIndexStream(Iterator<Tuple2<String,IndexInfo>> iterator, JexlNode node, StreamContext context, IndexStream debugDelegate) {
        this.rangeStreamScanner = null;
        this.entryParser = null;
        this.node = node;
        this.backingIter = Preconditions.checkNotNull(iterator);
        this.context = context;
        this.debugDelegate = debugDelegate;
    }
    
    // Empty constructor used by the Union and Intersection classes.
    public BaseIndexStream() {
        
    }
    
    /**
     * Reset the backing iterator after a seek. State must stay in sync with changes to the RangeStreamScanner.
     */
    public void resetBackingIterator() {
        if (rangeStreamScanner != null && entryParser != null) {
            this.peekedElement = null;
            this.hasPeeked = false;
            this.backingIter = Iterators.transform(this.rangeStreamScanner, this.entryParser);
        }
    }
    
    @Override
    public boolean hasNext() {
        return (hasPeeked && peekedElement != null) || backingIter.hasNext();
    }
    
    @Override
    public Tuple2<String,IndexInfo> peek() {
        if (!hasPeeked) {
            if (backingIter.hasNext()) {
                peekedElement = backingIter.next();
            } else {
                peekedElement = null;
            }
            hasPeeked = true;
        }
        return peekedElement;
    }
    
    @Override
    public Tuple2<String,IndexInfo> next() {
        if (!hasPeeked) {
            return backingIter.next();
        }
        Tuple2<String,IndexInfo> result = peekedElement;
        hasPeeked = false;
        peekedElement = null;
        return result;
    }
    
    @Override
    public Tuple2<String,IndexInfo> next(String context) {
        return next();
    }
    
    @Override
    public void remove() {}
    
    @Override
    public StreamContext context() {
        return context;
    }
    
    @Override
    public String getContextDebug() {
        if (debugDelegate == null) {
            return context + ": ScannerStream for " + JexlStringBuildingVisitor.buildQuery(node) + " (next = " + (hasNext() ? peek() : null) + ")";
        } else {
            return debugDelegate.getContextDebug();
        }
    }
    
    @Override
    public JexlNode currentNode() {
        return node;
    }
    
}
