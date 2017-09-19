package datawave.query.index.lookup;

import java.util.Collections;
import java.util.Iterator;

import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.util.Tuple2;

import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class ScannerStream extends ASTReference implements IndexStream {
    protected final PeekingIterator<Tuple2<String,IndexInfo>> itr;
    protected final StreamContext ctx;
    protected JexlNode currNode;
    // The debug delegate is for optionally delegating the contextDebug call
    protected final IndexStream debugDelegate;
    
    private ScannerStream(PeekingIterator<Tuple2<String,IndexInfo>> itr, StreamContext ctx, JexlNode currNode, IndexStream debugDelegate) {
        super(ParserTreeConstants.JJTREFERENCE);
        this.itr = itr;
        this.ctx = ctx;
        this.currNode = currNode;
        this.debugDelegate = debugDelegate;
    }
    
    private ScannerStream(Iterator<Tuple2<String,IndexInfo>> itr, StreamContext ctx) {
        this(Iterators.peekingIterator(itr), ctx, null, null);
    }
    
    private ScannerStream(Iterator<Tuple2<String,IndexInfo>> itr, StreamContext ctx, JexlNode currNode) {
        this(Iterators.peekingIterator(itr), ctx, currNode, null);
    }
    
    private ScannerStream(Iterator<Tuple2<String,IndexInfo>> itr, StreamContext ctx, JexlNode currNode, IndexStream debugDelegate) {
        this(Iterators.peekingIterator(itr), ctx, currNode, debugDelegate);
    }
    
    @Override
    public boolean hasNext() {
        return itr.hasNext();
    }
    
    @Override
    public Tuple2<String,IndexInfo> peek() {
        return itr.peek();
    }
    
    @Override
    public Tuple2<String,IndexInfo> next() {
        return itr.next();
    }
    
    @Override
    public void remove() {}
    
    @Override
    public StreamContext context() {
        return ctx;
    }
    
    @Override
    public String getContextDebug() {
        if (debugDelegate == null) {
            return ctx + ": ScannerStream for " + JexlStringBuildingVisitor.buildQuery(currNode) + " (next = " + (itr.hasNext() ? itr.peek() : null) + ")";
        } else {
            return debugDelegate.getContextDebug();
        }
    }
    
    public static ScannerStream unindexed(JexlNode currNode) {
        return new ScannerStream(Collections.<Tuple2<String,IndexInfo>> emptySet().iterator(), StreamContext.UNINDEXED, currNode);
    }
    
    public static ScannerStream unindexed(JexlNode currNode, IndexStream debugDelegate) {
        return new ScannerStream(Collections.<Tuple2<String,IndexInfo>> emptySet().iterator(), StreamContext.UNINDEXED, currNode, debugDelegate);
    }
    
    public static ScannerStream noData(JexlNode currNode) {
        return new ScannerStream(Collections.<Tuple2<String,IndexInfo>> emptySet().iterator(), StreamContext.ABSENT, currNode);
    }
    
    public static ScannerStream noData(JexlNode currNode, IndexStream debugDelegate) {
        return new ScannerStream(Collections.<Tuple2<String,IndexInfo>> emptySet().iterator(), StreamContext.ABSENT, currNode, debugDelegate);
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
    
    // exceeded value threshold, so we can evaluate with data but may need special handling
    public static ScannerStream exceededValueThreshold(Iterator<Tuple2<String,IndexInfo>> itr, JexlNode currNode) {
        
        JexlNode resultNode = JexlNodeFactory.wrap(currNode);
        return new ScannerStream(itr, StreamContext.EXCEEDED_VALUE_THRESHOLD, resultNode);
    }
    
    public static ScannerStream delayedExpression(JexlNode currNode) {
        return new ScannerStream(Collections.<Tuple2<String,IndexInfo>> emptySet().iterator(), StreamContext.DELAYED_FIELD, currNode);
    }
    
    public static ScannerStream unknownField(JexlNode currNode) {
        return new ScannerStream(Collections.<Tuple2<String,IndexInfo>> emptySet().iterator(), StreamContext.UNKNOWN_FIELD, currNode);
    }
    
    public static ScannerStream unknownField(JexlNode currNode, IndexStream debugDelegate) {
        return new ScannerStream(Collections.<Tuple2<String,IndexInfo>> emptySet().iterator(), StreamContext.UNKNOWN_FIELD, currNode, debugDelegate);
    }
    
    public static ScannerStream ignored(JexlNode currNode) {
        return new ScannerStream(Collections.<Tuple2<String,IndexInfo>> emptySet().iterator(), StreamContext.IGNORED, currNode);
    }
    
    public static ScannerStream ignored(JexlNode currNode, IndexStream debugDelegate) {
        return new ScannerStream(Collections.<Tuple2<String,IndexInfo>> emptySet().iterator(), StreamContext.IGNORED, currNode, debugDelegate);
    }
    
    // exceeded term threshold, so we cannot evaluate
    public static ScannerStream exceededTermThreshold(JexlNode currNode) {
        return new ScannerStream(Collections.<Tuple2<String,IndexInfo>> emptySet().iterator(), StreamContext.EXCEEDED_TERM_THRESHOLD, currNode);
    }
    
    /**
     * Create a stream in the initialized state
     * 
     * @param itr
     * @return
     */
    public static ScannerStream initialized(Iterator<Tuple2<String,IndexInfo>> itr, JexlNode currNode) {
        return new ScannerStream(itr, StreamContext.INITIALIZED, currNode);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.query.index.lookup.IndexStream#currentNode()
     */
    @Override
    public JexlNode currentNode() {
        return currNode;
    }
    
    public static IndexStream noOp(JexlNode node) {
        return new ScannerStream(Collections.<Tuple2<String,IndexInfo>> emptySet().iterator(), StreamContext.NO_OP, node);
    }
}
