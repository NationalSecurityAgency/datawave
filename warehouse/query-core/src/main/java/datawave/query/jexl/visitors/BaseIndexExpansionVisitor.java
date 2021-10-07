package datawave.query.jexl.visitors;

import com.google.common.collect.Maps;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.planner.pushdown.CostEstimator;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Abstract class which provides a framework for visitors which perform index lookups based on the contents of the Jexl tree.
 */
public abstract class BaseIndexExpansionVisitor extends RebuildingVisitor {
    private static final Logger log = Logger.getLogger(BaseIndexExpansionVisitor.class);
    private static final int MIN_THREADS = 1;
    
    protected ShardQueryConfiguration config;
    protected ScannerFactory scannerFactory;
    protected MetadataHelper helper;
    protected boolean expandFields;
    protected boolean expandValues;
    protected String threadName;
    
    protected Set<String> indexOnlyFields;
    protected Set<String> allFields;
    
    protected CostEstimator costAnalysis;
    
    protected ExecutorService executor;
    protected Map<String,IndexLookup> lookupMap = Maps.newConcurrentMap();
    protected List<FutureJexlNode> futureJexlNodes = new ArrayList<>();
    
    // The constructor should not be made public so that we can ensure that the executor is setup and shutdown correctly
    protected BaseIndexExpansionVisitor(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, String threadName)
                    throws TableNotFoundException {
        this.config = config;
        this.scannerFactory = scannerFactory;
        this.helper = helper;
        this.expandFields = config.isExpandFields();
        this.expandValues = config.isExpandValues();
        this.threadName = threadName;
        
        this.indexOnlyFields = helper.getIndexOnlyFields(config.getDatatypeFilter());
        this.allFields = helper.getAllFields(config.getDatatypeFilter());
        
        this.costAnalysis = new CostEstimator(config, scannerFactory, helper);
    }
    
    protected void setupExecutor() {
        int threads = Math.max(this.config.getNumIndexLookupThreads(), MIN_THREADS);
        executor = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(true), new IndexExpansionThreadFactory(
                        this.config, this.threadName));
    }
    
    protected void shutdownExecutor() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }
    
    /**
     * The expand method is the entrypoint which should be called to run index expansion on a given Jexl tree.
     * 
     * @param script
     *            the Jexl tree to expand, not null
     * @param <T>
     *            the Jexl node type
     * @return a rebuilt Jexl tree with applicable fields/terms expanded
     */
    @SuppressWarnings("unchecked")
    protected <T extends JexlNode> T expand(T script) {
        setupExecutor();
        try {
            if (null == config.getQueryFieldsDatatypes()) {
                QueryException qe = new QueryException(DatawaveErrorCode.DATATYPESFORINDEXFIELDS_MULTIMAP_MISSING);
                throw new DatawaveFatalQueryException(qe);
            }
            
            T rebuiltScript = (T) (script.jjtAccept(this, null));
            
            rebuildFutureJexlNodes();
            
            // handle the case where the root node was expanded
            if (rebuiltScript instanceof FutureJexlNode) {
                rebuiltScript = (T) ((FutureJexlNode) rebuiltScript).getRebuiltNode();
            }
            
            return rebuiltScript;
        } finally {
            shutdownExecutor();
        }
    }
    
    /**
     *
     * @param node
     *            the jexl node to expand, not null
     * @param ignoreComposites
     *            whether composite fields should be kept
     * @param keepOriginalNode
     *            whether the original node should be replaced
     * @param indexLookupSupplier
     *            the method used to create the index lookup, not null
     * @return the original Jexl node if keepOriginalNode is false, otherwise the expanded Jexl node
     */
    protected JexlNode buildIndexLookup(JexlNode node, boolean ignoreComposites, boolean keepOriginalNode, Supplier<IndexLookup> indexLookupSupplier) {
        String nodeString = JexlStringBuildingVisitor.buildQueryWithoutParse(TreeFlatteningRebuildingVisitor.flatten(node), true);
        IndexLookup lookup = lookupMap.get(nodeString);
        
        if (null == lookup) {
            lookup = indexLookupSupplier.get();
            
            if (lookup.supportReference()) {
                lookupMap.put(nodeString, lookup);
            }
        }
        
        return createFutureJexlNode(lookup, node, ignoreComposites, keepOriginalNode);
    }
    
    /**
     * Creates a special Jexl node which serves as an intermediate placeholder for the node which will be expanded
     * 
     * @param lookup
     *            the index lookup, not null
     * @param node
     *            the jexl node to expand, not null
     * @param ignoreComposites
     *            whether composite fields should be kept
     * @param keepOriginalNode
     *            whether the original node should be replaced
     * @return a FutureJexlNode
     */
    protected FutureJexlNode createFutureJexlNode(IndexLookup lookup, JexlNode node, boolean ignoreComposites, boolean keepOriginalNode) {
        lookup.lookupAsync(config, scannerFactory, config.getMaxIndexScanTimeMillis(), executor);
        
        FutureJexlNode futureNode = new FutureJexlNode(node, lookup, ignoreComposites, keepOriginalNode);
        futureNode.jjtSetParent(node.jjtGetParent());
        futureJexlNodes.add(futureNode);
        
        return futureNode;
    }
    
    protected void rebuildFutureJexlNodes() {
        while (!futureJexlNodes.isEmpty()) {
            List<FutureJexlNode> rebuiltNodes = new ArrayList<>();
            
            for (FutureJexlNode futureJexlNode : futureJexlNodes) {
                if (futureJexlNode.getLookup().hasStarted()) {
                    rebuildFutureJexlNode(futureJexlNode);
                    
                    JexlNode newNode = futureJexlNode.getRebuiltNode();
                    
                    // if the parent is not null, replace the child
                    // if the parent is null, this is the root node, and we will handle that in the expand method
                    if (futureJexlNode.jjtGetParent() != null) {
                        JexlNodes.replaceChild(futureJexlNode.jjtGetParent(), futureJexlNode, newNode);
                    }
                    
                    rebuiltNodes.add(futureJexlNode);
                }
            }
            
            // if we rebuilt some nodes, remove them and let's see if there's more work to do
            if (!rebuiltNodes.isEmpty()) {
                futureJexlNodes.removeAll(rebuiltNodes);
            }
            // if we didn't rebuild any nodes, and there's more work to do, sleep before checking again
            else if (!futureJexlNodes.isEmpty()) {
                try {
                    Thread.sleep(500L);
                } catch (InterruptedException e) {
                    log.warn("Interrupted while waiting for index lookups to finish running", e);
                }
            }
        }
    }
    
    /**
     * Each Index Expansion visitor should define it's own method for creating a final expanded node from a FutureJexlNode
     * 
     * @param futureJexlNode
     *            the future jexl node to rebuild, not null
     */
    protected abstract void rebuildFutureJexlNode(FutureJexlNode futureJexlNode);
    
    /**
     * Serves as a placeholder Jexl node which can eventually be replaced with an expanded Jexl node once the Index Lookup has finished
     */
    protected static class FutureJexlNode extends JexlNode {
        private final JexlNode origNode;
        private final IndexLookup lookup;
        private final boolean ignoreComposites;
        private final boolean keepOriginalNode;
        
        private JexlNode rebuiltNode;
        
        public FutureJexlNode(JexlNode origNode, IndexLookup lookup, boolean ignoreComposites, boolean keepOriginalNode) {
            super(ParserTreeConstants.JJTREFERENCE);
            this.origNode = origNode;
            this.lookup = lookup;
            this.ignoreComposites = ignoreComposites;
            this.keepOriginalNode = keepOriginalNode;
        }
        
        public JexlNode getOrigNode() {
            return origNode;
        }
        
        public IndexLookup getLookup() {
            return lookup;
        }
        
        public boolean isIgnoreComposites() {
            return ignoreComposites;
        }
        
        public boolean isKeepOriginalNode() {
            return keepOriginalNode;
        }
        
        public JexlNode getRebuiltNode() {
            return rebuiltNode;
        }
        
        public void setRebuiltNode(JexlNode rebuiltNode) {
            this.rebuiltNode = rebuiltNode;
        }
    }
    
    /**
     * Serves as a means to associate index lookup threads with a particular Index Expansion Visitor
     */
    protected static class IndexExpansionThreadFactory implements ThreadFactory {
        private final ShardQueryConfiguration config;
        private final ThreadFactory dtf = Executors.defaultThreadFactory();
        private int threadNum = 1;
        private final String threadIdentifier;
        protected String name;
        
        public IndexExpansionThreadFactory(ShardQueryConfiguration config, String name) {
            this.config = config;
            if (config.getQuery() == null || config.getQuery().getId() == null) {
                this.threadIdentifier = "(unknown)";
            } else {
                this.threadIdentifier = config.getQuery().getId().toString();
            }
            this.name = name;
        }
        
        public Thread newThread(Runnable r) {
            Thread thread = dtf.newThread(r);
            thread.setName(name + " Session " + threadIdentifier + " -" + threadNum++);
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler(config.getQuery().getUncaughtExceptionHandler());
            return thread;
        }
    }
}
