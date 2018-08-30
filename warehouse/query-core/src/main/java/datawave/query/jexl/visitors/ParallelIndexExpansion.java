package datawave.query.jexl.visitors;

import static datawave.query.jexl.JexlASTHelper.isIndexed;
import static datawave.query.jexl.JexlASTHelper.isLiteralEquality;
import static org.apache.commons.jexl2.parser.JexlNodes.children;
import static org.apache.commons.jexl2.parser.JexlNodes.id;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import datawave.data.type.Type;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.query.model.QueryModel;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.CannotExpandUnfieldedTermFatalException;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.JexlNodeFactory.ContainerType;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.jexl.lookups.IndexLookupMap;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.jexl.nodes.TreeHashNode;
import datawave.query.planner.pushdown.Cost;
import datawave.query.planner.pushdown.CostEstimator;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.Query;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTUnknownFieldERNode;
import org.apache.commons.jexl2.parser.ASTUnsatisfiableERNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.Node;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ParallelIndexExpansion extends RebuildingVisitor {
    
    protected ShardQueryConfiguration config;
    protected ScannerFactory scannerFactory;
    protected ExecutorService executor;
    protected Collection<IndexLookupCallable> todo;
    protected Set<Type<?>> allTypes;
    protected Collection<String> onlyUseThese;
    protected Set<String> expansionFields;
    protected MetadataHelper helper;
    protected Set<String> indexOnlyFields;
    protected CostEstimator costAnalysis;
    protected Set<String> allFields;
    protected Node newChild;
    protected Map<TreeHashNode,IndexLookup> lookupMap = Maps.newConcurrentMap();
    protected String threadName;
    private static final Logger log = Logger.getLogger(ParallelIndexExpansion.class);
    
    public ParallelIndexExpansion(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, Set<String> expansionFields)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        this(config, scannerFactory, helper, expansionFields, "Datawave Fielded Regex");
    }
    
    public ParallelIndexExpansion(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, Set<String> expansionFields,
                    String threadName) throws InstantiationException, IllegalAccessException, TableNotFoundException {
        this.allFields = helper.getAllFields(config.getDatatypeFilter());
        this.config = config;
        this.scannerFactory = scannerFactory;
        this.threadName = threadName;
        
        this.indexOnlyFields = helper.getIndexOnlyFields(config.getDatatypeFilter());
        
        todo = Lists.newArrayList();
        
        this.allTypes = helper.getAllDatatypes();
        
        if (config.isExpansionLimitedToModelContents()) {
            try {
                QueryModel queryModel = helper.getQueryModel(config.getModelTableName(), config.getModelName());
                this.onlyUseThese = queryModel.getForwardQueryMapping().values();
            } catch (ExecutionException e) {
                this.onlyUseThese = null;
            }
        } else {
            this.onlyUseThese = null;
        }
        if (null != expansionFields)
            this.expansionFields = expansionFields;
        else
            this.expansionFields = Sets.newHashSet();
        this.helper = helper;
        costAnalysis = new CostEstimator(config, scannerFactory, helper);
    }
    
    protected class ParallelExpansionFactory implements ThreadFactory {
        
        private ThreadFactory dtf = Executors.defaultThreadFactory();
        private int threadNum = 1;
        private String threadIdentifier;
        protected String name = "Datawave ParallelIndexExpansion";
        
        public ParallelExpansionFactory(Query query, String name) {
            if (query == null || query.getId() == null) {
                this.threadIdentifier = "(unknown)";
            } else {
                this.threadIdentifier = query.getId().toString();
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
    
    protected void setupThreadResources() {
        int threads = this.config.getNumIndexLookupThreads().intValue();
        executor = Executors.newFixedThreadPool((int) Math.max(Math.ceil(threads), 10), new ParallelExpansionFactory(this.config.getQuery(), this.threadName));
    }
    
    @Override
    public Object visit(ASTJexlScript incomingNode, Object data) {
        
        setupThreadResources();
        
        ASTJexlScript node = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        
        node = (ASTJexlScript) RebuildingVisitor.copy(incomingNode);
        
        int newIndex = 0;
        try {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                Object objectReturn = node.jjtGetChild(i).jjtAccept(this, data);
                
                if (objectReturn instanceof Node) {
                    Node newChild = (Node) objectReturn;
                    if (newChild != null) {
                        // When we have an AND or OR
                        if ((newChild instanceof ASTOrNode || newChild instanceof ASTAndNode)) {
                            // Only add that node if it actually has children
                            if (0 < newChild.jjtGetNumChildren()) {
                                node.jjtAddChild(newChild, newIndex);
                                newIndex++;
                            }
                        } else {
                            // Otherwise, we want to add the child regardless
                            node.jjtAddChild(newChild, newIndex);
                            newIndex++;
                        }
                    }
                } else {
                    // Otherwise, we want to add the child regardless
                    node.jjtAddChild(newChild, newIndex);
                    newIndex++;
                }
                
            }
            
            concurrentExecution();
        } finally {
            
            // no need for this anymore.
            executor.shutdown();
        }
        
        LookupRemark remark = new LookupRemark();
        node = (ASTJexlScript) node.jjtAccept(remark, data);
        
        if (node.jjtGetNumChildren() == 0) {
            log.warn("Did not find any matches in index for the expansion of unfielded terms.");
            throw new CannotExpandUnfieldedTermFatalException("Did not find any matches in index for the expansion of unfielded terms.");
        }
        
        return node;
    }
    
    /**
     * Performs a lookup in the global index for an ANYFIELD term and returns the field names where the term is found
     * 
     * @param node
     * @param positive
     * @return set of field names from the global index for the nodes value
     * @throws TableNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    protected IndexLookupCallable buildIndexLookup(JexlNode node, boolean positive) throws TableNotFoundException, IOException, InstantiationException,
                    IllegalAccessException {
        
        String fieldName = JexlASTHelper.getIdentifier(node);
        
        TreeHashNode nodeHash = TreeHashVisitor.getNodeHash(node);
        IndexLookup task = lookupMap.get(nodeHash);
        
        if (null == task) {
            
            task = ShardIndexQueryTableStaticMethods.expandRegexTerms((ASTERNode) node, fieldName, config.getQueryFieldsDatatypes().get(fieldName),
                            config.getDatatypeFilter(), helper);
            
            if (task.supportReference())
                lookupMap.put(nodeHash, task);
            
        }
        
        return new IndexLookupCallable(task, node, positive, true, false);
    }
    
    /**
     * Determines if we should expand a regular expression given the current AST.
     * 
     * A regex doesn't have to be expanded if we can work out the logic such that we can satisfy the query with term equality only. The simple case is when the
     * ERNode and an EQNode share an AND parent. There are more complicated variants involving grand parents and OR nodes that are considered.
     * 
     * @param node
     * @param markedParents
     * @return true - if a regex has to be expanded false - if a regex doesn't have to be expanded
     */
    public boolean shouldProcessRegexFromStructure(ASTERNode node, Set<JexlNode> markedParents) {
        // if we have already marked this regex as exceeding the threshold, then no
        if (markedParents != null) {
            for (JexlNode markedParent : markedParents) {
                if (ExceededValueThresholdMarkerJexlNode.instanceOf(markedParent)) {
                    return false;
                }
            }
        }
        
        // if expanding all terms, then yes
        if (config.isExpandAllTerms()) {
            return true;
        }
        
        String erField = JexlASTHelper.getIdentifier(node);
        if (this.indexOnlyFields.contains(erField)) {
            return true;
        }
        // else determine whether we truely need to expand this based on whether other terms will dominate
        else {
            return ascendTree(node, Maps.<JexlNode,Boolean> newIdentityHashMap());
        }
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        Set<JexlNode> markedParents = (data != null && data instanceof Set) ? (Set) data : null;
        
        // check to see if this is a delayed node already
        if (ExceededValueThresholdMarkerJexlNode.instanceOf(node) || ExceededOrThresholdMarkerJexlNode.instanceOf(node) || ASTDelayedPredicate.instanceOf(node)
                        || IndexHoleMarkerJexlNode.instanceOf(node)) {
            markedParents = new HashSet<>();
            markedParents.add(node);
        }
        
        return super.visit(node, markedParents);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        Set<JexlNode> markedParents = (data != null && data instanceof Set) ? (Set) data : null;
        
        // we've already been delayed.
        if (markedParents != null) {
            for (JexlNode markedParent : markedParents) {
                if (ExceededValueThresholdMarkerJexlNode.instanceOf(markedParent) || ExceededOrThresholdMarkerJexlNode.instanceOf(markedParent)
                                || ExceededValueThresholdMarkerJexlNode.instanceOf(markedParent))
                    return node;
            }
        }
        
        // Given the structure of the tree, we don't *have* to expand this regex node
        if (config.getMaxIndexScanTimeMillis() == Long.MAX_VALUE && (!config.isExpandAllTerms() && !shouldProcessRegexFromStructure(node, markedParents))) {
            // However, given the characteristics of the query terms, we may still want to
            // expand this regex because it would be more efficient to do so
            if (!shouldProcessRegexFromCost(node)) {
                
                if (log.isDebugEnabled()) {
                    log.debug("Determined we don't need to process regex node:");
                    for (String line : PrintingVisitor.formattedQueryStringList(node)) {
                        log.debug(line);
                    }
                    log.debug("");
                }
                if (markedParents != null) {
                    for (JexlNode markedParent : markedParents) {
                        if (ExceededValueThresholdMarkerJexlNode.instanceOf(markedParent) || ExceededOrThresholdMarkerJexlNode.instanceOf(markedParent)
                                        || ASTDelayedPredicate.instanceOf(markedParent) || IndexHoleMarkerJexlNode.instanceOf(markedParent))
                            return node;
                    }
                }
                
                return ASTDelayedPredicate.create(node); // wrap in a delayed predicate to avoid using in RangeStream
            }
        } else {
            if (config.getMaxIndexScanTimeMillis() != Long.MAX_VALUE)
                log.debug("Skipping cost estimation since we have a timeout ");
        }
        
        String fieldName = JexlASTHelper.getIdentifier(node);
        try {
            if (!helper.isIndexed(fieldName, config.getDatatypeFilter())) {
                log.debug("Not expanding regular expression node as the field is not indexed");
                for (String logLine : PrintingVisitor.formattedQueryStringList(node)) {
                    log.info(logLine);
                }
                
                // If we've *never* seen this field, we want to denote the difference against it not being indexed
                if (fieldName.equals(Constants.ANY_FIELD)) {
                    return node;
                } else if (!allFields.contains(fieldName)) {
                    return RebuildingVisitor.copyInto(node, ASTUnknownFieldERNode.create());
                } else {
                    return RebuildingVisitor.copyInto(node, ASTUnsatisfiableERNode.create());
                }
            }
        } catch (TableNotFoundException e) {
            throw new DatawaveFatalQueryException(e);
        }
        
        return expandFieldNames(node, true);
    }
    
    /**
     * Walks up an AST and evaluates subtrees as needed. This method will fail fast if we determine we do not have to process a regex, otherwise the entire tree
     * will be evaluated.
     * 
     * This method recurses upwards, searching for an AND or OR node in the lineage. Once of those nodes is found, then the subtree rooted at that node is
     * evaluated. The visit map is used to cache already evaluated subtrees, so moving to a parent will not cause a subtree to be evaluated along with its
     * unevaluated siblings.
     * 
     * @param node
     *            - node to consider
     * 
     * @param visited
     *            - a visit list that contains the computed values for subtrees already visited, in case they are needed
     * 
     * @return true - if a regex has to be expanded false - if a regex doesn't have to be expanded
     */
    private boolean ascendTree(JexlNode node, Map<JexlNode,Boolean> visited) {
        if (node == null) {
            return true;
        } else {
            switch (id(node)) {
                case ParserTreeConstants.JJTORNODE:
                case ParserTreeConstants.JJTANDNODE: {
                    boolean expand = descendIntoSubtree(node, visited);
                    if (expand) {
                        return ascendTree(node.jjtGetParent(), visited);
                    } else {
                        return expand;
                    }
                }
                default:
                    return ascendTree(node.jjtGetParent(), visited);
            }
        }
    }
    
    /**
     * Evaluates a subtree to see if it can prevent the expansion of a regular expression.
     * 
     * This method recurses down under three conditions:
     * 
     * 1) An OR is encountered. In this case the result of recursing down the subtrees rooted at each child is OR'd together and returned. 2) An AND is
     * encountered. In this case the result of recursing down the subtrees rooted at each child is AND'd together and returned. 3) Any node that is not an EQ
     * node and has only 1 child. If there are multiple children, this method returns true, indicating that the subtree cannot defeat a regex expansion.
     * 
     * If an EQ node is encountered, we check if it can defeat an expansion by returning the value of a call to `doesNodeSupportRegexExpansion` on the node.
     * 
     * @param node
     * 
     * @return true - if a regex has to be expanded false - if a regex doesn't have to be expanded
     */
    private boolean descendIntoSubtree(JexlNode node, Map<JexlNode,Boolean> visited) {
        switch (id(node)) {
            case ParserTreeConstants.JJTORNODE: {
                return computeExpansionForSubtree(node, Join.OR, visited);
            }
            case ParserTreeConstants.JJTANDNODE: {
                return computeExpansionForSubtree(node, Join.AND, visited);
            }
            case ParserTreeConstants.JJTEQNODE: {
                boolean expand = doesNodeSupportRegexExpansion(node);
                visited.put(node, expand);
                return expand;
            }
            default: {
                JexlNode[] children = children(node);
                if (children.length == 1 && !ASTDelayedPredicate.instanceOf(children[0])) {
                    boolean expand = descendIntoSubtree(children[0], visited);
                    visited.put(node, expand);
                    return expand;
                } else {
                    return true;
                }
            }
        }
    }
    
    /**
     * If we have a literal equality on an indexed field, then this can be used to defeat a wild card expansion.
     * 
     * @return `true` if we should expand a regular expression node given this subtree `false` if we should not expand a regular expression node given this
     *         subtree
     */
    private boolean doesNodeSupportRegexExpansion(JexlNode node) {
        return !(isLiteralEquality(node) && isIndexed(node, config));
    }
    
    /**
     * Abstraction to indicate whether to use {@code `&=` or `|=`} when processing a node's subtrees.
     */
    enum Join {
        AND, OR
    }
    
    /**
     * The cases for OR and AND in `descendIntoSubtree` were almost equal, save for the initial value for expand and the operator used to join the results of
     * each child. I made this little macro doohickey to allow the differences between the two processes to be abstracted away.
     * 
     */
    private boolean computeExpansionForSubtree(JexlNode node, Join join, Map<JexlNode,Boolean> visited) {
        boolean expand = Join.AND.equals(join);
        for (JexlNode child : children(node)) {
            Boolean computedValue = visited.get(child);
            if (computedValue == null) {
                computedValue = descendIntoSubtree(child, visited);
                visited.put(child, computedValue);
            }
            switch (join) {
                case AND:
                    expand &= computedValue;
                    break;
                case OR:
                    expand |= computedValue;
            }
        }
        visited.put(node, expand);
        return expand;
    }
    
    protected boolean isIdentifier(JexlNode node) {
        return node instanceof ASTERNode;
    }
    
    /**
     * Now, when nodes are expanded, we shall create a callable object that will be added to the todo list.
     * 
     * @param node
     * @param positive
     * @return
     */
    protected Object expandFieldNames(JexlNode node, Boolean positive) {
        
        if (isIdentifier(node)) {
            
            try {
                
                IndexLookupCallable runnableLookup = buildIndexLookup(node, positive.booleanValue());
                
                JexlNode newNode = new IndexLookupCallback(ParserTreeConstants.JJTREFERENCE, runnableLookup);
                todo.add(((IndexLookupCallback) newNode).get());
                
                return newNode;
            } catch (TableNotFoundException | IllegalAccessException | InstantiationException | IOException e) {
                throw new DatawaveFatalQueryException(e);
            }
        }
        
        // Return itself if we don't have an expansion to perform
        return node;
        
    }
    
    /**
     * Internal identifier so that when we re-build the tree, we can traverse and replace any latent references.
     */
    protected class IndexLookupCallback extends ASTReference {
        
        protected IndexLookupCallable callable;
        
        /**
         * @param id
         */
        public IndexLookupCallback(int id, IndexLookupCallable callable) {
            super(id);
            this.callable = callable;
        }
        
        public IndexLookupCallable get() {
            return callable;
        }
        
        @Override
        public String toString() {
            return "IndexLookupCallback";
        }
        
    }
    
    /**
     * Executes the tasks in the todo list
     */
    protected void concurrentExecution() {
        
        List<Future<JexlNode>> futures;
        try {
            futures = executor.invokeAll(todo);
            
            for (Future<JexlNode> future : futures) {
                Exception sawException = null;
                try {
                    if (future.get() != null) {
                        
                    }
                } catch (InterruptedException e) {
                    sawException = (Exception) e.getCause();
                } catch (ExecutionException e) {
                    sawException = (Exception) e.getCause();
                } catch (Exception e) {
                    sawException = e;
                }
                
                if (null != sawException) {
                    log.error(sawException);
                    throw new CannotExpandUnfieldedTermFatalException(sawException);
                }
            }
            
        } catch (InterruptedException e) {
            throw new CannotExpandUnfieldedTermFatalException(e.getMessage());
        } finally {
            todo.clear();
        }
        
    }
    
    protected class IndexLookupCallable implements Callable<JexlNode> {
        
        protected IndexLookup lookup;
        protected JexlNode node;
        protected boolean positive;
        protected boolean ignoreComposites;
        
        protected JexlNode parentNode = null;
        private int id;
        private JexlNode newNode;
        protected boolean enforceTimeout;
        
        public IndexLookupCallable(IndexLookup lookup, JexlNode currNode, boolean positive, boolean enforceTimeout, boolean ignoreComposites) {
            this.lookup = lookup;
            this.node = currNode;
            this.positive = positive;
            this.enforceTimeout = enforceTimeout;
            this.ignoreComposites = ignoreComposites;
            parentNode = currNode.jjtGetParent();
        }
        
        public void setParentId(JexlNode parentNode, int id) {
            this.parentNode = parentNode;
            this.id = id;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.concurrent.Callable#call()
         */
        @Override
        public JexlNode call() throws Exception {
            
            IndexLookupMap fieldsToValues = null;
            try {
                long timeout = -1;
                if (enforceTimeout)
                    timeout = config.getMaxIndexScanTimeMillis();
                fieldsToValues = lookup.lookup(config, scannerFactory, timeout);
            } catch (Exception e) {
                log.error(e);
                throw e;
            }
            newNode = null;
            
            if (ignoreComposites)
                removeCompositeFields(fieldsToValues);
            
            // If we have no children, it's impossible to find any records, so this query returns no results
            if (fieldsToValues.isEmpty()) {
                
                // When we're not within an Or, we cannot proceed with this query.
                // We can proceed an in Or because we can trim this term out of the query
                // The And (or single term) implies that all results must have this term,
                // but we don't know which field the term actually appears in.
                if (!JexlASTHelper.isWithinOr(node)) {
                    log.warn("Could not convert an 'anyfield' node to an 'or' node");
                    throw new CannotExpandUnfieldedTermFatalException("Did not find any matches in index for the expansion of unfielded terms");
                }
                
                if (log.isDebugEnabled()) {
                    try {
                        log.debug("Retaining _ANYFIELD_ node because of no mappings for {\"term\": \"" + JexlASTHelper.getLiteral(node) + "\"}");
                    } catch (Exception ex) {
                        // it's just a debug statement
                    }
                }
                newNode = node;
            } else {
                onlyRetainFieldNamesInTheModelForwardMapping(fieldsToValues);
                if (positive) {
                    // for a positive node, we want equalities in a OR
                    newNode = JexlNodeFactory.createNodeTreeFromFieldsToValues(ContainerType.OR_NODE, new ASTEQNode(ParserTreeConstants.JJTEQNODE), node,
                                    fieldsToValues);
                } else {
                    // for a negative node, we want negative equalities in an AND
                    newNode = JexlNodeFactory.createNodeTreeFromFieldsToValues(ContainerType.AND_NODE, new ASTNENode(ParserTreeConstants.JJTNENODE), node,
                                    fieldsToValues);
                }
            }
            
            if (parentNode != null) {
                // rewrite the parent node at id
                parentNode.jjtAddChild(newNode, id);
                
            }
            
            return newNode;
        }
        
        private void onlyRetainFieldNamesInTheModelForwardMapping(IndexLookupMap fieldsToValues) {
            if (null != onlyUseThese) {
                fieldsToValues.retainFields(onlyUseThese);
            }
        }
        
        private void removeCompositeFields(IndexLookupMap fieldsToValues) throws TableNotFoundException {
            if (null != helper.getCompositeToFieldMap()) {
                fieldsToValues.removeFields(helper.getCompositeToFieldMap().keySet());
            }
        }
        
    }
    
    /**
     * Protected class that ensures that re-built nodes replace themselves. This can occur in the case where the only part of the script is a callback to a
     * lookup ( a top level not ).
     */
    protected class LookupRemark extends RebuildingVisitor {
        @Override
        public Object visit(ASTReference node, Object data) {
            if (node instanceof IndexLookupCallback) {
                return ((IndexLookupCallback) node).callable.newNode;
            } else
                return super.visit(node, data);
        }
    }
    
    /**
     * 
     * @param node
     * @return
     */
    public boolean shouldProcessRegexFromCost(ASTERNode node) {
        JexlNode nodeToBaseEvaluation = node;
        JexlNode parent = node.jjtGetParent();
        
        ASTAndNode topMostAnd = null;
        while (null != parent) {
            switch (id(parent)) {
                case ParserTreeConstants.JJTORNODE: {
                    // if we have found an and node by this point, then lets use it
                    if (topMostAnd != null) {
                        parent = null;
                    }
                    // else we have an or node containing this is several other nodes.
                    // so lets evaluate whether we should expand this regex based on the cost of the or tree
                    else {
                        nodeToBaseEvaluation = node;
                        parent = parent.jjtGetParent();
                    }
                    break;
                }
                
                case ParserTreeConstants.JJTANDNODE: {
                    topMostAnd = (ASTAndNode) parent;
                    // Intentional lack of break. We want to still recurse up the tree
                }
                
                default: {
                    parent = parent.jjtGetParent();
                }
            }
        }
        
        // if we do not have an AND node which can be expanded when evaluating expansion based on cost, then we have an or node with possibly
        // multiple regexes. Lets assume we DO have to expand this node based on cost
        if (topMostAnd == null) {
            return true;
        }
        
        List<JexlNode> subTrees = Lists.newArrayList();
        
        // Get the direct children of that topMost AND node
        collapseAndSubtrees(topMostAnd, subTrees);
        
        // get the cost of this node (or the subtree that contains it)
        Cost regexCost = costAnalysis.computeCostForSubtree(nodeToBaseEvaluation);
        
        // Compute the cost to determine whether or not to expand this regex
        return shouldProcessRegexByCostWithChildren(subTrees, regexCost);
    }
    
    public void collapseAndSubtrees(ASTAndNode node, List<JexlNode> subTrees) {
        for (JexlNode child : children(node)) {
            if (ParserTreeConstants.JJTANDNODE == id(child)) {
                collapseAndSubtrees((ASTAndNode) child, subTrees);
            } else {
                subTrees.add(child);
            }
        }
    }
    
    public boolean shouldProcessRegexByCostWithChildren(List<JexlNode> children, Cost regexCost) {
        Preconditions.checkArgument(!children.isEmpty(), "We found an empty list of children for an AND which should at least contain an ERnode");
        
        Cost c = new Cost();
        
        for (JexlNode child : children) {
            Cost childCost = costAnalysis.computeCostForSubtree(child);
            
            if (log.isDebugEnabled()) {
                log.debug("Computed cost of " + childCost + " for:");
                for (String logLine : PrintingVisitor.formattedQueryStringList(child)) {
                    log.debug(logLine);
                }
            }
            
            // Use this child's cost if we have no current cost or it's less than the current cost
            if (0 != childCost.getOtherCost()) {
                if (0 != c.getOtherCost()) {
                    if (childCost.getOtherCost() < c.getOtherCost()) {
                        c = childCost;
                    }
                } else {
                    c = childCost;
                }
            }
        }
        
        return (regexCost.getERCost() + regexCost.getOtherCost()) < (c.getERCost() + c.getOtherCost());
    }
    
}
