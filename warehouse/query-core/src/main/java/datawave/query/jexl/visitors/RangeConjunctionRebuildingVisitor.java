package datawave.query.jexl.visitors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.IllegalRangeArgumentException;
import datawave.query.index.stats.IndexStatsClient;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.jexl.lookups.IndexLookupMap;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.planner.pushdown.Cost;
import datawave.query.planner.pushdown.CostEstimator;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.commons.jexl2.parser.JexlNodes.children;
import static org.apache.commons.jexl2.parser.JexlNodes.id;

/**
 * Visits an JexlNode tree, removing bounded ranges (a pair consisting of one GT or GE and one LT or LE node), and replacing them with concrete equality nodes.
 * The concrete equality nodes will be replaced with normalized values because the TextNormalizer interface can only normalize a value and cannot un-normalize a
 * value.
 *
 * 
 *
 */
public class RangeConjunctionRebuildingVisitor extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(RangeConjunctionRebuildingVisitor.class);
    
    private final ShardQueryConfiguration config;
    private final ScannerFactory scannerFactory;
    private final IndexStatsClient stats;
    protected CostEstimator costAnalysis;
    protected Set<String> indexOnlyFields;
    protected Set<String> allFields;
    protected MetadataHelper helper;
    protected boolean expandFields;
    protected boolean expandValues;
    
    public RangeConjunctionRebuildingVisitor(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, boolean expandFields,
                    boolean expandValues) throws TableNotFoundException, ExecutionException {
        this.config = config;
        this.helper = helper;
        this.indexOnlyFields = helper.getIndexOnlyFields(config.getDatatypeFilter());
        this.allFields = helper.getAllFields(config.getDatatypeFilter());
        this.scannerFactory = scannerFactory;
        stats = new IndexStatsClient(this.config.getConnector(), this.config.getIndexStatsTableName());
        costAnalysis = new CostEstimator(config, scannerFactory, helper);
        this.expandFields = expandFields;
        this.expandValues = expandValues;
    }
    
    /**
     * Expand all regular expression nodes into a conjunction of discrete terms mapping to that regular expression. For regular expressions that match nothing
     * in the global index, the regular expression node is left intact.
     *
     * @param config
     * @param helper
     * @param script
     * @return
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T expandRanges(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, T script,
                    boolean expandFields, boolean expandValues) throws TableNotFoundException, ExecutionException {
        // if not expanding fields or values, then this is a noop
        if (expandFields || expandValues) {
            RangeConjunctionRebuildingVisitor visitor = new RangeConjunctionRebuildingVisitor(config, scannerFactory, helper, expandFields, expandValues);
            
            if (null == visitor.config.getQueryFieldsDatatypes()) {
                QueryException qe = new QueryException(DatawaveErrorCode.DATATYPESFORINDEXFIELDS_MULTIMAP_MISSING);
                throw new DatawaveFatalQueryException(qe);
            }
            
            return (T) (script.jjtAccept(visitor, null));
        } else {
            return script;
        }
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (ASTDelayedPredicate.instanceOf(node) || IndexHoleMarkerJexlNode.instanceOf(node) || ASTEvaluationOnly.instanceOf(node)) {
            return node;
        } else if (ExceededValueThresholdMarkerJexlNode.instanceOf(node) || ExceededTermThresholdMarkerJexlNode.instanceOf(node)
                        || ExceededOrThresholdMarkerJexlNode.instanceOf(node)) {
            return node;
        } else
            return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        List<JexlNode> leaves = new ArrayList<>();
        Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRanges(node, this.config.getDatatypeFilter(), this.helper, leaves, false);
        
        JexlNode andNode = JexlNodes.newInstanceOfType(node);
        andNode.image = node.image;
        andNode.jjtSetParent(node.jjtGetParent());
        
        // We have a bounded range completely inside of an AND/OR
        if (!ranges.isEmpty()) {
            andNode = expandIndexBoundedRange(ranges, leaves, node, andNode, data);
        } else {
            // We have no bounded range to replace, just proceed as normal
            JexlNodes.ensureCapacity(andNode, node.jjtGetNumChildren());
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode newChild = (JexlNode) node.jjtGetChild(i).jjtAccept(this, data);
                
                andNode.jjtAddChild(newChild, i);
                newChild.jjtSetParent(andNode);
            }
        }
        
        return andNode;
    }
    
    protected JexlNode expandIndexBoundedRange(Map<LiteralRange<?>,List<JexlNode>> ranges, List<JexlNode> leaves, ASTAndNode currentNode, JexlNode newNode,
                    Object data) {
        // Add all children in this AND/OR which are not a part of the range
        JexlNodes.ensureCapacity(newNode, leaves.size() + ranges.size());
        int index = 0;
        for (; index < leaves.size(); index++) {
            log.debug(leaves.get(index).image);
            // Add each child which is not a part of the bounded range, visiting them first
            JexlNode visitedChild = (JexlNode) leaves.get(index).jjtAccept(this, null);
            newNode.jjtAddChild(visitedChild, index);
            visitedChild.jjtSetParent(newNode);
        }
        
        // Sanity check to ensure that we found some nodes (redundant since we couldn't have made a bounded LiteralRange in the first
        // place if we had found not range nodes)
        if (ranges.isEmpty()) {
            log.debug("Cannot find range operator nodes that encompass this query. Not proceeding with range expansion for this node.");
            return currentNode;
        }
        
        for (Map.Entry<LiteralRange<?>,List<JexlNode>> range : ranges.entrySet()) {
            IndexLookup lookup = ShardIndexQueryTableStaticMethods.expandRange(range.getKey());
            
            IndexLookupMap fieldsToTerms = null;
            
            try {
                fieldsToTerms = lookup.lookup(config, scannerFactory, config.getMaxIndexScanTimeMillis());
            } catch (IllegalRangeArgumentException e) {
                log.info("Cannot expand "
                                + range
                                + " because it creates an invalid Accumulo Range. This is likely due to bad user input or failed normalization. This range will be ignored.");
                return RebuildingVisitor.copy(currentNode);
            }
            
            // If we have any terms that we expanded, wrap them in parens and add them to the parent
            ASTAndNode onlyRangeNodes = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
            
            JexlNodes.ensureCapacity(onlyRangeNodes, range.getValue().size());
            for (int i = 0; i < range.getValue().size(); i++) {
                onlyRangeNodes.jjtAddChild(range.getValue().get(i), i);
            }
            
            JexlNode orNode = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, new ASTEQNode(
                            ParserTreeConstants.JJTEQNODE), onlyRangeNodes, fieldsToTerms, expandFields, expandValues);
            
            // Set the parent and child pointers accordingly
            orNode.jjtSetParent(newNode);
            newNode.jjtAddChild(orNode, index++);
            
        }
        
        // If we had no other nodes than this bounded range, we can strip out the original parent
        if (newNode.jjtGetNumChildren() == 1) {
            newNode.jjtGetChild(0).jjtSetParent(newNode.jjtGetParent());
            return newNode.jjtGetChild(0);
        }
        
        return newNode;
    }
    
    /**
     * We only want to expand a range if it is more selective than a node it is ANDed with.
     *
     * @param node
     * @param range
     * @return
     */
    public boolean shouldExpandRangeBasedOnSelectivity(JexlNode node, LiteralRange<?> range) {
        return shouldExpandRangeBasedOnSelectivity(node, range, IndexStatsClient.DEFAULT_VALUE);
    }
    
    /**
     * We only want to expand a range if it is more selective than a node it is ANDed with.
     *
     * @param node
     * @param range
     * @param rangeSelectivity
     * @return
     */
    protected boolean shouldExpandRangeBasedOnSelectivity(JexlNode node, LiteralRange<?> range, Double rangeSelectivity) {
        switch (id(node)) {
            case ParserTreeConstants.JJTGENODE:
            case ParserTreeConstants.JJTGTNODE:
            case ParserTreeConstants.JJTLENODE:
            case ParserTreeConstants.JJTLTNODE:
            case ParserTreeConstants.JJTREFERENCE:
            case ParserTreeConstants.JJTREFERENCEEXPRESSION:
                // recurse up the tree
                return shouldExpandRangeBasedOnSelectivity(node.jjtGetParent(), range, rangeSelectivity);
            case ParserTreeConstants.JJTANDNODE:
                boolean foundChildSelectivity = false;
                if (rangeSelectivity.equals(IndexStatsClient.DEFAULT_VALUE)) {
                    // only want to fetch the range selectivity once
                    rangeSelectivity = JexlASTHelper.getNodeSelectivity(Sets.newHashSet(range.getFieldName()), config, stats);
                    if (log.isDebugEnabled())
                        log.debug("Range selectivity:" + rangeSelectivity);
                }
                for (JexlNode child : JexlASTHelper.getEQNodes(node)) {
                    // Try to get selectivity for each child
                    Double childSelectivity = JexlASTHelper.getNodeSelectivity(child, config, stats);
                    
                    if (childSelectivity.equals(IndexStatsClient.DEFAULT_VALUE)) {
                        continue;
                    } else {
                        foundChildSelectivity = true;
                    }
                    
                    if (log.isDebugEnabled() && foundChildSelectivity)
                        log.debug("Max Child selectivity: " + childSelectivity);
                    
                    // If the child is an EQ node, is indexed, and is more
                    // selective than the regex we don't need to process the regex
                    if (JexlASTHelper.getIdentifierOpLiteral(child) != null && JexlASTHelper.isIndexed(child, config) && rangeSelectivity < childSelectivity) {
                        return false;
                    }
                }
                
                return shouldExpandRangeBasedOnSelectivity(node.jjtGetParent(), range, rangeSelectivity);
            default:
                return true;
        }
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
                if (children.length == 1) {
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
        return !(node instanceof ASTEQNode && JexlASTHelper.getIdentifierOpLiteral(node) != null && JexlASTHelper.isIndexed(node, config));
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
