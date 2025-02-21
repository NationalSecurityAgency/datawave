package datawave.query.jexl.visitors;

import static datawave.query.jexl.JexlASTHelper.isIndexed;
import static datawave.query.jexl.JexlASTHelper.isLiteralEquality;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DROPPED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_TERM;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static org.apache.commons.jexl3.parser.JexlNodes.id;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.EmptyUnfieldedTermExpansionException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.jexl.lookups.IndexLookupMap;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.model.QueryModel;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.planner.pushdown.Cost;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;

/**
 * Visits a Jexl tree, looks for regex terms, and replaces them with concrete values from the index
 */
public class RegexIndexExpansionVisitor extends BaseIndexExpansionVisitor {
    private static final Logger log = LoggerFactory.getLogger(RegexIndexExpansionVisitor.class);

    protected boolean expandUnfieldedNegations;

    protected Collection<String> onlyUseThese;

    // This flag keeps track of whether we are in a negated portion of the tree.
    protected boolean negated = false;

    /**
     * Abstraction to indicate whether to use {@code `&=` or `|=`} when processing a node's subtrees.
     */
    enum Join {
        AND, OR
    }

    // The constructor should not be made public so that we can ensure that the executor is setup and shutdown correctly
    protected RegexIndexExpansionVisitor(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper,
                    Map<String,IndexLookup> lookupMap) throws TableNotFoundException {
        this(config, scannerFactory, helper, lookupMap, "RegexIndexExpansion");
    }

    // The constructor should not be made public so that we can ensure that the executor is setup and shutdown correctly
    protected RegexIndexExpansionVisitor(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper,
                    Map<String,IndexLookup> lookupMap, String threadName) throws TableNotFoundException {
        super(config, scannerFactory, helper, lookupMap, threadName);

        this.expandUnfieldedNegations = config.isExpandUnfieldedNegations();

        if (config.isLimitTermExpansionToModel()) {
            try {
                QueryModel queryModel = helper.getQueryModel(config.getModelTableName(), config.getModelName());
                this.onlyUseThese = queryModel.getForwardQueryMapping().values();
            } catch (ExecutionException e) {
                this.onlyUseThese = null;
            }
        } else {
            this.onlyUseThese = null;
        }
        this.stage = "regex";
    }

    /**
     * Visits the Jexl script, looks for regex terms, and replaces them with concrete values from the index <br>
     * This version allows for reuse of the index lookup map
     *
     * @param config
     *            the query configuration, not null
     * @param scannerFactory
     *            the scanner factory, not null
     * @param helper
     *            the metadata helper, not null
     * @param script
     *            the Jexl script to expand, not null
     * @param lookupMap
     *            the index lookup map to use (or reuse), may be null
     * @param <T>
     *            the Jexl node type
     * @return a rebuilt Jexl tree with it's regex terms expanded
     * @throws TableNotFoundException
     *             if we fail to retrieve fields from the metadata helper
     */
    public static <T extends JexlNode> T expandRegex(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper,
                    Map<String,IndexLookup> lookupMap, T script) throws TableNotFoundException {
        RegexIndexExpansionVisitor visitor = new RegexIndexExpansionVisitor(config, scannerFactory, helper, lookupMap);
        return ensureTreeNotEmpty(visitor.expand(script));
    }

    private static <T extends JexlNode> T ensureTreeNotEmpty(T script) throws EmptyUnfieldedTermExpansionException {
        if (script.jjtGetNumChildren() == 0) {
            log.warn("Did not find any matches in index for the expansion of unfielded terms.");
            throw new EmptyUnfieldedTermExpansionException("Did not find any matches in index for the expansion of unfielded terms.");
        }
        return script;
    }

    /**
     * Visits the Jexl script, looks for regex terms, and replaces them with concrete values from the index
     *
     * @param config
     *            the query configuration, not null
     * @param scannerFactory
     *            the scanner factory, not null
     * @param helper
     *            the metadata helper, not null
     * @param script
     *            the Jexl script to expand, not null
     * @param <T>
     *            the Jexl node type
     * @return a rebuilt Jexl tree with it's regex terms expanded
     * @throws TableNotFoundException
     *             if we fail to retrieve fields from the metadata helper
     */
    public static <T extends JexlNode> T expandRegex(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, T script)
                    throws TableNotFoundException {
        return expandRegex(config, scannerFactory, helper, null, script);
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        Set<JexlNode> markedParents = (data instanceof Set) ? (Set) data : null;

        // check to see if this is a delayed node already
        if (QueryPropertyMarker.findInstance(node).isAnyType()) {
            markedParents = new HashSet<>();
            markedParents.add(node);
        }

        return super.visit(node, markedParents);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        toggleNegation();
        try {
            return super.visit(node, data);
        } finally {
            toggleNegation();
        }
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        Set<JexlNode> markedParents = (data instanceof Set) ? (Set) data : null;

        String fieldName = JexlASTHelper.getIdentifier(node);

        // if its evaluation only tag an exceeded value marker for a deferred ivarator
        if (markedParents != null) {
            boolean evalOnly = false;
            boolean exceededValueMarker = false;
            boolean exceededTermMarker = false;
            for (JexlNode markedParent : markedParents) {
                QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(markedParent);
                if (instance.isAnyTypeOf(EVALUATION_ONLY, DROPPED)) {
                    evalOnly = true;
                } else if (instance.isType(EXCEEDED_VALUE)) {
                    exceededValueMarker = true;
                } else if (instance.isType(EXCEEDED_TERM)) {
                    exceededTermMarker = true;
                }
            }

            boolean nonEvent;
            try {
                nonEvent = helper.getNonEventFields(config.getDatatypeFilter()).contains(fieldName);
            } catch (TableNotFoundException e) {
                throw new DatawaveFatalQueryException(e);
            }

            if (evalOnly && !exceededValueMarker && !exceededTermMarker && nonEvent) {
                return QueryPropertyMarker.create(node, EXCEEDED_VALUE);
            } else if (exceededValueMarker || exceededTermMarker) {
                // already did this expansion
                return node;
            } else if (!nonEvent && evalOnly) {
                // no need to expand it is going to come out of the event
                return node;
            }
        }

        // determine whether we have the tools to expand this in the first place
        try {
            // check special case NO_FIELD
            if (fieldName.equals(Constants.NO_FIELD)) {
                return node;
            }

            if (!isExpandable(node)) {
                if (mustExpand(node)) {
                    throw new DatawaveFatalQueryException("We must expand but yet cannot expand a regex: " + PrintingVisitor.formattedQueryString(node));
                }
                return QueryPropertyMarker.create(node, DELAYED); // wrap in a delayed predicate to avoid using in RangeStream
            }
        } catch (TableNotFoundException | JavaRegexAnalyzer.JavaRegexParseException e) {
            throw new DatawaveFatalQueryException(e);
        }

        // Given the structure of the tree, we don't *have* to expand this regex node
        if (config.getMaxIndexScanTimeMillis() == Long.MAX_VALUE && (!config.isExpandAllTerms() && !shouldProcessRegexFromStructure(node, markedParents))) {
            // However, given the characteristics of the query terms, we may still want to
            // expand this regex because it would be more efficient to do so
            if (!shouldProcessRegexFromCost(node)) {

                if (log.isTraceEnabled()) {
                    log.trace("Determined we don't need to process regex node:");
                    for (String line : PrintingVisitor.formattedQueryStringList(node)) {
                        log.trace(line);
                    }
                    log.trace("");
                }
                if (markedParents != null) {
                    for (JexlNode markedParent : markedParents) {
                        if (QueryPropertyMarker.findInstance(markedParent).isAnyType())
                            return node;
                    }
                }

                return QueryPropertyMarker.create(node, DELAYED); // wrap in a delayed predicate to avoid using in RangeStream
            }
        } else {
            if (config.getMaxIndexScanTimeMillis() != Long.MAX_VALUE) {
                log.trace("Skipping cost estimation because there is a timeout configured for index expansion");
            }
        }

        try {
            if (!helper.isIndexed(fieldName, config.getDatatypeFilter())) {
                log.debug("Not expanding regex [{}] because the field is not indexed", JexlStringBuildingVisitor.buildQuery(node));
                return node;
            }
        } catch (TableNotFoundException e) {
            throw new DatawaveFatalQueryException(e);
        }

        return buildIndexLookup(node, false, false, () -> createLookup(node));
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        toggleNegation();
        try {
            return super.visit(node, data);
        } finally {
            toggleNegation();
        }
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        toggleNegation();
        try {
            return super.visit(node, data);
        } finally {
            toggleNegation();
        }
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        ASTReference ref = (ASTReference) super.visit(node, data);
        if (ref.jjtGetNumChildren() == 0) {
            return null;
        } else {
            return ref;
        }
    }

    /**
     * Determines if we should expand a regular expression given the current AST.
     *
     * A regex doesn't have to be expanded if we can work out the logic such that we can satisfy the query with term equality only. The simple case is when the
     * ERNode and an EQNode share an AND parent. There are more complicated variants involving grand parents and OR nodes that are considered.
     *
     * @param node
     *            the regex node to be expanded
     * @param markedParents
     *            the marked ancestor nodes
     * @return true - if a regex has to be expanded, false - if a regex doesn't have to be expanded
     */
    public boolean shouldProcessRegexFromStructure(ASTERNode node, Set<JexlNode> markedParents) {
        // if we have already marked this regex as exceeding the threshold, then no
        if (markedParents != null) {
            for (JexlNode markedParent : markedParents) {
                if (QueryPropertyMarker.findInstance(markedParent).isType(EXCEEDED_VALUE)) {
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
            return ascendTree(node, Maps.newIdentityHashMap());
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
     *            the node to consider
     * @param visited
     *            a visit list that contains the computed values for subtrees already visited, in case they are needed
     * @return true - if a regex has to be expanded, false - if a regex doesn't have to be expanded
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
                        return false;
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
     *            the node to consider
     * @param visited
     *            mapping of visited nodes
     * @return true - if a regex has to be expanded, false - if a regex doesn't have to be expanded
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
                if (node.jjtGetNumChildren() == 1 && !QueryPropertyMarker.findInstance(node.jjtGetChild(0)).isAnyType()) {
                    boolean expand = descendIntoSubtree(node.jjtGetChild(0), visited);
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
     * @param node
     *            a jexl node
     * @return `true` if we should expand a regular expression node given this subtree, `false` if we should not expand a regular expression node given this
     *         subtree
     */
    private boolean doesNodeSupportRegexExpansion(JexlNode node) {
        return !(isLiteralEquality(node) && isIndexed(node, config));
    }

    /**
     * The cases for OR and AND in `descendIntoSubtree` were almost equal, save for the initial value for expand and the operator used to join the results of
     * each child. I made this little macro doohickey to allow the differences between the two processes to be abstracted away.
     *
     * @param node
     *            a jexl node
     * @param join
     *            the join
     * @param visited
     *            visited mappings
     * @return boolean
     */
    private boolean computeExpansionForSubtree(JexlNode node, Join join, Map<JexlNode,Boolean> visited) {
        boolean expand = Join.AND.equals(join);
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
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

    /**
     * Given a JexlNode, get all grandchildren which follow a path from ASTReference to ASTIdentifier, returning true if the image of the ASTIdentifier is equal
     * to {@link Constants#ANY_FIELD}
     *
     * @param node
     *            the starting node to check
     * @return whether or not this node has an unfielded identifier
     */
    protected boolean hasUnfieldedIdentifier(JexlNode node) {
        return Constants.ANY_FIELD.equals(JexlASTHelper.getIdentifier(node, false));
    }

    protected void toggleNegation() {
        this.negated = !this.negated;
    }

    /**
     * The default implementation only expands ERnodes
     *
     * @param node
     *            the node to consider
     * @return True if an ER node
     */
    protected boolean shouldExpand(JexlNode node) {
        return (!negated || expandUnfieldedNegations || !hasUnfieldedIdentifier(node)) && (node instanceof ASTERNode);
    }

    protected IndexLookup createLookup(JexlNode node) {
        String fieldName = JexlASTHelper.getIdentifier(node);
        return ShardIndexQueryTableStaticMethods.expandRegexTerms((ASTERNode) node, config, scannerFactory, fieldName,
                        config.getQueryFieldsDatatypes().get(fieldName), helper, executor);
    }

    /**
     * Return true if this is a NR, NE, or NOT node.
     *
     * @param node
     *            the node to consider
     * @return true of a negative node
     */
    private boolean isNegativeNode(JexlNode node) {
        return (node instanceof ASTNENode || node instanceof ASTNRNode || node instanceof ASTNotNode);
    }

    /**
     *
     * @param node
     *            the node to consider
     * @return whether the regex should be processed based on it's cost
     */
    public boolean shouldProcessRegexFromCost(ASTERNode node) {
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
        Cost regexCost = costAnalysis.computeCostForSubtree(node);

        // Compute the cost to determine whether or not to expand this regex
        return shouldProcessRegexByCostWithChildren(subTrees, regexCost);
    }

    /**
     * Determine whether we can actually expand this regex based on whether it is indexed appropriately.
     *
     * @param node
     *            the node to consider
     * @return whether the node is expandable
     * @throws TableNotFoundException
     *             if table is not found
     * @throws datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException
     *             for parse exceptions
     */
    public boolean isExpandable(ASTERNode node) throws TableNotFoundException, JavaRegexAnalyzer.JavaRegexParseException {
        // if full table scan enabled, then we can expand anything
        if (config.getFullTableScanEnabled()) {
            return true;
        }

        String regex = JexlASTHelper.getLiteralValue(node).toString();
        JavaRegexAnalyzer analyzer = new JavaRegexAnalyzer(regex);

        // if the regex is double ended, then we cannot expand it
        if (analyzer.isNgram()) {
            return false;
        }

        String fieldName = JexlASTHelper.getIdentifier(node);

        if (analyzer.isLeadingLiteral() && helper.isIndexed(fieldName, config.getDatatypeFilter())) {
            return true;
        } else {
            return analyzer.isTrailingLiteral() && helper.isReverseIndexed(fieldName, config.getDatatypeFilter());
        }
    }

    /**
     * Determine whether we can actually expand this regex based on whether it is indexed appropriately.
     *
     * @param node
     *            the node to consider
     * @return whether the node must be expanded
     * @throws TableNotFoundException
     *             if table is not found
     */
    public boolean mustExpand(ASTERNode node) throws TableNotFoundException {
        String fieldName = JexlASTHelper.getIdentifier(node);

        // If the identifier is an index-only field, then we must expand it.
        // This use to check for nonEvent fields, but we decided that tokenization
        // can be handled now at evaluation time, so we only need to force index-only
        // fields to expand.
        return helper.getIndexOnlyFields(config.getDatatypeFilter()).contains(fieldName);
    }

    public void collapseAndSubtrees(ASTAndNode node, List<JexlNode> subTrees) {
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
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
                log.debug("cost for term [{}] is {}", JexlStringBuildingVisitor.buildQuery(child), childCost);
            }

            // Use this child's cost if we have no current cost, or it's less than the current cost
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

    private void onlyRetainFieldNamesInTheModelForwardMapping(IndexLookupMap fieldsToValues) {
        if (null != onlyUseThese) {
            fieldsToValues.retainFields(onlyUseThese);
        }
    }

    private void removeCompositeFields(IndexLookupMap fieldsToValues) {
        try {
            if (null != helper.getCompositeToFieldMap()) {
                fieldsToValues.removeFields(helper.getCompositeToFieldMap().keySet());
            }
        } catch (TableNotFoundException e) {
            log.error("Failed to load composite field map using MetadataHelper.", e);
        }
    }

    @Override
    protected JexlNode buildIndexLookup(JexlNode node, boolean ignoreComposites, boolean keepOriginalNode, Supplier<IndexLookup> indexLookupSupplier) {
        JexlNode newNode;
        if (shouldExpand(node)) {
            newNode = super.buildIndexLookup(node, ignoreComposites, keepOriginalNode, indexLookupSupplier);
        } else {
            newNode = RebuildingVisitor.copy(node);
        }
        return newNode;
    }

    @Override
    protected void rebuildFutureJexlNode(FutureJexlNode futureJexlNode) {
        JexlNode currentNode = futureJexlNode.getOrigNode();
        IndexLookupMap fieldsToTerms = futureJexlNode.getLookup().lookup();

        if (log.isDebugEnabled()) {
            logResult(currentNode, fieldsToTerms);
        }

        if (futureJexlNode.isIgnoreComposites()) {
            // composites should be removed prior to building the index expansion iterators
            // and should never be returned to the webservice
            removeCompositeFields(fieldsToTerms);
        }

        JexlNode newNode;

        // If we have no children, it's impossible to find any records, so this query returns no results
        if (fieldsToTerms.isEmpty()) {
            // simply replace the _ANYFIELD_ with _NOFIELD_ denoting that there was no expansion. This will naturally evaluate correctly when applying
            // the query against the document
            for (ASTIdentifier id : JexlASTHelper.getIdentifiers(currentNode)) {
                if (!futureJexlNode.isKeepOriginalNode() && Constants.ANY_FIELD.equals(id.getName())) {
                    JexlNodes.setIdentifier(id, Constants.NO_FIELD);
                }
            }
            newNode = currentNode;
        } else {
            onlyRetainFieldNamesInTheModelForwardMapping(fieldsToTerms);
            if (isNegativeNode(currentNode)) {
                // for a negative node, we want negative equalities in an AND
                newNode = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.AND_NODE, true, currentNode, fieldsToTerms,
                                expandFields, expandValues, futureJexlNode.isKeepOriginalNode());
            } else {
                // for a positive node, we want equalities in a OR
                newNode = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, false, currentNode, fieldsToTerms,
                                expandFields, expandValues, futureJexlNode.isKeepOriginalNode());
            }
        }

        futureJexlNode.setRebuiltNode(newNode);
    }
}
