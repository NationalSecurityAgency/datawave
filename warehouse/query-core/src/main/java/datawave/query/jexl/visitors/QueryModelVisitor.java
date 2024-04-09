package datawave.query.jexl.visitors;

import static datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;
import static datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType.LENIENT;
import static datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType.STRICT;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.Node;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.jexl.JexlNodeFactory;
import datawave.core.query.jexl.LiteralRange;
import datawave.core.query.jexl.nodes.QueryPropertyMarker;
import datawave.core.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.core.query.jexl.visitors.PrintingVisitor;
import datawave.core.query.jexl.visitors.RebuildingVisitor;
import datawave.core.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.Constants;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.model.QueryModel;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * Apply the forward mapping
 */
public class QueryModelVisitor extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(QueryModelVisitor.class);

    private final QueryModel queryModel;
    private final HashSet<ASTAndNode> expandedNodes;
    private final Set<String> validFields;
    private final SimpleQueryModelVisitor simpleQueryModelVisitor;
    // lenient fields set by the user
    private final Set<String> lenientFields;
    // strict fields set by the user
    private final Set<String> strictFields;

    // set of fields that are excluded from model expansion
    private final Set<String> noExpansionFields;

    public QueryModelVisitor(QueryModel queryModel, Set<String> validFields) {
        this.queryModel = queryModel;
        this.expandedNodes = Sets.newHashSet();
        this.validFields = new HashSet<>(validFields);
        this.simpleQueryModelVisitor = new SimpleQueryModelVisitor(queryModel, validFields);
        this.noExpansionFields = new HashSet<>();
        this.lenientFields = new HashSet<>();
        this.strictFields = new HashSet<>();
    }

    /**
     * Get the aliases for the field, and retain only those in the "validFields" set.
     *
     * @param field
     *            string field
     * @return the list of field aliases
     */
    protected Collection<String> getAliasesForField(String field) {
        List<String> aliases = new ArrayList<>(this.queryModel.getMappingsForAlias(field));
        aliases.retainAll(validFields);
        return aliases;
    }

    public static ASTJexlScript applyModel(ASTJexlScript script, QueryModel queryModel, Set<String> validFields) {
        return applyModel(script, queryModel, validFields, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    }

    public static ASTJexlScript applyModel(ASTJexlScript script, QueryModel queryModel, Set<String> validFields, Set<String> noExpansionFields,
                    Set<String> lenientFields, Set<String> strictFields) {
        QueryModelVisitor visitor = new QueryModelVisitor(queryModel, validFields);

        if (!noExpansionFields.isEmpty()) {
            visitor.setNoExpansionFields(noExpansionFields);
        }

        if (!lenientFields.isEmpty()) {
            visitor.setLenientFields(lenientFields);
        }

        if (!strictFields.isEmpty()) {
            visitor.setStrictFields(strictFields);
        }

        script = TreeFlatteningRebuildingVisitor.flatten(script);
        return (ASTJexlScript) script.jjtAccept(visitor, null);
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return expandBinaryNodeFromModel(node, data);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return expandBinaryNodeFromModel(node, data);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return expandBinaryNodeFromModel(node, data);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return expandBinaryNodeFromModel(node, data);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return expandBinaryNodeFromModel(node, data);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return expandBinaryNodeFromModel(node, data);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return expandBinaryNodeFromModel(node, data);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return expandBinaryNodeFromModel(node, data);
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return node.jjtAccept(this.simpleQueryModelVisitor, data);
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        if (JexlASTHelper.HasMethodVisitor.hasMethod(node)) {
            // this reference has a child that is a method
            return node.jjtAccept(this.simpleQueryModelVisitor, null);
        } else {
            return super.visit(node, data);
        }
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return node.jjtAccept(this.simpleQueryModelVisitor, data);
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (this.expandedNodes.contains(node)) {
            return node;
        }

        LiteralRange range = JexlASTHelper.findRange().getRange(node);
        if (range != null) {
            return expandRangeNodeFromModel(range, node, data);
        } else {
            return super.visit(node, data);
        }
    }

    public Object expandRangeNodeFromModel(LiteralRange range, ASTAndNode node, Object data) {

        if (isFieldExcluded(range.getFieldName(), noExpansionFields)) {
            return node;
        }

        // this is the set of fields that have an upper and a lower bound operand
        // make a copy of the intersection, as I will be modifying lowererBounds and upperBounds below
        List<JexlNode> aliasedBounds = Lists.newArrayList();

        Collection<String> aliases = getAliasesForField(range.getFieldName());
        if (aliases.isEmpty()) {
            aliases = Lists.newArrayList(range.getFieldName());
        }

        for (String alias : aliases) {
            if (alias != null) {
                JexlNode rangeNode = QueryPropertyMarker.create(JexlNodes.setChildren(new ASTAndNode(ParserTreeConstants.JJTANDNODE),
                                JexlASTHelper.setField(RebuildingVisitor.copy(range.getLowerNode()), alias),
                                JexlASTHelper.setField(RebuildingVisitor.copy(range.getUpperNode()), alias)), BOUNDED_RANGE);
                aliasedBounds.add(rangeNode);
                this.expandedNodes.add((ASTAndNode) JexlASTHelper.dereference(rangeNode));
            }
        }

        JexlNode nodeToAdd;
        if (1 == aliasedBounds.size()) {
            nodeToAdd = JexlASTHelper.dereference(aliasedBounds.get(0));
        } else {
            ASTOrNode unionOfAliases = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            nodeToAdd = JexlNodes.setChildren(unionOfAliases, aliasedBounds.toArray(new JexlNode[aliasedBounds.size()]));
        }

        return nodeToAdd;
    }

    /**
     * Applies the forward mapping from the QueryModel to a node, expanding the node into an Or if needed.
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return a jexlnode
     */
    protected JexlNode expandBinaryNodeFromModel(JexlNode node, Object data) {
        // Count the immediate children:
        int childCount = node.jjtGetNumChildren();

        if (childCount != 2) {
            QueryException qe = new QueryException(DatawaveErrorCode.BINARY_NODE_TOO_MANY_CHILDREN,
                            MessageFormat.format("Node: {0}", PrintingVisitor.formattedQueryString(node)));
            throw new DatawaveFatalQueryException(qe);
        }

        // Find identifiers
        List<ASTIdentifier> allidentifiers = JexlASTHelper.getIdentifiers(node);

        // If we don't have any identifiers, we have nothing to expand
        if (allidentifiers.isEmpty()) {
            return node;
        }

        JexlNode leftNode = node.jjtGetChild(0);
        JexlNode rightNode = node.jjtGetChild(1);
        if (log.isTraceEnabled()) {
            log.trace("leftNode:" + PrintingVisitor.formattedQueryString(leftNode));
            log.trace("leftNodeQuery:" + JexlStringBuildingVisitor.buildQuery(leftNode));
            log.trace("rightNode:" + PrintingVisitor.formattedQueryString(rightNode));
            log.trace("rightNodeQuery:" + JexlStringBuildingVisitor.buildQuery(rightNode));
        }

        // expand any identifiers inside of methods/functions in the left and right nodes
        leftNode = (JexlNode) leftNode.jjtAccept(this, null);
        rightNode = (JexlNode) rightNode.jjtAccept(this, null);
        if (log.isTraceEnabled()) {
            log.trace("after expansion, leftNode:" + PrintingVisitor.formattedQueryString(leftNode));
            log.trace("after expansion, leftNodeQuery:" + JexlStringBuildingVisitor.buildQuery(leftNode));
            log.trace("after expansion, rightNode:" + PrintingVisitor.formattedQueryString(rightNode));
            log.trace("after expansion, rightNodeQuery:" + JexlStringBuildingVisitor.buildQuery(rightNode));
        }

        boolean isNullEquality = false;
        if (node instanceof ASTEQNode && (leftNode instanceof ASTNullLiteral || rightNode instanceof ASTNullLiteral)) {
            isNullEquality = true;
        }

        SetMultimap<String,JexlNode> left = null;
        SetMultimap<String,JexlNode> right = null;

        JexlNode leftSeed, rightSeed;
        // the query has been previously groomed so that identifiers are on the left and literals are on the right
        // an identifier with a method attached will have already been substituted above (and will return null for the IdentifierOpLiteral)
        // The normal case of `IDENTIFIER op 'literal'`
        JexlASTHelper.IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (op != null) {
            // One identifier
            leftSeed = op.getIdentifier();
            rightSeed = op.getLiteral();
            if (rightSeed instanceof ASTNullLiteral && node instanceof ASTEQNode) {
                isNullEquality = true;
            }
        } else {
            // We know from above that childCount == 2. We may have a reference on both sides of the expression
            leftSeed = leftNode;
            rightSeed = rightNode;
        }

        // expand the left and right seeds
        left = expandNodeFromModel(leftSeed);
        right = expandNodeFromModel(rightSeed);

        boolean requiresAnd = isNullEquality || node instanceof ASTNENode || node instanceof ASTNRNode;

        @SuppressWarnings("unchecked")
        // retrieve the cartesian product of the model field names
        Set<List<String>> fieldPairs = Sets.cartesianProduct(left.keySet(), right.keySet());
        Set<List<JexlNode>> product = new HashSet<>();

        if (!fieldPairs.isEmpty()) {
            List<JexlNode> expressions = new ArrayList<>();
            for (List<String> fieldPair : fieldPairs) {
                boolean strict = isStrict(fieldPair.get(0)) || isStrict(fieldPair.get(1));
                boolean lenient = isLenient(fieldPair.get(0)) || isLenient(fieldPair.get(1));

                // if somehow this expression is marked as lenient and strict, then default to old behaviour
                if (strict && lenient) {
                    log.warn("Field pair " + fieldPair + " is marked as both strict and lenient.  Applying neither.");
                    strict = false;
                    lenient = false;
                }

                Set<JexlNode> leftSet = left.get(fieldPair.get(0));
                Set<JexlNode> rightSet = right.get(fieldPair.get(1));
                product.addAll(Sets.cartesianProduct(leftSet, rightSet));

                /**
                 * use the product transformer to shallow copy the jexl nodes. We've created new nodes that will be embedded within an ast reference. As a
                 * result, we need to ensure that if we create a logical structure ( such as an or ) -- each literal references a unique identifier from the
                 * right. Otherwise, subsequent visitors will reference incorrect sub-trees, and potentially negate the activity of the query model visitor
                 */
                product = product.stream().map(list -> list.stream().map(RebuildingVisitor::copy).collect(Collectors.toList())).collect(Collectors.toSet());

                JexlNode expanded;
                List<JexlNode> nodes = createNodeTreeFromPairs(node, product);
                if (nodes.size() == 1) {
                    expanded = nodes.get(0);
                } else {
                    if (requiresAnd) {
                        expanded = JexlNodeFactory.createAndNode(nodes);
                    } else {
                        expanded = JexlNodeFactory.createOrNode(nodes);
                    }
                    if (strict) {
                        expanded = QueryPropertyMarker.create(expanded, STRICT);
                    } else if (lenient) {
                        expanded = QueryPropertyMarker.create(expanded, LENIENT);
                    }
                }
                if (log.isTraceEnabled())
                    log.trace("expanded:" + PrintingVisitor.formattedQueryString(expanded));
                expressions.add(expanded);
            }

            if (expressions.size() == 1) {
                return expressions.get(0);
            } else if (requiresAnd) {
                return JexlNodeFactory.createAndNode(expressions);
            } else {
                return JexlNodeFactory.createOrNode(expressions);
            }
        }

        // If we couldn't map anything, return a copy
        if (log.isTraceEnabled())
            log.trace("Not sure how we got here, but just returning the original:" + PrintingVisitor.formattedQueryString(node));

        return node;
    }

    /**
     * Expands a binary node into multiple pairs.
     *
     * @param node
     *            The node exemplar
     * @param pairs
     *            The node pairs
     * @return A list of jexl nodes created from each pair
     */
    public List<JexlNode> createNodeTreeFromPairs(JexlNode node, Set<List<JexlNode>> pairs) {
        if (1 == pairs.size()) {
            List<JexlNode> pair = pairs.iterator().next();

            if (2 != pair.size()) {
                throw new UnsupportedOperationException("Cannot construct a node from a non-binary pair: " + pair);
            }

            return Collections.singletonList(JexlNodeFactory.buildUntypedBinaryNode(node, pair.get(0), pair.get(1)));
        }

        List<JexlNode> children = new ArrayList<>();
        for (List<JexlNode> pair : pairs) {
            if (2 != pair.size()) {
                throw new UnsupportedOperationException("Cannot construct a node from a non-binary pair: " + pair);
            }

            JexlNode leftNode = pair.get(0);
            JexlNode rightNode = pair.get(1);
            JexlNode child = JexlNodeFactory.buildUntypedBinaryNode(node, leftNode, rightNode);
            children.add(child);
        }

        return children;
    }

    protected SetMultimap<String,JexlNode> expandNodeFromModel(JexlNode seed) {
        SetMultimap<String,JexlNode> nodes = HashMultimap.create();
        if (seed instanceof ASTReference) {
            // we need to be careful here. We do not want to pull identifiers if this node is
            // anything but identifiers allowing or/and/ref/expression nodes
            // Hence we are not using the JexlASTHelper.getIdentifiers method.
            List<ASTIdentifier> identifiers = getIdentifiers(seed);
            if (identifiers.size() > 1) {
                log.warn("I did not expect to see more than one Identifier here for " + JexlStringBuildingVisitor.buildQuery(seed));
            }
            for (ASTIdentifier identifier : identifiers) {
                String field = JexlASTHelper.deconstructIdentifier(identifier);
                if (isFieldExcluded(JexlASTHelper.getIdentifier(identifier, true), noExpansionFields)) {
                    nodes.put(field, RebuildingVisitor.copy(identifier));
                } else {
                    for (String fieldName : getAliasesForField(field)) {
                        nodes.put(field, JexlNodeFactory.buildIdentifier(fieldName));
                    }
                }
            }
        } else if (seed instanceof ASTIdentifier) {
            String field = JexlASTHelper.deconstructIdentifier((ASTIdentifier) seed);
            if (isFieldExcluded(JexlASTHelper.getIdentifier(seed, true), noExpansionFields)) {
                nodes.put(field, RebuildingVisitor.copy(seed));
            } else {
                for (String fieldName : getAliasesForField(field)) {
                    nodes.put(field, JexlNodeFactory.buildIdentifier(fieldName));
                }
            }
        }
        if (nodes.isEmpty()) {
            // Not an identifier, therefore it's probably a literal
            nodes.put(Constants.NO_FIELD, RebuildingVisitor.copy(seed));
        }
        return nodes;
    }

    public static List<ASTIdentifier> getIdentifiers(JexlNode node) {
        List<ASTIdentifier> identifiers = Lists.newArrayList();
        if (getIdentifiers(node, identifiers)) {
            return identifiers;
        } else {
            return Collections.emptyList();
        }
    }

    private static boolean getIdentifiers(JexlNode node, List<ASTIdentifier> identifiers) {
        if (null != node) {
            if (node instanceof ASTIdentifier) {
                identifiers.add((ASTIdentifier) node);
            } else if (node instanceof ASTReferenceExpression || node instanceof ASTAndNode || node instanceof ASTOrNode) {
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    if (!getIdentifiers(node.jjtGetChild(i), identifiers)) {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * Is this field excluded from query model expansion
     *
     * @param field
     *            the field to be expanded
     * @param noExpansionFields
     *            fields to not expand on
     * @return true if the field is excluded
     */
    public static boolean isFieldExcluded(String field, Set<String> noExpansionFields) {
        return noExpansionFields != null && noExpansionFields.contains(field);
    }

    public void setNoExpansionFields(Set<String> noExpansionFields) {
        this.noExpansionFields.addAll(noExpansionFields);
        if (this.simpleQueryModelVisitor != null) {
            this.simpleQueryModelVisitor.setNoExpansionFields(noExpansionFields);
        }
    }

    public void setLenientFields(Set<String> lenientFields) {
        this.lenientFields.addAll(lenientFields);
    }

    public void setStrictFields(Set<String> strictFields) {
        this.strictFields.addAll(strictFields);
    }

    /**
     * Determine if field marked as lenient
     *
     * @param field
     *            The field to test
     * @return true if explicitly marked as lenient
     */
    public boolean isLenient(String field) {
        // user specifications will override the model settings
        boolean userLenient = lenientFields.contains(field);
        boolean userStrict = strictFields.contains(field);
        if (userLenient || userStrict) {
            if (userLenient && userStrict) {
                throw new IllegalArgumentException("Cannot specify the same field as being strict and lenient");
            }
        }

        // determine what the model is treating this as
        boolean modelLenient = queryModel.getModelFieldAttributes(field).contains(QueryModel.LENIENT);
        boolean modelStrict = queryModel.getModelFieldAttributes(field).contains(QueryModel.STRICT);
        if (modelLenient || modelStrict) {
            if (modelLenient && modelStrict) {
                throw new IllegalStateException("The model cannot have both a strict and lenient marker for the same field");
            }
        }

        // lenient if explicitly marked as lenient
        return userLenient || (modelLenient && !userStrict);
    }

    /**
     * Determine if field marked as strict
     *
     * @param field
     *            The field to test
     * @return true if explicitly marked as strict
     */
    public boolean isStrict(String field) {
        // user specifications will override the model settings
        boolean userLenient = lenientFields.contains(field);
        boolean userStrict = strictFields.contains(field);
        if (userLenient || userStrict) {
            if (userLenient && userStrict) {
                throw new IllegalArgumentException("Cannot specify the same field as being strict and lenient");
            }
        }

        // determine what the model is treating this as
        boolean modelLenient = queryModel.getModelFieldAttributes(field).contains(QueryModel.LENIENT);
        boolean modelStrict = queryModel.getModelFieldAttributes(field).contains(QueryModel.STRICT);
        if (modelLenient || modelStrict) {
            if (modelLenient && modelStrict) {
                throw new IllegalStateException("The model cannot have both a strict and lenient marker for the same field");
            }
        }

        // strict if explicitly marked as strict
        return userStrict || (modelStrict && !userLenient);
    }

    /**
     * The SimpleQueryModelVisitor will only change identifiers into a disjunction of their aliases: FOO becomes (ALIASONE||ALIASTWO) It is used within function
     * and method node arguments and in the reference that a method is called on
     */
    protected static class SimpleQueryModelVisitor extends RebuildingVisitor {

        private final QueryModel queryModel;
        private final Set<String> validFields;
        private Set<String> noExpansionFields;

        public SimpleQueryModelVisitor(QueryModel queryModel, Set<String> validFields) {
            this.queryModel = queryModel;
            this.validFields = validFields;
        }

        public void setNoExpansionFields(Set<String> noExpansionFields) {
            this.noExpansionFields = noExpansionFields;
        }

        @Override
        public Object visit(ASTIdentifier node, Object data) {
            JexlNode newNode;
            String fieldName = JexlASTHelper.getIdentifier(node);

            if (isFieldExcluded(fieldName, noExpansionFields)) {
                return node;
            }

            Collection<String> aliases = Sets.newLinkedHashSet(getAliasesForField(fieldName)); // de-dupe

            Set<ASTIdentifier> nodes = Sets.newLinkedHashSet();

            if (aliases.isEmpty()) {
                return super.visit(node, data);
            }
            for (String alias : aliases) {
                ASTIdentifier newKid = JexlNodes.makeIdentifier(JexlASTHelper.rebuildIdentifier(alias));
                nodes.add(newKid);
            }
            if (nodes.size() == 1) {
                newNode = JexlNodeFactory.wrap(nodes.iterator().next());
            } else {
                newNode = JexlNodeFactory.createOrNode(nodes);
            }
            newNode.jjtSetParent(node.jjtGetParent());

            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                newNode.jjtAddChild((Node) node.jjtGetChild(i).jjtAccept(this, data), i);
            }
            return newNode;
        }

        /**
         * Get the aliases for the field, and retain only those in the "validFields" set.
         *
         * @param field
         *            the field string
         * @return the list of field aliases
         */
        protected Collection<String> getAliasesForField(String field) {
            List<String> aliases = new ArrayList<>(this.queryModel.getMappingsForAlias(field));
            aliases.retainAll(validFields);
            return aliases;
        }
    }
}
