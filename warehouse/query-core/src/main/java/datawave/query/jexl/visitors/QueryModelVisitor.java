package datawave.query.jexl.visitors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.JexlNodeFactory.ContainerType;
import datawave.query.model.QueryModel;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.Node;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.jexl2.parser.JexlNodes.id;

/**
 * Apply the forward mapping
 */
public class QueryModelVisitor extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(QueryModelVisitor.class);
    
    private final QueryModel queryModel;
    private final HashSet<ASTAndNode> expandedNodes;
    private final Set<String> validFields;
    private final IdentifierExpansionVisitor identifierExpansionVisitor;
    
    public QueryModelVisitor(QueryModel queryModel, Set<String> validFields) {
        this.queryModel = queryModel;
        this.expandedNodes = Sets.newHashSet();
        this.validFields = validFields;
        this.identifierExpansionVisitor = new IdentifierExpansionVisitor(queryModel, validFields);
    }
    
    
    
    public static ASTJexlScript applyModel(ASTJexlScript script, QueryModel queryModel, Set<String> validFields) {
        QueryModelVisitor visitor = new QueryModelVisitor(queryModel, validFields);
        
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
        return node.jjtAccept(this.identifierExpansionVisitor, data);
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (hasMethod(node)) {
            return node.jjtAccept(this.identifierExpansionVisitor, null);
        } else {
            return super.visit(node, data);
        }
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return node.jjtAccept(this.identifierExpansionVisitor, data);
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        return node.jjtAccept(this.identifierExpansionVisitor, data);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (this.expandedNodes.contains(node)) {
            return node;
        }
        
        ASTAndNode smashed = TreeFlatteningRebuildingVisitor.flatten(node);
        Multimap<String,JexlNode> lowerBounds = ArrayListMultimap.create(), upperBounds = ArrayListMultimap.create();
        List<JexlNode> others = Lists.newArrayList();
        for (JexlNode child : JexlNodes.children(node)) {
            if (log.isTraceEnabled()) {
                log.trace("visiting:" + JexlStringBuildingVisitor.buildQuery(child));
            }
            // if this child has a method attached, be sure to descend into it for model substitutions
            if (JexlASTHelper.HasMethodVisitor.hasMethod(child)) {
                child = (JexlNode) child.jjtAccept(this.identifierExpansionVisitor, null);
            }
            
            switch (id(child)) {
                case ParserTreeConstants.JJTGENODE:
                case ParserTreeConstants.JJTGTNODE:
                    upperBounds.put(JexlASTHelper.getIdentifier(child), child);
                    break;
                case ParserTreeConstants.JJTLENODE:
                case ParserTreeConstants.JJTLTNODE:
                    lowerBounds.put(JexlASTHelper.getIdentifier(child), child);
                    break;
                default:
                    others.add(child);
            }
        }
        
        if (!lowerBounds.isEmpty() && !upperBounds.isEmpty()) {
            // this is the set of fields that have an upper and a lower bound operand
            // make a copy of the intersection, as I will be modifying lowerBounds and upperBounds below
            Set<String> tightBounds = Sets.newHashSet(Sets.intersection(lowerBounds.keySet(), upperBounds.keySet()));
            if (log.isDebugEnabled()) {
                log.debug("Found bounds to match: " + tightBounds);
            }
            for (String field : tightBounds) {
                // String field = JexlASTHelper.getIdentifier(theNode);
                List<ASTAndNode> aliasedBounds = Lists.newArrayList();
                
                Collection<String> aliases = getValidAliases(field);
                if (aliases.isEmpty()) {
                    aliases.add(field);
                }
                
                for (String alias : aliases) {
                    if (alias != null) {
                        Collection<JexlNode> lowers = lowerBounds.get(field);
                        Collection<JexlNode> uppers = upperBounds.get(field);
                        Iterator<JexlNode> lowIterator = lowers.iterator();
                        Iterator<JexlNode> upIterator = uppers.iterator();
                        while (lowIterator.hasNext() && upIterator.hasNext()) {
                            JexlNode low = lowIterator.next();
                            JexlNode up = upIterator.next();
                            aliasedBounds.add(JexlNodes.children(new ASTAndNode(ParserTreeConstants.JJTANDNODE),
                                            JexlASTHelper.setField(RebuildingVisitor.copy(low), alias),
                                            JexlASTHelper.setField(RebuildingVisitor.copy(up), alias)));
                        }
                    }
                }
                // we don't need the original, unexpanded nodes any more
                if (!aliasedBounds.isEmpty()) {
                    lowerBounds.removeAll(field);
                    upperBounds.removeAll(field);
                    this.expandedNodes.addAll(aliasedBounds);
                }
                JexlNode nodeToAdd;
                if (aliasedBounds.isEmpty()) {
                    continue;
                } else if (1 == aliasedBounds.size()) {
                    // We know we only have one bound to process, avoid the extra parens
                    nodeToAdd = JexlASTHelper.wrapInParens(aliasedBounds).get(0);
                } else {
                    ASTOrNode unionOfAliases = new ASTOrNode(ParserTreeConstants.JJTORNODE);
                    List<ASTReferenceExpression> var = JexlASTHelper.wrapInParens(aliasedBounds);
                    JexlNodes.children(unionOfAliases, var.toArray(new JexlNode[0]));
                    nodeToAdd = JexlNodes.wrap(unionOfAliases);
                }
                
                others.add(nodeToAdd);
            }
        }
        
        // we could have some unmatched bounds left over
        others.addAll(lowerBounds.values());
        others.addAll(upperBounds.values());
        
        
        // The rebuilding visitor adds whatever {visit()} returns to the parent's child list, so we shouldn't have some weird object graph that means old nodes
        // never get GC'd because {super.visit()} will reset the parent in the call to {copy()}
        return super.visit(JexlNodes.children(smashed, others.toArray(new JexlNode[0])), data);
    }
    
    /**
     * Applies the forward mapping from the QueryModel to a node, expanding the node into an Or if needed.
     *
     * @param node the node
     * @param data the node's data
     * @return the expanded node
     */
    private JexlNode expandBinaryNodeFromModel(JexlNode node, Object data) {
        // Count the immediate children:
        int childCount = node.jjtGetNumChildren();
        if (childCount != 2) {
            QueryException qe = new QueryException(DatawaveErrorCode.BINARY_NODE_TOO_MANY_CHILDREN,
                            MessageFormat.format("Node: {0}", PrintingVisitor.formattedQueryString(node)));
            throw new DatawaveFatalQueryException(qe);
        }
        
        // There is nothing to expand if there are no identifiers.
        if (hasNoIdentifiers(node)) {
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
    
        // If either side has a method, return a new binary node with expanded identifiers.
        if (hasMethod(leftNode) || hasMethod(rightNode)) {
            return expandMethods(node, leftNode, rightNode);
        } else {
            return expandIdentifiersWithinNode(node);
        }
    }
    
    // Expand identifiers in the left and right nodes that have a method connected to them.
    private JexlNode expandMethods(JexlNode parent, JexlNode left, JexlNode right) {
        if (hasMethod(left)) {
            left = (JexlNode) left.jjtAccept(this.identifierExpansionVisitor, null);
        }
        if (hasMethod(right)) {
            right = (JexlNode) right.jjtAccept(this.identifierExpansionVisitor, null);
        }
    
        // Expand identifiers inside of methods/functions in the left and right nodes.
        left = (JexlNode) left.jjtAccept(this, null);
        right = (JexlNode) right.jjtAccept(this, null);
        if (log.isTraceEnabled()) {
            log.trace("after expansion, leftNode:" + PrintingVisitor.formattedQueryString(left));
            log.trace("after expansion, leftNodeQuery:" + JexlStringBuildingVisitor.buildQuery(left));
            log.trace("after expansion, rightNode:" + PrintingVisitor.formattedQueryString(right));
            log.trace("after expansion, rightNodeQuery:" + JexlStringBuildingVisitor.buildQuery(right));
        }
    
        JexlNode node = JexlNodeFactory.buildUntypedBinaryNode(parent, left, right);
        if (log.isTraceEnabled()) {
            log.trace("done early. returning:" + JexlStringBuildingVisitor.buildQuery(node));
        }
        return node;
    }
    
    // Expand any remaining identifiers within the node's expression.
    private JexlNode expandIdentifiersWithinNode(JexlNode node) {
        // Determine if the current node is part of a null equality. It has been established at this point that there are only two children.
        boolean isNullEquality = false;
        if (node instanceof ASTEQNode && (node.jjtGetChild(0) instanceof ASTNullLiteral || node.jjtGetChild(1) instanceof ASTNullLiteral)) {
            isNullEquality = true;
        }
    
        // The query has been previously groomed so that identifiers are on the left and literals are on the right. There should be no identifiers with a method
        // attached at this point. Instead, the normal case should be `IDENTIFIER op 'literal'`
        Object leftSeed;
        Object rightSeed;
        JexlASTHelper.IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (op != null) {
            // If there is an identifier and literal, establish the identifier on the left, and the literal on the right.
            leftSeed = op.getIdentifier();
            rightSeed = op.getLiteral();
        } else {
            // Otherwise, there may be a reference on both sides of the expression.
            leftSeed = node.jjtGetChild(0);
            rightSeed = node.jjtGetChild(1);
        }
    
        // Expand the identifiers on both sides.
        Set<Object> left = expandIdentifiersWithinObject(leftSeed);
        Set<Object> right = expandIdentifiersWithinObject(rightSeed);
    
        boolean requiresAnd = isNullEquality || node instanceof ASTNENode;
    
        // Retrieve the cartesian product and make shallow copies of any jexl nodes. We've created new nodes that will be embedded within an ast reference. As a
        // result, we need to ensure that if we create a logical structure ( such as an or ) -- each literal references a unique identifier from the right.
        // Otherwise, subsequent visitors will reference incorrect sub trees, and potentially negate the activity of the query model visitor.
        Set<List<Object>> product = Sets.cartesianProduct(left, right);
        Set<List<Object>> newSet = product.stream().map(this::copyJexlNodes).collect(Collectors.toSet());
    
        if (product.size() > 1) {
            JexlNode expanded;
            if (requiresAnd) {
                expanded = JexlNodeFactory.createNodeTreeFromPairs(ContainerType.AND_NODE, node, newSet);
            } else {
                expanded = JexlNodeFactory.createNodeTreeFromPairs(ContainerType.OR_NODE, node, newSet);
            }
            if (log.isTraceEnabled()) {
                log.trace("expanded:" + PrintingVisitor.formattedQueryString(expanded));
            }
            return expanded;
        } else if (1 == product.size()) {
            List<Object> pair = product.iterator().next();
            JexlNode expanded = JexlNodeFactory.buildUntypedBinaryNode(node, pair.get(0), pair.get(1));
            if (log.isTraceEnabled()) {
                log.trace("expanded:" + PrintingVisitor.formattedQueryString(expanded));
            }
            return expanded;
        }
    
        // If we couldn't map anything, return a copy.
        if (log.isTraceEnabled()) {
            log.trace("just returning the original:" + PrintingVisitor.formattedQueryString(node));
        }
        return node;
    }
    
    // Return whether or not the provided node has a method within its structure.
    private boolean hasMethod(JexlNode node) {
        return JexlASTHelper.HasMethodVisitor.hasMethod(node);
    }
    
    // Return all valid aliases for the provided identifier.
    private Collection<String> getValidAliases(ASTIdentifier identifier) {
        return getValidAliases(JexlASTHelper.deconstructIdentifier(identifier));
    }
    
    // Return all valid aliases for the provided field.
    private Collection<String> getValidAliases(String field) {
        List<String> aliases = new ArrayList<>(this.queryModel.getMappingsForAlias(field));
        aliases.retainAll(validFields);
        return aliases;
    }
    
    // Return a transformed list that contain either the original object or copies of any JexlNode elements.
    private List<Object> copyJexlNodes(List<Object> objects) {
        return objects.stream().map(o -> {
            if (o instanceof JexlNode) {
                return copy((JexlNode) o);
            } else {
                return o;
            }
        }).collect(Collectors.toList());
    }
    
    // Return whether or not the node has no identifiers.
    private boolean hasNoIdentifiers(JexlNode node) {
        return JexlASTHelper.getIdentifiers(node).isEmpty();
    }
    
    // Return a set of expanded nodes for the given object, depending on the type of JEXL node it is (if one).
    private Set<Object> expandIdentifiersWithinObject(Object object) {
        Set<Object> set = new HashSet<>();
        if (object instanceof ASTReference) {
            List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers((ASTReference) object);
            // @formatter::off
            identifiers.stream()
                            .map(this::getValidAliases)
                            .flatMap(Collection::stream)
                            .map(JexlNodeFactory::buildIdentifier)
                            .forEach(set::add);
            // @formatter::on
        } else if (object instanceof ASTIdentifier) {
            // @formatter::off
            getValidAliases((ASTIdentifier) object).stream()
                            .map(JexlNodeFactory::buildIdentifier)
                            .forEach(set::add);
            // @formatter::on
        } else {
            set.add(object);
        }
        return set;
    }
    
    /**
     * The {@link IdentifierExpansionVisitor} will expand identifiers into their aliases, if any exists. If multiple aliases exist for an identifier, it will be
     * replace with a disjunction of the aliases, e.g. 'FOO' becomes '( ALIASONE || ALIASTWO )'. It is used within function and method node arguments, as well
     * as in the reference that a method is called on.
     */
    private static class IdentifierExpansionVisitor extends RebuildingVisitor {
        
        private final QueryModel queryModel;
        private final Set<String> validFields;
        
        public IdentifierExpansionVisitor(QueryModel queryModel, Set<String> validFields) {
            this.queryModel = queryModel;
            this.validFields = validFields;
        }
        
        @Override
        public Object visit(ASTIdentifier node, Object data) {
            String fieldName = JexlASTHelper.getIdentifier(node);
            
            // @formatter::off
            Set<ASTIdentifier> aliases = getValidAliases(fieldName).stream()
                            .distinct()
                            .map(this::createIdentifier)
                            .collect(Collectors.toCollection(LinkedHashSet::new));
            // @formatter:on
            
            if (aliases.isEmpty()) {
                return super.visit(node, data);
            }
            
            return wrapAliases(aliases, node, data);
        }
        
        // Return all valid aliases for the provided field.
        private Collection<String> getValidAliases(String field) {
            List<String> aliases = new ArrayList<>(this.queryModel.getMappingsForAlias(field));
            aliases.retainAll(validFields);
            return aliases;
        }
        
        // Return a JEXL Identifier for the given field name.
        private ASTIdentifier createIdentifier(String fieldName) {
            ASTIdentifier identifier = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
            identifier.image = JexlASTHelper.rebuildIdentifier(fieldName);
            return identifier;
        }
    
        // Create a replacement node wrapping the provided aliases.
        private JexlNode wrapAliases(Set<ASTIdentifier> aliases, JexlNode original, Object data) {
            JexlNode node;
            if (aliases.size() == 1) {
                node = JexlNodeFactory.wrap(aliases.iterator().next());
            } else {
                node = JexlNodeFactory.createOrNode(aliases);
            }
            node.jjtSetParent(original.jjtGetParent());
            for (int i = 0; i < original.jjtGetNumChildren(); i++) {
                node.jjtAddChild((Node) original.jjtGetChild(i).jjtAccept(this, data), i);
            }
            return node;
        }
        
    }
}
