package nsa.datawave.query.rewrite.jexl.visitors;

import static org.apache.commons.jexl2.parser.JexlNodes.id;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Multimap;
import nsa.datawave.query.model.QueryModel;
import nsa.datawave.query.rewrite.exceptions.DatawaveFatalQueryException;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper.IdentifierOpLiteral;
import nsa.datawave.query.rewrite.jexl.JexlNodeFactory;
import nsa.datawave.query.rewrite.jexl.JexlNodeFactory.ContainerType;
import nsa.datawave.query.rewrite.jexl.functions.RefactoredJexlFunctionArgumentDescriptorFactory;
import nsa.datawave.query.rewrite.jexl.functions.arguments.RefactoredJexlArgumentDescriptor;
import nsa.datawave.webservice.common.logging.ThreadConfigurableLogger;

import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.exception.QueryException;
import org.apache.commons.jexl2.parser.*;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Apply the forward mapping
 */
public class QueryModelVisitor extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(QueryModelVisitor.class);
    
    private QueryModel queryModel;
    private HashSet<ASTAndNode> expandedNodes;
    private Set<String> validFields;
    
    public QueryModelVisitor(QueryModel queryModel, Set<String> validFields) {
        this.queryModel = queryModel;
        this.expandedNodes = Sets.newHashSet();
        this.validFields = validFields;
    }
    
    /**
     * Get the aliases for the field, and retain only those in the "validFields" set.
     * 
     * @param field
     * @return the list of field aliases
     */
    protected Collection<String> getAliasesForField(String field) {
        List<String> aliases = new ArrayList<>(this.queryModel.getMappingsForAlias(field));
        aliases.retainAll(validFields);
        return aliases;
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
        // Get the field names for this FunctionNode
        List<ASTIdentifier> identifiers = JexlASTHelper.getFunctionIdentifiers(node);
        
        // Map the field names to the aliases
        LinkedListMultimap<String,String> mappedIdentifiers = expandIdentifiers(identifiers);
        
        // Compute all combinations of aliased field names
        // List<Map<String,String>> flattenedIdentifiers = flattenMultimap(mappedIdentifiers);
        
        // Rebuild a new node for all supplied combinations of field names
        return expandFunctions(node, mappedIdentifiers);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (this.expandedNodes.contains(node)) {
            return node;
        }
        
        ASTAndNode smashed = TreeFlatteningRebuildingVisitor.flatten(node);
        HashMap<String,JexlNode> lowerBounds = Maps.newHashMap(), upperBounds = Maps.newHashMap();
        List<JexlNode> others = Lists.newArrayList();
        for (JexlNode child : JexlNodes.children(node)) {
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
            Set<String> tightBounds = Sets.intersection(lowerBounds.keySet(), upperBounds.keySet());
            if (log.isDebugEnabled())
                log.debug("Found bounds to match: " + tightBounds);
            for (String field : tightBounds) {
                List<ASTAndNode> aliasedBounds = Lists.newArrayList();
                
                Collection<String> aliases = getAliasesForField(field);
                if (aliases.isEmpty()) {
                    aliases = Lists.newArrayList(field);
                }
                
                for (String alias : aliases) {
                    if (alias != null) {
                        aliasedBounds.add(JexlNodes.children(new ASTAndNode(ParserTreeConstants.JJTANDNODE),
                                        JexlASTHelper.setField(RebuildingVisitor.copy(lowerBounds.get(field)), alias),
                                        JexlASTHelper.setField(RebuildingVisitor.copy(upperBounds.get(field)), alias)));
                    }
                }
                // we don't need the original, unexpanded nodes any more
                if (aliasedBounds.isEmpty() == false) {
                    lowerBounds.remove(field);
                    upperBounds.remove(field);
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
                    JexlNodes.children(unionOfAliases, var.toArray(new JexlNode[var.size()]));
                    nodeToAdd = JexlNodes.wrap(unionOfAliases);
                }
                
                others.add(nodeToAdd);
            }
        }
        
        // we could have some unmatched bounds left over
        others.addAll(lowerBounds.values());
        others.addAll(upperBounds.values());
        
        /*
         * The rebuilding visitor adds whatever {visit()} returns to the parent's child list, so we shouldn't have some weird object graph that means old nodes
         * never get GC'd because {super.visit()} will reset the parent in the call to {copy()}
         */
        return super.visit(JexlNodes.children(smashed, others.toArray(new JexlNode[others.size()])), data);
    }
    
    /**
     * Given a list of ASTIdentifiers (field names), construct a mapping for that field name to all field names aliased specified by the query model.
     * 
     * @param identifiers
     * @return
     */
    protected LinkedListMultimap<String,String> expandIdentifiers(List<ASTIdentifier> identifiers) {
        LinkedListMultimap<String,String> mappedIdentifiers = LinkedListMultimap.create();
        
        for (ASTIdentifier identifier : identifiers) {
            String fieldName = identifier.image;
            
            // Get all the aliases for the original field name
            Collection<String> aliases = getAliasesForField(fieldName);
            
            // Make sure to leave the original identifier in the map if we found
            // no alias for the field name in the model
            if (aliases.isEmpty()) {
                mappedIdentifiers.put(fieldName, fieldName);
            } else {
                mappedIdentifiers.putAll(fieldName, aliases);
            }
        }
        
        return mappedIdentifiers;
    }
    
    /**
     * Generate the product of all querymodel mappings for fields.
     * 
     * e.g. For the multimap: { a={1,2}, b={3,4} }
     * 
     * the following will be created: [ {a=1,b=3}, {a=1,b=4}, {a=2,b=3}, {a=2,b=4} ]
     * 
     * @param mappings
     * @return
     */
    protected List<Map<String,String>> flattenMultimap(LinkedListMultimap<String,String> mappings) {
        List<Map<String,String>> flattenedMappings = Lists.newArrayList();
        
        List<String> keys = Lists.newArrayList(mappings.keySet());
        List<Integer> offsets = Lists.newArrayList();
        for (int i = 0; i < keys.size(); i++) {
            offsets.add(0);
        }
        
        flattenMultimap(mappings, flattenedMappings, keys, offsets, 0);
        
        return flattenedMappings;
    }
    
    /**
     * Generate the product of all querymodel mappings for fields.
     * 
     * e.g. For the multimap: { a={1,2}, b={3,4} }
     * 
     * the following will be created: [ {a=1,b=3}, {a=1,b=4}, {a=2,b=3}, {a=2,b=4} ]
     * 
     * @param mappings
     * @param flattenedMappings
     * @param keys
     * @param offsets
     * @param index
     */
    protected void flattenMultimap(LinkedListMultimap<String,String> mappings, List<Map<String,String>> flattenedMappings, List<String> keys,
                    List<Integer> offsets, int index) {
        
        if (index < (offsets.size() - 1)) {
            flattenMultimap(mappings, flattenedMappings, keys, offsets, index + 1);
        } else {
            Map<String,String> flattened = Maps.newHashMap();
            
            // Walk the offset and key list, constructing a map that contains each element
            for (int i = 0; i < offsets.size(); i++) {
                String key = keys.get(i);
                Integer offset = offsets.get(i);
                
                // Get the element for the given key at the current offset
                flattened.put(key, mappings.get(key).get(offset));
            }
            
            flattenedMappings.add(flattened);
            
            // Unwind the offset counts
            while (index >= 0 && (offsets.get(index) + 1) >= mappings.get(keys.get(index)).size()) {
                offsets.set(index, 0);
                index--;
            }
            
            // We have more iterations to make
            if (index >= 0) {
                // Increment the current offset
                offsets.set(index, offsets.get(index) + 1);
                
                // Recurse
                flattenMultimap(mappings, flattenedMappings, keys, offsets, index);
            }
        }
    }
    
    /**
     * Use each alias mapping to create a new ASTFunctionNode. Return a List of these new ASTFunctionNodes
     * 
     * @param original
     * @param mappings
     * @return
     */
    protected JexlNode expandFunctions(ASTFunctionNode original, Multimap<String,String> mappings) {
        // If we have no mappings for the identifiers, we can only return the original node
        if (0 == mappings.size() || noModelChange(mappings)) {
            return FunctionQueryModelRebuildingVisitor.copyNode(original);
        } else {
            JexlNode expanded = FunctionQueryModelRebuildingVisitor.copyNode(original, mappings);
            if (log.isTraceEnabled())
                log.trace("expanded:" + PrintingVisitor.formattedQueryString(expanded));
            return expanded;
        }
    }
    
    /**
     * Try to determine when the query model gives us a mapping which is itself.
     * 
     * @param mappings
     * @return
     */
    protected boolean noModelChange(List<Map<String,String>> mappings) {
        for (Map<String,String> mapping : mappings) {
            for (Entry<String,String> entry : mapping.entrySet()) {
                if (!entry.getKey().equals(entry.getValue())) {
                    return false;
                }
            }
        }
        
        return true;
    }
    
    protected boolean noModelChange(Multimap<String,String> mappings) {
        for (Entry<String,String> entry : mappings.entries()) {
            if (!entry.getKey().equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Applies the forward mapping from the QueryModel to a node, expanding the node into an Or if needed.
     * 
     * @param node
     * @param data
     * @return
     */
    protected JexlNode expandBinaryNodeFromModel(JexlNode node, Object data) {
        
        // Count the immediate children:
        int childCount = node.jjtGetNumChildren();
        
        if (childCount != 2) {
            QueryException qe = new QueryException(DatawaveErrorCode.BINARY_NODE_TOO_MANY_CHILDREN, MessageFormat.format("Node: {0}",
                            PrintingVisitor.formattedQueryString(node)));
            throw new DatawaveFatalQueryException(qe);
        }
        
        // Find identifiers
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        
        // If we don't have any identifiers, we have nothing to expand
        if (identifiers.isEmpty()) {
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
        // expand any identifiers in the left and right nodes (this will take care of expanding identifiers within functions/methods):
        leftNode = (JexlNode) leftNode.jjtAccept(this, null);
        rightNode = (JexlNode) rightNode.jjtAccept(this, null);
        if (log.isTraceEnabled()) {
            log.trace("after expansion, leftNode:" + PrintingVisitor.formattedQueryString(leftNode));
            log.trace("after expansion, leftNodeQuery:" + JexlStringBuildingVisitor.buildQuery(leftNode));
            log.trace("after expansion, rightNode:" + PrintingVisitor.formattedQueryString(rightNode));
            log.trace("after expansion, rightNodeQuery:" + JexlStringBuildingVisitor.buildQuery(rightNode));
        }
        
        List<JexlNode> leftIdentifierSiblings = Lists.newArrayList();
        List<JexlNode> rightIdentifierSiblings = Lists.newArrayList();
        
        /*
         * Unusual Cases: 1. an identifier with a method: AGE.getValuesForGroups(0) makes this query tree: Reference <<< ReferenceNode has 2 children,
         * Identifier and MethodNode Identifier:AGE <<< child0 of referencenode MethodNode <<< child1 of referencenode Identifier:getValuesForGroups <<< child0
         * of methodnode NumberLiteral:0 <<< child1 of methodnode
         * 
         * 
         * 
         * 2. an identifier with a method that has a function argument: AGE.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAME, 'MEADOW', GENDER,
         * 'FEMALE')) makes this query tree: Reference <<< ReferenceNode has 2 children, Identifier and MethodNode Identifier:AGE MethodNode << MethodNode has 2
         * children, Identifier and Reference Identifier:getValuesForGroups << method name (this method accepts a collection of integers as its argument)
         * Reference <<< ReferenceNode has 1 child FunctionNode <<< FunctionNode has 6 children Identifier:grouping << function namespace
         * Identifier:getGroupsForMatchesInGroup << function name (this function returns a collection of integers) Reference << function arg Identifier:NAME
         * Reference << function arg StringLiteral:MEADOW Reference << function arg Identifier:GENDER Reference << function arg StringLiteral:FEMALE
         * 
         * 
         * 3. a function with a method: includeRegex(NAME, 'MICHAEL').size()
         * 
         * makes this query tree: Reference <<< ReferenceNode has 2 children, FunctionNode and MethodNode FunctionNode <<< child0 of the ReferenceNode
         * Identifier:filter Identifier:includeRegex Reference Identifier:NAME Reference StringLiteral:MICHAEL SizeMethod <<< child1 of the ReferenceNode
         */
        
        Object leftSeed = null, rightSeed = null;
        Set<Object> left = Sets.newHashSet(), right = Sets.newHashSet();
        boolean isNullEquality = false;
        
        if (node instanceof ASTEQNode && (leftNode instanceof ASTNullLiteral || rightNode instanceof ASTNullLiteral)) {
            isNullEquality = true;
        }
        
        boolean requiresAnd = isNullEquality || node instanceof ASTNENode;
        
        if (leftNode instanceof ASTReference) {
            // check to see if there is an identifier at child[0] and if there are siblings
            // if there are, then I may need to expand the identifier with the model, and re-attach the original siblings to the new ones
            for (int i = 0; i < leftNode.jjtGetNumChildren(); i++) {
                JexlNode kid = leftNode.jjtGetChild(i);
                if (i == 0 && (kid instanceof ASTIdentifier || JexlASTHelper.isLiteral(kid))) {
                    leftSeed = kid;
                } else {
                    leftIdentifierSiblings.add(kid); // i already copied the kids above when i expanded for mode names in the functions
                }
            }
            // if so, then there was no identifier (to expand), so just make the leftSeed the leftNode
            if (leftSeed == null) {
                leftSeed = leftNode;
            }
        } else if (JexlASTHelper.isLiteral(leftNode)) {
            leftSeed = leftNode;
        } else {
            // leftNode could be an AdditiveNode or any number of other things.
            leftSeed = leftNode;
        }
        
        if (rightNode instanceof ASTReference) {
            // check to see if there is an identifier at child[0] and if there are siblings
            for (int i = 0; i < rightNode.jjtGetNumChildren(); i++) {
                JexlNode kid = rightNode.jjtGetChild(i);
                if (i == 0 && (kid instanceof ASTIdentifier || JexlASTHelper.isLiteral(kid))) {
                    rightSeed = kid;
                } else {
                    rightIdentifierSiblings.add(kid); // i already copied the kids above when i expanded for mode names in the functions
                }
            }
            if (rightSeed == null) {
                rightSeed = rightNode;
            }
        } else if (JexlASTHelper.isLiteral(rightNode)) {
            rightSeed = rightNode;
        } else {
            // rightNode could be an AdditiveNode or any number of other things.
            rightSeed = rightNode;
        }
        
        if (leftSeed instanceof ASTIdentifier) {
            Collection<String> aliases = getAliasesForField(JexlASTHelper.deconstructIdentifier((ASTIdentifier) leftSeed));
            for (String fieldName : aliases) {
                if (leftIdentifierSiblings.isEmpty() == false) {
                    // make a reference add the identifier then add all the identifierSiblings
                    ASTReference reference = new ASTReference(ParserTreeConstants.JJTREFERENCE);
                    int i = 0;
                    reference.jjtAddChild(JexlNodeFactory.buildIdentifier(fieldName), i++);
                    for (JexlNode kid : leftIdentifierSiblings) {
                        reference.jjtAddChild(kid, i++);
                        kid.jjtSetParent(reference);
                    }
                    left.add(reference);
                } else {
                    left.add(JexlNodeFactory.buildIdentifier(fieldName));
                }
            }
            if (aliases.isEmpty()) {
                if (leftNode.jjtGetNumChildren() == 1) {
                    left.add(leftSeed);
                } else {
                    left.add(leftNode);
                }
            }
            
        } else {
            // Not an identifier, therefore it's a literal or a function, etc
            left.add(leftSeed);
        }
        
        if (rightSeed instanceof ASTIdentifier) {
            Collection<String> aliases = getAliasesForField(JexlASTHelper.deconstructIdentifier((ASTIdentifier) rightSeed));
            for (String fieldName : aliases) {
                if (rightIdentifierSiblings.isEmpty() == false) {
                    // make a reference add the identifier then add all the identifierSiblings
                    ASTReference reference = new ASTReference(ParserTreeConstants.JJTREFERENCE);
                    int i = 0;
                    reference.jjtAddChild(JexlNodeFactory.buildIdentifier(fieldName), i++);
                    for (JexlNode kid : rightIdentifierSiblings) {
                        reference.jjtAddChild(kid, i++);
                        kid.jjtSetParent(reference);
                    }
                    right.add(reference);
                } else {
                    right.add(JexlNodeFactory.buildIdentifier(fieldName));
                }
            }
            if (aliases.isEmpty()) {
                if (rightNode.jjtGetNumChildren() == 1) {
                    right.add(rightSeed);
                } else {
                    right.add(rightNode);
                }
            }
            
        } else {
            right.add(rightSeed);
        }
        
        if (leftSeed == null) {
            leftSeed = leftNode;
        }
        if (rightSeed == null) {
            rightSeed = rightNode;
        }
        
        @SuppressWarnings("unchecked")
        // retrieve the cartesian product
        Set<List<Object>> product = Sets.cartesianProduct(left, right);
        
        /**
         * use the product transformer to shallow copy the jexl nodes. We've created new nodes that will be embedded within an ast reference. As a result, we
         * need to ensure that if we create a logical structure ( such as an or ) -- each literal references a unique identifier from the right. Otherwise,
         * subsequent visitors will reference incorrection sub trees, and potentially negate the activity of the query model visitor
         */
        Set<List<Object>> newSet = Sets.newHashSet(FluentIterable.from(product).transform(new ProductTransformer()));
        
        if (product.size() > 1) {
            if (requiresAnd) {
                JexlNode expanded = JexlNodeFactory.createNodeTreeFromPairs(ContainerType.AND_NODE, node, newSet);
                if (log.isTraceEnabled())
                    log.trace("expanded:" + PrintingVisitor.formattedQueryString(expanded));
                return expanded;
            } else {
                JexlNode expanded = JexlNodeFactory.createNodeTreeFromPairs(ContainerType.OR_NODE, node, newSet);
                if (log.isTraceEnabled())
                    log.trace("expanded:" + PrintingVisitor.formattedQueryString(expanded));
                return expanded;
            }
        } else if (1 == product.size()) {
            List<Object> pair = product.iterator().next();
            JexlNode expanded = JexlNodeFactory.buildUntypedBinaryNode(node, pair.get(0), pair.get(1));
            if (log.isTraceEnabled())
                log.trace("expanded:" + PrintingVisitor.formattedQueryString(expanded));
            return expanded;
        }
        
        // If we couldn't map anything, return a copy
        return node;
    }
    
    /**
     * Ensures that each object created as a result of the cartesian product of the literal and identifiers gives us unique references within the tree. Without
     * this functional transformation you may have subsequent methods that use your objects to create nodes, referencing the embedded literals
     */
    protected static class ProductTransformer implements Function<List<Object>,List<Object>> {
        @Override
        public List<Object> apply(List<Object> objects) {
            List<Object> newObjectList = Lists.newArrayListWithCapacity(objects.size());
            for (Object obj : objects) {
                Object newObj = obj;
                if (obj instanceof JexlNode) {
                    newObj = RebuildingVisitor.copy((JexlNode) obj);
                }
                newObjectList.add(newObj);
            }
            return newObjectList;
            
        }
    }
}
