package nsa.datawave.query.rewrite.jexl.visitors;

import com.google.common.base.Function;
import com.google.common.collect.*;
import nsa.datawave.query.model.QueryModel;
import nsa.datawave.query.rewrite.exceptions.DatawaveFatalQueryException;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.query.rewrite.jexl.JexlNodeFactory;
import nsa.datawave.query.rewrite.jexl.JexlNodeFactory.ContainerType;
import nsa.datawave.webservice.common.logging.ThreadConfigurableLogger;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.exception.QueryException;
import org.apache.commons.jexl2.parser.*;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.commons.jexl2.parser.JexlNodes.id;

/**
 * Apply the forward mapping
 */
public class QueryModelVisitor extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(QueryModelVisitor.class);
    
    private QueryModel queryModel;
    private HashSet<ASTAndNode> expandedNodes;
    private Set<String> validFields;
    private SimpleQueryModelVisitor simpleQueryModelVisitor;
    private JexlASTHelper.HasMethodVisitor hasMethodVisitor = new JexlASTHelper.HasMethodVisitor();
    
    public QueryModelVisitor(QueryModel queryModel, Set<String> validFields) {
        this.queryModel = queryModel;
        this.expandedNodes = Sets.newHashSet();
        this.validFields = validFields;
        this.simpleQueryModelVisitor = new SimpleQueryModelVisitor(queryModel, validFields);
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
        return node.jjtAccept(this.simpleQueryModelVisitor, data);
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        AtomicBoolean state = (AtomicBoolean) node.jjtAccept(hasMethodVisitor, new AtomicBoolean(false));
        if (state.get()) {
            // this reference has a child that is a method
            return (ASTReference) node.jjtAccept(this.simpleQueryModelVisitor, null);
        } else {
            return super.visit(node, data);
        }
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return node.jjtAccept(this.simpleQueryModelVisitor, data);
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        return node.jjtAccept(this.simpleQueryModelVisitor, data);
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
            AtomicBoolean state = (AtomicBoolean) child.jjtAccept(hasMethodVisitor, new AtomicBoolean(false));
            if (state.get()) {
                child = (JexlNode) child.jjtAccept(this.simpleQueryModelVisitor, null);
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
            // make a copy of the intersection, as I will be modifying lowererBounds and upperBounds below
            Set<String> tightBounds = Sets.newHashSet(Sets.intersection(lowerBounds.keySet(), upperBounds.keySet()));
            if (log.isDebugEnabled())
                log.debug("Found bounds to match: " + tightBounds);
            for (String field : tightBounds) {
                // String field = JexlASTHelper.getIdentifier(theNode);
                List<ASTAndNode> aliasedBounds = Lists.newArrayList();
                
                Collection<String> aliases = getAliasesForField(field);
                if (aliases.isEmpty()) {
                    aliases = Lists.newArrayList(field);
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
                if (aliasedBounds.isEmpty() == false) {
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
        // this will expand identifiers that have a method connected to them
        AtomicBoolean leftState = (AtomicBoolean) leftNode.jjtAccept(hasMethodVisitor, new AtomicBoolean(false));
        if (leftState.get()) {
            // there is a method under leftNode
            leftNode = (JexlNode) leftNode.jjtAccept(this.simpleQueryModelVisitor, null);
        }
        AtomicBoolean rightState = (AtomicBoolean) rightNode.jjtAccept(hasMethodVisitor, new AtomicBoolean(false));
        if (rightState.get() == false) {
            // there is a method under rightNode
            rightNode = (JexlNode) rightNode.jjtAccept(this.simpleQueryModelVisitor, null);
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
        
        // if state == true on either side, then there is a method on one side and we are done applying the model
        if (leftState.get() || rightState.get()) {
            JexlNode toReturn = JexlNodeFactory.buildUntypedBinaryNode(node, leftNode, rightNode);
            if (log.isTraceEnabled()) {
                log.trace("done early. returning:" + JexlStringBuildingVisitor.buildQuery(toReturn));
            }
            return toReturn;
        }
        
        Object leftSeed = null, rightSeed = null;
        Set<Object> left = Sets.newHashSet(), right = Sets.newHashSet();
        boolean isNullEquality = false;
        
        if (node instanceof ASTEQNode && (leftNode instanceof ASTNullLiteral || rightNode instanceof ASTNullLiteral)) {
            isNullEquality = true;
        }
        
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
        } else if (1 <= childCount && childCount <= 2) {
            // I could have a reference on both sides of the expression
            leftSeed = node.jjtGetChild(0);
            rightSeed = node.jjtGetChild(1);
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.BINARY_NODE_TOO_MANY_CHILDREN, MessageFormat.format("Node: {0}",
                            PrintingVisitor.formattedQueryString(node)));
            throw new DatawaveFatalQueryException(qe);
        }
        
        if (leftSeed instanceof ASTReference) {
            // String fieldName = JexlASTHelper.getIdentifier((JexlNode)leftSeed);
            List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers((ASTReference) leftSeed);
            if (identifiers.size() > 1) {
                log.warn("I did not expect to see more than one Identifier here for " + JexlStringBuildingVisitor.buildQuery((ASTReference) leftSeed)
                                + " from " + JexlStringBuildingVisitor.buildQuery(leftNode));
            }
            for (ASTIdentifier identifier : identifiers) {
                for (String fieldName : getAliasesForField(JexlASTHelper.deconstructIdentifier(identifier))) {
                    left.add(JexlNodeFactory.buildIdentifier(fieldName));
                }
            }
        } else if (leftSeed instanceof ASTIdentifier) {
            for (String fieldName : getAliasesForField(JexlASTHelper.deconstructIdentifier((ASTIdentifier) leftSeed))) {
                left.add(JexlNodeFactory.buildIdentifier(fieldName));
            }
        } else {
            // Not an identifier, therefore it's probably a literal
            left.add(leftSeed);
        }
        
        if (rightSeed instanceof ASTReference) {
            List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers((ASTReference) rightSeed);
            if (identifiers.size() > 1) {
                log.warn("I did not expect to see more than one Identifier here for " + JexlStringBuildingVisitor.buildQuery((ASTReference) rightSeed)
                                + " from " + JexlStringBuildingVisitor.buildQuery(rightNode));
            }
            for (ASTIdentifier identifier : identifiers) {
                for (String fieldName : getAliasesForField(JexlASTHelper.deconstructIdentifier(identifier))) {
                    right.add(JexlNodeFactory.buildIdentifier(fieldName));
                }
            }
        } else if (rightSeed instanceof ASTIdentifier) {
            for (String fieldName : getAliasesForField(JexlASTHelper.deconstructIdentifier((ASTIdentifier) rightSeed))) {
                right.add(JexlNodeFactory.buildIdentifier(fieldName));
            }
            
        } else {
            // Not an identifier, therefore it's probably a literal
            right.add(rightSeed);
        }
        boolean requiresAnd = isNullEquality || node instanceof ASTNENode;
        
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
        if (log.isTraceEnabled())
            log.trace("just returning the original:" + PrintingVisitor.formattedQueryString(node));
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
    
    /**
     * The SimpleQueryModelVisitor will only change identifiers into a disjunction of their aliases: FOO becomes (ALIASONE||ALIASTWO) It is used within function
     * and method node arguments and in the reference that a method is called on
     */
    protected static class SimpleQueryModelVisitor extends RebuildingVisitor {
        
        private static final Logger log = ThreadConfigurableLogger.getLogger(SimpleQueryModelVisitor.class);
        private QueryModel queryModel;
        private Set<String> validFields;
        
        public SimpleQueryModelVisitor(QueryModel queryModel, Set<String> validFields) {
            this.queryModel = queryModel;
            this.validFields = validFields;
        }
        
        @Override
        public Object visit(ASTIdentifier node, Object data) {
            JexlNode newNode = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
            String fieldName = JexlASTHelper.getIdentifier(node);
            
            Collection<String> aliases = Sets.newLinkedHashSet(getAliasesForField(fieldName)); // de-dupe
            
            Set<ASTIdentifier> nodes = Sets.newLinkedHashSet();
            
            if (aliases.isEmpty()) {
                return super.visit(node, data);
            }
            for (String alias : aliases) {
                ASTIdentifier newKid = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
                newKid.image = JexlASTHelper.rebuildIdentifier(alias);
                nodes.add(newKid);
            }
            newNode = JexlNodeFactory.createOrNode(nodes);
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
         * @return the list of field aliases
         */
        protected Collection<String> getAliasesForField(String field) {
            List<String> aliases = new ArrayList<>(this.queryModel.getMappingsForAlias(field));
            aliases.retainAll(validFields);
            return aliases;
        }
    }
}
