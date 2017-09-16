package datawave.query.jexl.visitors;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.function.IndexOnlyContextCreator;
import datawave.query.jexl.IndexOnlyJexlContext;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;

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
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;

/** 
 *
 */
public class SetMembershipVisitor extends BaseVisitor {
    /**
     * A suffix appended to an index-only node's "image" value if found as a child of an ASTFunctionNode. The added suffix causes the field to be temporarily
     * renamed and specially handled for fetching and evaluation via the {@link IndexOnlyJexlContext}.
     * 
     * Note: The logic that originally appended this suffix appeared to have been inadvertently removed from this class (renamed from IndexOnlyVisitor) when the
     * dev branch was merged into version2.x. It has since been reapplied in conjunction with the two internal helper classes.
     */
    public final static String INDEX_ONLY_FUNCTION_SUFFIX = "@LAZY_SET_FOR_INDEX_ONLY_FUNCTION_EVALUATION";
    public final static String FILTER = "filter";
    protected final Set<String> expectedFields;
    protected final MetadataHelper metadataHelper;
    protected final DateIndexHelper dateIndexHelper;
    protected final ShardQueryConfiguration config;
    protected final Set<String> discoveredFields;
    protected final boolean fullTraversal;
    
    /**
     * Create a SetMembershopVisitor that will visit the query in search of the specified fields
     * 
     * @param expectedFields
     * @param config
     * @param metadataHelper
     * @param dateIndexHelper
     * @param fullTraversal
     */
    public SetMembershipVisitor(Set<String> expectedFields, ShardQueryConfiguration config, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    boolean fullTraversal) {
        this.config = config;
        this.expectedFields = expectedFields;
        this.metadataHelper = metadataHelper;
        this.dateIndexHelper = dateIndexHelper;
        this.discoveredFields = new TreeSet<>();
        this.fullTraversal = fullTraversal;
    }
    
    /**
     * Return true if the query contains fields that are present in the expectedFields set for the specified datatypes.
     * 
     * @param expectedFields
     * @param metadataHelper
     * @param dateIndexHelper
     * @param tree
     * @return true if the query contains fields that are present in the expectedFields set for the specified datatypes
     */
    
    public static Boolean contains(Set<String> expectedFields, ShardQueryConfiguration config, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    JexlNode tree) {
        SetMembershipVisitor visitor = new SetMembershipVisitor(expectedFields, config, metadataHelper, dateIndexHelper, false);
        return (Boolean) tree.jjtAccept(visitor, false);
    }
    
    /**
     * Return a set of field names that are present in the expectedFields set for the specified datatypes.
     * 
     * @param expectedFields
     * @param config
     * @param metadataHelper
     * @param dateIndexHelper
     * @param tree
     * @return a set of field names that are present in the expectedFields set for the specified datatypes
     */
    public static Set<String> getMembers(Set<String> expectedFields, ShardQueryConfiguration config, MetadataHelper metadataHelper,
                    DateIndexHelper dateIndexHelper, JexlNode tree) {
        return getMembers(expectedFields, config, metadataHelper, dateIndexHelper, tree, false);
    }
    
    /**
     * Return a set of field names that are present in the expectedFields set for the specified datatypes.
     * 
     * @param expectedFields
     * @param config
     * @param metadataHelper
     * @param dateIndexHelper
     * @param tree
     * @param indexOnlyFieldTaggingEnabled
     *            If true, allow tagging to occur for index-only fields
     * @return a set of field names that are present in the expectedFields set for the specified datatypes
     */
    public static Set<String> getMembers(Set<String> expectedFields, ShardQueryConfiguration config, MetadataHelper metadataHelper,
                    DateIndexHelper dateIndexHelper, JexlNode tree, boolean indexOnlyFieldTaggingEnabled) {
        final SetMembershipVisitor visitor = new SetMembershipVisitor(expectedFields, config, metadataHelper, dateIndexHelper, true);
        final Boolean contains = (Boolean) tree.jjtAccept(visitor, false);
        if (indexOnlyFieldTaggingEnabled && (null != contains) && contains) {
            final IndexOnlyTaggingVisitor fieldTagger = new IndexOnlyTaggingVisitor(expectedFields, visitor.fullTraversal);
            fieldTagger.visit(tree, null);
        }
        
        return visitor.discoveredFields;
    }
    
    /*
     * Search for a parent filter function, limiting the search depth to prevent the possibility of infinite recursion
     * 
     * @param node an identifier node
     * 
     * @return true, if the node is associated with a filter function
     */
    private static boolean isParentAFilterFunction(final ASTIdentifier node) {
        JexlNode parent = node.jjtGetParent();
        boolean isParentAFilterFunction = false;
        for (int i = 0; (null != parent) && (i < 7); i++) {
            if (parent instanceof ASTFunctionNode) {
                final ASTFunctionNode function = (ASTFunctionNode) parent;
                int children = function.jjtGetNumChildren();
                if (children > 0) {
                    final JexlNode child = function.jjtGetChild(0);
                    if ((null != child) && FILTER.equals(child.image)) {
                        isParentAFilterFunction = true;
                        parent = null;
                    } else {
                        parent = parent.jjtGetParent();
                    }
                } else {
                    parent = parent.jjtGetParent();
                }
            } else {
                parent = parent.jjtGetParent();
            }
        }
        
        return isParentAFilterFunction;
    }
    
    private static final boolean traverse(Object data, boolean fullTraversal) {
        if (fullTraversal)
            return true;
        return !(Boolean) data;
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        final Object intermediateResult = visit(node, data, fullTraversal, expectedFields);
        final Object finalResult;
        if (intermediateResult instanceof DiscoveredField) {
            discoveredFields.add(((DiscoveredField) intermediateResult).getFieldName());
            finalResult = true;
        } else if (intermediateResult instanceof Boolean) {
            finalResult = intermediateResult;
        } else {
            finalResult = data;
        }
        
        return finalResult;
    }
    
    private static Object visit(final ASTIdentifier node, final Object data, boolean fullTraversal, final Collection<String> expectedFields) {
        // Declare a return value
        final Object result;
        
        // Evaluate and process the parameters to assign a return value
        if ((data instanceof IndexOnlyTaggingVisitor) || (traverse(data, fullTraversal))) {
            final String fieldName = JexlASTHelper.deconstructIdentifier(node);
            if (expectedFields.contains(fieldName)) {
                // If the parent is a function, append a specially recognized suffix that
                // will be used for lazy fetching downstream in the processing operation.
                // See the IndexOnlyJexlContext for more details.
                if (data instanceof IndexOnlyTaggingVisitor) { // Verify we are purposely renaming index-only function nodes
                    if ((!node.image.endsWith(INDEX_ONLY_FUNCTION_SUFFIX)) && // Verify the node is not already renamed
                                    (isParentAFilterFunction(node))) { // Verify the node is part of a function
                        node.image = node.image + INDEX_ONLY_FUNCTION_SUFFIX;
                    }
                    result = true;
                }
                // Otherwise, return a value that can be added to the set of discovered fields
                else {
                    result = new DiscoveredField(fieldName);
                }
            } else {
                result = data;
            }
        } else {
            result = data;
        }
        
        return result;
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        if (traverse(data, fullTraversal)) {
            // apply it to the index query first
            JexlArgumentDescriptor d = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
            JexlNode tree = d.getIndexQuery(this.config, this.metadataHelper, this.dateIndexHelper, this.config.getDatatypeFilter());
            data = tree.jjtAccept(this, data);
            
            // and then apply it to the args directory
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        if (traverse(data, fullTraversal)) {
            int i = 0;
            while (traverse(data, fullTraversal) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }
        
        return data;
    }
    
    private static class DiscoveredField {
        private final String fieldName;
        
        public DiscoveredField(String fieldName) {
            this.fieldName = fieldName;
        }
        
        public String getFieldName() {
            return fieldName;
        }
    }
    
    /**
     * Appends a tag to index-only field names for specialized recognition and handling by JEXL components, such as the IndexOnlyContextCreator
     * 
     * @see IndexOnlyContextCreator
     */
    private static class IndexOnlyTaggingVisitor extends BaseVisitor {
        private final Collection<String> indexOnlyFields;
        private final boolean fullTraversal;
        
        public IndexOnlyTaggingVisitor(final Collection<String> indexOnlyFields, boolean fullTraversal) {
            this.indexOnlyFields = indexOnlyFields;
            this.fullTraversal = fullTraversal;
        }
        
        @Override
        public Object visit(final ASTIdentifier node, final Object data) {
            // Visit the node and tag index-only field names belonging to a top-level function
            SetMembershipVisitor.visit(node, this, this.fullTraversal, this.indexOnlyFields);
            
            // Always returns false since this visitor's only job is to tag index-only field names belonging to a top-level function
            return false;
        }
    }
    
}
