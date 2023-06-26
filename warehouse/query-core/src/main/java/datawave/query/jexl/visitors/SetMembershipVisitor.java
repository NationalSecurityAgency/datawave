package datawave.query.jexl.visitors;

import java.util.Set;
import java.util.TreeSet;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.IndexOnlyJexlContext;
import datawave.query.jexl.JexlASTHelper;

import datawave.query.jexl.functions.EvaluationPhaseFilterFunctions;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNamespaceIdentifier;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

/**
 * This visitor provides methods for determining if a query tree contains any of the provided fields, and retrieving those matching fields if desired.
 * Optionally, this visitor will also tag index-only fields within filter functions for lazily-handled fetching and evaluation if specified.
 */
public class SetMembershipVisitor extends BaseVisitor {

    /**
     * A suffix appended to an index-only node's "image" value if found as a child of an ASTFunctionNode. The added suffix causes the field to be temporarily
     * renamed and specially handled for fetching and evaluation via the {@link IndexOnlyJexlContext}.
     *
     * Note: The logic that originally appended this suffix appeared to have been inadvertently removed from this class (renamed from IndexOnlyVisitor) when the
     * dev branch was merged into version2.x. It has since been reapplied in conjunction with the two internal helper classes.
     */
    public static final String INDEX_ONLY_FUNCTION_SUFFIX = "@LAZY_SET_FOR_INDEX_ONLY_FUNCTION_EVALUATION";
    private static final int FUNCTION_SEARCH_DEPTH_LIMIT = 7;

    private final Set<String> fields;
    private final ShardQueryConfiguration config;
    private final Set<String> discoveredFields;
    private final boolean fullTraversal;
    private final boolean tagIndexOnlyFields;

    /**
     * Return true if the query contains any of the given fields.
     *
     * @param fields
     *            the fields of interest
     * @param tree
     *            the query tree
     * @param config
     *            the config
     * @return true if the query contains any of the fields present in the given fields set
     */

    public static Boolean contains(Set<String> fields, ShardQueryConfiguration config, JexlNode tree) {
        SetMembershipVisitor visitor = new SetMembershipVisitor(fields, config, false, false);
        return (Boolean) tree.jjtAccept(visitor, false);
    }

    /**
     * Return the intersection of fields found in the given query tree and the given set of fields.
     *
     * @param fields
     *            the fields of interest
     * @param config
     *            the configuration
     * @param tree
     *            the query tree
     * @return the set of fields found in the query that were in the given set of fields
     */
    public static Set<String> getMembers(Set<String> fields, ShardQueryConfiguration config, JexlNode tree) {
        return getMembers(fields, config, tree, false);
    }

    /**
     * Return the intersection of fields found in the given query tree and the given set of fields. If tagIndexOnlyFields is true, the provided query tree will
     * be modified such that matching fields will be tagged with {@value #INDEX_ONLY_FUNCTION_SUFFIX} where they are found in filter functions.
     * <b>IMPORTANT:</b> ONLY specify true for tagIndexOnlyFields if the set of provided fields consists only of index-only fields.
     *
     * @param fields
     *            the fields of interest
     * @param config
     *            the configuration
     * @param tree
     *            the query tree
     * @param tagIndexOnlyFields
     *            If true, tag any matching fields found in filter functions, supported only for index-only fields
     * @return the set of fields found in the query that were in the given set of fields
     * @throws DatawaveFatalQueryException
     *             if tagIndexOnlyFields is true, but lazySetMechanism in the given config is false and an index-only field was encountered. Note, in this case,
     *             it is assumed that the given set of fields consists of index-only fields.
     */
    public static Set<String> getMembers(Set<String> fields, ShardQueryConfiguration config, JexlNode tree, boolean tagIndexOnlyFields) {
        final SetMembershipVisitor visitor = new SetMembershipVisitor(fields, config, true, tagIndexOnlyFields);
        tree.jjtAccept(visitor, false);
        return visitor.discoveredFields;
    }

    private SetMembershipVisitor(Set<String> fields, ShardQueryConfiguration config, boolean fullTraversal, boolean tagIndexOnlyFields) {
        this.config = config;
        this.fields = fields;
        this.discoveredFields = new TreeSet<>();
        this.fullTraversal = fullTraversal;
        this.tagIndexOnlyFields = tagIndexOnlyFields;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        if (tagIndexOnlyFields || traverse(data)) {
            String field = JexlASTHelper.deconstructIdentifier(node);

            // If this is a previously-tagged field, strip the tag to check if the field itself is a match.
            if (isTagged(field)) {
                // Ensure we fail any queries with the LAZY_SET mechanism if it's not explicitly enabled, even if tagIndexOnlyFields is false.
                if (!config.isLazySetMechanismEnabled()) {
                    throw new DatawaveFatalQueryException("LAZY_SET mechanism is disabled for index-only fields");
                }
                int endPos = field.length() - INDEX_ONLY_FUNCTION_SUFFIX.length();
                field = field.substring(0, endPos);
            }

            // If this is a matching field, add it to discovered field, and tag it if specified.
            if (fields.contains(field)) {
                if (tagIndexOnlyFields) {
                    tagField(node);
                }
                discoveredFields.add(field);
                return true;
            }
        }
        return data;
    }

    /**
     * Tag the field in the given node with {@value #INDEX_ONLY_FUNCTION_SUFFIX} if it is part of a filter function.
     *
     * @param node
     *            the node to tag
     * @throws DatawaveFatalQueryException
     *             if the LAZY_SET mechanism is not enabled in the config
     */
    private void tagField(ASTIdentifier node) {
        // Tag the field only if the LAZY_SET mechanism is explicitly enabled.
        if (config.isLazySetMechanismEnabled()) {
            // Only tag index-only fields within filter functions.
            if (!isTagged(node) && parentFilterFunction(node)) {
                JexlNodes.setImage(node, JexlNodes.getImage(node) + INDEX_ONLY_FUNCTION_SUFFIX);
            }
        } else {
            // Otherwise, throw a fatal exception.
            if (isTagged(node) || parentFilterFunction(node)) {
                throw new DatawaveFatalQueryException("LAZY_SET mechanism is disabled for index-only fields");
            }
        }
    }

    /**
     * Returns whether the node's image ends with {@value #INDEX_ONLY_FUNCTION_SUFFIX}.
     *
     * @param node
     *            the node
     * @return true if the node has a tagged field, or false otherwise
     */
    private boolean isTagged(ASTIdentifier node) {
        return isTagged(String.valueOf(JexlNodes.getImage(node)));
    }

    /**
     * Returns whether the field ends with {@value #INDEX_ONLY_FUNCTION_SUFFIX}.
     *
     * @param field
     *            the field
     * @return true if the field is tagged, or false otherwise
     */
    private boolean isTagged(String field) {
        return field.endsWith(INDEX_ONLY_FUNCTION_SUFFIX);
    }

    /**
     * Searches for a parent filter function, limiting the search depth to {@value #FUNCTION_SEARCH_DEPTH_LIMIT} to prevent the possibility of infinite
     * recursion.
     *
     * @param node
     *            an identifier node
     *
     * @return true, if the node is associated with a filter function
     */
    private boolean parentFilterFunction(final ASTIdentifier node) {
        JexlNode parent = node.jjtGetParent();
        for (int i = 0; i < FUNCTION_SEARCH_DEPTH_LIMIT; i++) {
            if (parent != null) {
                if (filterFunction(parent)) {
                    return true;
                } else {
                    parent = parent.jjtGetParent();
                }
            }
        }
        return false;
    }

    /**
     * Return whether the node is a function node whose first child has the image {@value EvaluationPhaseFilterFunctions#EVAL_PHASE_FUNCTION_NAMESPACE}.
     *
     * @param node
     *            the node
     * @return true if the node is a filter function or false otherwise
     */
    private boolean filterFunction(JexlNode node) {
        boolean isFilterFunction = false;
        if (node instanceof ASTFunctionNode) {
            ASTNamespaceIdentifier namespaceNode = (ASTNamespaceIdentifier) node.jjtGetChild(0);
            isFilterFunction = namespaceNode.getNamespace().equals(EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE);
        }
        return isFilterFunction;
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTArguments node, Object data) {
        return traverseChildren(node, data);
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return traverseChildren(node, data);
    }

    /**
     * Returns true if {@link SetMembershipVisitor#fullTraversal} is true, otherwise return the inverse of matchFound. This allows us to stop traversing the
     * query tree as soon as a match has been found for {@link SetMembershipVisitor#contains(Set, ShardQueryConfiguration, JexlNode)} where a full traversal may
     * not be required.
     *
     * @param matchFound
     *            whether a match has been found yet
     * @return whether a node's children should be traversed by this visitor
     */
    private boolean traverse(Object matchFound) {
        if (fullTraversal)
            return true;
        return !(Boolean) matchFound;
    }

    /**
     * Will visit each child of the given node given {@link #traverse(Object)} returns true.
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the traversal result
     */
    private Object traverseChildren(JexlNode node, Object data) {
        if (traverse(data)) {
            int i = 0;
            while (traverse(data) && i < node.jjtGetNumChildren()) {
                data = node.jjtGetChild(i).jjtAccept(this, data);
                i++;
            }
        }

        return data;
    }
}
