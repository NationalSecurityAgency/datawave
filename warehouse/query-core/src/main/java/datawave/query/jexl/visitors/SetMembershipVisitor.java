package datawave.query.jexl.visitors;

import java.util.Set;
import java.util.TreeSet;

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

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctions;

/**
 * This visitor provides methods for determining if a query tree contains any of the provided fields, and retrieving those matching fields if desired.
 */
public class SetMembershipVisitor extends BaseVisitor {

    private final Set<String> fields;
    private final Set<String> discoveredFields;
    private final boolean fullTraversal;

    /**
     * Return true if the query contains any of the given fields.
     *
     * @param fields
     *            the fields of interest
     * @param tree
     *            the query tree
     *
     * @return true if the query contains any of the fields present in the given fields set
     */

    public static Boolean contains(Set<String> fields, JexlNode tree) {
        SetMembershipVisitor visitor = new SetMembershipVisitor(fields, false);
        return (Boolean) tree.jjtAccept(visitor, false);
    }

    /**
     * Return the intersection of fields found in the given query tree and the given set of fields.
     *
     * @param fields
     *            the fields of interest
     * @param tree
     *            the query tree
     * @return the set of fields found in the query that were in the given set of fields
     */
    public static Set<String> getMembers(Set<String> fields, JexlNode tree) {
        final SetMembershipVisitor visitor = new SetMembershipVisitor(fields, true);
        tree.jjtAccept(visitor, false);
        return visitor.discoveredFields;
    }

    private SetMembershipVisitor(Set<String> fields, boolean fullTraversal) {
        this.fields = fields;
        this.discoveredFields = new TreeSet<>();
        this.fullTraversal = fullTraversal;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        if (traverse(data)) {
            String field = JexlASTHelper.deconstructIdentifier(node);
            // If this is a matching field, add it to discovered field, and tag it if specified.
            if (fields.contains(field)) {
                discoveredFields.add(field);
                return true;
            }
        }
        return data;
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
     * query tree as soon as a match has been found for {@link SetMembershipVisitor#contains(Set, JexlNode)} where a full traversal may not be required.
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
