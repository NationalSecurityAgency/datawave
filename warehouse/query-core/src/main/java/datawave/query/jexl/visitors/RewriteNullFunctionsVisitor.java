package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTArrayAccess;
import org.apache.commons.jexl3.parser.ASTArrayLiteral;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl3.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl3.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl3.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl3.parser.ASTBlock;
import org.apache.commons.jexl3.parser.ASTConstructorNode;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEmptyFunction;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTForeachStatement;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIfStatement;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMapEntry;
import org.apache.commons.jexl3.parser.ASTMapLiteral;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTReturnStatement;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTernaryNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTVar;
import org.apache.commons.jexl3.parser.ASTWhileStatement;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor.IS_NULL;

/**
 * This visitor prepares the query tree for the {@link IsNotNullPruningVisitor}. It should be run after QueryModel expansion.
 * <p>
 * It is similar to the {@link IsNotNullIntentVisitor} in that it rewrites certain filter functions into their ASTEq equivalents.
 * <ul>
 * <li><code>filter:isNull(FIELD)</code> becomes <code>FIELD == null</code></li>
 * <li><code>filter:isNull(F1 || F2)</code> becomes <code>F1 == null &amp;&amp; F2 == null</code></li>
 * <li><code>filter:isNotNull(FIELD)</code> becomes <code>!(FIELD == null)</code></li>
 * <li><code>filter:isNotNull(F1 || F2)</code> becomes <code>!(F1 == null) || !(F2 == null)</code></li>
 * </ul>
 */
public class RewriteNullFunctionsVisitor extends BaseVisitor {

    // used to rebuild flattened unions
    private boolean rebuiltMultiFieldedFunction = false;

    private static final String IS_NOT_NULL = "isNotNull";

    private RewriteNullFunctionsVisitor() {}

    /**
     *
     * @param node
     *            an arbitrary JexlNode
     * @param <T>
     *            type of node
     * @return the original query tree
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T rewriteNullFunctions(JexlNode node) {
        node.jjtAccept(new RewriteNullFunctionsVisitor(), null);
        return (T) node;
    }

    /**
     * Rewrite <code>filter:isNull</code> and <code>filter:isNotNull</code> functions to the more efficient <code>FIELD == null</code> or
     * <code>!(FIELD == null)</code>
     *
     * @param node
     *            a function node
     * @param data
     *            an object
     * @return the rewritten node, or the original node if no rewrite was required
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {

        FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
        node.jjtAccept(visitor, null);

        if (visitor.namespace().equals(EVAL_PHASE_FUNCTION_NAMESPACE) && (visitor.name().equals(IS_NULL) || visitor.name().equals(IS_NOT_NULL))) {
            JexlNode rewritten = rewriteFilterFunction(visitor);
            JexlNodes.replaceChild(node.jjtGetParent(), node, rewritten);
        }

        return data;
    }

    /**
     * Given the output of a {@link FunctionJexlNodeVisitor}, produce a rewritten filter function
     *
     * @param visitor
     *            a {@link FunctionJexlNodeVisitor}
     * @return the rewritten function
     */
    private JexlNode rewriteFilterFunction(FunctionJexlNodeVisitor visitor) {

        JexlNode args = visitor.args().get(0);
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(args);
        List<JexlNode> children = new ArrayList<>(identifiers.size());

        for (ASTIdentifier identifier : identifiers) {
            if (visitor.name().equals(IS_NOT_NULL)) {
                children.add(buildIsNotNullNode(identifier));
            } else {
                children.add(buildIsNullNode(identifier));
            }
        }

        switch (children.size()) {
            case 0:
                return null;
            case 1:
                // single fielded case
                return children.get(0);
            default:
                // multi fielded case
                rebuiltMultiFieldedFunction = true;
                if (visitor.name().equals(IS_NULL)) {
                    return JexlNodeFactory.createAndNode(children);
                } else {
                    return JexlNodeFactory.createOrNode(children);
                }
        }
    }

    /**
     * Builds a rewritten "isNotNull" function for the provided identifier
     *
     * @param identifier
     *            the field
     * @return a is not null node
     */
    private JexlNode buildIsNotNullNode(ASTIdentifier identifier) {
        return JexlNodes.negate(buildIsNullNode(identifier));
    }

    /**
     * Builds a rewritten "isNull" function
     *
     * @param identifier
     *            the field
     * @return a is null node
     */
    private JexlNode buildIsNullNode(ASTIdentifier identifier) {
        ASTNullLiteral nullLiteral = new ASTNullLiteral(ParserTreeConstants.JJTNULLLITERAL);
        return JexlNodeFactory.buildNode(new ASTEQNode(ParserTreeConstants.JJTEQNODE), identifier.getName(), nullLiteral);
    }

    /**
     * This is required to produce a flattened query tree
     *
     * @param node
     *            an ASTOrNode
     * @param data
     *            an object
     * @return the same node
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        return visitJunction(node, data);
    }

    /**
     * This is required to produce a flattened query tree
     *
     * @param node
     *            an ASTOrNode
     * @param data
     *            an object
     * @return the same node
     */
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return visitJunction(node, data);
    }

    private Object visitJunction(JexlNode node, Object data) {
        node.childrenAccept(this, data);

        if (rebuiltMultiFieldedFunction) {
            flattenJunction(node);
            rebuiltMultiFieldedFunction = false;
        }

        return data;
    }

    private void flattenJunction(JexlNode node) {
        boolean junctionType = node instanceof ASTAndNode;
        List<JexlNode> children = new ArrayList<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            if (QueryPropertyMarkerVisitor.getInstance(child).isAnyType()) {
                children.add(child);
            } else {
                JexlNode deref = JexlASTHelper.dereference(child);
                if (junctionType ? (deref instanceof ASTAndNode) : (deref instanceof ASTOrNode)) {
                    List<JexlNode> derefChildren = new ArrayList<>();
                    for (int derefChildIdx = 0; derefChildIdx < deref.jjtGetNumChildren(); derefChildIdx++) {
                        derefChildren.add(deref.jjtGetChild(derefChildIdx));
                    }
                    Collections.addAll(children, derefChildren.toArray(new JexlNode[0]));
                } else {
                    children.add(child);
                }
            }
        }
        JexlNodes.setChildren(node, children.toArray(new JexlNode[0]));
    }

    // +-----------------------------+
    // | Descend through these nodes |
    // +-----------------------------+

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    // +-----------------------------+
    // | Short circuit at leaf nodes |
    // +-----------------------------+

    @Override
    public Object visit(ASTBlock node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTAddNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTSubNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTMulNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTDivNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTModNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTVar node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return data;
    }
}
