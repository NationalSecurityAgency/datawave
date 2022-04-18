package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This visitor replaces any occurrences of <code>FIELD =~'.*?'</code> with the more efficient equivalent <code>FIELD != null</code>.
 *
 * Also handles the following cases for filter functions
 * <ul>
 * <li><code>filter:isNull(FIELD)</code></li>
 * <li><code>filter:isNull(FIELD_1 || FIELD_2)</code></li>
 * <li><code>filter:isNotNull(FIELD)</code></li>
 * <li><code>filter:isNotNull(FIELD_1 || FIELD_2)</code></li>
 * </ul>
 */
public class IsNotNullIntentVisitor extends BaseVisitor {
    
    // used to rebuild flattened unions
    private boolean rebuiltMultiFieldedFunction = false;
    
    /**
     * Apply this visitor to the provided node and return the result.
     * 
     * @param node
     *            a JexlNode
     * @return the same node
     */
    public static <T extends JexlNode> T fixNotNullIntent(T node) {
        node.jjtAccept(new IsNotNullIntentVisitor(), null);
        return node;
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
        node.childrenAccept(this, data);
        
        if (rebuiltMultiFieldedFunction) {
            List<JexlNode> children = new ArrayList<>();
            for (JexlNode child : JexlNodes.children(node)) {
                JexlNode deref = JexlASTHelper.dereference(child);
                if (deref instanceof ASTOrNode) {
                    children.addAll(Arrays.asList(JexlNodes.children(deref)));
                } else {
                    children.add(child);
                }
            }
            JexlNodes.children(node, children.toArray(new JexlNode[0]));
            rebuiltMultiFieldedFunction = false;
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        // If the ER node is meant to match any string, it can be replaced with FIELD != null.
        Object value = JexlASTHelper.getLiteralValue(node);
        if (".*?".equals(value)) {
            List<JexlNode> children = new ArrayList<>();
            children.add(node.jjtGetChild(0));
            children.add(new ASTNullLiteral(ParserTreeConstants.JJTNULLLITERAL));
            
            JexlNode neNode = new ASTNENode(ParserTreeConstants.JJTNENODE);
            JexlNodes.children(neNode, children.toArray(new JexlNode[0]));
            
            JexlNodes.replaceChild(node.jjtGetParent(), node, neNode);
        }
        return data;
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
        
        if (visitor.namespace().equals("filter")) {
            if (visitor.name().equals(EvaluationPhaseFilterFunctionsDescriptor.IS_NULL)) {
                JexlNode rewritten = rewriteFilterFunction(visitor, false);
                JexlNodes.replaceChild(node.jjtGetParent(), node, rewritten);
            } else if (visitor.name().equals("isNotNull")) {
                JexlNode rewritten = rewriteFilterFunction(visitor, true);
                JexlNodes.replaceChild(node.jjtGetParent(), node, rewritten);
            }
        }
        
        return data;
    }
    
    /**
     * Given the output of a {@link FunctionJexlNodeVisitor}, produce a rewritten filter function
     *
     * @param visitor
     *            a {@link FunctionJexlNodeVisitor}
     * @param negated
     *            a flag indicating this method should negate the built node
     * @return the rewritten function
     */
    private JexlNode rewriteFilterFunction(FunctionJexlNodeVisitor visitor, boolean negated) {
        JexlNode args = visitor.args().get(0);
        if (args instanceof ASTIdentifier) {
            // single fielded case
            if (negated) {
                return buildIsNotNullNode((ASTIdentifier) args);
            } else {
                return buildIsNullNode((ASTIdentifier) args);
            }
        } else if (args instanceof ASTOrNode) {
            // multi fielded case
            rebuiltMultiFieldedFunction = true;
            List<JexlNode> children = new ArrayList<>(args.jjtGetNumChildren());
            for (JexlNode child : JexlNodes.children(args)) {
                child = JexlASTHelper.dereference(child);
                if (child instanceof ASTIdentifier) {
                    if (negated) {
                        children.add(buildIsNotNullNode((ASTIdentifier) child));
                    } else {
                        children.add(buildIsNullNode((ASTIdentifier) child));
                    }
                } else {
                    throw new IllegalStateException("Encountered an unexpected state while rewriting IsNotNull functions");
                }
            }
            return JexlNodeFactory.createOrNode(children);
        }
        return null;
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
        return JexlNodeFactory.buildNode(new ASTEQNode(ParserTreeConstants.JJTEQNODE), identifier.image, nullLiteral);
    }
}
