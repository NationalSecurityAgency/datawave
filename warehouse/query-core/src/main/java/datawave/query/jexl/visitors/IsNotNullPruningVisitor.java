package datawave.query.jexl.visitors;

import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor.GET_ALL_MATCHES;
import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor.INCLUDE_REGEX;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.commons.jexl3.parser.ASTReference;
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
import org.apache.log4j.Logger;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;

/**
 * This visitor prunes unnecessary 'is not null' functions from the query tree.
 * <p>
 * Lucene <code>#ISNOTNULL(field)</code> functions are rewritten into Jexl to look like <code>filter:isNotNull(field)</code> or
 * <code>filter:not(isNull(field))</code>.
 * <p>
 * Additionally, the {@link IsNotNullIntentVisitor} can produce logically equivalent nodes that look like <code>!(FIELD == null)</code>.
 * <p>
 * These nodes can be pruned when intersected with an inclusive node for the same field. By definition, all matched documents will have non-null instances the
 * field. For example,
 * <ul>
 * <li><code>!(FOO == null) &amp;&amp; FOO == 'bar'</code></li>
 * <li><code>!(FOO == null) &amp;&amp; (FOO == 'bar' || FOO == 'baz')</code></li>
 * <li><code>!(FOO == null) &amp;&amp; FOO =~ 'ba.*'</code></li>
 * <li><code>!(FOO == null) &amp;&amp; filter:regexInclude(FOO, 'ba.*')</code></li>
 * </ul>
 * <p>
 * In addition to reducing the number of nodes in the query, this pruning also stops negations from pushing into large subtrees. For example,
 * <p>
 * <code>!(FOO == null) &amp;&amp; FOO == 'bar' &amp;&amp; (F1 == 'v1' || F2 == 'v2' ... Fn == 'vn')</code>
 * </p>
 * In this case the two 'FOO' terms would be distributed into each component of the union, increasing the query node count considerably. Pruning the negated
 * term prevents this.
 */
public class IsNotNullPruningVisitor extends BaseVisitor {

    private static final Logger log = ThreadConfigurableLogger.getLogger(IsNotNullPruningVisitor.class);

    private IsNotNullPruningVisitor() {}

    /**
     * Generic entrypoint, applies pruning logic to subtree
     *
     * @param node
     *            an arbitrary Jexl node
     * @return the node
     */
    public static JexlNode prune(JexlNode node) {
        node.jjtAccept(new IsNotNullPruningVisitor(), null);
        return node;
    }

    /**
     * Prune 'is not null' terms that share a common field with other terms in this intersection
     *
     * @param node
     *            an intersection
     * @param data
     *            null object
     * @return the same ASTAnd node
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // do not process marker nodes
        if (!QueryPropertyMarkerVisitor.getInstance(node).isAnyType()) {
            boolean notNullExists = false;
            Set<String> fields = new HashSet<>(node.jjtGetNumChildren());
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                notNullExists |= findEqualityFieldsForNode(node.jjtGetChild(i), fields);
            }

            // only rebuild if it's possible
            if (notNullExists && !fields.isEmpty()) {
                List<JexlNode> next = new ArrayList<>(node.jjtGetNumChildren());
                JexlNode built;
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    built = pruneNode(node.jjtGetChild(i), fields);
                    if (built != null) {
                        next.add(built);
                    }
                }

                // rebuild
                if (next.size() == 1) {
                    JexlNodes.replaceChild(node.jjtGetParent(), node, next.get(0));
                    return data; // no sense visiting a single node we just built, so return here
                } else {
                    JexlNodes.setChildren(node, next.toArray(new JexlNode[0]));
                }
            }

            node.childrenAccept(this, data);
        }
        return data;
    }

    /**
     * Identifies equality fields, adds them to the provided set
     *
     * @param node
     *            a JexlNode
     * @param fields
     *            a set of fields representing anchor terms in the query
     * @return true if this node was instead a <code>ISNOTNULL</code> function
     */
    private boolean findEqualityFieldsForNode(JexlNode node, Set<String> fields) {
        node = JexlASTHelper.dereference(node);

        String field = null;
        if (node instanceof ASTEQNode || node instanceof ASTERNode) {
            field = fieldForNode(node);
        } else if (node instanceof ASTFunctionNode) {
            field = fieldForFunction(node);
        } else if (node instanceof ASTOrNode) {
            // in addition to single term children, we may have a union comprised of a single field
            field = fieldForUnion(node);
        }

        if (field != null)
            fields.add(field);

        return isIsNotNullFunction(node);
    }

    /**
     * Determines if this node is a 'ISNOTNULL' function and prunes based on the provided set of fields
     *
     * @param node
     *            a JexlNode
     * @param fields
     *            fields representing known anchor terms in the query tree
     * @return the original node, a partially pruned child, or null if this child was entirely pruned
     */
    private JexlNode pruneNode(JexlNode node, Set<String> fields) {
        JexlNode deref = JexlASTHelper.dereference(node);

        if (isIsNotNullFunction(deref)) {
            if (deref instanceof ASTNotNode) {
                String field = fieldForNode(deref);
                if (field != null && fields.contains(field)) {
                    return null;
                } else {
                    return node;
                }
            } else if (deref instanceof ASTOrNode) {
                // every child is a isNotNull node
                return pruneUnion(deref, fields);
            }
        }

        return node;
    }

    /**
     * Prunes this union if every child is a 'isNotNull' term that also has a corresponding anchor field
     *
     * @param node
     *            a union
     * @param fields
     *            a set of fields
     * @return the original node, or null if it is pruned
     */
    private JexlNode pruneUnion(JexlNode node, Set<String> fields) {
        // if there is a isNotNull in the union, and we know we have an equality node involving one of the isNotNull nodes,
        // we have the means to prune the entire union.
        boolean willPrune = false;

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode deref = JexlASTHelper.dereference(node.jjtGetChild(i));
            if (isIsNotNullFunction(deref) && !willPrune) {
                String field = fieldForNode(deref);
                if (fields.contains(field)) {
                    willPrune = true;
                }
            }

        }

        if (!willPrune) {
            return node;
        }

        return null;
    }

    /**
     * Determines if a node is <code>!(FOO == null)</code>, or if every child in a union is such a node
     *
     * @param node
     *            a JexlNode
     * @return true if at least one not null function exists
     */
    protected boolean isIsNotNullFunction(JexlNode node) {
        if (node instanceof ASTNotNode) {
            JexlNode child = node.jjtGetChild(0);
            child = JexlASTHelper.dereference(child);
            if (child instanceof ASTEQNode && child.jjtGetNumChildren() == 2) {
                child = JexlASTHelper.dereference(child.jjtGetChild(1));
                return child instanceof ASTNullLiteral;
            }
        } else if (node instanceof ASTOrNode) {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                if (!isIsNotNullFunction(JexlASTHelper.dereference(node.jjtGetChild(i)))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Extract the field for the provided Jexl node
     *
     * @param node
     *            an arbitrary Jexl node
     * @return the field, or null if no such field exists
     */
    protected String fieldForNode(JexlNode node) {
        if (!(node instanceof ASTAndNode || node instanceof ASTOrNode)) {
            Set<String> names = JexlASTHelper.getIdentifierNames(node);
            if (names.size() == 1) {
                return names.iterator().next();
            }
        }
        return null;
    }

    /**
     * Determine if this node is a <code>filter:includeRegex</code> function, and extract fields
     *
     * @param node
     *            an ASTFunction node
     * @return a single field if it exists, otherwise null
     */
    protected String fieldForFunction(JexlNode node) {
        FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
        node.jjtAccept(visitor, null);

        if (visitor.namespace().equals(EVAL_PHASE_FUNCTION_NAMESPACE) && (visitor.name().equals(INCLUDE_REGEX) || visitor.name().equals(GET_ALL_MATCHES))) {
            Set<String> args = JexlASTHelper.getIdentifierNames(visitor.args().get(0));
            if (args.size() == 1) {
                return args.iterator().next();
            }
        }
        return null;
    }

    /**
     * Determine if the provided union contains children that share a singular field
     *
     * @param node
     *            an ASTOrNode
     * @return a singular field if it exists, or null
     */
    protected String fieldForUnion(JexlNode node) {
        String field;
        Set<String> fields = new HashSet<>(node.jjtGetNumChildren());
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = JexlASTHelper.dereference(node.jjtGetChild(i));
            field = null;
            if (child instanceof ASTEQNode || child instanceof ASTERNode) {
                Set<String> names = JexlASTHelper.getIdentifierNames(child);
                if (names.size() == 1) {
                    field = names.iterator().next();
                } else {
                    return null; // EQ or ER node had more than one identifier...this is not correct
                }
            } else if (child instanceof ASTFunctionNode) {
                field = fieldForFunction(child);
            }

            if (field == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Unexpected node type [" + child.getClass().getSimpleName() + "] " + JexlStringBuildingVisitor.buildQueryWithoutParse(child));
                }
                return null; // encountered an unexpected node type, the union cannot have a single shared field
            } else {
                fields.add(field);
            }
        }
        return fields.size() == 1 ? fields.iterator().next() : null;
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
    public Object visit(ASTOrNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        // do not descend into marker nodes
        if (!QueryPropertyMarkerVisitor.getInstance(node).isAnyType()) {
            node.childrenAccept(this, data);
        }
        return data;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        node.childrenAccept(this, data);

        if (node.jjtGetNumChildren() == 1 && !JexlNodes.findNegatedParent(node)) {
            JexlNode child = JexlASTHelper.dereference(node.jjtGetChild(0));

            if (child instanceof ASTEQNode || child instanceof ASTERNode || child instanceof ASTFunctionNode) {
                JexlNodes.replaceChild(node.jjtGetParent(), node, child);
            }
        }
        return data;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    // +-----------------------------------+
    // | Do not descend through leaf nodes |
    // +-----------------------------------+

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
