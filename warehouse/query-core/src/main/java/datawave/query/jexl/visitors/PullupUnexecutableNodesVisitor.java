package datawave.query.jexl.visitors;

import static datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;
import static org.apache.commons.jexl3.parser.JexlNodes.findNegatedParent;
import static org.apache.commons.jexl3.parser.JexlNodes.swap;
import static org.apache.commons.jexl3.parser.JexlNodes.wrap;

import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
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
import org.apache.log4j.Logger;

import datawave.core.query.jexl.nodes.QueryPropertyMarker;
import datawave.core.query.jexl.visitors.BaseVisitor;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MetadataHelper;

/**
 * Visitor meant to 'pull up' delayed predicates for expressions that are not executable. Essentially if we have an OR of nodes in which some of the nodes are
 * expanded and some of them are not, then we need to expand the other nodes.
 */
public class PullupUnexecutableNodesVisitor extends BaseVisitor {

    protected MetadataHelper helper;
    protected ShardQueryConfiguration config;
    protected Set<String> nonEventFields;
    protected Set<String> indexOnlyFields;
    protected Set<String> indexedFields;
    protected boolean forFieldIndex;

    public PullupUnexecutableNodesVisitor(ShardQueryConfiguration config, boolean forFieldIndex, Set<String> indexedFields, Set<String> indexOnlyFields,
                    Set<String> nonEventFields, MetadataHelper helper) {
        this.helper = helper;
        this.config = config;
        this.forFieldIndex = forFieldIndex;
        this.indexedFields = indexedFields;
        this.indexOnlyFields = indexOnlyFields;
        this.nonEventFields = nonEventFields;
        if (this.indexedFields == null) {
            if (config.getIndexedFields() != null && !config.getIndexedFields().isEmpty()) {
                this.indexedFields = config.getIndexedFields();
            } else {
                try {
                    this.indexedFields = this.helper.getIndexedFields(config.getDatatypeFilter());
                } catch (Exception ex) {
                    log.error("Could not determine indexed fields", ex);
                    throw new RuntimeException("got exception when using MetadataHelper to get indexed fields", ex);
                }
            }
        }
        if (this.indexOnlyFields == null) {
            try {
                this.indexOnlyFields = this.helper.getIndexOnlyFields(config.getDatatypeFilter());
            } catch (TableNotFoundException e) {
                log.error("Could not determine index only fields", e);
                throw new RuntimeException("got exception when using MetadataHelper to get index only fields", e);
            }
        }
        if (this.nonEventFields == null) {
            try {
                this.nonEventFields = this.helper.getNonEventFields(config.getDatatypeFilter());
            } catch (TableNotFoundException e) {
                log.error("Could not determine nont-event fields", e);
                throw new RuntimeException("got exception when using MetadataHelper to get non-event fields", e);
            }
        }
    }

    private static final Logger log = Logger.getLogger(PullupUnexecutableNodesVisitor.class);

    public static JexlNode pullupDelayedPredicates(JexlNode queryTree, boolean forFieldIndex, ShardQueryConfiguration config, Set<String> indexedFields,
                    Set<String> indexOnlyFields, Set<String> nonEventFields, MetadataHelper helper) {
        PullupUnexecutableNodesVisitor visitor = new PullupUnexecutableNodesVisitor(config, forFieldIndex, indexedFields, indexOnlyFields, nonEventFields,
                        helper);

        // rewrite the trees by pushing down all negations first
        JexlNode pushDownTree = PushdownNegationVisitor.pushdownNegations(queryTree);
        return (JexlNode) pushDownTree.jjtAccept(visitor, null);
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        if (!ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, forFieldIndex, null, helper)) {
            super.visit(node, data);
        }
        return node;
    }

    private boolean containsChild(JexlNode node, ExecutableDeterminationVisitor.STATE state) {
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            if (state == ExecutableDeterminationVisitor.getState(child, config, indexedFields, indexOnlyFields, nonEventFields, forFieldIndex, null, helper)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        // if a delayed predicate, then change it to a regular reference
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isType(DELAYED)) {
            JexlNode source = QueryPropertyMarker.unwrapFully(node, DELAYED);

            JexlNode parent = (node.jjtGetParent() instanceof ASTReferenceExpression) ? node.jjtGetParent() : node;
            source = (source instanceof ASTReferenceExpression) ? source.jjtGetChild(0) : source;

            // when pulling up a delayed marker that is negated, a reference expression must be persisted
            source = wrapIfNeeded(source);

            swap(parent.jjtGetParent(), parent, source);
            return source;
        } else if (!ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, forFieldIndex, null, helper)) {
            // we need to make sure one of these nodes executable, and we have no partials
            // TODO we should use some cost/index stats info

            // determine if we have any executable children
            boolean executable = containsChild(node, ExecutableDeterminationVisitor.STATE.EXECUTABLE);

            // TODO: this will basically eliminate any work we have done based on cost to delayed
            // contained predicates. However cost got us into a non-executable situation in the first
            // place so now we are looking for anything we can do to make this query executable.

            // first flip the error states regardless of the executable state
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode child = node.jjtGetChild(i);
                ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(child, config, indexedFields, indexOnlyFields,
                                nonEventFields, forFieldIndex, null, helper);
                if (state == ExecutableDeterminationVisitor.STATE.ERROR) {
                    child.jjtAccept(this, data);
                    state = ExecutableDeterminationVisitor.getState(child, config, indexedFields, indexOnlyFields, nonEventFields, forFieldIndex, null, helper);
                }
                if (state == ExecutableDeterminationVisitor.STATE.EXECUTABLE) {
                    executable = true;
                }
            }

            // then try flipping the partial states
            for (int i = 0; i < node.jjtGetNumChildren() && !executable; i++) {
                JexlNode child = node.jjtGetChild(i);
                ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(child, config, indexedFields, indexOnlyFields,
                                nonEventFields, forFieldIndex, null, helper);
                if (state == ExecutableDeterminationVisitor.STATE.PARTIAL) {
                    child.jjtAccept(this, data);
                    state = ExecutableDeterminationVisitor.getState(child, config, indexedFields, indexOnlyFields, nonEventFields, forFieldIndex, null, helper);
                }
                if (state == ExecutableDeterminationVisitor.STATE.EXECUTABLE) {
                    executable = true;
                }
            }

            // if no executable nodes found, then flip any non-executable nodes
            for (int i = 0; i < node.jjtGetNumChildren() && !executable; i++) {
                JexlNode child = node.jjtGetChild(i);
                ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(child, config, indexedFields, indexOnlyFields,
                                nonEventFields, forFieldIndex, null, helper);
                if (state == ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE) {
                    child.jjtAccept(this, data);
                    state = ExecutableDeterminationVisitor.getState(child, config, indexedFields, indexOnlyFields, nonEventFields, forFieldIndex, null, helper);
                }
                if (state == ExecutableDeterminationVisitor.STATE.EXECUTABLE) {
                    executable = true;
                }
            }

            super.visit(node, data);
        }

        return node;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        // if not executable, then visit all non-executable children
        if (!ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, forFieldIndex, null, helper)) {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode child = node.jjtGetChild(i);
                if (!ExecutableDeterminationVisitor.isExecutable(child, config, indexedFields, indexOnlyFields, nonEventFields, forFieldIndex, null, helper)) {
                    child.jjtAccept(this, data);
                }
            }
        }
        return node;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        // if not executable, then visit all children
        if (!ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, forFieldIndex, null, helper)) {
            super.visit(node, data);
        }
        return node;
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        // if not executable, then visit all children
        if (!ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, forFieldIndex, null, helper)) {
            super.visit(node, data);
        }
        return node;
    }

    /**
     * When pulling up a delayed marker that is negated a reference expression must be persisted for correct query evaluation
     *
     * @param node
     *            a JexlNode
     * @return the node, possible wrapped in a reference expression
     */
    private JexlNode wrapIfNeeded(JexlNode node) {
        node = JexlASTHelper.dereference(node);
        if (node instanceof ASTOrNode || node instanceof ASTAndNode || findNegatedParent(node)) {
            node = wrap(node);
        }
        return node;
    }

    @Override
    public Object visit(ASTBlock node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTAddNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTSubNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTMulNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTDivNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTModNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTVar node, Object data) {
        return node;
    }

    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return node;
    }

}
