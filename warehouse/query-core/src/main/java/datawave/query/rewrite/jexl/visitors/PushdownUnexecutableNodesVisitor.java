package datawave.query.rewrite.jexl.visitors;

import java.util.Set;
import datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import datawave.query.rewrite.jexl.visitors.ExecutableDeterminationVisitor.STATE;
import datawave.query.util.MetadataHelper;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.SimpleNode;
import org.apache.log4j.Logger;

/**
 * Visitor meant to 'push down' predicates for expressions that are only partially executable. Essentially if we have an AND node in which some of the nodes are
 * expanded and some completely expanded, then we need to push down the partial expansions.
 */
public class PushdownUnexecutableNodesVisitor extends BaseVisitor {
    
    protected MetadataHelper helper;
    protected RefactoredShardQueryConfiguration config;
    protected Set<String> nonEventFields;
    protected Set<String> indexOnlyFields;
    protected Set<String> indexedFields;
    
    public PushdownUnexecutableNodesVisitor(RefactoredShardQueryConfiguration config, Set<String> indexedFields, Set<String> indexOnlyFields,
                    Set<String> nonEventFields, MetadataHelper helper) {
        this.helper = helper;
        this.config = config;
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
                log.error("Could not determine non-event fields", e);
                throw new RuntimeException("got exception when using MetadataHelper to get non-event fields", e);
            }
        }
    }
    
    private static final Logger log = Logger.getLogger(PushdownUnexecutableNodesVisitor.class);
    
    public static JexlNode pushdownPredicates(JexlNode queryTree, RefactoredShardQueryConfiguration config, Set<String> indexedFields,
                    Set<String> indexOnlyFields, Set<String> nonEventFields, MetadataHelper helper) {
        PushdownUnexecutableNodesVisitor visitor = new PushdownUnexecutableNodesVisitor(config, indexedFields, indexOnlyFields, nonEventFields, helper);
        return (JexlNode) queryTree.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        if (!ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, false, null, helper)) {
            super.visit(node, data);
        }
        return node;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // if we have a non-executable and node, then we may be able to resolve this by pushing down the partial children
        if (!ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, false, null, helper)) {
            // first attempt to fix this by visiting the underlying nodes
            super.visit(node, data);
            // if still not executable, then we may be able to resolve this by pushing down the partial children
            if (!ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, false, null, helper)) {
                // push down any partial states
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode child = node.jjtGetChild(i);
                    STATE state = ExecutableDeterminationVisitor.getState(child, config, indexedFields, indexOnlyFields, nonEventFields, false, null, helper);
                    if (state == STATE.PARTIAL) {
                        ASTDelayedPredicate.create(child);
                    }
                }
            }
        }
        return node;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        // if not executable, then we may be able to resolve this by fixing the children children
        if (!ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, false, null, helper)) {
            super.visit(node, data);
        }
        return node;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        // if not executable, then visit all children
        if (!ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, false, null, helper)) {
            super.visit(node, data);
        }
        return node;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        // if a delayed predicate, then leave it alone
        if (ASTDelayedPredicate.instanceOf(node)) {
            return node;
        } else if (!ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, false, null, helper)) {
            super.visit(node, data);
        }
        return node;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        // if not executable, then visit all children
        if (!ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, false, null, helper)) {
            super.visit(node, data);
        }
        return node;
    }
    
    @Override
    public Object visit(SimpleNode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
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
    public Object visit(ASTAdditiveNode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
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
    public Object visit(ASTIntegerLiteral node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTFloatLiteral node, Object data) {
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
    public Object visit(ASTSizeMethod node, Object data) {
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
