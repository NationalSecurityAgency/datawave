package datawave.query.jexl.visitors;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.jexl2.parser.JexlNodes.children;
import static org.apache.commons.jexl2.parser.JexlNodes.newInstanceOfType;

import java.text.MessageFormat;
import java.util.ArrayList;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.QueryStopwatch;
import datawave.util.time.TraceStopwatch;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

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
import org.apache.commons.jexl2.parser.ASTUnknownFieldERNode;
import org.apache.commons.jexl2.parser.ASTUnsatisfiableERNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.Node;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

import com.google.common.base.Function;

/**
 * Base Visitor class that returns a new AST. Each visit method should return a copy of the visited node.
 *
 */
@SuppressWarnings("deprecation")
public class RebuildingVisitor extends BaseVisitor implements Function<ASTJexlScript,ASTJexlScript> {
    
    protected QueryStopwatch timers = null;
    protected String description = "Configurable script";
    protected String scriptName = "";
    
    public RebuildingVisitor(final QueryStopwatch timers, final String scriptName) {
        this.timers = timers;
        this.scriptName = scriptName;
    }
    
    public RebuildingVisitor() {
        
    }
    
    /**
     * Helper method to return a copy of the tree denoted by the given root
     *
     * @param root
     * @return
     */
    public static JexlNode copy(JexlNode root) {
        RebuildingVisitor visitor = new RebuildingVisitor();
        return (JexlNode) root.jjtAccept(visitor, null);
    }
    
    public static JexlNode copyInto(JexlNode root, JexlNode target) {
        RebuildingVisitor visitor = new RebuildingVisitor();
        
        JexlNode copyRoot = (JexlNode) root.jjtAccept(visitor, null);
        
        if (null != copyRoot) {
            target.jjtSetParent(copyRoot.jjtGetParent());
            for (int i = 0; i < copyRoot.jjtGetNumChildren(); i++) {
                JexlNode child = copyRoot.jjtGetChild(i);
                target.jjtAddChild(child, i);
                child.jjtSetParent(target);
            }
        }
        
        return target;
    }
    
    private <T extends JexlNode> T copy(T node, Object data) {
        T newNode = newInstanceOfType(node);
        newNode.image = node.image;
        ArrayList<JexlNode> children = newArrayList();
        for (JexlNode child : children(node)) {
            JexlNode copiedChild = (JexlNode) child.jjtAccept(this, data);
            if (copiedChild != null) {
                children.add(copiedChild);
            }
        }
        return children(newNode, children.toArray(new JexlNode[children.size()]));
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTIntegerLiteral node, Object data) {
        ASTNumberLiteral newNode = new ASTNumberLiteral(ParserTreeConstants.JJTNUMBERLITERAL);
        newNode.setNatural(node.getLiteral().toString());
        newNode.jjtSetParent(node.jjtGetParent());
        
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            newNode.jjtAddChild((Node) node.jjtGetChild(i).jjtAccept(this, data), i);
        }
        
        return newNode;
    }
    
    @Override
    public Object visit(ASTFloatLiteral node, Object data) {
        ASTNumberLiteral newNode = new ASTNumberLiteral(ParserTreeConstants.JJTNUMBERLITERAL);
        newNode.setReal(node.getLiteral().toString());
        newNode.jjtSetParent(node.jjtGetParent());
        
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            newNode.jjtAddChild((Node) node.jjtGetChild(i).jjtAccept(this, data), i);
        }
        
        return newNode;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        ASTNumberLiteral newNode = copy(node, data);
        
        if (JexlNodeFactory.NATURAL_NUMBERS.contains(node.getLiteralClass())) {
            newNode.setNatural(node.image);
        } else if (JexlNodeFactory.REAL_NUMBERS.contains(node.getLiteralClass())) {
            newNode.setReal(node.image);
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.ASTNUMBERLITERAL_TYPE_ASCERTAIN_ERROR, MessageFormat.format("Node: {0}", node));
            throw new DatawaveFatalQueryException(qe);
        }
        
        return newNode;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        
        return copy(node, data);
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        
        return copy(node, data);
    }
    
    public Object visit(ASTUnknownFieldERNode node, Object data) {
        return copy(node, data);
    }
    
    public Object visit(ASTUnsatisfiableERNode node, Object data) {
        return copy(node, data);
    }
    
    /**
     * Base setup so that we can eventually move to a more functional model
     */
    @Override
    public ASTJexlScript apply(final ASTJexlScript input) {
        TraceStopwatch stopwatch = null;
        if (null != timers) {
            stopwatch = timers.newStartedStopwatch(scriptName + " - " + description);
        }
        ASTJexlScript script = (ASTJexlScript) input.jjtAccept(this, null);
        if (null != stopwatch)
            stopwatch.stop();
        return script;
    }
    
    public RebuildingVisitor setDescription(String description) {
        this.description = description;
        return this;
    }
    
}
