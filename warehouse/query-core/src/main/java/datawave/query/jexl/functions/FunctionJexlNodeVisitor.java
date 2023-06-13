package datawave.query.jexl.functions;

import datawave.query.jexl.visitors.BaseVisitor;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTNamespaceIdentifier;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Reusable visitor to getting common function JexlNodes.
 *
 */
public class FunctionJexlNodeVisitor extends BaseVisitor {
    private List<JexlNode> args;
    private String namespace, name;
    
    public List<JexlNode> args() {
        return args;
    }
    
    public String namespace() {
        return namespace;
    }
    
    public String name() {
        return name;
    }
    
    public static FunctionJexlNodeVisitor eval(JexlNode node) {
        FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
        node.jjtAccept(visitor, null);
        return visitor;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        if (node.jjtGetNumChildren() == 2 && node.jjtGetChild(0) instanceof ASTNamespaceIdentifier && node.jjtGetChild(1) instanceof ASTArguments) {
            ASTNamespaceIdentifier namespaceNode = (ASTNamespaceIdentifier) node.jjtGetChild(0);
            this.namespace = namespaceNode.getNamespace();
            this.name = namespaceNode.getName();
            
            ASTArguments argsNode = (ASTArguments) node.jjtGetChild(1);
            JexlNode[] args = new JexlNode[argsNode.jjtGetNumChildren()];
            for (int i = 0; i < argsNode.jjtGetNumChildren(); i++) {
                args[i] = argsNode.jjtGetChild(i);
            }
            
            this.args = Collections.unmodifiableList(Arrays.asList(args));
        }
        return null;
    }
    
    public static ASTFunctionNode makeFunctionFrom(String ns, String functionName, JexlNode... arguments) {
        return JexlNodes.makeFunction(ns, functionName, arguments);
    }
}
