package datawave.query.jexl.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.BaseVisitor;

import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

import com.google.common.collect.Lists;

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
        int childN = 0;
        this.namespace = node.jjtGetChild(childN++).image;
        this.name = node.jjtGetChild(childN++).image;

        JexlNode[] args = new JexlNode[node.jjtGetNumChildren() - 2];
        for (int i = childN; i < node.jjtGetNumChildren(); i++) {
            args[i - childN] = JexlASTHelper.dereference(node.jjtGetChild(i));
        }
        this.args = Collections.unmodifiableList(Arrays.asList(args));

        return null;
    }

    public static ASTFunctionNode makeFunctionFrom(String ns, String functionName, JexlNode... arguments) {
        ASTFunctionNode fn = new ASTFunctionNode(ParserTreeConstants.JJTFUNCTIONNODE);
        ASTIdentifier namespace = JexlNodes.makeIdentifierWithImage(ns);
        ASTIdentifier function = JexlNodes.makeIdentifierWithImage(functionName);
        ArrayList<JexlNode> nodes = Lists.newArrayList();
        nodes.add(namespace);
        nodes.add(function);
        Collections.addAll(nodes, arguments);
        return JexlNodes.children(fn, nodes.toArray(new JexlNode[nodes.size()]));
    }
}
