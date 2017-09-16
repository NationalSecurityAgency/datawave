package datawave.query.postprocessing.tf;

import java.util.Collection;
import java.util.LinkedList;

import datawave.query.jexl.visitors.BaseVisitor;

import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * A visitor to find all functions in an expression/script. After running the visitor on an AST, clients should call functions(), which will return a mapping of
 * namespaces to functions.
 */
public class FunctionReferenceVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(FunctionReferenceVisitor.class);
    
    private final Multimap<String,Function> functions;
    
    public FunctionReferenceVisitor() {
        functions = ArrayListMultimap.create();
    }
    
    public Multimap<String,Function> functions() {
        return functions;
    }
    
    /**
     * This method attempts to build a Function object and map it to a namespace.
     * 
     * The supplied node must have at least three children- a namespace, a function name and at least one argument. The namespace and function name are readily
     * available as the image values of the first two children.
     * 
     * The arguments are accessed and aggregated by passing a linked list onto the children via their visit method.
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        final int nChildren = node.jjtGetNumChildren();
        if (nChildren < 3) {
            log.error("Function node does have 3 children-- must supply a namespace, function name and at least one argument.");
        } else {
            int child = 0;
            String namespace = node.jjtGetChild(child++).image;
            String functionName = node.jjtGetChild(child++).image;
            LinkedList<String> args = Lists.newLinkedList();
            for (; child < node.jjtGetNumChildren(); ++child) {
                node.jjtGetChild(child).jjtAccept(this, args);
            }
            functions.put(namespace, new Function(functionName, args));
        }
        return null;
    }
    
    /**
     * If data is not null and a collection, this method will add the node's image value to the collection.
     */
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        if (data != null && data instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<String> collection = (Collection<String>) data;
            collection.add(node.image);
        }
        return null;
    }
    
    /**
     * If data is not null and a collection, this method will add the node's image value to the collection.
     */
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        if (data != null && data instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<String> collection = (Collection<String>) data;
            collection.add(node.image);
        }
        return null;
    }
    
    /**
     * If data is not null and a collection, this method will add the node's image value to the collection.
     */
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        if (data != null && data instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<String> collection = (Collection<String>) data;
            collection.add(node.image);
        }
        return null;
    }
}
