package datawave.query.postprocessing.tf;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTNamespaceIdentifier;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import datawave.query.jexl.visitors.BaseVisitor;

/**
 * A visitor that finds all functions in an expression/script. Clients may supply an optional namespace filter to limit the number of returned functions.
 * <p>
 * <code>FunctionReferenceVisitor.functions(script)</code>
 */
public class FunctionReferenceVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(FunctionReferenceVisitor.class);

    private final Set<String> namespaceFilter;
    private final Multimap<String,Function> functions;

    /**
     * Default constructor that delegates to {@link #FunctionReferenceVisitor(Set)}
     */
    private FunctionReferenceVisitor() {
        this(Collections.emptySet());
    }

    /**
     * Constructor that accepts a namespace filter
     *
     * @param namespaceFilter
     *            a set of namespaces
     */
    private FunctionReferenceVisitor(Set<String> namespaceFilter) {
        this.functions = ArrayListMultimap.create();
        this.namespaceFilter = namespaceFilter;
    }

    /**
     * Static entrypoint. Caller provides a JexlNode
     *
     * @param node
     *            a node in a query tree
     * @return a mapping of namespaces to functions
     */
    public static Multimap<String,Function> functions(JexlNode node) {
        FunctionReferenceVisitor visitor = new FunctionReferenceVisitor();
        node.jjtAccept(visitor, null);
        return visitor.functions();
    }

    /**
     * Static entrypoint. Caller provides a JexlNode and a namespace filter.
     *
     * @param node
     *            a node in the query tree
     * @param namespaceFilter
     *            a filter that limits the returned functions
     * @return a mapping of namespaces to functions
     */
    public static Multimap<String,Function> functions(JexlNode node, Set<String> namespaceFilter) {
        FunctionReferenceVisitor visitor = new FunctionReferenceVisitor(namespaceFilter);
        node.jjtAccept(visitor, null);
        return visitor.functions();
    }

    /**
     * Getter for the function multimap
     *
     * @return a Multimap of functions
     */
    public Multimap<String,Function> functions() {
        return functions;
    }

    /**
     * This method attempts to build a Function object and map it to a namespace.
     * <p>
     * The supplied node must have at least three children- a namespace, a function name and at least one argument. The namespace and function name are readily
     * available as the image values of the first two children.
     * <p>
     * The arguments are accessed and aggregated by passing a linked list onto the children via their visit method.
     *
     * @param node
     *            an {@link ASTFunctionNode}
     * @param data
     *            an Object, unused
     * @return null
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        final int nChildren = node.jjtGetNumChildren();
        if (nChildren < 2) {
            log.error("Function node does have 2 children-- must supply an ASTNamespaceIdentifier, and ASTArguments.");
            return null;
        }

        ASTNamespaceIdentifier namespaceNode = (ASTNamespaceIdentifier) node.jjtGetChild(0);
        ASTArguments argNodes = (ASTArguments) node.jjtGetChild(1);
        LinkedList<JexlNode> args = Lists.newLinkedList();
        for (int i = 0; i < argNodes.jjtGetNumChildren(); i++) {
            args.add(argNodes.jjtGetChild(i));
        }

        if (namespaceFilter.isEmpty() || namespaceFilter.contains(namespaceNode.getNamespace())) {
            functions.put(namespaceNode.getNamespace(), new Function(namespaceNode.getName(), args));
        }

        return null;
    }
}
