package datawave.query.jexl;

import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
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
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
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
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * A builder class that supports fluent-style building of {@link JexlNode} instances.
 * 
 * @param <T>
 *            the {@link JexlNode} type
 */
public class JexlNodeBuilder<T extends JexlNode> {
    
    private static final Logger log = Logger.getLogger(JexlNodeBuilder.class);
    
    private final Supplier<T> instanceSupplier;
    private final List<Supplier<? extends JexlNode>> children = new ArrayList<>();
    private String image;
    private Object value;
    private boolean throwOnNullChild = true;
    
    public JexlNodeBuilder(JexlNodeInstance<T> instance) {
        this.instanceSupplier = instance.getConstructor();
    }
    
    /**
     * Set whether an exception should be thrown when a null child is supplied to this builder. By default, this is true. This value impacts behavior when
     * adding children to this builder only, and will never result in null children being included in the resulting node from {@link #build()}.
     * @param throwOnNullChild whether to throw an exception
     * @return this builder
     */
    public JexlNodeBuilder<T> throwOnNullChild(boolean throwOnNullChild) {
        this.throwOnNullChild = throwOnNullChild;
        return this;
    }
    
    /**
     * Set the image for the node this builder will create.
     * 
     * @param image
     *            the image
     * @return this builder
     */
    public JexlNodeBuilder<T> withImage(String image) {
        this.image = image;
        return this;
    }
    
    /**
     * Set the value for the node this builder will create.
     * 
     * @param value
     *            the value
     * @return this builder
     */
    public JexlNodeBuilder<T> withValue(Object value) {
        this.value = value;
        return this;
    }
    
    /**
     * Add a child node to this builder. Children in the resulting node from {@link #build()} will be in the order they're added.
     * 
     * @param node
     *            the child node
     * @return this builder
     */
    public JexlNodeBuilder<T> withChild(JexlNode node) {
        if (isNonNullChild(node)) {
            children.add(() -> node);
        }
        return this;
    }
    
    /**
     * Add each node in the provided vararg as a child to this builder. Children in the resulting node from {@link #build()} will be in the order they're added.
     * 
     * @param children
     *            the children nodes
     * @return this builder
     */
    public JexlNodeBuilder<T> withChildren(JexlNode... children) {
        for (JexlNode child : children) {
            withChild(child);
        }
        return this;
    }
    
    /**
     * Add each node in the provided iterable as a child to this builder. Children in the resulting node from {@link #build()} will be in the order they're
     * added.
     * 
     * @param children
     *            the children nodes
     * @return this builder
     */
    public JexlNodeBuilder<T> withChildren(Iterable<? extends JexlNode> children) {
        children.forEach(this::withChild);
        return this;
    }
    
    /**
     * Add a child node builder to this builder. The final child will be built when {@link JexlNodeBuilder#build()} is called on this builder. Children in the
     * resulting node from {@link #build()} will be in the order they're added.
     * 
     * @param builder
     *            the builder
     * @return this builder
     */
    public JexlNodeBuilder<T> withChild(JexlNodeBuilder<?> builder) {
        if (isNonNullChild(builder)) {
            children.add(builder::build);
        }
        return this;
    }
    
    /**
     * Return whether the given child is null, and if so, throw an exception if {@link #throwOnNullChild} is true.
     * @param child the child to evaluate
     * @return true if the child is not null or false otherwise
     * @throws IllegalArgumentException if the child is null and {@link #throwOnNullChild} is true for this builder
     */
    private boolean isNonNullChild(Object child) {
        if (child != null) {
            return true;
        } else {
            if (throwOnNullChild) {
                throw new IllegalArgumentException("Child must not be null");
            } else {
                log.warn("Attempted to add null child to " + JexlNodeBuilder.class.getSimpleName());
                return false;
            }
        }
    }
    
    /**
     * Build and return the new {@link JexlNode} instance with the specified image, value, and children. Any child nodes or child node suppliers that were
     * provided to this builder that are either null or result in null nodes will not be included in the node's children.
     * 
     * @return the node
     */
    public T build() {
        // Create the initial node.
        T node = instanceSupplier.get();
        node.image = this.image;
        node.jjtSetValue(this.value);
        
        // Create the finalized children.
        // @formatter:off
        JexlNode[] childArray = children.stream()
                        .map(Supplier::get)       // Get the actual child nodes.
                        .toArray(JexlNode[]::new);
        // @formatter:on
        
        // Set the children for the node.
        JexlNodes.children(node, childArray);
        return node;
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link JexlNode} of the same type as the provided node instance.
     * 
     * @param node
     *            the node
     * @return the new builder
     */
    public static JexlNodeBuilder<?> forType(JexlNode node) {
        return of(JexlNodeInstance.forType(node));
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link JexlNode} of the provided type.
     * 
     * @param type
     *            the type
     * @return the new builder
     */
    public static JexlNodeBuilder<?> forType(Class<? extends JexlNode> type) {
        return of(JexlNodeInstance.forType(type));
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link JexlNode} of the type specified by the provided {@link JexlNodeInstance}.
     * 
     * @param instance
     *            the instance
     * @param <T>
     *            the target {@link JexlNode} type
     * @return the new builder
     */
    public static <T extends JexlNode> JexlNodeBuilder<T> of(JexlNodeInstance<T> instance) {
        return new JexlNodeBuilder<>(instance);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTJexlScript} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTJexlScript> ASTJexlScript() {
        return of(JexlNodeInstance.ASTJexlScript);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTBlock} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTBlock> ASTBlock() {
        return of(JexlNodeInstance.ASTBlock);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTAmbiguous} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTAmbiguous> ASTAmbiguous() {
        return of(JexlNodeInstance.ASTAmbiguous);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTIfStatement} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTIfStatement> ASTIfStatement() {
        return of(JexlNodeInstance.ASTIfStatement);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTWhileStatement} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTWhileStatement> ASTWhileStatement() {
        return of(JexlNodeInstance.ASTWhileStatement);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTForeachStatement} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTForeachStatement> ASTForeachStatement() {
        return of(JexlNodeInstance.ASTForeachStatement);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTReturnStatement} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTReturnStatement> ASTReturnStatement() {
        return of(JexlNodeInstance.ASTReturnStatement);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTAssignment} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTAssignment> ASTAssignment() {
        return of(JexlNodeInstance.ASTAssignment);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTVar} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTVar> ASTVar() {
        return of(JexlNodeInstance.ASTVar);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTReference} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTReference> ASTReference() {
        return of(JexlNodeInstance.ASTReference);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTTernaryNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTTernaryNode> ASTTernaryNode() {
        return of(JexlNodeInstance.ASTTernaryNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTOrNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTOrNode> ASTOrNode() {
        return of(JexlNodeInstance.ASTOrNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTAndNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTAndNode> ASTAndNode() {
        return of(JexlNodeInstance.ASTAndNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTBitwiseOrNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTBitwiseOrNode> ASTBitwiseOrNode() {
        return of(JexlNodeInstance.ASTBitwiseOrNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTBitwiseXorNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTBitwiseXorNode> ASTBitwiseXorNode() {
        return of(JexlNodeInstance.ASTBitwiseXorNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTBitwiseAndNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTBitwiseAndNode> ASTBitwiseAndNode() {
        return of(JexlNodeInstance.ASTBitwiseAndNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTEQNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTEQNode> ASTEQNode() {
        return of(JexlNodeInstance.ASTEQNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTNENode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTNENode> ASTNENode() {
        return of(JexlNodeInstance.ASTNENode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTLTNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTLTNode> ASTLTNode() {
        return of(JexlNodeInstance.ASTLTNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTGTNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTGTNode> ASTGTNode() {
        return of(JexlNodeInstance.ASTGTNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTLENode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTLENode> ASTLENode() {
        return of(JexlNodeInstance.ASTLENode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTGENode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTGENode> ASTGENode() {
        return of(JexlNodeInstance.ASTGENode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTERNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTERNode> ASTERNode() {
        return of(JexlNodeInstance.ASTERNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTNRNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTNRNode> ASTNRNode() {
        return of(JexlNodeInstance.ASTNRNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTAdditiveNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTAdditiveNode> ASTAdditiveNode() {
        return of(JexlNodeInstance.ASTAdditiveNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTAdditiveOperator} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTAdditiveOperator> ASTAdditiveOperator() {
        return of(JexlNodeInstance.ASTAdditiveOperator);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTMulNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTMulNode> ASTMulNode() {
        return of(JexlNodeInstance.ASTMulNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTDivNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTDivNode> ASTDivNode() {
        return of(JexlNodeInstance.ASTDivNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTModNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTModNode> ASTModNode() {
        return of(JexlNodeInstance.ASTModNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTUnaryMinusNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTUnaryMinusNode> ASTUnaryMinusNode() {
        return of(JexlNodeInstance.ASTUnaryMinusNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTBitwiseComplNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTBitwiseComplNode> ASTBitwiseComplNode() {
        return of(JexlNodeInstance.ASTBitwiseComplNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTNotNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTNotNode> ASTNotNode() {
        return of(JexlNodeInstance.ASTNotNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTIdentifier} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTIdentifier> ASTIdentifier() {
        return of(JexlNodeInstance.ASTIdentifier);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTNullLiteral} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTNullLiteral> ASTNullLiteral() {
        return of(JexlNodeInstance.ASTNullLiteral);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTTrueNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTTrueNode> ASTTrueNode() {
        return of(JexlNodeInstance.ASTTrueNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTFalseNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTFalseNode> ASTFalseNode() {
        return of(JexlNodeInstance.ASTFalseNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTNumberLiteral} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTNumberLiteral> ASTNumberLiteral() {
        return of(JexlNodeInstance.ASTNumberLiteral);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTStringLiteral} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTStringLiteral> ASTStringLiteral() {
        return of(JexlNodeInstance.ASTStringLiteral);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTMapEntry} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTMapEntry> ASTMapEntry() {
        return of(JexlNodeInstance.ASTMapEntry);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTEmptyFunction} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTEmptyFunction> ASTEmptyFunction() {
        return of(JexlNodeInstance.ASTEmptyFunction);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTSizeFunction} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTSizeFunction> ASTSizeFunction() {
        return of(JexlNodeInstance.ASTSizeFunction);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTFunctionNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTFunctionNode> ASTFunctionNode() {
        return of(JexlNodeInstance.ASTFunctionNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTMethodNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTMethodNode> ASTMethodNode() {
        return of(JexlNodeInstance.ASTMethodNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTSizeMethod} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTSizeMethod> ASTSizeMethod() {
        return of(JexlNodeInstance.ASTSizeMethod);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTConstructorNode} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTConstructorNode> ASTConstructorNode() {
        return of(JexlNodeInstance.ASTConstructorNode);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTArrayAccess} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTArrayAccess> ASTArrayAccess() {
        return of(JexlNodeInstance.ASTArrayAccess);
    }
    
    /**
     * Return a new {@link JexlNodeBuilder} that will create a {@link ASTReferenceExpression} instance.
     * 
     * @return the builder
     */
    public static JexlNodeBuilder<ASTReferenceExpression> ASTReferenceExpression() {
        return of(JexlNodeInstance.ASTReferenceExpression);
    }
}
