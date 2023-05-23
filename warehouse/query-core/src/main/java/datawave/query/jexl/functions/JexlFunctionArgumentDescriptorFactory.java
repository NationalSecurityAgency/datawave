package datawave.query.jexl.functions;

import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.visitors.BaseVisitor;

import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

/**
 * This interface can be implemented by a class supplying JEXL functions to provide additional information about the arguments. The initial purpose of this is
 * to determine ranges from the global index for a given function.
 */
public interface JexlFunctionArgumentDescriptorFactory {
    /**
     * Return an argument descriptor for a given tree node.
     * 
     * @param node
     *            A node that must be for a JEXL function in the implementing class.
     * @return The argument descriptor.
     */
    JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node);
    
    JexlNode TRUE_NODE = new ASTTrueNode(ParserTreeConstants.JJTTRUENODE);
    
    /** An encapsulation of methods that can be used with this interface */
    class F {
        /**
         * A convenience method to get the argument descriptor from a node
         * 
         * @param node
         *            the function node
         * @return the argument descriptor
         */
        public static JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
            if (node == null)
                throw new IllegalArgumentException("Calling " + JexlFunctionArgumentDescriptorFactory.class.getSimpleName()
                                + ".getArgumentDescriptor with a null tree node");
            
            Class<?> funcClass = extractFunctionClass(node);
            
            // find the descriptor annotation
            JexlFunctions d = funcClass.getAnnotation(JexlFunctions.class);
            if (d != null) {
                String factoryClassName = d.descriptorFactory();
                if (factoryClassName != null) {
                    try {
                        @SuppressWarnings("unchecked")
                        Class<? extends JexlFunctionArgumentDescriptorFactory> factoryClass = (Class<? extends JexlFunctionArgumentDescriptorFactory>) (funcClass
                                        .getClassLoader().loadClass(factoryClassName));
                        // get the factory
                        JexlFunctionArgumentDescriptorFactory factory = factoryClass.getDeclaredConstructor().newInstance();
                        // get the descriptor
                        return factory.getArgumentDescriptor(node);
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Unable to get jexl function argument descriptor", e);
                    }
                }
            }
            return null;
        }
        
        /*
         * Uses the FunctionReferenceVisitor to try and get the namespace from a function node
         */
        public static Class<?> extractFunctionClass(ASTFunctionNode node) {
            String namespace = node.jjtGetChild(0).image;
            
            Object possibleMatch = ArithmeticJexlEngines.functions().get(namespace);
            
            if (possibleMatch != null && possibleMatch instanceof Class<?>) {
                return (Class<?>) possibleMatch;
            } else {
                throw new IllegalArgumentException("Found a possible match in namespace " + namespace + " was not a class, was a "
                                + (possibleMatch != null ? possibleMatch.getClass() : null));
            }
        }
    }
}

class FunctionVisitor extends BaseVisitor {
    private ImmutableList.Builder<ASTFunctionNode> functions = ImmutableList.builder();
    
    public ImmutableList<ASTFunctionNode> functions() {
        return functions.build();
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        functions.add(node);
        
        // we may be passed a function as an argument
        node.childrenAccept(this, null);
        return null;
    }
}

class GetNamespace implements Function<ASTFunctionNode,String> {
    @Override
    public String apply(ASTFunctionNode from) {
        return from.jjtGetChild(0).image;
    }
}
