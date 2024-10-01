package datawave.query.jexl.functions;

import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTNamespaceIdentifier;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

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
                throw new IllegalArgumentException(
                                "Calling " + JexlFunctionArgumentDescriptorFactory.class.getSimpleName() + ".getArgumentDescriptor with a null tree node");

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
                        BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_SYNTAX_PARSE_ERROR,
                                        "Unable to get jexl function argument descriptor");
                        throw new IllegalArgumentException(qe);
                    }
                }
            }
            return null;
        }

        /*
         * Uses the FunctionReferenceVisitor to try and get the namespace from a function node
         */
        public static Class<?> extractFunctionClass(ASTFunctionNode node) {
            if (node.jjtGetNumChildren() == 2 && node.jjtGetChild(0) instanceof ASTNamespaceIdentifier && node.jjtGetChild(1) instanceof ASTArguments) {
                ASTNamespaceIdentifier namespaceNode = (ASTNamespaceIdentifier) node.jjtGetChild(0);

                Object possibleMatch = ArithmeticJexlEngines.functions().get(namespaceNode.getNamespace());
                if (possibleMatch instanceof Class<?>) {
                    return (Class<?>) possibleMatch;
                } else {
                    throw new IllegalArgumentException("Found a possible match in namespace " + namespaceNode.getNamespace() + " was not a class, was a "
                                    + (possibleMatch != null ? possibleMatch.getClass() : null));
                }
            } else {
                QueryException qe = new QueryException(DatawaveErrorCode.NODE_PROCESSING_ERROR, "Expected children not found in ASTFunctionNode");
                throw new IllegalArgumentException(qe);
            }
        }
    }
}
