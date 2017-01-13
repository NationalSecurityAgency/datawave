package nsa.datawave.query.functions;

import nsa.datawave.query.functions.arguments.JexlArgumentDescriptor;
import nsa.datawave.query.parser.DatawaveTreeNode;

/**
 * This interface can be implemented by a class supplying JEXL functions to provide additionally information about the arguments. The initial purpose of this is
 * to allow the DatawaveQueryAnalyzer and RangeCalculator to determine ranges from the global index for a given function.
 *
 * 
 *
 */
@Deprecated
public interface JexlFunctionArgumentDescriptorFactory {
    /**
     * Return an argument descriptor for a given tree node.
     *
     * @param node
     *            A node that must be for a JEXL function in the implementing class.
     * @return The argument descriptor.
     */
    public JexlArgumentDescriptor getArgumentDescriptor(DatawaveTreeNode node);
    
    /** An encapsolation of methods that can be used with this interface */
    public static class F {
        /**
         * A convienence method to get the argument descriptor from a node
         */
        public static JexlArgumentDescriptor getArgumentDescriptor(DatawaveTreeNode node) {
            if (node == null)
                throw new IllegalArgumentException("Calling " + JexlFunctionArgumentDescriptorFactory.class.getSimpleName()
                                + ".getArgumentDescriptor with a null tree node");
            if (!node.isFunctionNode())
                throw new IllegalArgumentException("Calling " + JexlFunctionArgumentDescriptorFactory.class.getSimpleName()
                                + ".getArgumentDescriptor with a non-function tree node");
            
            // get the class that maps to this function
            Class<?> funcClass = node.getFunctionClass();
            
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
                        JexlFunctionArgumentDescriptorFactory factory = factoryClass.newInstance();
                        // get the descriptor
                        return factory.getArgumentDescriptor(node);
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Unable to get jexl function argument descriptor: " + e.getMessage(), e);
                    }
                }
            }
            return null;
        }
    }
}
