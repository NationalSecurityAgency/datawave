package datawave.query.jexl.functions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Until vfs-classloader supports dependency injection from a resource we need to statically define our jexl function namespaces
 */
public class JexlFunctionNamespaceRegistry {

    private static final Logger log = Logger.getLogger(JexlFunctionNamespaceRegistry.class);

    public static final String JEXL_FUNCTION_NAMESPACE_CONTEXT = "classpath*:/JexlFunctionNamespaceRegistryContext.xml";
    public static final String JEXL_FUNCTION_NAMESPACE_BEAN_REF = "jexlEngineFunctionMap";
    public static final String JEXL_FUNCTION_NAMESPACE_PROPERTY = "jexl.function.namespace.registry";

    public static final Map<String,Object> registeredFunctions = new HashMap<>();

    static {
        registeredFunctions.put(ContentFunctions.CONTENT_FUNCTION_NAMESPACE, ContentFunctions.class);
        registeredFunctions.put(NormalizationFunctions.NORMALIZATION_FUNCTION_NAMESPACE, NormalizationFunctions.class);
        registeredFunctions.put(EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE, EvaluationPhaseFilterFunctions.class);
        registeredFunctions.put(GroupingRequiredFilterFunctions.GROUPING_REQUIRED_FUNCTION_NAMESPACE, GroupingRequiredFilterFunctions.class);
    }

    public static Map<String,Object> getConfiguredFunctions() {
        ClassLoader thisClassLoader = JexlFunctionNamespaceRegistry.class.getClassLoader();
        if (thisClassLoader instanceof VFSClassLoader) {
            log.debug("thisClassLoader is a VFSClassLoader with resources:" + Arrays.toString(((VFSClassLoader) thisClassLoader).getFileObjects()));
        } else {
            log.debug("thisClassLoader is a :" + thisClassLoader.getClass());
        }
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext();
        try {
            // To prevent failure when this is run on the tservers:
            // The VFS ClassLoader has been created and has been made the current thread's context classloader, but its resource paths are empty at this time.
            // The spring ApplicationContext will prefer the current thread's context classloader, so the spring context would fail to find
            // any classes or context files to load.
            // Instead, set the classloader on the ApplicationContext to be the one that is loading this class.
            // It is a VFSClassLoader that has the accumulo lib/ext jars set as its resources.
            // After setting the classloader, then set the config locations and refresh the context.
            context.setClassLoader(thisClassLoader);
            context.setConfigLocation(JEXL_FUNCTION_NAMESPACE_CONTEXT);
            context.refresh();
            // If the vfs classloader works with Spring resource dependency injection, we can pull the configured map
            // from the application context
            Map<String,String> functionDefs = context.getBean(JEXL_FUNCTION_NAMESPACE_BEAN_REF, java.util.Map.class); // setRegisteredFunctions gets called with
            // String,String map
            Map<String,Object> funcs = new HashMap<>();
            for (Map.Entry<String,String> entry : functionDefs.entrySet()) {
                try {

                    Class clazz = Class.forName(entry.getValue());
                    funcs.put(entry.getKey(), clazz);
                } catch (ClassNotFoundException e) {
                    log.error(JexlFunctionNamespaceRegistry.class.getName() + " could not load function. " + e);
                }
            }

            return funcs;
        } catch (Throwable t) {
            log.error(t);
            // If vfs classloader could not populate the bean, lets fail back to loading via system property and generate
            // a populated helper class through reflection
            String className = System.getProperty(JEXL_FUNCTION_NAMESPACE_PROPERTY);
            log.debug(JEXL_FUNCTION_NAMESPACE_PROPERTY + "=" + className);
            if (className != null) {
                try {
                    JexlFunctionNamespaceRegistry base = (JexlFunctionNamespaceRegistry) Class.forName(className).newInstance();
                    return base.getRegisteredFunctions();

                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    log.warn("unable to create " + className + " with property " + JEXL_FUNCTION_NAMESPACE_PROPERTY, e);
                    throw new RuntimeException("Could not create Jexl Function Registry object");
                }
            } else {
                log.warn("className was null, unable to " + className + " with property " + JEXL_FUNCTION_NAMESPACE_PROPERTY);
                throw new RuntimeException("Could not create Jexl Function Registry object");
            }
        } finally {
            context.close();
        }
    }

    public Map<String,Object> getRegisteredFunctions() {
        return Collections.unmodifiableMap(registeredFunctions);
    }

}
