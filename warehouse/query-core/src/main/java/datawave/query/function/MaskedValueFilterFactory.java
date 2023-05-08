package datawave.query.function;

import java.util.Arrays;

import org.apache.commons.vfs2.impl.VFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class MaskedValueFilterFactory {
    
    public static final Logger log = LoggerFactory.getLogger(MaskedValueFilterFactory.class);
    public static final String MASKED_VALUE_FILTER_CLASSNAME = "masked.value.filter.classname";
    
    private static MaskedValueFilterInterface[] maskedValueFilters;
    
    static {
        initializeCache();
    }
    
    private static MaskedValueFilterInterface createInstance() {
        
        ClassLoader thisClassLoader = MaskedValueFilterFactory.class.getClassLoader();
        if (log.isDebugEnabled()) {
            if (thisClassLoader instanceof VFSClassLoader) {
                log.debug("thisClassLoader is a VFSClassLoader with resources:" + Arrays.toString(((VFSClassLoader) thisClassLoader).getFileObjects()));
            } else {
                log.debug("thisClassLoader is a :" + thisClassLoader.getClass());
            }
        }
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext();
        MaskedValueFilterInterface instance = null;
        try {
            // To prevent failure when this is run on the tservers:
            // The VFS ClassLoader has been created and has been made the current thread's context classloader, but its resource paths are empty at this time.
            // The spring ApplicationContext will prefer the current thread's context classloader, so the spring context would fail to find
            // any classes or context files to load.
            // Instead, set the classloader on the ApplicationContext to be the one that is loading this class.
            // It is a VFSClassLoader that has the accumulo lib/ext jars set as its resources.
            // After setting the classloader, then set the config locations and refresh the context.
            context.setClassLoader(thisClassLoader);
            context.setConfigLocation("classpath*:/MaskingFilterContext.xml");
            context.refresh();
            instance = context.getBean("maskedValueFilter", MaskedValueFilterInterface.class);
        } catch (Throwable t) {
            // got here because the VFSClassLoader on the tservers does not implement findResources
            // none of the spring wiring will work, so fall back to reflection:
            log.warn("Failed to get maskedValueFilter bean from application context", t);
            String className = System.getProperty(MASKED_VALUE_FILTER_CLASSNAME);
            if (className != null) {
                log.warn("Attempting to instantiate masked value filter from -D{}={}", MASKED_VALUE_FILTER_CLASSNAME, className);
                try {
                    instance = (MaskedValueFilterInterface) Class.forName(className).newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    log.warn("Failed to create MaskedValueFilterInterface from {}", e);
                    throw new RuntimeException("Could not create MaskedValueFilterInterface object");
                }
            } else {
                log.warn("className was null, unable to create {}", MASKED_VALUE_FILTER_CLASSNAME);
            }
            log.debug("{}: {}", MASKED_VALUE_FILTER_CLASSNAME, (null != instance ? instance : "null"));
        } finally {
            try {
                context.close();
            } catch (Exception e) {
                log.warn("Failed to close application context", e);
            }
        }
        return instance;
    }
    
    public static MaskedValueFilterInterface get(boolean includeGroupingContext, boolean reducedResponse) {
        int index = 2 * (includeGroupingContext ? 1 : 0) + (reducedResponse ? 1 : 0);
        if (null == maskedValueFilters) {
            synchronized (maskedValueFilters) {
                if (null == maskedValueFilters) {
                    initializeCache();
                }
            }
        }
        return maskedValueFilters[index];
    }
    
    private static void initializeCache() {
        maskedValueFilters = new MaskedValueFilterInterface[] {createInstance(false, false), createInstance(false, true), createInstance(true, false),
                createInstance(true, true)};
    }
    
    private static MaskedValueFilterInterface createInstance(boolean includeGroupingContext, boolean reducedResponse) {
        MaskedValueFilterInterface result = createInstance();
        result.setIncludeGroupingContext(includeGroupingContext);
        result.setReducedResponse(reducedResponse);
        return result;
    }
}
