package datawave.marking;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import datawave.configuration.spring.SpringBean;

/**
 * This version of the MarkingFunctionsFactory is to be used on the webservers, and is optimized to load the application context via CDI when possible.
 *
 */
@ApplicationScoped
public class MarkingFunctionsFactory {

    @Inject
    @SpringBean(refreshable = true)
    private MarkingFunctions applicationMarkingFunctions;

    public static final Logger log = LoggerFactory.getLogger(MarkingFunctionsFactory.class);

    private static MarkingFunctions markingFunctions;

    public static synchronized MarkingFunctions createMarkingFunctions() {
        if (markingFunctions != null)
            return markingFunctions;

        ClassLoader thisClassLoader = MarkingFunctionsFactory.class.getClassLoader();

        // ignore calls to close as this blows away the cache manager
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext();
        try {
            context.setClassLoader(thisClassLoader);
            context.setConfigLocations("classpath*:/MarkingFunctionsContext.xml", "classpath*:/CacheContext.xml");
            context.refresh();
            markingFunctions = context.getBean("markingFunctions", MarkingFunctions.class);
        } catch (Throwable t) {
            log.warn("Could not load spring context files! Got " + t);
            if (log.isDebugEnabled()) {
                log.debug("Failed to load Spring contexts", t);
            }
        } finally {
            context.close();
        }

        return markingFunctions;
    }

    public void init(@SuppressWarnings("UnusedParameters") @Observes @Initialized(ApplicationScoped.class) Object init) {
        // Nothing to do here. Observing the initialization of the ApplicationScoped scope will in turn cause this
        // bean to be instantiated at application startup. However, this happens once per war in an ear, and since
        // we might have several, we use to post construct of this bean to ensure our work is only done once.
    }

    @PostConstruct
    public void postContruct() {
        markingFunctions = applicationMarkingFunctions;
    }
}
