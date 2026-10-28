package datawave.configuration.spring;

import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionTarget;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A provider for allowing injection onto non-CDI-managed beans. We have this provider instead of using
 * {@link org.apache.deltaspike.core.api.provider.BeanProvider} since we can control when this provider is initialized, and need that to be done during the
 * setup of {@link SpringCDIExtension}, which happens before the notification to {@link org.apache.deltaspike.core.api.provider.BeanProvider} happens. Without
 * this, {@link InjectCDIBeanPostProcessor} would not be able to perform injection on non-prototype spring beans.
 */
public class BeanProvider {
    private static Logger log = LoggerFactory.getLogger(BeanProvider.class);
    private static Map<String,BeanManager> beanManagerMap = new HashMap<>();

    /**
     * Perform CDI injection on the non-managed object {@code bean}.
     *
     * @param bean
     *            - a bean
     */
    public static InjectionTarget injectFields(String id, Object bean) {
        BeanManager beanManager;
        if (id == null) {
            if (beanManagerMap.size() == 1) {
                beanManager = beanManagerMap.values().iterator().next();
            } else {
                throw new IllegalArgumentException("Requested default BeanManager, but there are " + beanManagerMap.size() + " BeanManagers available.");
            }
        }
        if (beanManagerMap.containsKey(id)) {
            beanManager = beanManagerMap.get(id);
        } else {
            throw new IllegalArgumentException("BeanManager with id " + id + " not found.");
        }

        CreationalContext creationalContext = beanManager.createCreationalContext(null);
        AnnotatedType annotatedType = beanManager.createAnnotatedType(bean.getClass());
        InjectionTarget injectionTarget = beanManager.createInjectionTarget(annotatedType);
        if (log.isDebugEnabled()) {
            log.debug("Injecting bean " + bean.getClass() + " at injection points " + injectionTarget.getInjectionPoints());
        }
        // noinspection unchecked
        injectionTarget.inject(bean, creationalContext);
        return injectionTarget;
    }

    public static boolean isActive(String id) {
        return beanManagerMap.containsKey(id);
    }

    public static void registerBeanManager(String id, BeanManager beanManager) {
        if (beanManager != null) {
            beanManagerMap.put(id, beanManager);
        }
    }
}
