package datawave.configuration.spring;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionTarget;

/**
 * A provider for allowing injection onto non-CDI-managed beans. We have this provider instead of using
 * {@link org.apache.deltaspike.core.api.provider.BeanProvider} since we can control when this provider is initialized, and need that to be done during the
 * setup of {@link SpringCDIExtension}, which happens before the notification to {@link org.apache.deltaspike.core.api.provider.BeanProvider} happens. Without
 * this, {@link InjectCDIBeanPostProcessor} would not be able to perform injection on non-prototype spring beans.
 */
public class BeanProvider {
    private static BeanProvider instance = null;
    private BeanManager beanManager;
    
    /**
     * Perform CDI injection on the non-managed object {@code bean}.
     * 
     * @param bean
     *            - a bean
     */
    public static void injectFields(Object bean) {
        if (instance == null) {
            throw new IllegalStateException("BeanManager is null. Cannot perform injection.");
        }
        
        BeanManager beanManager = instance.getBeanManager();
        CreationalContext creationalContext = beanManager.createCreationalContext(null);
        AnnotatedType annotatedType = beanManager.createAnnotatedType(bean.getClass());
        InjectionTarget injectionTarget = beanManager.createInjectionTarget(annotatedType);
        // noinspection unchecked
        injectionTarget.inject(bean, creationalContext);
    }
    
    public static boolean isActive() {
        return instance != null;
    }
    
    static void initializeBeanProvider(BeanManager beanManager) {
        instance = new BeanProvider(beanManager);
    }
    
    private BeanProvider(BeanManager beanManager) {
        this.beanManager = beanManager;
    }
    
    public BeanManager getBeanManager() {
        return beanManager;
    }
}
