package datawave.configuration.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

/**
 * A Spring bean post-processor to ensure that CDI injection is performed on beans created by Spring. That is, if a bean created by Spring uses @Inject, then we
 * want to be sure and satisfy the injection using CDI.
 */
public class InjectCDIBeanPostProcessor implements BeanPostProcessor {
    private Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Object beanToInject = bean;
        if (BeanProvider.isActive()) {
            // Spring may wrap objects in a proxy, for example if a bean is marked as cached. In this case, the proxy
            // will not have the fields we want to (potentially) inject. Note that we are injecting by directly modifying
            // fields, not calling setter methods that the proxy object could wrap. Instead, attempt to find the source
            // object that the proxy is wrapping and perform the injection on that object instead. Log a warning if we
            // can't find the source object.
            if (AopUtils.isAopProxy(bean)) {
                boolean updated = false;
                if (bean instanceof Advised) {
                    TargetSource source = ((Advised) bean).getTargetSource();
                    if (source != null) {
                        try {
                            beanToInject = source.getTarget();
                            updated = true;
                        } catch (Exception e) {
                            // ignore -- we'll log below
                        }
                    }
                }
                if (!updated) {
                    log.warn("Unable to retrieve target from proxy bean {}. Injection will not be performed for bean {}.", bean, beanName);
                }
            }
            BeanProvider.injectFields(beanToInject);
        } else {
            log.warn("BeanProvider not initialized yet. Non-prototype beans will not have dependency injection performed on them.");
        }
        return bean;
    }
}
