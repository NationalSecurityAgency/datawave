package datawave.configuration.spring;

import java.util.Collection;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.PropertyValues;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.CommonAnnotationBeanPostProcessor;

public class CustomCommonAnnotationBeanPostProcessor extends CommonAnnotationBeanPostProcessor {

    private Logger log = LoggerFactory.getLogger(getClass());
    private Collection<String> postConstructExcludeByRegex = new HashSet<>();
    private Collection<String> preDestroyExcludeByRegex = new HashSet<>();
    private Collection<String> processPropertiesExcludeByRegex = new HashSet<>();
    private Collection<String> ignoreResourceTypes = new HashSet<>();

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        log.info("Bean name:{} class:{} in postProcessBeforeInitialization", beanName, bean.getClass().getCanonicalName());
        if (processBeanPostConstruct(bean.getClass())) {
            return super.postProcessBeforeInitialization(bean, beanName);
        } else {
            return bean;
        }
    }

    @Override
    public void postProcessBeforeDestruction(Object bean, String beanName) throws BeansException {
        log.info("Bean name:{} class:{} in postProcessBeforeDestruction", beanName, bean.getClass().getCanonicalName());
        if (!processBeanPreDestroy(bean.getClass())) {
            super.postProcessBeforeDestruction(bean, beanName);
        }
    }

    @Override
    public PropertyValues postProcessProperties(PropertyValues pvs, Object bean, String beanName) {
        log.info("Bean name:{} class:{} in postProcessProperties", beanName, bean.getClass().getCanonicalName());
        if (!processBeanProperties(bean.getClass())) {
            return super.postProcessProperties(pvs, bean, beanName);
        } else {
            return pvs;
        }
    }

    @Override
    public void postProcessMergedBeanDefinition(RootBeanDefinition beanDefinition, Class<?> beanType, String beanName) {
        log.info("Bean name:{} class:{} in postProcessMergedBeanDefinition", beanName, beanType.getCanonicalName());
        super.postProcessMergedBeanDefinition(beanDefinition, beanType, beanName);
    }

    @Override
    protected Object autowireResource(BeanFactory factory, LookupElement element, String requestingBeanName) throws NoSuchBeanDefinitionException {
        return super.autowireResource(factory, element, requestingBeanName);
    }

    private boolean processBeanPostConstruct(Class clazz) {
        String className = clazz.getCanonicalName();
        for (String exclude : postConstructExcludeByRegex) {
            if (className.matches(exclude)) {
                log.warn("Excluded bean from postConstruct by class:{}", className);
                return false;
            }
        }
        return true;
    }

    private boolean processBeanPreDestroy(Class clazz) {
        String className = clazz.getCanonicalName();
        for (String exclude : preDestroyExcludeByRegex) {
            if (className.matches(exclude)) {
                log.warn("Excluded bean from preDestroy by class:{}", className);
                return false;
            }
        }
        return true;
    }

    private boolean processBeanProperties(Class clazz) {
        String className = clazz.getCanonicalName();
        for (String exclude : processPropertiesExcludeByRegex) {
            if (className.matches(exclude)) {
                log.warn("Excluded bean from processProperties by class:{}", className);
                return false;
            }
        }
        return true;
    }

    public void setPostConstructExcludeByRegex(Collection<String> postConstructExcludeByRegex) {
        this.postConstructExcludeByRegex = postConstructExcludeByRegex;
    }

    public void setPreDestroyExcludeByRegex(Collection<String> preDestroyExcludeByRegex) {
        this.preDestroyExcludeByRegex = preDestroyExcludeByRegex;
    }

    public void setProcessPropertiesExcludeByRegex(Collection<String> processPropertiesExcludeByRegex) {
        this.processPropertiesExcludeByRegex = processPropertiesExcludeByRegex;
    }

    public void setIgnoreResourceTypes(Collection<String> ignoreResourceTypes) {
        ignoreResourceTypes.forEach(ignoreResourceType -> {
            ignoreResourceType(ignoreResourceType);
        });
    }
}
