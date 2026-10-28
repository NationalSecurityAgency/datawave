package datawave.webservice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.ClassUtils;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactoryImpl;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUserService;
import datawave.webservice.common.audit.AuditService;
import datawave.webservice.common.audit.NoOpAuditor;
import datawave.webservice.query.result.event.ResponseObjectFactory;

public class DatawaveResourceFactory implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    /*
     * Get a bean of the given base class, using the implementation class name from the environment. If the fully-qualified implementation class is not found,
     * then get beanNamesForType baseClass and filter by simple class name. If there is exactly one match, return that bean. Otherwise, throw an exception. The
     * nameOfBeanBeingCreated parameter is used to prevent a circular reference.
     */
    @SuppressWarnings("unchecked")
    private Object getBeanOfClass(Class baseClass, String implementationClass, String nameOfBeanBeingCreated) {
        try {
            Class<?> implementation;
            try {
                implementation = ClassUtils.forName(implementationClass, ClassUtils.getDefaultClassLoader());
                if (!baseClass.isAssignableFrom(implementation)) {
                    throw new RuntimeException("class " + implementationClass + " is not a subclass of " + baseClass.getName());
                }
                return applicationContext.getBean(implementation);
            } catch (ClassNotFoundException e) {
                List<String> beanNames = new ArrayList<>(Arrays.asList(BeanFactoryUtils.beanNamesForTypeIncludingAncestors(applicationContext, baseClass)));
                // this prevents a circular reference when the bean being looked up is the same as the bean being created
                beanNames.remove(nameOfBeanBeingCreated);
                // @format:off
                List<String> candidates = beanNames.stream().filter(beanName -> {
                    Class<?> beanType = applicationContext.getType(beanName);
                    return beanType != null && beanType.getSimpleName().equals(implementationClass);
                }).collect(Collectors.toList());
                // @format:on
                if (candidates.size() == 1) {
                    return applicationContext.getBean(candidates.get(0));
                } else if (candidates.size() > 1) {
                    throw new RuntimeException("Multiple beans found for simple class name " + implementationClass + ": " + candidates);
                } else {
                    throw new RuntimeException("No beans found for simple class name " + implementationClass);
                }
            }
        } catch (BeansException e) {
            throw new RuntimeException("bean for class " + implementationClass + " not found", e);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /*
     * These methods can be called from either the ClasspathXmlApplicationContext or AnnotationConfigApplicationContext but the child application context will
     * not have the beans defined in the parent context. Child contexts can see beans in their parent context, but not the other way around.
     */

    public DatawaveUserService datawaveUserService() {
        String className = applicationContext.getEnvironment().getRequiredProperty("datawave.active-implementation.datawave-user-service");
        return (DatawaveUserService) getBeanOfClass(DatawaveUserService.class, className, "datawaveUserService");
    }

    public Optional<CachedDatawaveUserService> cachedDatawaveUserService() {
        String className = applicationContext.getEnvironment().getRequiredProperty("datawave.active-implementation.cached-datawave-user-service");
        try {
            return Optional.of((CachedDatawaveUserService) getBeanOfClass(CachedDatawaveUserService.class, className, "cachedDatawaveUserService"));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public AuditService auditService() {
        String className = applicationContext.getEnvironment().getProperty("datawave.active-implementation.auditor", NoOpAuditor.class.getName());
        return (AuditService) getBeanOfClass(AuditService.class, className, "auditService");
    }

    public ResponseObjectFactory responseObjectFactory() {
        String className = applicationContext.getEnvironment().getProperty("datawave.active-implementation.response-object-factory",
                        DefaultResponseObjectFactory.class.getName());
        return (ResponseObjectFactory) getBeanOfClass(ResponseObjectFactory.class, className, "responseObjectFactory");
    }

    public AccumuloConnectionFactory accumuloConnectionFactory() {
        String className = applicationContext.getEnvironment().getProperty("datawave.active-implementation.accumulo-connection-factory",
                        AccumuloConnectionFactoryImpl.class.getName());
        return (AccumuloConnectionFactory) getBeanOfClass(AccumuloConnectionFactory.class, className, "accumuloConnectionFactory");
    }
}
