package datawave.configuration.spring;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.context.NoSuchMessageException;
import org.springframework.core.ResolvableType;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;

/**
 * A delegating wrapper around {@link ConfigurableApplicationContext}. This implements all methods of {@link ConfigurableApplicationContext}, delegating each
 * one to the supplied delegate instance. However, each read method is guarded using the read lock from a supplied {@link ReadWriteLock} and each write method
 * is guarded using the write lock form the supplied {@link ReadWriteLock}.
 */
@SuppressWarnings("unused")
public class ThreadSafeClassPathXmlApplicationContext implements ConfigurableApplicationContext {
    private ConfigurableApplicationContext configurableApplicationContext;
    private ReadWriteLock lock;
    
    public ThreadSafeClassPathXmlApplicationContext(ConfigurableApplicationContext configurableApplicationContext, ReadWriteLock lock) {
        this.configurableApplicationContext = configurableApplicationContext;
        this.lock = lock;
    }
    
    @Override
    public void setId(String id) {
        lock.writeLock().lock();
        try {
            configurableApplicationContext.setId(id);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public ConfigurableListableBeanFactory getBeanFactory() {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBeanFactory();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String getId() {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getId();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String getApplicationName() {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getApplicationName();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String getDisplayName() {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getDisplayName();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public ApplicationContext getParent() {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getParent();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public ConfigurableEnvironment getEnvironment() {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getEnvironment();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public void setEnvironment(ConfigurableEnvironment environment) {
        lock.writeLock().lock();
        try {
            configurableApplicationContext.setEnvironment(environment);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public AutowireCapableBeanFactory getAutowireCapableBeanFactory() throws IllegalStateException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getAutowireCapableBeanFactory();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public long getStartupDate() {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getStartupDate();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public void publishEvent(ApplicationEvent event) {
        lock.writeLock().lock();
        try {
            configurableApplicationContext.publishEvent(event);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public void publishEvent(Object o) {
        this.configurableApplicationContext.publishEvent(o);
    }
    
    @Override
    public void setParent(ApplicationContext parent) {
        lock.writeLock().lock();
        try {
            configurableApplicationContext.setParent(parent);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public void addBeanFactoryPostProcessor(BeanFactoryPostProcessor beanFactoryPostProcessor) {
        lock.writeLock().lock();
        try {
            configurableApplicationContext.addBeanFactoryPostProcessor(beanFactoryPostProcessor);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public void addApplicationListener(ApplicationListener<?> listener) {
        lock.writeLock().lock();
        try {
            configurableApplicationContext.addApplicationListener(listener);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public void addProtocolResolver(ProtocolResolver protocolResolver) {
        this.configurableApplicationContext.addProtocolResolver(protocolResolver);
    }
    
    @Override
    public void refresh() throws BeansException, IllegalStateException {
        lock.writeLock().lock();
        try {
            configurableApplicationContext.refresh();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public void registerShutdownHook() {
        lock.writeLock().lock();
        try {
            configurableApplicationContext.registerShutdownHook();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            configurableApplicationContext.close();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public boolean isActive() {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.isActive();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public Object getBean(String name) throws BeansException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBean(name);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public <T> T getBean(String name, Class<T> requiredType) throws BeansException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBean(name, requiredType);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public <T> T getBean(Class<T> requiredType) throws BeansException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBean(requiredType);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public Object getBean(String name, Object... args) throws BeansException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBean(name, args);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public <T> T getBean(Class<T> requiredType, Object... args) throws BeansException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBean(requiredType, args);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public <T> ObjectProvider<T> getBeanProvider(Class<T> aClass) {
        return configurableApplicationContext.getBeanProvider(aClass);
    }
    
    @Override
    public <T> ObjectProvider<T> getBeanProvider(ResolvableType resolvableType) {
        return configurableApplicationContext.getBeanProvider(resolvableType);
    }
    
    @Override
    public boolean containsBean(String name) {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.containsBean(name);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public boolean isSingleton(String name) throws NoSuchBeanDefinitionException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.isSingleton(name);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public boolean isPrototype(String name) throws NoSuchBeanDefinitionException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.isPrototype(name);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public boolean isTypeMatch(String s, ResolvableType resolvableType) throws NoSuchBeanDefinitionException {
        return this.configurableApplicationContext.isTypeMatch(s, resolvableType);
    }
    
    @Override
    public boolean isTypeMatch(String name, Class<?> targetType) throws NoSuchBeanDefinitionException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.isTypeMatch(name, targetType);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public Class<?> getType(String name) throws NoSuchBeanDefinitionException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getType(name);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public Class<?> getType(String name, boolean allowFactoryBeanInit) throws NoSuchBeanDefinitionException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getType(name, allowFactoryBeanInit);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String[] getAliases(String name) {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getAliases(name);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public boolean containsBeanDefinition(String beanName) {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.containsBeanDefinition(beanName);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public int getBeanDefinitionCount() {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBeanDefinitionCount();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String[] getBeanDefinitionNames() {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBeanDefinitionNames();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String[] getBeanNamesForType(ResolvableType resolvableType) {
        return this.configurableApplicationContext.getBeanNamesForType(resolvableType);
    }
    
    @Override
    public String[] getBeanNamesForType(Class<?> type) {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBeanNamesForType(type);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String[] getBeanNamesForType(Class<?> type, boolean includeNonSingletons, boolean allowEagerInit) {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBeanNamesForType(type, includeNonSingletons, allowEagerInit);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String[] getBeanNamesForType(ResolvableType type, boolean includeNonSingletons, boolean allowEagerInit) {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBeanNamesForType(type, includeNonSingletons, allowEagerInit);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public <T> Map<String,T> getBeansOfType(Class<T> type) throws BeansException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBeansOfType(type);
        } finally {
            lock.readLock().unlock();
        }
        
    }
    
    @Override
    public <T> Map<String,T> getBeansOfType(Class<T> type, boolean includeNonSingletons, boolean allowEagerInit) throws BeansException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBeansOfType(type, includeNonSingletons, allowEagerInit);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String[] getBeanNamesForAnnotation(Class<? extends Annotation> annotationType) {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBeanNamesForAnnotation(annotationType);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public Map<String,Object> getBeansWithAnnotation(Class<? extends Annotation> annotationType) throws BeansException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getBeansWithAnnotation(annotationType);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public <A extends Annotation> A findAnnotationOnBean(String beanName, Class<A> annotationType) throws NoSuchBeanDefinitionException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.findAnnotationOnBean(beanName, annotationType);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public BeanFactory getParentBeanFactory() {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getParentBeanFactory();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public boolean containsLocalBean(String name) {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.containsLocalBean(name);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String getMessage(String code, Object[] args, String defaultMessage, Locale locale) {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getMessage(code, args, defaultMessage, locale);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String getMessage(String code, Object[] args, Locale locale) throws NoSuchMessageException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getMessage(code, args, locale);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String getMessage(MessageSourceResolvable resolvable, Locale locale) throws NoSuchMessageException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getMessage(resolvable, locale);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public Resource[] getResources(String locationPattern) throws IOException {
        lock.readLock().lock();
        try {
            return configurableApplicationContext.getResources(locationPattern);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public void start() {
        lock.readLock().lock();
        try {
            configurableApplicationContext.start();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public void stop() {
        lock.readLock().lock();
        try {
            configurableApplicationContext.stop();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public boolean isRunning() {
        lock.writeLock().lock();
        try {
            return configurableApplicationContext.isRunning();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public ClassLoader getClassLoader() {
        lock.writeLock().lock();
        try {
            return configurableApplicationContext.getClassLoader();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public Resource getResource(String location) {
        lock.writeLock().lock();
        try {
            return configurableApplicationContext.getResource(location);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
