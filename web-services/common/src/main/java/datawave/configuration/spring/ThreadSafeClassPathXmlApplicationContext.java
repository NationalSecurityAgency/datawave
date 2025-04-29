package datawave.configuration.spring;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;

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

    private final ConfigurableApplicationContext configurableApplicationContext;
    private final ReadWriteLock lock;

    public ThreadSafeClassPathXmlApplicationContext(ConfigurableApplicationContext configurableApplicationContext, ReadWriteLock lock) {
        this.configurableApplicationContext = configurableApplicationContext;
        this.lock = lock;
    }

    @Override
    public void setId(String id) {
        lockAndWrite(() -> configurableApplicationContext.setId(id));
    }

    @Override
    public ConfigurableListableBeanFactory getBeanFactory() {
        return lockAndRead(configurableApplicationContext::getBeanFactory);
    }

    @Override
    public String getId() {
        return lockAndRead(configurableApplicationContext::getId);
    }

    @Override
    public String getApplicationName() {
        return lockAndRead(configurableApplicationContext::getApplicationName);
    }

    @Override
    public String getDisplayName() {
        return lockAndRead(configurableApplicationContext::getDisplayName);
    }

    @Override
    public ApplicationContext getParent() {
        return lockAndRead(configurableApplicationContext::getParent);
    }

    @Override
    public ConfigurableEnvironment getEnvironment() {
        return lockAndRead(configurableApplicationContext::getEnvironment);
    }

    @Override
    public void setEnvironment(ConfigurableEnvironment environment) {
        lockAndWrite(() -> configurableApplicationContext.setEnvironment(environment));
    }

    @Override
    public AutowireCapableBeanFactory getAutowireCapableBeanFactory() throws IllegalStateException {
        return lockAndRead(configurableApplicationContext::getAutowireCapableBeanFactory);
    }

    @Override
    public long getStartupDate() {
        return lockAndRead(configurableApplicationContext::getStartupDate);
    }

    @Override
    public void publishEvent(ApplicationEvent event) {
        lockAndWrite(() -> configurableApplicationContext.publishEvent(event));
    }

    @Override
    public void publishEvent(Object o) {
        lockAndWrite(() -> configurableApplicationContext.publishEvent(o));
    }

    @Override
    public void setParent(ApplicationContext parent) {
        lockAndWrite(() -> configurableApplicationContext.setParent(parent));
    }

    @Override
    public void addBeanFactoryPostProcessor(BeanFactoryPostProcessor beanFactoryPostProcessor) {
        lockAndWrite(() -> configurableApplicationContext.addBeanFactoryPostProcessor(beanFactoryPostProcessor));
    }

    @Override
    public void addApplicationListener(ApplicationListener<?> listener) {
        lockAndWrite(() -> configurableApplicationContext.addApplicationListener(listener));
    }

    @Override
    public void addProtocolResolver(ProtocolResolver protocolResolver) {
        lockAndWrite(() -> configurableApplicationContext.addProtocolResolver(protocolResolver));
    }

    @Override
    public void refresh() throws BeansException, IllegalStateException {
        lockAndWrite(configurableApplicationContext::refresh);
    }

    @Override
    public void registerShutdownHook() {
        lockAndWrite(configurableApplicationContext::registerShutdownHook);
    }

    @Override
    public void close() {
        lockAndWrite(configurableApplicationContext::close);
    }

    @Override
    public boolean isActive() {
        return lockAndRead(configurableApplicationContext::isActive);
    }

    @Override
    public Object getBean(String name) throws BeansException {
        return lockAndRead(() -> configurableApplicationContext.getBean(name));
    }

    @Override
    public <T> T getBean(String name, Class<T> requiredType) throws BeansException {
        return lockAndRead(() -> configurableApplicationContext.getBean(name, requiredType));
    }

    @Override
    public <T> T getBean(Class<T> requiredType) throws BeansException {
        return lockAndRead(() -> configurableApplicationContext.getBean(requiredType));
    }

    @Override
    public Object getBean(String name, Object... args) throws BeansException {
        return lockAndRead(() -> configurableApplicationContext.getBean(name, args));
    }

    @Override
    public <T> T getBean(Class<T> requiredType, Object... args) throws BeansException {
        return lockAndRead(() -> configurableApplicationContext.getBean(requiredType, args));
    }

    @Override
    public <T> ObjectProvider<T> getBeanProvider(Class<T> requiredType) {
        return lockAndRead(() -> configurableApplicationContext.getBeanProvider(requiredType));
    }

    @Override
    public <T> ObjectProvider<T> getBeanProvider(ResolvableType resolvableType) {
        return lockAndRead(() -> configurableApplicationContext.getBeanProvider(resolvableType));
    }

    @Override
    public boolean containsBean(String name) {
        return lockAndRead(() -> configurableApplicationContext.containsBean(name));
    }

    @Override
    public boolean isSingleton(String name) throws NoSuchBeanDefinitionException {
        return lockAndRead(() -> configurableApplicationContext.isSingleton(name));
    }

    @Override
    public boolean isPrototype(String name) throws NoSuchBeanDefinitionException {
        return lockAndRead(() -> configurableApplicationContext.isPrototype(name));
    }

    @Override
    public boolean isTypeMatch(String name, ResolvableType resolvableType) throws NoSuchBeanDefinitionException {
        return lockAndRead(() -> configurableApplicationContext.isTypeMatch(name, resolvableType));
    }

    @Override
    public boolean isTypeMatch(String name, Class<?> targetType) throws NoSuchBeanDefinitionException {
        return lockAndRead(() -> configurableApplicationContext.isTypeMatch(name, targetType));
    }

    @Override
    public Class<?> getType(String name) throws NoSuchBeanDefinitionException {
        return lockAndRead(() -> configurableApplicationContext.getType(name));
    }

    @Override
    public Class<?> getType(String name, boolean allowFactoryBeanInit) throws NoSuchBeanDefinitionException {
        return lockAndRead(() -> configurableApplicationContext.getType(name, allowFactoryBeanInit));
    }

    @Override
    public String[] getAliases(String name) {
        return lockAndRead(() -> configurableApplicationContext.getAliases(name));
    }

    @Override
    public boolean containsBeanDefinition(String beanName) {
        return lockAndRead(() -> configurableApplicationContext.containsBeanDefinition(beanName));
    }

    @Override
    public int getBeanDefinitionCount() {
        return lockAndRead(configurableApplicationContext::getBeanDefinitionCount);
    }

    @Override
    public String[] getBeanDefinitionNames() {
        return lockAndRead(configurableApplicationContext::getBeanDefinitionNames);
    }

    @Override
    public String[] getBeanNamesForType(ResolvableType resolvableType) {
        return lockAndRead(() -> configurableApplicationContext.getBeanNamesForType(resolvableType));
    }

    @Override
    public String[] getBeanNamesForType(Class<?> type) {
        return lockAndRead(() -> configurableApplicationContext.getBeanNamesForType(type));
    }

    @Override
    public String[] getBeanNamesForType(Class<?> type, boolean includeNonSingletons, boolean allowEagerInit) {
        return lockAndRead(() -> configurableApplicationContext.getBeanNamesForType(type, includeNonSingletons, allowEagerInit));
    }

    @Override
    public String[] getBeanNamesForType(ResolvableType type, boolean includeNonSingletons, boolean allowEagerInit) {
        return lockAndRead(() -> configurableApplicationContext.getBeanNamesForType(type, includeNonSingletons, allowEagerInit));
    }

    @Override
    public <T> Map<String,T> getBeansOfType(Class<T> type) throws BeansException {
        return lockAndRead(() -> configurableApplicationContext.getBeansOfType(type));
    }

    @Override
    public <T> Map<String,T> getBeansOfType(Class<T> type, boolean includeNonSingletons, boolean allowEagerInit) throws BeansException {
        return lockAndRead(() -> configurableApplicationContext.getBeansOfType(type, includeNonSingletons, allowEagerInit));
    }

    @Override
    public String[] getBeanNamesForAnnotation(Class<? extends Annotation> annotationType) {
        return lockAndRead(() -> configurableApplicationContext.getBeanNamesForAnnotation(annotationType));
    }

    @Override
    public Map<String,Object> getBeansWithAnnotation(Class<? extends Annotation> annotationType) throws BeansException {
        return lockAndRead(() -> configurableApplicationContext.getBeansWithAnnotation(annotationType));
    }

    @Override
    public <A extends Annotation> A findAnnotationOnBean(String beanName, Class<A> annotationType) throws NoSuchBeanDefinitionException {
        return lockAndRead(() -> configurableApplicationContext.findAnnotationOnBean(beanName, annotationType));
    }

    @Override
    public BeanFactory getParentBeanFactory() {
        return lockAndRead(configurableApplicationContext::getParentBeanFactory);
    }

    @Override
    public boolean containsLocalBean(String name) {
        return lockAndRead(() -> configurableApplicationContext.containsLocalBean(name));
    }

    @Override
    public String getMessage(String code, Object[] args, String defaultMessage, Locale locale) {
        return lockAndRead(() -> configurableApplicationContext.getMessage(code, args, defaultMessage, locale));
    }

    @Override
    public String getMessage(String code, Object[] args, Locale locale) throws NoSuchMessageException {
        return lockAndRead(() -> configurableApplicationContext.getMessage(code, args, locale));
    }

    @Override
    public String getMessage(MessageSourceResolvable resolvable, Locale locale) throws NoSuchMessageException {
        return lockAndRead(() -> configurableApplicationContext.getMessage(resolvable, locale));
    }

    @Override
    public Resource[] getResources(String locationPattern) {
        return lockAndRead(() -> {
            try {
                return configurableApplicationContext.getResources(locationPattern);
            } catch (IOException e) {
                // Ensure IOException is not suppressed.
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void start() {
        lockAndWrite(configurableApplicationContext::start);
    }

    @Override
    public void stop() {
        lockAndWrite(configurableApplicationContext::stop);
    }

    @Override
    public boolean isRunning() {
        return lockAndRead(configurableApplicationContext::isRunning);
    }

    @Override
    public ClassLoader getClassLoader() {
        return lockAndRead(configurableApplicationContext::getClassLoader);
    }

    @Override
    public Resource getResource(String location) {
        return lockAndRead(() -> configurableApplicationContext.getResource(location));
    }

    /**
     * Execute and return the result of the given delegate method after obtaining a lock for the read lock. The read lock will always be unlocked afterward.
     *
     * @param delegateMethod
     *            the delegate method to run to return the targeted resource
     * @return the delegate method's result
     * @param <T>
     *            the return type
     */
    private <T> T lockAndRead(Supplier<T> delegateMethod) {
        lock.readLock().lock();
        try {
            return delegateMethod.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Execute the given delegate method after obtaining a lock for the write lock. The write lock will always be unlocked afterward.
     *
     * @param delegateMethod
     *            the delegate method to execute
     */
    private void lockAndWrite(Runnable delegateMethod) {
        lock.writeLock().lock();
        try {
            delegateMethod.run();
        } finally {
            lock.writeLock().unlock();
        }
    }

}
