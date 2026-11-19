package datawave.configuration.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.core.ResolvableType;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;

class ThreadSafeClassPathXmlApplicationContextTest {

    private ConfigurableApplicationContext delegateContext;
    private ReadWriteLock readWriteLock;
    private Lock readLock;
    private Lock writeLock;

    private ThreadSafeClassPathXmlApplicationContext threadSafeContext;

    @BeforeEach
    void setUp() {
        // Verify lock acquisition, delegation, and release together with Mockito InOrder.
        delegateContext = mock(ConfigurableApplicationContext.class);
        readWriteLock = mock(ReadWriteLock.class);
        readLock = mock(Lock.class);
        writeLock = mock(Lock.class);

        threadSafeContext = new ThreadSafeClassPathXmlApplicationContext(delegateContext, readWriteLock);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#setId(String)} is only executed after obtaining a write lock.
     */
    @Test
    void testSetId() {
        assertExecutedWithWriteLock(delegate -> delegate.setId("id"), () -> threadSafeContext.setId("id"));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanFactory()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanFactory() {
        ConfigurableListableBeanFactory expected = mock(ConfigurableListableBeanFactory.class);
        assertExecutedWithReadLock(delegate -> delegate.getBeanFactory(), threadSafeContext::getBeanFactory, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getId()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetId() {
        assertExecutedWithReadLock(delegate -> delegate.getId(), threadSafeContext::getId, "id");
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getApplicationName()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetApplicationName() {
        assertExecutedWithReadLock(delegate -> delegate.getApplicationName(), threadSafeContext::getApplicationName, "name");
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getDisplayName()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetDisplayName() {
        assertExecutedWithReadLock(delegate -> delegate.getDisplayName(), threadSafeContext::getDisplayName, "name");
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getParent()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetParent() {
        ApplicationContext expected = mock(ApplicationContext.class);
        assertExecutedWithReadLock(delegate -> delegate.getParent(), threadSafeContext::getParent, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getEnvironment()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetEnvironment() {
        ConfigurableEnvironment expected = mock(ConfigurableEnvironment.class);
        assertExecutedWithReadLock(delegate -> delegate.getEnvironment(), threadSafeContext::getEnvironment, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#setEnvironment(ConfigurableEnvironment)} is only executed after obtaining a write lock.
     */
    @Test
    void testSetEnvironment() {
        ConfigurableEnvironment environment = mock(ConfigurableEnvironment.class);
        assertExecutedWithWriteLock(delegate -> delegate.setEnvironment(environment), () -> threadSafeContext.setEnvironment(environment));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getAutowireCapableBeanFactory()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetAutowireCapableBeanFactory() {
        AutowireCapableBeanFactory beanFactory = mock(AutowireCapableBeanFactory.class);
        assertExecutedWithReadLock(delegate -> delegate.getAutowireCapableBeanFactory(), threadSafeContext::getAutowireCapableBeanFactory, beanFactory);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getStartupDate()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetStartupDate() {
        long expected = 1L;
        assertExecutedWithReadLock(delegate -> delegate.getStartupDate(), threadSafeContext::getStartupDate, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#publishEvent(ApplicationEvent)} is only executed after obtaining a write lock.
     */
    @Test
    void testPublishEventGivenApplicationEvent() {
        ApplicationEvent event = mock(ApplicationEvent.class);
        assertExecutedWithWriteLock(delegate -> delegate.publishEvent(event), () -> threadSafeContext.publishEvent(event));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#publishEvent(Object)} is only executed after obtaining a write lock.
     */
    @Test
    void testPublishEventGivenObject() {
        Object event = mock(Object.class);
        assertExecutedWithWriteLock(delegate -> delegate.publishEvent(event), () -> threadSafeContext.publishEvent(event));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#setParent(ApplicationContext)} is only executed after obtaining a write lock.
     */
    @Test
    void testSetParent() {
        ApplicationContext context = mock(ApplicationContext.class);
        assertExecutedWithWriteLock(delegate -> delegate.setParent(context), () -> threadSafeContext.setParent(context));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#addBeanFactoryPostProcessor(BeanFactoryPostProcessor)} is only executed after obtaining a
     * write lock.
     */
    @Test
    void testAddBeanFactoryPostProcessor() {
        BeanFactoryPostProcessor processor = mock(BeanFactoryPostProcessor.class);
        assertExecutedWithWriteLock(delegate -> delegate.addBeanFactoryPostProcessor(processor),
                        () -> threadSafeContext.addBeanFactoryPostProcessor(processor));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#addApplicationListener(ApplicationListener)} is only executed after obtaining a write lock.
     */
    @Test
    void addApplicationListener() {
        ApplicationListener<?> listener = mock(ApplicationListener.class);
        assertExecutedWithWriteLock(delegate -> delegate.addApplicationListener(listener), () -> threadSafeContext.addApplicationListener(listener));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#addProtocolResolver(ProtocolResolver)} is only executed after obtaining a write lock.
     */
    @Test
    void testAddProtocolResolver() {
        ProtocolResolver protocolResolver = mock(ProtocolResolver.class);
        assertExecutedWithWriteLock(delegate -> delegate.addProtocolResolver(protocolResolver), () -> threadSafeContext.addProtocolResolver(protocolResolver));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#refresh()} is only executed after obtaining a write lock.
     */
    @Test
    void testRefresh() {
        assertExecutedWithWriteLock(delegate -> delegate.refresh(), threadSafeContext::refresh);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#registerShutdownHook()} is only executed after obtaining a write lock.
     */
    @Test
    void testRegisterShutdownHook() {
        assertExecutedWithWriteLock(delegate -> delegate.registerShutdownHook(), threadSafeContext::registerShutdownHook);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#close()} is only executed after obtaining a write lock.
     */
    @Test
    void testClose() {
        assertExecutedWithWriteLock(delegate -> delegate.close(), threadSafeContext::close);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#isActive()} is only executed after obtaining a read lock.
     */
    @Test
    void testIsActive() {
        assertExecutedWithReadLock(delegate -> delegate.isActive(), threadSafeContext::isActive, true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBean(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanGivenName() {
        Object expected = mock(Object.class);
        assertExecutedWithReadLock(delegate -> delegate.getBean("beanName"), () -> threadSafeContext.getBean("beanName"), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBean(String, Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanBeanGivenNameAndClass() {
        Object expected = mock(Object.class);
        assertExecutedWithReadLock(delegate -> delegate.getBean("beanName", Object.class), () -> threadSafeContext.getBean("beanName", Object.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBean(Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanGivenClass() {
        Object expected = mock(Object.class);
        assertExecutedWithReadLock(delegate -> delegate.getBean(Object.class), () -> threadSafeContext.getBean(Object.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBean(String, Object...)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanGivenNameAndVarArg() {
        Object expected = mock(Object.class);
        assertExecutedWithReadLock(delegate -> delegate.getBean("beanName", "arg1", "arg2"), () -> threadSafeContext.getBean("beanName", "arg1", "arg2"),
                        expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBean(Class, Object...)} is only executed after obtaining a read lock.
     */
    @Test
    void testBeanGivenClassAndVarArg() {
        Object expected = mock(Object.class);
        assertExecutedWithReadLock(delegate -> delegate.getBean(Object.class, "arg1", "arg2"), () -> threadSafeContext.getBean(Object.class, "arg1", "arg2"),
                        expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanProvider(Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanProviderGivenClass() {
        ObjectProvider<?> expected = mock(ObjectProvider.class);
        assertExecutedWithReadLock(delegate -> delegate.getBeanProvider(Object.class), () -> threadSafeContext.getBeanProvider(Object.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanProvider(ResolvableType)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanProviderGivenResolvableType() {
        ObjectProvider<?> expected = mock(ObjectProvider.class);
        ResolvableType resolvableType = mock(ResolvableType.class);
        assertExecutedWithReadLock(delegate -> delegate.getBeanProvider(resolvableType), () -> threadSafeContext.getBeanProvider(resolvableType), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#containsBean(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testContainsBean() {
        assertExecutedWithReadLock(delegate -> delegate.containsBean("beanName"), () -> threadSafeContext.containsBean("beanName"), true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#isSingleton(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testIsSingleton() {
        assertExecutedWithReadLock(delegate -> delegate.isSingleton("beanName"), () -> threadSafeContext.isSingleton("beanName"), true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#isPrototype(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testIsPrototype() {
        assertExecutedWithReadLock(delegate -> delegate.isPrototype("beanName"), () -> threadSafeContext.isPrototype("beanName"), false);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#isTypeMatch(String, ResolvableType)} is only executed after obtaining a read lock.
     */
    @Test
    void testIsTypeMatchGivenNameAndResolvableType() {
        ResolvableType resolvableType = mock(ResolvableType.class);
        assertExecutedWithReadLock(delegate -> delegate.isTypeMatch("beanName", resolvableType),
                        () -> threadSafeContext.isTypeMatch("beanName", resolvableType), true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#isTypeMatch(String, Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testIsTypeMatchGivenNameAndClass() {
        assertExecutedWithReadLock(delegate -> delegate.isTypeMatch("beanName", Object.class), () -> threadSafeContext.isTypeMatch("beanName", Object.class),
                        true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getType(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetTypeGivenName() {
        Class<?> expected = Object.class;
        assertExecutedWithReadLock(delegate -> delegate.getType("beanName"), () -> threadSafeContext.getType("beanName"), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getType(String, boolean)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetTypeGivenNameAndAllowFactoryInit() {
        Class<?> expected = Object.class;
        assertExecutedWithReadLock(delegate -> delegate.getType("beanName", true), () -> threadSafeContext.getType("beanName", true), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getAliases(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetAliases() {
        String[] expected = {"alias1", "alias2"};
        assertExecutedWithReadLock(delegate -> delegate.getAliases("beanName"), () -> threadSafeContext.getAliases("beanName"), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#containsBeanDefinition(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testContainsBeanDefinition() {
        assertExecutedWithReadLock(delegate -> delegate.containsBeanDefinition("beanName"), () -> threadSafeContext.containsBeanDefinition("beanName"), true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanDefinitionCount()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanDefinitionCount() {
        assertExecutedWithReadLock(delegate -> delegate.getBeanDefinitionCount(), threadSafeContext::getBeanDefinitionCount, 5);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanDefinitionNames()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanDefinitionNames() {
        String[] expected = {"bean1", "bean2"};
        assertExecutedWithReadLock(delegate -> delegate.getBeanDefinitionNames(), threadSafeContext::getBeanDefinitionNames, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanNamesForType(ResolvableType)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanNamesForResolvableType() {
        String[] expected = {"bean1", "bean2"};
        ResolvableType resolvableType = mock(ResolvableType.class);
        assertExecutedWithReadLock(delegate -> delegate.getBeanNamesForType(resolvableType), () -> threadSafeContext.getBeanNamesForType(resolvableType),
                        expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanNamesForType(Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanNamesForClassType() {
        String[] expected = {"bean1", "bean2"};
        assertExecutedWithReadLock(delegate -> delegate.getBeanNamesForType(Object.class), () -> threadSafeContext.getBeanNamesForType(Object.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanNamesForType(Class, boolean, boolean)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanNamesForClassTypeWithOptions() {
        String[] expected = {"bean1", "bean2"};
        assertExecutedWithReadLock(delegate -> delegate.getBeanNamesForType(Object.class, true, false),
                        () -> threadSafeContext.getBeanNamesForType(Object.class, true, false), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanNamesForType(ResolvableType, boolean, boolean)} is only executed after obtaining a
     * read lock.
     */
    @Test
    void testGetBeanNamesForResolvableTypeWithOptions() {
        String[] expected = {"bean1", "bean2"};
        ResolvableType resolvableType = mock(ResolvableType.class);
        assertExecutedWithReadLock(delegate -> delegate.getBeanNamesForType(resolvableType, true, false),
                        () -> threadSafeContext.getBeanNamesForType(resolvableType, true, false), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeansOfType(Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeansOfType() {
        Map<String,Object> expected = mock(Map.class);
        assertExecutedWithReadLock(delegate -> delegate.getBeansOfType(Object.class), () -> threadSafeContext.getBeansOfType(Object.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeansOfType(Class, boolean, boolean)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeansOfTypeWithOptions() {
        Map<String,Object> expected = mock(Map.class);
        assertExecutedWithReadLock(delegate -> delegate.getBeansOfType(Object.class, true, false),
                        () -> threadSafeContext.getBeansOfType(Object.class, true, false), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanNamesForAnnotation(Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanNamesForAnnotation() {
        String[] expected = {"bean1", "bean2"};
        assertExecutedWithReadLock(delegate -> delegate.getBeanNamesForAnnotation(Annotation.class),
                        () -> threadSafeContext.getBeanNamesForAnnotation(Annotation.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeansWithAnnotation(Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeansWithAnnotation() {
        Map<String,Object> expected = mock(Map.class);
        assertExecutedWithReadLock(delegate -> delegate.getBeansWithAnnotation(Annotation.class),
                        () -> threadSafeContext.getBeansWithAnnotation(Annotation.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#findAnnotationOnBean(String, Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testFindAnnotationOnBean() {
        Annotation expected = mock(Annotation.class);
        assertExecutedWithReadLock(delegate -> delegate.findAnnotationOnBean("beanName", Annotation.class),
                        () -> threadSafeContext.findAnnotationOnBean("beanName", Annotation.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getParentBeanFactory()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetParentBeanFactory() {
        BeanFactory expected = mock(BeanFactory.class);
        assertExecutedWithReadLock(delegate -> delegate.getParentBeanFactory(), threadSafeContext::getParentBeanFactory, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#containsLocalBean(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testContainsLocalBean() {
        assertExecutedWithReadLock(delegate -> delegate.containsLocalBean("beanName"), () -> threadSafeContext.containsLocalBean("beanName"), true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getMessage(String, Object[], String, Locale)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetMessageGivenCodeAndArgsAndDefaultAndLocale() {
        String expected = "message";
        assertExecutedWithReadLock(delegate -> delegate.getMessage("code", null, "default", Locale.ENGLISH),
                        () -> threadSafeContext.getMessage("code", null, "default", Locale.ENGLISH), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getMessage(String, Object[], Locale)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetMessageGivenCodeAndArgsAndLocale() {
        String expected = "message";
        assertExecutedWithReadLock(delegate -> delegate.getMessage("code", new Object[] {"arg1"}, Locale.ENGLISH),
                        () -> threadSafeContext.getMessage("code", new Object[] {"arg1"}, Locale.ENGLISH), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getMessage(MessageSourceResolvable, Locale)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetMessageByResolvableAndLocale() {
        MessageSourceResolvable resolvable = mock(MessageSourceResolvable.class);
        String expected = "message";
        assertExecutedWithReadLock(delegate -> delegate.getMessage(resolvable, Locale.ENGLISH), () -> threadSafeContext.getMessage(resolvable, Locale.ENGLISH),
                        expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getResources(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetResources() {
        Resource[] expected = {mock(Resource.class)};
        assertExecutedWithReadLock(delegate -> {
            try {
                return delegate.getResources("locationPattern");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, () -> threadSafeContext.getResources("locationPattern"), expected);
    }

    /**
     * Verify that if an exception occurs when calling {@link ThreadSafeClassPathXmlApplicationContext#getResources(String)}, that the exception is not
     * suppressed and the read lock was obtained and released.
     */
    @Test
    void testGetResourcesWithException() throws IOException {
        // Expect the read lock to be obtained.
        doReturn(readLock).when(readWriteLock).readLock();

        // Expect the delegate method to be called and throw an IOException.
        doThrow(new IOException("test exception")).when(delegateContext).getResources("locationPattern");

        // Expect the read lock to be unlocked.

        // Call the method under test and assert that it throws a RuntimeException.
        Exception exception = assertThrows(RuntimeException.class, () -> threadSafeContext.getResources("locationPattern"));

        InOrder order = inOrder(readWriteLock, readLock, delegateContext);
        order.verify(readWriteLock).readLock();
        order.verify(readLock).lock();
        order.verify(delegateContext).getResources("locationPattern");
        order.verify(readWriteLock).readLock();
        order.verify(readLock).unlock();
        order.verifyNoMoreInteractions();

        // Assert the exception is not suppressed and the cause is the original exception.
        Throwable cause = exception.getCause();
        assertInstanceOf(IOException.class, cause);
        assertEquals("test exception", cause.getMessage());
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#start()} is only executed after obtaining a write lock.
     */
    @Test
    void testStart() {
        assertExecutedWithWriteLock(delegate -> delegate.start(), threadSafeContext::start);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#stop()} is only executed after obtaining a write lock.
     */
    @Test
    void testStop() {
        assertExecutedWithWriteLock(delegate -> delegate.stop(), threadSafeContext::stop);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#isRunning()} is only executed after obtaining a read lock.
     */
    @Test
    void testIsRunning() {
        assertExecutedWithReadLock(delegate -> delegate.isRunning(), threadSafeContext::isRunning, true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getClassLoader()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetClassLoader() {
        ClassLoader expected = mock(ClassLoader.class);
        assertExecutedWithReadLock(delegate -> delegate.getClassLoader(), threadSafeContext::getClassLoader, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getResource(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetResource() {
        Resource expected = mock(Resource.class);
        assertExecutedWithReadLock(delegate -> delegate.getResource("location"), () -> threadSafeContext.getResource("location"), expected);
    }

    /**
     * Assert that the given delegate method is executed with a write lock and that the method under test delegates to the delegate method.
     *
     * @param delegateMethod
     *            the delegate method that should execute within a write lock
     * @param methodUnderTest
     *            the method under test
     */
    private void assertExecutedWithWriteLock(Consumer<ConfigurableApplicationContext> delegateMethod, Runnable methodUnderTest) {
        doReturn(writeLock).when(readWriteLock).writeLock();

        methodUnderTest.run();

        InOrder order = inOrder(readWriteLock, writeLock, delegateContext);
        order.verify(readWriteLock).writeLock();
        order.verify(writeLock).lock();
        delegateMethod.accept(order.verify(delegateContext));
        order.verify(readWriteLock).writeLock();
        order.verify(writeLock).unlock();
        order.verifyNoMoreInteractions();
    }

    /**
     * Assert that delegation occurs after acquiring the read lock and before releasing it.
     *
     * @param delegateMethod
     *            the delegate method to stub and verify
     * @param methodUnderTest
     *            the method under test
     * @param expected
     *            the expected result
     * @param <T>
     *            the result type
     */
    private <T> void assertExecutedWithReadLock(Function<ConfigurableApplicationContext,T> delegateMethod, Supplier<T> methodUnderTest, T expected) {
        doReturn(readLock).when(readWriteLock).readLock();
        delegateMethod.apply(doReturn(expected).when(delegateContext));

        T actual = methodUnderTest.get();
        assertSame(expected, actual);

        InOrder order = inOrder(readWriteLock, readLock, delegateContext);
        order.verify(readWriteLock).readLock();
        order.verify(readLock).lock();
        delegateMethod.apply(order.verify(delegateContext));
        order.verify(readWriteLock).readLock();
        order.verify(readLock).unlock();
        order.verifyNoMoreInteractions();
    }
}
