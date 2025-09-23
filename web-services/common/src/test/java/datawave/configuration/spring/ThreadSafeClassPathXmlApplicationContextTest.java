package datawave.configuration.spring;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;

import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.easymock.IMocksControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

@ExtendWith(EasyMockExtension.class)
class ThreadSafeClassPathXmlApplicationContextTest extends EasyMockSupport {

    private ConfigurableApplicationContext delegateContext;
    private ReadWriteLock readWriteLock;
    private Lock readLock;
    private Lock writeLock;

    private ThreadSafeClassPathXmlApplicationContext threadSafeContext;

    @BeforeEach
    void setUp() {
        // Create the mocks with a strict control so that the order of method calls between the different mocks is tested when we verify the mock calls.
        IMocksControl mockMaker = createStrictControl();

        delegateContext = mockMaker.createMock(ConfigurableApplicationContext.class);
        readWriteLock = mockMaker.mock(ReadWriteLock.class);
        readLock = mockMaker.createMock(Lock.class);
        writeLock = mockMaker.createMock(Lock.class);

        threadSafeContext = new ThreadSafeClassPathXmlApplicationContext(delegateContext, readWriteLock);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#setId(String)} is only executed after obtaining a write lock.
     */
    @Test
    void testSetId() {
        assertExecutedWithWriteLock(() -> delegateContext.setId("id"), () -> threadSafeContext.setId("id"));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanFactory()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanFactory() {
        ConfigurableListableBeanFactory expected = EasyMock.mock(ConfigurableListableBeanFactory.class);
        assertExecutedWithReadLock(delegateContext::getBeanFactory, threadSafeContext::getBeanFactory, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getId()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetId() {
        assertExecutedWithReadLock(delegateContext::getId, threadSafeContext::getId, "id");
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getApplicationName()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetApplicationName() {
        assertExecutedWithReadLock(delegateContext::getApplicationName, threadSafeContext::getApplicationName, "name");
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getDisplayName()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetDisplayName() {
        assertExecutedWithReadLock(delegateContext::getDisplayName, threadSafeContext::getDisplayName, "name");
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getParent()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetParent() {
        ApplicationContext expected = EasyMock.mock(ApplicationContext.class);
        assertExecutedWithReadLock(delegateContext::getParent, threadSafeContext::getParent, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getEnvironment()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetEnvironment() {
        ConfigurableEnvironment expected = EasyMock.mock(ConfigurableEnvironment.class);
        assertExecutedWithReadLock(delegateContext::getEnvironment, threadSafeContext::getEnvironment, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#setEnvironment(ConfigurableEnvironment)} is only executed after obtaining a write lock.
     */
    @Test
    void testSetEnvironment() {
        ConfigurableEnvironment environment = EasyMock.mock(ConfigurableEnvironment.class);
        assertExecutedWithWriteLock(() -> delegateContext.setEnvironment(environment), () -> threadSafeContext.setEnvironment(environment));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getAutowireCapableBeanFactory()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetAutowireCapableBeanFactory() {
        AutowireCapableBeanFactory beanFactory = EasyMock.mock(AutowireCapableBeanFactory.class);
        assertExecutedWithReadLock(delegateContext::getAutowireCapableBeanFactory, threadSafeContext::getAutowireCapableBeanFactory, beanFactory);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getStartupDate()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetStartupDate() {
        long expected = 1L;
        assertExecutedWithReadLock(delegateContext::getStartupDate, threadSafeContext::getStartupDate, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#publishEvent(ApplicationEvent)} is only executed after obtaining a write lock.
     */
    @Test
    void testPublishEventGivenApplicationEvent() {
        ApplicationEvent event = EasyMock.mock(ApplicationEvent.class);
        assertExecutedWithWriteLock(() -> delegateContext.publishEvent(event), () -> threadSafeContext.publishEvent(event));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#publishEvent(Object)} is only executed after obtaining a write lock.
     */
    @Test
    void testPublishEventGivenObject() {
        Object event = EasyMock.mock(Object.class);
        assertExecutedWithWriteLock(() -> delegateContext.publishEvent(event), () -> threadSafeContext.publishEvent(event));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#setParent(ApplicationContext)} is only executed after obtaining a write lock.
     */
    @Test
    void testSetParent() {
        ApplicationContext context = EasyMock.mock(ApplicationContext.class);
        assertExecutedWithWriteLock(() -> delegateContext.setParent(context), () -> threadSafeContext.setParent(context));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#addBeanFactoryPostProcessor(BeanFactoryPostProcessor)} is only executed after obtaining a
     * write lock.
     */
    @Test
    void testAddBeanFactoryPostProcessor() {
        BeanFactoryPostProcessor processor = EasyMock.mock(BeanFactoryPostProcessor.class);
        assertExecutedWithWriteLock(() -> delegateContext.addBeanFactoryPostProcessor(processor),
                        () -> threadSafeContext.addBeanFactoryPostProcessor(processor));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#addApplicationListener(ApplicationListener)} is only executed after obtaining a write lock.
     */
    @Test
    void addApplicationListener() {
        ApplicationListener<?> listener = EasyMock.mock(ApplicationListener.class);
        assertExecutedWithWriteLock(() -> delegateContext.addApplicationListener(listener), () -> threadSafeContext.addApplicationListener(listener));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#addProtocolResolver(ProtocolResolver)} is only executed after obtaining a write lock.
     */
    @Test
    void testAddProtocolResolver() {
        ProtocolResolver protocolResolver = EasyMock.mock(ProtocolResolver.class);
        assertExecutedWithWriteLock(() -> delegateContext.addProtocolResolver(protocolResolver), () -> threadSafeContext.addProtocolResolver(protocolResolver));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#refresh()} is only executed after obtaining a write lock.
     */
    @Test
    void testRefresh() {
        assertExecutedWithWriteLock(delegateContext::refresh, threadSafeContext::refresh);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#registerShutdownHook()} is only executed after obtaining a write lock.
     */
    @Test
    void testRegisterShutdownHook() {
        assertExecutedWithWriteLock(delegateContext::registerShutdownHook, threadSafeContext::registerShutdownHook);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#close()} is only executed after obtaining a write lock.
     */
    @Test
    void testClose() {
        assertExecutedWithWriteLock(delegateContext::close, threadSafeContext::close);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#isActive()} is only executed after obtaining a read lock.
     */
    @Test
    void testIsActive() {
        assertExecutedWithReadLock(delegateContext::isActive, threadSafeContext::isActive, true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBean(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanGivenName() {
        Object expected = EasyMock.mock(Object.class);
        assertExecutedWithReadLock(() -> delegateContext.getBean("beanName"), () -> threadSafeContext.getBean("beanName"), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBean(String, Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanBeanGivenNameAndClass() {
        Object expected = EasyMock.mock(Object.class);
        assertExecutedWithReadLock(() -> delegateContext.getBean("beanName", Object.class), () -> threadSafeContext.getBean("beanName", Object.class),
                        expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBean(Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanGivenClass() {
        Object expected = EasyMock.mock(Object.class);
        assertExecutedWithReadLock(() -> delegateContext.getBean(Object.class), () -> threadSafeContext.getBean(Object.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBean(String, Object...)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanGivenNameAndVarArg() {
        Object expected = EasyMock.mock(Object.class);
        assertExecutedWithReadLock(() -> delegateContext.getBean("beanName", "arg1", "arg2"), () -> threadSafeContext.getBean("beanName", "arg1", "arg2"),
                        expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBean(Class, Object...)} is only executed after obtaining a read lock.
     */
    @Test
    void testBeanGivenClassAndVarArg() {
        Object expected = EasyMock.mock(Object.class);
        assertExecutedWithReadLock(() -> delegateContext.getBean(Object.class, "arg1", "arg2"), () -> threadSafeContext.getBean(Object.class, "arg1", "arg2"),
                        expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanProvider(Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanProviderGivenClass() {
        ObjectProvider<?> expected = EasyMock.mock(ObjectProvider.class);
        assertExecutedWithReadLock(() -> delegateContext.getBeanProvider(Object.class), () -> threadSafeContext.getBeanProvider(Object.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanProvider(ResolvableType)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanProviderGivenResolvableType() {
        ObjectProvider<?> expected = EasyMock.mock(ObjectProvider.class);
        ResolvableType resolvableType = EasyMock.mock(ResolvableType.class);
        assertExecutedWithReadLock(() -> delegateContext.getBeanProvider(resolvableType), () -> threadSafeContext.getBeanProvider(resolvableType), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#containsBean(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testContainsBean() {
        assertExecutedWithReadLock(() -> delegateContext.containsBean("beanName"), () -> threadSafeContext.containsBean("beanName"), true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#isSingleton(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testIsSingleton() {
        assertExecutedWithReadLock(() -> delegateContext.isSingleton("beanName"), () -> threadSafeContext.isSingleton("beanName"), true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#isPrototype(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testIsPrototype() {
        assertExecutedWithReadLock(() -> delegateContext.isPrototype("beanName"), () -> threadSafeContext.isPrototype("beanName"), false);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#isTypeMatch(String, ResolvableType)} is only executed after obtaining a read lock.
     */
    @Test
    void testIsTypeMatchGivenNameAndResolvableType() {
        ResolvableType resolvableType = EasyMock.mock(ResolvableType.class);
        assertExecutedWithReadLock(() -> delegateContext.isTypeMatch("beanName", resolvableType),
                        () -> threadSafeContext.isTypeMatch("beanName", resolvableType), true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#isTypeMatch(String, Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testIsTypeMatchGivenNameAndClass() {
        assertExecutedWithReadLock(() -> delegateContext.isTypeMatch("beanName", Object.class), () -> threadSafeContext.isTypeMatch("beanName", Object.class),
                        true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getType(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetTypeGivenName() {
        Class<?> expected = Object.class;
        assertExecutedWithReadLock(() -> delegateContext.getType("beanName"), () -> threadSafeContext.getType("beanName"), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getType(String, boolean)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetTypeGivenNameAndAllowFactoryInit() {
        Class<?> expected = Object.class;
        assertExecutedWithReadLock(() -> delegateContext.getType("beanName", true), () -> threadSafeContext.getType("beanName", true), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getAliases(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetAliases() {
        String[] expected = {"alias1", "alias2"};
        assertExecutedWithReadLock(() -> delegateContext.getAliases("beanName"), () -> threadSafeContext.getAliases("beanName"), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#containsBeanDefinition(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testContainsBeanDefinition() {
        assertExecutedWithReadLock(() -> delegateContext.containsBeanDefinition("beanName"), () -> threadSafeContext.containsBeanDefinition("beanName"), true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanDefinitionCount()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanDefinitionCount() {
        assertExecutedWithReadLock(delegateContext::getBeanDefinitionCount, threadSafeContext::getBeanDefinitionCount, 5);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanDefinitionNames()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanDefinitionNames() {
        String[] expected = {"bean1", "bean2"};
        assertExecutedWithReadLock(delegateContext::getBeanDefinitionNames, threadSafeContext::getBeanDefinitionNames, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanNamesForType(ResolvableType)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanNamesForResolvableType() {
        String[] expected = {"bean1", "bean2"};
        ResolvableType resolvableType = EasyMock.mock(ResolvableType.class);
        assertExecutedWithReadLock(() -> delegateContext.getBeanNamesForType(resolvableType), () -> threadSafeContext.getBeanNamesForType(resolvableType),
                        expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanNamesForType(Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanNamesForClassType() {
        String[] expected = {"bean1", "bean2"};
        assertExecutedWithReadLock(() -> delegateContext.getBeanNamesForType(Object.class), () -> threadSafeContext.getBeanNamesForType(Object.class),
                        expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanNamesForType(Class, boolean, boolean)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanNamesForClassTypeWithOptions() {
        String[] expected = {"bean1", "bean2"};
        assertExecutedWithReadLock(() -> delegateContext.getBeanNamesForType(Object.class, true, false),
                        () -> threadSafeContext.getBeanNamesForType(Object.class, true, false), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanNamesForType(ResolvableType, boolean, boolean)} is only executed after obtaining a
     * read lock.
     */
    @Test
    void testGetBeanNamesForResolvableTypeWithOptions() {
        String[] expected = {"bean1", "bean2"};
        ResolvableType resolvableType = EasyMock.mock(ResolvableType.class);
        assertExecutedWithReadLock(() -> delegateContext.getBeanNamesForType(resolvableType, true, false),
                        () -> threadSafeContext.getBeanNamesForType(resolvableType, true, false), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeansOfType(Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeansOfType() {
        Map<String,Object> expected = EasyMock.mock(Map.class);
        assertExecutedWithReadLock(() -> delegateContext.getBeansOfType(Object.class), () -> threadSafeContext.getBeansOfType(Object.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeansOfType(Class, boolean, boolean)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeansOfTypeWithOptions() {
        Map<String,Object> expected = EasyMock.mock(Map.class);
        assertExecutedWithReadLock(() -> delegateContext.getBeansOfType(Object.class, true, false),
                        () -> threadSafeContext.getBeansOfType(Object.class, true, false), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanNamesForAnnotation(Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanNamesForAnnotation() {
        String[] expected = {"bean1", "bean2"};
        assertExecutedWithReadLock(() -> delegateContext.getBeanNamesForAnnotation(Annotation.class),
                        () -> threadSafeContext.getBeanNamesForAnnotation(Annotation.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeansWithAnnotation(Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeansWithAnnotation() {
        Map<String,Object> expected = EasyMock.mock(Map.class);
        assertExecutedWithReadLock(() -> delegateContext.getBeansWithAnnotation(Annotation.class),
                        () -> threadSafeContext.getBeansWithAnnotation(Annotation.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#findAnnotationOnBean(String, Class)} is only executed after obtaining a read lock.
     */
    @Test
    void testFindAnnotationOnBean() {
        Annotation expected = EasyMock.mock(Annotation.class);
        assertExecutedWithReadLock(() -> delegateContext.findAnnotationOnBean("beanName", Annotation.class),
                        () -> threadSafeContext.findAnnotationOnBean("beanName", Annotation.class), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getParentBeanFactory()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetParentBeanFactory() {
        BeanFactory expected = EasyMock.mock(BeanFactory.class);
        assertExecutedWithReadLock(delegateContext::getParentBeanFactory, threadSafeContext::getParentBeanFactory, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#containsLocalBean(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testContainsLocalBean() {
        assertExecutedWithReadLock(() -> delegateContext.containsLocalBean("beanName"), () -> threadSafeContext.containsLocalBean("beanName"), true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getMessage(String, Object[], String, Locale)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetMessageGivenCodeAndArgsAndDefaultAndLocale() {
        String expected = "message";
        assertExecutedWithReadLock(() -> delegateContext.getMessage("code", null, "default", Locale.ENGLISH),
                        () -> threadSafeContext.getMessage("code", null, "default", Locale.ENGLISH), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getMessage(String, Object[], Locale)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetMessageGivenCodeAndArgsAndLocale() {
        String expected = "message";
        assertExecutedWithReadLock(() -> delegateContext.getMessage("code", new Object[] {"arg1"}, Locale.ENGLISH),
                        () -> threadSafeContext.getMessage("code", new Object[] {"arg1"}, Locale.ENGLISH), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getMessage(MessageSourceResolvable, Locale)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetMessageByResolvableAndLocale() {
        MessageSourceResolvable resolvable = EasyMock.mock(MessageSourceResolvable.class);
        String expected = "message";
        assertExecutedWithReadLock(() -> delegateContext.getMessage(resolvable, Locale.ENGLISH), () -> threadSafeContext.getMessage(resolvable, Locale.ENGLISH),
                        expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getResources(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetResources() {
        Resource[] expected = {EasyMock.mock(Resource.class)};
        assertExecutedWithReadLock(() -> {
            try {
                return delegateContext.getResources("locationPattern");
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
        expectReadLocked();

        // Expect the delegate method to be called and throw an IOException.
        EasyMock.expect(delegateContext.getResources("locationPattern")).andThrow(new IOException("test exception"));

        // Expect the read lock to be unlocked.
        expectReadUnlocked();

        replayAll();

        // Call the method under test and assert that it throws a RuntimeException.
        Exception exception = Assertions.assertThrows(RuntimeException.class, () -> threadSafeContext.getResources("locationPattern"));

        verifyAll();

        // Assert the exception is not suppressed and the cause is the original exception.
        Throwable cause = exception.getCause();
        Assertions.assertInstanceOf(IOException.class, cause);
        Assertions.assertEquals("test exception", cause.getMessage());
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#start()} is only executed after obtaining a write lock.
     */
    @Test
    void testStart() {
        assertExecutedWithWriteLock(delegateContext::start, threadSafeContext::start);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#stop()} is only executed after obtaining a write lock.
     */
    @Test
    void testStop() {
        assertExecutedWithWriteLock(delegateContext::stop, threadSafeContext::stop);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#isRunning()} is only executed after obtaining a read lock.
     */
    @Test
    void testIsRunning() {
        assertExecutedWithReadLock(delegateContext::isRunning, threadSafeContext::isRunning, true);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getClassLoader()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetClassLoader() {
        ClassLoader expected = EasyMock.mock(ClassLoader.class);
        assertExecutedWithReadLock(delegateContext::getClassLoader, threadSafeContext::getClassLoader, expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getResource(String)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetResource() {
        Resource expected = EasyMock.mock(Resource.class);
        assertExecutedWithReadLock(() -> delegateContext.getResource("location"), () -> threadSafeContext.getResource("location"), expected);
    }

    /**
     * Assert that the given delegate method is executed with a write lock and that the method under test delegates to the delegate method.
     *
     * @param delegateMethod
     *            the delegate method that should execute within a write lock
     * @param methodUnderTest
     *            the method under test
     */
    private void assertExecutedWithWriteLock(Runnable delegateMethod, Runnable methodUnderTest) {
        // Expect the write lock to be obtained.
        expectWriteLocked();

        // Expect the delegate method to be called.
        delegateMethod.run();

        // Expected the write lock to be unlocked.
        expectWriteUnlocked();

        replayAll();
        methodUnderTest.run();

        // Verify the order of method calls and that the write lock was obtained and released.
        verifyAll();
    }

    /**
     * Assert that the given delegate method is executed with a read lock and that the method under test returns the expected value.
     *
     * @param delegateMethod
     *            the delegate method that should execute within a read lock
     * @param methodUnderTest
     *            the method under test
     * @param expected
     *            the expected result of the delegate method
     * @param <T>
     *            the type of the expected result
     */
    private <T> void assertExecutedWithReadLock(Supplier<T> delegateMethod, Supplier<T> methodUnderTest, T expected) {
        // Expect the read lock to be obtained.
        expectReadLocked();

        // Expect the delegate method to be called and return the expected value.
        EasyMock.expect(delegateMethod.get()).andReturn(expected);

        // Expected the read lock to be unlocked.
        expectReadUnlocked();

        replayAll();
        T actual = methodUnderTest.get();
        Assertions.assertSame(expected, actual);

        // Verify the order of method calls and that the read lock was obtained and released.
        verifyAll();
    }

    private void expectReadLocked() {
        EasyMock.expect(readWriteLock.readLock()).andReturn(readLock);
        readLock.lock();
    }

    private void expectReadUnlocked() {
        EasyMock.expect(readWriteLock.readLock()).andReturn(readLock);
        readLock.unlock();
    }

    private void expectWriteLocked() {
        EasyMock.expect(readWriteLock.writeLock()).andReturn(writeLock);
        writeLock.lock();
    }

    private void expectWriteUnlocked() {
        EasyMock.expect(readWriteLock.writeLock()).andReturn(writeLock);
        writeLock.unlock();
    }
}
