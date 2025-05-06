package datawave.configuration.spring;

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
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.core.metrics.ApplicationStartup;

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
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#setApplicationStartup(ApplicationStartup)} is only executed after obtaining a write lock.
     */
    @Test
    void testSetApplicationStartup() {
        ApplicationStartup startup = EasyMock.createMock(ApplicationStartup.class);
        assertExecutedWithWriteLock(() -> delegateContext.setApplicationStartup(startup), () -> threadSafeContext.setApplicationStartup(startup));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getApplicationStartup()} is only executed after obtaining a read lock.
     */
    @Test
    void testGetApplicationStartup() {
        ApplicationStartup expected = EasyMock.createMock(ApplicationStartup.class);
        assertExecutedWithReadLock(() -> delegateContext.getApplicationStartup(), () -> threadSafeContext.getApplicationStartup(), expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#setClassLoader(ClassLoader)} is only executed after obtaining a write lock.
     */
    @Test
    void testSetClassLoader() {
        ClassLoader classLoader = EasyMock.createMock(ClassLoader.class);
        assertExecutedWithWriteLock(() -> delegateContext.setClassLoader(classLoader), () -> threadSafeContext.setClassLoader(classLoader));
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanProvider(Class, boolean)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanProviderGivenClassAndAllowEagerInit() {
        ObjectProvider<String> expected = EasyMock.createMock(ObjectProvider.class);
        assertExecutedWithReadLock(() -> delegateContext.getBeanProvider(String.class, true), () -> threadSafeContext.getBeanProvider(String.class, true),
                        expected);
    }

    /**
     * Verify that {@link ThreadSafeClassPathXmlApplicationContext#getBeanProvider(ResolvableType, boolean)} is only executed after obtaining a read lock.
     */
    @Test
    void testGetBeanProviderGivenResolvableTypeAndAllowEagerInit() {
        ObjectProvider<?> expected = EasyMock.createMock(ObjectProvider.class);
        ResolvableType resolvableType = EasyMock.createMock(ResolvableType.class);
        assertExecutedWithReadLock(() -> delegateContext.getBeanProvider(resolvableType, true), () -> threadSafeContext.getBeanProvider(resolvableType, true),
                        expected);
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
