package datawave.test.helpers;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

import com.google.common.collect.ForwardingIterator;

/**
 * This class creates a new instance of an {@link InterruptibleScanner}
 */
public class InterruptibleScannerFactory {

    private InterruptibleScannerFactory() {
        throw new IllegalStateException("Do not instantiate utility class");
    }

    public static InterruptibleScanner create(Scanner delegate) {
        return (InterruptibleScanner) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[] {InterruptibleScanner.class},
                        new InterruptibleScannerHandler(delegate));
    }

    /**
     * Wrap an iterator to save the last emitted key
     */
    public static class LastKeyIterator extends ForwardingIterator<Map.Entry<Key,Value>> {
        Iterator<Map.Entry<Key,Value>> delegate;
        Key lastKey;

        public LastKeyIterator(Iterator<Map.Entry<Key,Value>> other) {
            delegate = other;
        }

        @Override
        protected Iterator<Map.Entry<Key,Value>> delegate() {
            return delegate;
        }

        @Override
        public Map.Entry<Key,Value> next() {
            Map.Entry<Key,Value> entry = delegate.next();
            lastKey = entry.getKey();
            return entry;
        }

        public Key getLastKey() {
            return lastKey;
        }
    }

    private static class InterruptibleScannerHandler implements InvocationHandler {
        private Scanner delegate;
        private LastKeyIterator inner;

        public InterruptibleScannerHandler(Scanner delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "interrupt":
                    return interrupt();
                case "iterator":
                    return iterator();
                default:
                    return method.invoke(delegate, args);
            }
        }

        private Iterator<Map.Entry<Key,Value>> interrupt() {
            Range interruptPoint = new Range(inner.getLastKey(), false, delegate.getRange().getEndKey(), delegate.getRange().isEndKeyInclusive());
            delegate.setRange(interruptPoint);
            return iterator();
        }

        private Iterator<Map.Entry<Key,Value>> iterator() {
            inner = new LastKeyIterator(delegate.iterator());
            return inner;
        }

    }
}
