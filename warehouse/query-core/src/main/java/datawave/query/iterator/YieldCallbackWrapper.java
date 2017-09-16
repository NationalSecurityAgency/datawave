package datawave.query.iterator;

import java.lang.reflect.Method;

/**
 * This callback handles the state of yielding within an iterator
 */
public class YieldCallbackWrapper<K> {
    private final Object delegate;
    private Method yieldMethod;
    private Method hasYieldedMethod;
    private Method getPositionAndResetMethod;
    
    public YieldCallbackWrapper(Object yieldCallback) {
        this.delegate = yieldCallback;
        for (Method m : delegate.getClass().getMethods()) {
            if (m.getName().equals("yield")) {
                yieldMethod = m;
            } else if (m.getName().equals("hasYielded")) {
                hasYieldedMethod = m;
            } else if (m.getName().equals("getPositionAndReset")) {
                getPositionAndResetMethod = m;
            }
        }
        if (yieldMethod == null || hasYieldedMethod == null || getPositionAndResetMethod == null) {
            throw new IllegalStateException("Expected to find a yield, hasYielded, and getPositionAndResetMethod method on the YieldCallback");
        }
    }
    
    /**
     * Called by the iterator when a next or seek call yields control.
     *
     * @param key
     *            the key position at which the iterator yielded.
     */
    public void yield(K key) {
        try {
            yieldMethod.invoke(delegate, key);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to call yield on yield callback", e);
        }
    }
    
    /**
     * Called by the client to see if the iterator yielded
     *
     * @return true if iterator yielded control
     */
    public boolean hasYielded() {
        try {
            return (Boolean) (hasYieldedMethod.invoke(delegate));
        } catch (Exception e) {
            throw new IllegalStateException("Unable to call hasYielded on yield callback", e);
        }
    }
    
    /**
     * Called by the client to get the yield position used as the start key (non-inclusive) of the range in a subsequent seek call when the iterator is rebuilt.
     * This will also reset the state returned by hasYielded.
     *
     * @return <tt>K</tt> The key position
     */
    public K getPositionAndReset() {
        try {
            return (K) (getPositionAndResetMethod.invoke(delegate));
        } catch (Exception e) {
            throw new IllegalStateException("Unable to call getPositionAndReset on yield callback", e);
        }
    }
}
