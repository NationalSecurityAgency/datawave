package datawave.query.util;

/**
 * Utility for checking whether a throwable is an Accumulo IterationInterruptedException without importing the non-public class directly. Uses
 * {@link ClassLoader#loadClass(String)} so that a {@link ClassNotFoundException} is thrown at class-load time if the exception class is ever removed from
 * Accumulo, rather than silently failing to match.
 */
public class IterationInterruptedCheck {

    private static final Class<?> IIE_CLASS;

    static {
        try {
            IIE_CLASS = IterationInterruptedCheck.class.getClassLoader()
                            .loadClass("org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException");
        } catch (ClassNotFoundException e) {
            throw new NoClassDefFoundError("IterationInterruptedException not found in Accumulo; "
                            + "this class may have been removed or relocated in a newer version: " + e.getMessage());
        }
    }

    private IterationInterruptedCheck() {
        throw new UnsupportedOperationException();
    }

    /**
     * Check if the given throwable is an instance of Accumulo's non-public IterationInterruptedException.
     *
     * @param t
     *            the throwable to check
     * @return true if t is an IterationInterruptedException
     */
    public static boolean isIterationInterruptedException(Throwable t) {
        return t != null && IIE_CLASS.isInstance(t);
    }
}
