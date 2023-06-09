package datawave.common.util;

public class ArgumentChecker {
    private static final String NULL_ARG_MSG = "argument was null";
    
    private ArgumentChecker() {
        throw new IllegalStateException("Do not instantiate utility class.");
    }
    
    public static final void notNull(final Object arg1) {
        if (arg1 == null)
            throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? true");
    }
    
    public static final void notNull(final Object arg1, final Object arg2) {
        if (arg1 == null || arg2 == null)
            throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? " + (arg1 == null) + " arg2? " + (arg2 == null));
    }
    
    public static final void notNull(final Object arg1, final Object arg2, final Object arg3) {
        if (arg1 == null || arg2 == null || arg3 == null)
            throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? " + (arg1 == null) + " arg2? " + (arg2 == null) + " arg3? " + (arg3 == null));
    }
    
    public static final void notNull(final Object arg1, final Object arg2, final Object arg3, final Object arg4) {
        if (arg1 == null || arg2 == null || arg3 == null || arg4 == null)
            throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? " + (arg1 == null) + " arg2? " + (arg2 == null) + " arg3? " + (arg3 == null)
                            + " arg4? " + (arg4 == null));
    }
}
