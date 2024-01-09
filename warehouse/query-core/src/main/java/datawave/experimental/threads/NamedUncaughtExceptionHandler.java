package datawave.experimental.threads;

import org.apache.log4j.Logger;

/**
 * UncaughtExceptionHandler that logs the name of a thread or thread pool for additional context
 */
public class NamedUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger log = Logger.getLogger(NamedUncaughtExceptionHandler.class);
    private final String name;

    public NamedUncaughtExceptionHandler(String name) {
        this.name = name;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error("[" + name + "] exception: " + e);
    }
}
