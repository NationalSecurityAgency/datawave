package datawave.next.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextualUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ContextualUncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        t.interrupt();
        log.error("Uncaught exception in thread {}", t.getName(), e);
        throw new RuntimeException(e);
    }
}
