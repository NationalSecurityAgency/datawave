package datawave.next.async;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.next.scanner.DocumentRangeScan;
import datawave.next.scanner.QueryDataConsumer;

/**
 * A {@link ThreadFactory} that allows threads to be created with a default name, or context.
 * <p>
 * It is expected that the {@link QueryDataConsumer} or {@link DocumentRangeScan} will update their thread name with something more specific.
 */
public class ContextThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(ContextThreadFactory.class);

    private final String context;
    private final ThreadFactory threadFactory;
    private UncaughtExceptionHandler uncaughtExceptionHandler;

    private long threadsCreated = 0L;

    public ContextThreadFactory(String context) {
        this.context = context;
        this.threadFactory = Executors.defaultThreadFactory();
        this.uncaughtExceptionHandler = null;
    }

    @Override
    public Thread newThread(Runnable r) {
        log.trace("creating new thread");
        threadsCreated++;
        Thread thread = threadFactory.newThread(r);
        thread.setName(context);
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return thread;
    }

    public void setUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    }

    public void logThreadsCreated() {
        log.info("created {} threads for context: {}", threadsCreated, context);
    }
}
