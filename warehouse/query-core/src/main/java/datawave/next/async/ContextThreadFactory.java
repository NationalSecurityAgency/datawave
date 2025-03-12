package datawave.next.async;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import datawave.next.scanner.DocumentRangeScan;
import datawave.next.scanner.QueryDataConsumer;

/**
 * A {@link ThreadFactory} that allows threads to be created with a default name, or context.
 * <p>
 * It is expected that the {@link QueryDataConsumer} or {@link DocumentRangeScan} will update their thread name with something more specific.
 */
public class ContextThreadFactory implements ThreadFactory {

    private final String context;
    private final ThreadFactory threadFactory;
    private final ContextualUncaughtExceptionHandler uncaughtExceptionHandler;

    public ContextThreadFactory(String context) {
        this.context = context;
        this.threadFactory = Executors.defaultThreadFactory();
        this.uncaughtExceptionHandler = new ContextualUncaughtExceptionHandler();
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = threadFactory.newThread(r);
        thread.setName(context);
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return thread;
    }
}
