package datawave.experimental.threads;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * ThreadFactory that sets both a name and an UncaughtExceptionHandler on threads
 */
public class NamedThreadFactory implements ThreadFactory {

    private final String name;
    private final ThreadFactory factory = Executors.defaultThreadFactory();

    private final Thread.UncaughtExceptionHandler handler;

    public NamedThreadFactory(String name, Thread.UncaughtExceptionHandler handler) {
        this.name = name;
        this.handler = handler;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = factory.newThread(r);
        thread.setName(name);
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler(handler);
        return thread;
    }
}
