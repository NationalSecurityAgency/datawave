package datawave.webservice.query.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryUncaughtExceptionHandler implements UncaughtExceptionHandler {

    private Thread thread;
    private Throwable throwable;
    private List<String> messages = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        // keep only the first one
        if (this.throwable == null) {
            synchronized (this) {
                this.thread = t;
                this.throwable = e;
            }
        }
    }

    public Thread getThread() {
        return thread;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void addMessage(String message) {
        messages.add(message);
    }

    public List<String> getMessages() {
        synchronized (messages) {
            return Collections.unmodifiableList(new ArrayList<>(messages));
        }
    }
}
