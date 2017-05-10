package datawave.webservice.query.util;

import java.lang.Thread.UncaughtExceptionHandler;

public class QueryUncaughtExceptionHandler implements UncaughtExceptionHandler {
    
    private Thread thread;
    private Throwable throwable;
    
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
}
