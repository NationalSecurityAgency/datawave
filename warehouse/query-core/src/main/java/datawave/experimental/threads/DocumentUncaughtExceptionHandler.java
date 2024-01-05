package datawave.experimental.threads;

import org.apache.log4j.Logger;

public class DocumentUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger log = Logger.getLogger(DocumentUncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error("exception: " + e);
    }
}
