package datawave.experimental.threads;

import org.apache.log4j.Logger;

public class UidUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger log = Logger.getLogger(UidUncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error("exception: " + e);
    }
}
