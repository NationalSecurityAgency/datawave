package datawave.experimental.threads;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

public class FutureCompleteThread implements Runnable, FutureCallback<FutureCompleteThread> {

    private static final Logger log = Logger.getLogger(FutureCompleteThread.class);

    private final AtomicBoolean flag;
    private final List<Future<?>> futures;

    public FutureCompleteThread(AtomicBoolean flag, List<Future<?>> futures) {
        this.flag = flag;
        this.futures = futures;
    }

    @Override
    public void run() {
        boolean allFinished = false;
        while (!allFinished) {
            for (Future<?> future : futures) {
                allFinished = true;
                if (!(future.isDone() || future.isCancelled())) {
                    allFinished = false;
                    break;
                }
            }
        }
        flag.set(false);
    }

    @Override
    public void onSuccess(FutureCompleteThread result) {
        // do nothing
    }

    @Override
    public void onFailure(Throwable t) {
        log.error(t);
        Throwables.propagateIfPossible(t, RuntimeException.class);
    }
}
