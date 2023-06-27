package datawave.query.index.lookup;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Lists;

import datawave.query.index.lookup.IndexStream.StreamContext;

/**
 * Callable class that the executor will use to stand up the ScannerSessions
 *
 * We are not concerned about the threads in this pool as we are simply building a scanner session when we call hasNext(), if records exist.
 */
public class ConcurrentScannerInitializer implements Callable<BaseIndexStream> {

    private BaseIndexStream stream;

    public ConcurrentScannerInitializer(BaseIndexStream stream) {
        this.stream = stream;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public BaseIndexStream call() throws Exception {
        if (stream.context() == StreamContext.INITIALIZED) {
            if (stream.hasNext()) {
                // The RangeStream created a scanner with a context of INITIALIZED and a next value exists.
                // Update the scanner context to PRESENT.
                // This avoids the unfortunate situation when a scanner stream is double initialized using
                // a constructor meant for the SHARDS_AND_DAYS case (where seeking is effectively disabled)
                stream.context = StreamContext.PRESENT;
                return stream;
            } else {
                return ScannerStream.noData(stream.currentNode());
            }
        } else {
            return stream;
        }
    }

    public static Collection<BaseIndexStream> initializeScannerStreams(List<ConcurrentScannerInitializer> todo, ExecutorService executor) {

        List<Future<BaseIndexStream>> futures;
        List<BaseIndexStream> streams = Lists.newArrayList();
        try {
            futures = executor.invokeAll(todo);

            for (Future<BaseIndexStream> future : futures) {
                Exception sawException = null;
                try {
                    BaseIndexStream newStream = null;

                    while (!executor.isShutdown()) {
                        try {
                            newStream = future.get(1, TimeUnit.SECONDS);
                            break;
                        } catch (TimeoutException e) {

                        }
                    }
                    if (executor.isShutdown())
                        future.cancel(true);
                    if (newStream != null) {
                        streams.add(newStream);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    sawException = (Exception) e.getCause();
                }

                if (null != sawException) {
                    throw new RuntimeException(sawException);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            todo.clear();
        }
        return streams;
    }
}
