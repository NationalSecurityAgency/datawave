package datawave.webservice.common.remote;

import java.io.IOException;
import java.util.List;

import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;

public class DatawaveRetryHandler extends DefaultHttpRequestRetryHandler {
    private static final Logger log = LoggerFactory.getLogger(DatawaveRetryHandler.class);
    private int unavailableRetryCount;
    private int unavailableRetryDelay;
    private final Counter retryCounter;
    private List<Class<? extends IOException>> unavailableRetryClasses;

    public DatawaveRetryHandler(int retryCount, int unavailableRetryCount, int unavailableRetryDelay, Counter retryCounter,
                    List<Class<? extends IOException>> nonRetriableClasses, List<Class<? extends IOException>> unavailableRetryClasses) {
        super(retryCount, false, nonRetriableClasses);
        this.unavailableRetryCount = unavailableRetryCount;
        this.unavailableRetryDelay = unavailableRetryDelay;
        this.retryCounter = retryCounter;
        this.unavailableRetryClasses = unavailableRetryClasses;
    }

    @Override
    public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
        boolean shouldRetry = super.retryRequest(exception, executionCount, context);
        // if any class e is the same as exception or any class e is a superclass of exception then retryWithDelay
        boolean retryWithDelay = unavailableRetryClasses.stream().anyMatch(e -> e.isAssignableFrom(exception.getClass()));
        if (retryWithDelay) {
            shouldRetry = (executionCount <= unavailableRetryCount);
            if (shouldRetry) {
                try {
                    if (log.isTraceEnabled()) {
                        log.trace("retrying call after exception {}, executionCount {}, sleeping for {}ms", exception.getClass().getName(), executionCount,
                                        unavailableRetryDelay);
                    }
                    Thread.sleep(unavailableRetryDelay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // Ignore -- we'll just end up retrying a little too fast
                }
            }
        } else {
            if (log.isTraceEnabled()) {
                log.trace("retrying call after exception {}, executionCount {}", exception.getClass().getName(), executionCount);
            }
        }
        if (shouldRetry) {
            retryCounter.inc();
        }
        return shouldRetry;
    }

    public void setUnavailableRetryCount(int unavailableRetryCount) {
        this.unavailableRetryCount = unavailableRetryCount;
    }

    public void setUnavailableRetryDelay(int unavailableRetryDelay) {
        this.unavailableRetryDelay = unavailableRetryDelay;
    }
}
