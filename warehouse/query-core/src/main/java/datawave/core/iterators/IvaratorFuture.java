package datawave.core.iterators;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IvaratorFuture implements Future {

    final private Future future;
    final private DatawaveFieldIndexCachingIteratorJexl ivarator;

    public IvaratorFuture(Future future, DatawaveFieldIndexCachingIteratorJexl ivarator) {
        this.future = future;
        this.ivarator = ivarator;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return this.future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return this.future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return this.future.isDone();
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        return this.future.get();
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return this.future.get(timeout, unit);
    }

    public DatawaveFieldIndexCachingIteratorJexl getIvarator() {
        return ivarator;
    }
}
