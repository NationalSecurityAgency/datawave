package datawave.core.iterators;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IvaratorFuture implements Future {

    private Future future;
    private IvaratorRunnable ivaratorRunnable;

    public IvaratorFuture(Future future, IvaratorRunnable ivaratorRunnable) {
        this.future = future;
        this.ivaratorRunnable = ivaratorRunnable;
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
        return ivaratorRunnable.getIvarator();
    }

    public IvaratorRunnable getIvaratorRunnable() {
        return ivaratorRunnable;
    }
}
