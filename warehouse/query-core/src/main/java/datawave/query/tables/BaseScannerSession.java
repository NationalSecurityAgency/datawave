package datawave.query.tables;

import com.google.common.util.concurrent.AbstractExecutionThreadService;

import java.util.Iterator;

/**
 * This will handles running a scan against a set of ranges. The actual scan is performed in a separate thread which places the results in a result queue. The
 * result queue is polled in the actual next() and hasNext() calls. Note that the uncaughtExceptionHandler from the Query is used to pass exceptions up which
 * will also fail the overall query if something happens. If this is not desired then a local handler should be set.
 */
public abstract class BaseScannerSession<T> extends AbstractExecutionThreadService implements Iterator<T> {
    public abstract void close();
}
