package datawave.query.tables;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Cleaner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

public class BatchScannerImpl extends ScannerOptions implements BatchScanner {
    private static final Logger log = LoggerFactory.getLogger(BatchScannerImpl.class);
    private static final AtomicInteger nextBatchReaderInstance = new AtomicInteger(1);

    private final int batchReaderInstance = nextBatchReaderInstance.getAndIncrement();
    private final TableId tableId;
    private final String tableName;
    private final int numThreads;
    private final ThreadPoolExecutor queryThreadPool;
    private final ClientContext context;
    private final Authorizations authorizations;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Cleaner.Cleanable cleanable;

    private ArrayList<Range> ranges = null;

    public BatchScannerImpl(ClientContext context, TableId tableId,
                                      String tableName, Authorizations authorizations, int numQueryThreads) {
        checkArgument(context != null, "context is null");
        checkArgument(tableId != null, "tableId is null");
        checkArgument(authorizations != null, "authorizations is null");
        this.context = context;
        this.authorizations = authorizations;
        this.tableId = tableId;
        this.tableName = tableName;
        this.numThreads = numQueryThreads;

        queryThreadPool = (ThreadPoolExecutor)Executors.newFixedThreadPool(numQueryThreads); /*
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread("batch scanner " + batchReaderInstance + "-");
                    }
                });*/
        // Call shutdown on this thread pool in case the caller does not call close().
        cleanable = CleanerUtil.shutdownThreadPoolExecutor(queryThreadPool, closed, log);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Shutdown the pool
            queryThreadPool.shutdownNow();
            // deregister the cleaner, will not call shutdownNow() because closed is now true
            cleanable.clean();
        }
    }

    @Override
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public void setRanges(Collection<Range> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            throw new IllegalArgumentException("ranges must be non null and contain at least 1 range");
        }

        if (closed.get()) {
            throw new IllegalStateException("batch reader closed");
        }

        this.ranges = new ArrayList<>(ranges);
    }

    @Override
    public Iterator<Map.Entry<Key, Value>> iterator() {
        if (ranges == null) {
            throw new IllegalStateException("ranges not set");
        }

        if (closed.get()) {
            throw new IllegalStateException("batch reader closed");
        }

        return new BatchScan(context, tableId, authorizations, ranges,
                numThreads, queryThreadPool, this, timeOut, log.isTraceEnabled());
    }
}