package datawave.query.tables.document.batch;


import datawave.query.DocumentSerialization;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.clientImpl.AccumuloServerException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.clientImpl.TabletType;
import org.apache.accumulo.core.clientImpl.ThriftScanner;
import org.apache.accumulo.core.clientImpl.TimeoutTabletLocator;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialMultiScan;
import org.apache.accumulo.core.dataImpl.thrift.InitialScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.dataImpl.thrift.ScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.TSampleNotPresentException;
import org.apache.accumulo.core.tabletserver.thrift.TabletScanClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class DocumentScan implements Iterator<SerializedDocumentIfc> {

    private static final Logger log = LoggerFactory.getLogger(DocumentScan.class);

    private final ClientContext context;
    private final TableId tableId;
    private final DocumentSerialization.ReturnType returnType;
    private final boolean docRawFields;
    private final int maxTabletThreshold;
//    private final AtomicBoolean singleRange;
    private int maxTabletsPerThread=0;
    private Authorizations authorizations = Authorizations.EMPTY;
    private final int numThreads;
    private final ExecutorService queryThreadPool;
    private final ScannerOptions options;

    private ArrayBlockingQueue<List<SerializedDocumentIfc>> resultsQueue;
    private Iterator<SerializedDocumentIfc> batchIterator;
    private List<SerializedDocumentIfc> batch;
    private static final List<SerializedDocumentIfc> LAST_BATCH = new ArrayList<>();
    private final Object nextLock = new Object();
    private long failSleepTime = 100;

    private volatile Throwable fatalException = null;

    private Map<String, TimeoutTracker> timeoutTrackers;
    private Set<String> timedoutServers;
    private final long timeout;

    private TabletLocator locator;
    private List<ByteBuffer> sharedByteBuffers=null;
//    private List<TColumn> translatedColumns;


    public interface ResultReceiver {
        void receive(List<SerializedDocumentIfc> entries);
    }



    public DocumentScan(ClientContext context, TableId tableId,
                        Authorizations authorizations, ArrayList<Range> ranges, int numThreads,
                        ExecutorService queryThreadPool, ScannerOptions scannerOptions, long timeout, boolean printOutput, DocumentSerialization.ReturnType returnType, boolean docRawFields, int queueCapacity, int maxTabletsPerThread, int maxTabletThreshold) {

        this.context = context;
        this.tableId = tableId;
        this.authorizations = authorizations;
        this.numThreads = numThreads;
        this.queryThreadPool = queryThreadPool;
        this.options = new ScannerOptions(scannerOptions);
        resultsQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.returnType = returnType;
        this.docRawFields=docRawFields;
        this.maxTabletsPerThread = maxTabletsPerThread;
        this.maxTabletThreshold=maxTabletThreshold;
        this.locator = new TimeoutTabletLocator(timeout, context, tableId);
        this.timeoutTrackers = Collections.synchronizedMap(new HashMap());
        this.timedoutServers = Collections.synchronizedSet(new HashSet());

        //this.singleRange = new AtomicBoolean(ranges.size()==1);
        //this.singleRange = new AtomicBoolean(true);
        this.timeout = timeout;
        if (options.getFetchedColumns().size() > 0) {
            ArrayList<Range> ranges2 = new ArrayList<>(ranges.size());
            for (Range range : ranges) {
                ranges2.add(range.bound(options.getFetchedColumns().first(), options.getFetchedColumns().last()));
            }

            ranges = ranges2;
        }

        ResultReceiver rr = printOutput ? entries -> {
            try {
                log.trace("Received " + entries.size() + " " + resultsQueue.size());
                resultsQueue.put(entries);
            } catch (InterruptedException e) {
                if (this.queryThreadPool.isShutdown())
                    log.debug("Failed to add Batch Scan result", e);
                else
                    log.warn("Failed to add Batch Scan result", e);
                fatalException = e;
                throw new RuntimeException(e);

            }
        } : entries -> {
            try {
                resultsQueue.put(entries);
            } catch (InterruptedException e) {
                if (this.queryThreadPool.isShutdown())
                    log.debug("Failed  to add Batch Scan result", e);
                else
                    log.warn("Failed to add Batch Scan result", e);
                fatalException = e;
                throw new RuntimeException(e);

            }
        };

        try {
            lookup(ranges, rr);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create iterator", e);
        }
    }

    @Override
    public boolean hasNext() {
        synchronized (nextLock) {
            if (batch == LAST_BATCH)
                return false;

            if (batch != null && batchIterator.hasNext())
                return true;

            // don't have one cached, try to cache one and return success
            try {
                batch = null;
                while (batch == null && fatalException == null && !queryThreadPool.isShutdown())
                    batch = resultsQueue.poll(5, TimeUnit.MILLISECONDS);

                if (fatalException != null)
                    if (fatalException instanceof RuntimeException)
                        throw (RuntimeException) fatalException;
                    else
                        throw new RuntimeException(fatalException);

                if (queryThreadPool.isShutdown()) {
                    String shortMsg =
                            "The BatchScanner was unexpectedly closed while" + " this Iterator was still in use.";
                    log.error("{} Ensure that a reference to the BatchScanner is retained"
                            + " so that it can be closed when this Iterator is exhausted. Not"
                            + " retaining a reference to the BatchScanner guarantees that you are"
                            + " leaking threads in your client JVM.", shortMsg);
                    throw new RuntimeException(shortMsg + " Ensure proper handling of the BatchScanner.");
                }

                batchIterator = batch.iterator();
                return batch != LAST_BATCH;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public SerializedDocumentIfc next() {
        // if there's one waiting, or hasNext() can get one, return it
        synchronized (nextLock) {
            if (hasNext())
                return batchIterator.next();
            else
                throw new NoSuchElementException();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private synchronized void lookup(List<Range> ranges, ResultReceiver receiver)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        List<Column> columns = new ArrayList<>(options.getFetchedColumns());
        ranges = Range.mergeOverlapping(ranges);

        if (log.isTraceEnabled()){
            log.trace("After merging overlapping ranges we now have " + ranges.size() + " ranges");
        }

        Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();

        binRanges(locator, ranges, binnedRanges);

        List<TColumn> translatedColumns = columns.stream().map(Column::toThrift).collect(Collectors.toList());

        doLookups(binnedRanges, receiver, translatedColumns);
    }

    private void binRanges(TabletLocator tabletLocator, List<Range> ranges,
                           Map<String,Map<KeyExtent,List<Range>>> binnedRanges)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

        int lastFailureSize = Integer.MAX_VALUE;

        while (true) {

            binnedRanges.clear();
            List<Range> failures = tabletLocator.binRanges(context, ranges, binnedRanges);

            if (failures.size() > 0) {
                // tried to only do table state checks when failures.size() == ranges.size(), however this
                // did
                // not work because nothing ever invalidated entries in the tabletLocator cache... so even
                // though
                // the table was deleted the tablet locator entries for the deleted table were not
                // cleared... so
                // need to always do the check when failures occur
                if (failures.size() >= lastFailureSize)
                    if (!context.tableNodeExists(tableId))
                        throw new TableDeletedException(tableId.canonical());
                    if (context.getTableState(tableId) == TableState.OFFLINE)
                        throw new TableOfflineException("Table (" + tableId.canonical() + ") is offline");

                lastFailureSize = failures.size();

                if (log.isTraceEnabled())
                    log.trace("Failed to bin {} ranges, tablet locations were null, retrying in 100ms",
                            failures.size());

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                break;
            }

        }

        // truncate the ranges to within the tablets... this makes it easier to know what work
        // needs to be redone when failures occurs and tablets have merged or split
        Map<String,Map<KeyExtent,List<Range>>> binnedRanges2 = new HashMap<>();
        for (Entry<String,Map<KeyExtent,List<Range>>> entry : binnedRanges.entrySet()) {
            Map<KeyExtent,List<Range>> tabletMap = new HashMap<>();
            binnedRanges2.put(entry.getKey(), tabletMap);
            for (Entry<KeyExtent,List<Range>> tabletRanges : entry.getValue().entrySet()) {
                Range tabletRange = tabletRanges.getKey().toDataRange();
                List<Range> clippedRanges = new ArrayList<>();
                tabletMap.put(tabletRanges.getKey(), clippedRanges);
                for (Range range : tabletRanges.getValue())
                    clippedRanges.add(tabletRange.clip(range));
            }
        }

        binnedRanges.clear();
        binnedRanges.putAll(binnedRanges2);
    }

    private void processFailures(Map<KeyExtent,List<Range>> failures, ResultReceiver receiver,
                                 List<TColumn> translatedColumns)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (log.isTraceEnabled())
            log.trace("Failed to execute multiscans against {} tablets, retrying...", failures.size());

        try {
            Thread.sleep(failSleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            // We were interrupted (close called on batchscanner) just exit
            log.debug("Exiting failure processing on interrupt");
            return;
        }

        failSleepTime = Math.min(5000, failSleepTime * 2);

        Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();
        List<Range> allRanges = new ArrayList<>();

        for (List<Range> ranges : failures.values())
            allRanges.addAll(ranges);

        // since the first call to binRanges clipped the ranges to within a tablet, we should not get
        // only
        // bin to the set of failed tablets
        binRanges(locator, allRanges, binnedRanges);

        doLookups(binnedRanges, receiver, translatedColumns);
    }

    private String getTableInfo() {

        return context.getPrintableTableInfoFromId(tableId);
    }

    private static class TimeoutTracker {
        String server;
        Set<String> badServers;
        long timeOut;
        long activityTime;
        Long firstErrorTime;

        TimeoutTracker(String server, Set<String> badServers, long timeOut) {
            this(timeOut);
            this.server = server;
            this.badServers = badServers;
        }

        TimeoutTracker(long timeOut) {
            this.firstErrorTime = null;
            this.timeOut = timeOut;
        }

        void startingScan() {
            this.activityTime = System.currentTimeMillis();
        }

        void check() throws IOException {
            if (System.currentTimeMillis() - this.activityTime > this.timeOut) {
                this.badServers.add(this.server);
                throw new IOException("Time exceeded " + (System.currentTimeMillis() - this.activityTime) + " " + this.server);
            }
        }

        void madeProgress() {
            this.activityTime = System.currentTimeMillis();
            this.firstErrorTime = null;
        }

        void errorOccured() {
            if (this.firstErrorTime == null) {
                this.firstErrorTime = this.activityTime;
            } else if (System.currentTimeMillis() - this.firstErrorTime > this.timeOut) {
                this.badServers.add(this.server);
            }

        }

        public long getTimeOut() {
            return this.timeOut;
        }
    }

    private class QueryTask implements Runnable {

        private final AtomicBoolean singleRange;
        private String tsLocation;
        private Map<KeyExtent,List<Range>> tabletsRanges;
        private ResultReceiver receiver;
        private Semaphore semaphore = null;
        private final Map<KeyExtent,List<Range>> failures;
        private List<TColumn> translatedColumns;
        private int semaphoreSize;

        QueryTask(String tsLocation, Map<KeyExtent, List<Range>> tabletsRanges,
                  Map<KeyExtent, List<Range>> failures, ResultReceiver receiver, List<TColumn> columns, DocumentSerialization.ReturnType returnType, AtomicBoolean singleRange) {
            this.tsLocation = tsLocation;
            this.tabletsRanges = tabletsRanges;
            this.receiver = receiver;
            this.translatedColumns = columns;
            this.failures = failures;
            this.singleRange = singleRange;
        }

        void setSemaphore(Semaphore semaphore, int semaphoreSize) {
            this.semaphore = semaphore;
            this.semaphoreSize = semaphoreSize;
        }

        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            Thread.currentThread()
                    .setName(threadName + " looking up " + tabletsRanges.size() + " ranges at " + tsLocation);
            Map<KeyExtent,List<Range>> unscanned = new HashMap<>();
            Map<KeyExtent,List<Range>> tsFailures = new HashMap<>();
            try {
                TimeoutTracker timeoutTracker = timeoutTrackers.get(tsLocation);
                if (timeoutTracker == null) {
                    timeoutTracker = new TimeoutTracker(tsLocation, timedoutServers, timeout);
                    timeoutTrackers.put(tsLocation, timeoutTracker);
                }
                doLookup(context, tsLocation, tabletsRanges, tsFailures, unscanned, receiver, translatedColumns,
                        options, sharedByteBuffers,authorizations,timeoutTracker, returnType,docRawFields, singleRange);
                if (tsFailures.size() > 0) {
                    locator.invalidateCache(tsFailures.keySet());
                    synchronized (failures) {
                        failures.putAll(tsFailures);
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
                if (!DocumentScan.this.queryThreadPool.isShutdown()) {
                    synchronized (failures) {
                        failures.putAll(tsFailures);
                        failures.putAll(unscanned);
                    }

                    locator.invalidateCache(context, tsLocation);
                }
                log.debug("IOException thrown", e);
            } catch (AccumuloSecurityException e) {
                e.printStackTrace();
                e.setTableInfo(getTableInfo());
                log.debug("AccumuloSecurityException thrown", e);

                if (!context.tableNodeExists(tableId))
                fatalException = new TableDeletedException(tableId.canonical());
                else
                fatalException = e;
            } catch (SampleNotPresentException e) {
                fatalException = e;
            } catch (Throwable t) {
                t.printStackTrace();
                if (queryThreadPool.isShutdown())
                    log.debug("Caught exception, but queryThreadPool is shutdown", t);
                else
                    log.warn("Caught exception, but queryThreadPool is not shutdown", t);
                fatalException = t;
            } finally {
                semaphore.release();
                Thread.currentThread().setName(threadName);
                if (semaphore.tryAcquire(semaphoreSize)) {
                    // finished processing all queries
                    if (fatalException == null && failures.size() > 0) {
                        // there were some failures
                        try {
                            processFailures(failures, receiver, translatedColumns);
                        } catch (TableNotFoundException | AccumuloException e) {
                            log.debug("{}", e.getMessage(), e);
                            fatalException = e;
                        } catch (AccumuloSecurityException e) {
                            e.setTableInfo(getTableInfo());
                            log.debug("{}", e.getMessage(), e);
                            fatalException = e;
                        } catch (Throwable t) {
                            log.debug("{}", t.getMessage(), t);
                            fatalException = t;
                        }

                        if (fatalException != null) {
                            // we are finished with this batch query
                            if (!resultsQueue.offer(LAST_BATCH)) {
                                log.debug(
                                        "Could not add to result queue after seeing fatalException in processFailures",
                                        fatalException);
                            }
                        }
                    } else {
                        // we are finished with this batch query
                        if (fatalException != null) {
                            if (!resultsQueue.offer(LAST_BATCH)) {
                                log.debug("Could not add to result queue after seeing fatalException",
                                        fatalException);
                            }
                        } else {
                            try {
                                resultsQueue.put(LAST_BATCH);
                            } catch (InterruptedException e) {
                                fatalException = e;
                                if (!resultsQueue.offer(LAST_BATCH)) {
                                    log.debug("Could not add to result queue after seeing fatalException",
                                            fatalException);
                                }
                            }
                        }
                    }
                }
            }
        }

    }

    public static boolean isEventSpecific(Range range) {
        Text holder = new Text();
        Key startKey = range.getStartKey();
        startKey.getColumnFamily(holder);
        if (holder.getLength() > 0) {
            if (holder.find("\0") > 0) {
                return true;
            }
        }
        return false;
    }

    private void doLookups(Map<String,Map<KeyExtent,List<Range>>> binnedRanges,
                           final ResultReceiver receiver, List<TColumn> translatedColumns) {

        if (timedoutServers.containsAll(binnedRanges.keySet())) {
            // all servers have timed out
            throw new TimedOutException(timedoutServers);
        }
        // when there are lots of threads and a few tablet servers
        // it is good to break request to tablet servers up, the
        // following code determines if this is the case
        int maxTabletsPerRequest = Integer.MAX_VALUE;
        sharedByteBuffers = ByteBufferUtil.toByteBuffers(authorizations.getAuthorizations());
        log.trace("maxTabletsPerRequest for " + numThreads + " " + binnedRanges.size());
        int totalNumberOfTablets = 0;
        if (numThreads / binnedRanges.size() > 1) {
            for (Entry<String,Map<KeyExtent,List<Range>>> entry : binnedRanges.entrySet()) {
                totalNumberOfTablets += entry.getValue().size();
            }

            maxTabletsPerRequest = totalNumberOfTablets / (numThreads);
            log.trace("maxTabletsPerRequest should be " + maxTabletsPerRequest + " " + totalNumberOfTablets);
            if (maxTabletsPerRequest == 0) {
                maxTabletsPerRequest = 1;
            }


        }

        if (maxTabletsPerThread > 0) {
            if (totalNumberOfTablets >= maxTabletThreshold) {
                log.trace("maxTabletsPerRequest should be " + maxTabletsPerThread +  " " + totalNumberOfTablets);
                maxTabletsPerRequest = maxTabletsPerThread;
            } else {
                log.trace("maxTabletsPerRequest left at " + maxTabletsPerRequest + " because the threshold is " + maxTabletThreshold + "  on " + totalNumberOfTablets );
            }
        }

        Map<KeyExtent,List<Range>> failures = new HashMap<>();
        if (timedoutServers.size() > 0) {
            // go ahead and fail any timed out servers
            for (Iterator<Entry<String,Map<KeyExtent,List<Range>>>> iterator =
                 binnedRanges.entrySet().iterator(); iterator.hasNext();) {
                Entry<String,Map<KeyExtent,List<Range>>> entry = iterator.next();
                if (timedoutServers.contains(entry.getKey())) {
                    failures.putAll(entry.getValue());
                    iterator.remove();
                }
            }
        }

        // randomize tabletserver order... this will help when there are multiple
        // batch readers and writers running against accumulo
        List<String> locations = new ArrayList<>(binnedRanges.keySet());
        Collections.shuffle(locations);

        List<QueryTask> queryTasks = new ArrayList<>();
        log.trace("Creating {} query locations", locations.size());
        for (final String tsLocation : locations) {

            final Map<KeyExtent,List<Range>> tabletsRanges = binnedRanges.get(tsLocation);
            AtomicBoolean singleRange = new AtomicBoolean(true);
            log.trace("For {} we have {}", tsLocation, tabletsRanges.size());
            if (tabletsRanges.size() > 1){
                log.trace("tablet ranges", locations.size());
                singleRange.set(false);
            }
            else{
                log.trace("For {} we have {}", tsLocation, tabletsRanges.entrySet().iterator().next().getValue().size());
                if (tabletsRanges.entrySet().iterator().next().getValue().size() > 1){
                    singleRange.set(false);
                }
                else{
                    if (!isEventSpecific(tabletsRanges.entrySet().iterator().next().getValue().get(0))){
                        singleRange.set(false);
                    }
                }
            }
            if (maxTabletsPerRequest == Integer.MAX_VALUE || tabletsRanges.size() == 1) {
                QueryTask queryTask = new QueryTask(tsLocation, tabletsRanges, failures, receiver, translatedColumns, this.returnType, singleRange);
                queryTasks.add(queryTask);
            } else {
                HashMap<KeyExtent,List<Range>> tabletSubset = new HashMap<>();
                for (Entry<KeyExtent,List<Range>> entry : tabletsRanges.entrySet()) {
                    tabletSubset.put(entry.getKey(), entry.getValue());
                    if (tabletSubset.size() >= maxTabletsPerRequest) {
                        QueryTask queryTask =
                                new QueryTask(tsLocation, tabletSubset, failures, receiver, translatedColumns, this.returnType, singleRange);
                        queryTasks.add(queryTask);
                        tabletSubset = new HashMap<>();
                    }
                }

                if (tabletSubset.size() > 0) {
                    QueryTask queryTask =
                            new QueryTask(tsLocation, tabletSubset, failures, receiver, translatedColumns, this.returnType, singleRange);
                    queryTasks.add(queryTask);
                }
            }
        }
        log.trace("created query tasks");
        final Semaphore semaphore = new Semaphore(queryTasks.size());
        semaphore.acquireUninterruptibly(queryTasks.size());

        for (QueryTask queryTask : queryTasks) {
            queryTask.setSemaphore(semaphore, queryTasks.size());
            queryThreadPool.execute(queryTask);
        }
    }

    static void trackScanning(Map<KeyExtent,List<Range>> failures,
                              Map<KeyExtent,List<Range>> unscanned, MultiScanResult scanResult) {

        // translate returned failures, remove them from unscanned, and add them to failures
        Map<KeyExtent, List<Range>> retFailures = scanResult.failures.entrySet().stream().collect(Collectors.toMap(
                (entry) -> KeyExtent.fromThrift(entry.getKey()),
                (entry) -> entry.getValue().stream().map(Range::new).collect(Collectors.toList())
        ));
        unscanned.keySet().removeAll(retFailures.keySet());
        failures.putAll(retFailures);

        // translate full scans and remove them from unscanned
        Set<KeyExtent> fullScans =
                scanResult.fullScans.stream().map(KeyExtent::fromThrift).collect(Collectors.toSet());
        unscanned.keySet().removeAll(fullScans);

        // remove partial scan from unscanned
        if (scanResult.partScan != null) {
            KeyExtent ke = KeyExtent.fromThrift(scanResult.partScan);
            Key nextKey = new Key(scanResult.partNextKey);

            ListIterator<Range> iterator = unscanned.get(ke).listIterator();
            while (iterator.hasNext()) {
                Range range = iterator.next();

                if (range.afterEndKey(nextKey) || (nextKey.equals(range.getEndKey())
                        && scanResult.partNextKeyInclusive != range.isEndKeyInclusive())) {
                    iterator.remove();
                } else if (range.contains(nextKey)) {
                    iterator.remove();
                    Range partRange = new Range(nextKey, scanResult.partNextKeyInclusive, range.getEndKey(),
                            range.isEndKeyInclusive());
                    iterator.add(partRange);
                }
            }
        }
    }



    static void doLookup(ClientContext context, String server, Map<KeyExtent,List<Range>> requested,
                         Map<KeyExtent,List<Range>> failures, Map<KeyExtent,List<Range>> unscanned,
                         ResultReceiver receiver, List<TColumn> columns, ScannerOptions options,
                         List<ByteBuffer> sharedByteBuffers,
                         Authorizations authorizations  , TimeoutTracker timeoutTracker , DocumentSerialization.ReturnType returnType, boolean docRawFields, AtomicBoolean singleRange)
            throws IOException, AccumuloSecurityException, AccumuloServerException {

        if (singleRange.get()){
            log.trace("Doing single lookup on {}",requested);
            doSingleLookup(context,server,requested,failures,unscanned,receiver,columns,options,sharedByteBuffers,authorizations,timeoutTracker,returnType,docRawFields);
        }
        else{
            log.trace("Doing multi lookup on {}",requested);
            doMultiLookup(context,server,requested,failures,unscanned,receiver,columns,options,sharedByteBuffers,authorizations,timeoutTracker,returnType,docRawFields);
        }
    }

    static void doMultiLookup(ClientContext context, String server, Map<KeyExtent,List<Range>> requested,
                         Map<KeyExtent,List<Range>> failures, Map<KeyExtent,List<Range>> unscanned,
                         ResultReceiver receiver, List<TColumn> columns, ScannerOptions options,
                         List<ByteBuffer> sharedByteBuffers,
                         Authorizations authorizations  , TimeoutTracker timeoutTracker , DocumentSerialization.ReturnType returnType, boolean docRawFields)
            throws IOException, AccumuloSecurityException, AccumuloServerException {

        if (requested.size() == 0) {
            return;
        }

        // copy requested to unscanned map. we will remove ranges as they are scanned in trackScanning()
        for (Entry<KeyExtent,List<Range>> entry : requested.entrySet()) {
            ArrayList<Range> ranges = new ArrayList<>();
            for (Range range : entry.getValue()) {
                ranges.add(new Range(range));
            }
            unscanned.put(KeyExtent.copyOf(entry.getKey()), ranges);
        }

        timeoutTracker.startingScan();
        TTransport transport = null;
        try {
            final HostAndPort parsedServer = HostAndPort.fromString(server);
            final TabletScanClientService.Client client;
            if (timeoutTracker.getTimeOut() < context.getClientTimeoutInMillis())
                client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SCAN,parsedServer, context, timeoutTracker.getTimeOut());
            else
                client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SCAN,parsedServer, context);
            MyScannerOptions opts = new MyScannerOptions(options);
            try {

                OpTimer timer = null;

                TabletType ttype = TabletType.type(requested.keySet());
                boolean waitForWrites = !ThriftScanner.serversWaitedForWrites.get(ttype).contains(server);

                Map<TKeyExtent, List<TRange>> thriftTabletRanges = requested.entrySet().stream().collect(Collectors.toMap(
                        (entry) -> entry.getKey().toThrift(),
                        (entry) -> entry.getValue().stream().map(Range::toThrift).collect(Collectors.toList())
                ));
                Map<String,String> execHints = null;

                InitialMultiScan imsr = client.startMultiScan(TraceUtil.traceInfo(), context.rpcCreds(),
                        thriftTabletRanges, columns,
                        opts.getServerSideIteratorList(), opts.getServerSideIteratorOptions(),
                        sharedByteBuffers, waitForWrites,
                        SamplerConfigurationImpl.toThrift(options.getSamplerConfiguration()),
                        Long.MAX_VALUE, options.getClassLoaderContext(), execHints, Long.MAX_VALUE);
                if (waitForWrites)
                    ThriftScanner.serversWaitedForWrites.get(ttype).add(server.toString());

                MultiScanResult scanResult = imsr.result;

                if (timer != null) {
                    timer.stop();
                    log.trace("tid={} Got 1st multi scan results, #results={} {} in {}",
                            Thread.currentThread().getId(), scanResult.results.size(),
                            (scanResult.more ? "scanID=" + imsr.scanID : ""),
                            String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
                }

                if (scanResult.results.size() > 0 || scanResult.fullScans.size() > 0)
                    timeoutTracker.madeProgress();

                if (scanResult.results.size() > 0)
                    receiver.receive(scanResult.results.parallelStream().map( x -> {
                        return DocumentKeyConversion.getDocument(returnType,docRawFields,x);
                    }).collect(Collectors.toList()));


                trackScanning(failures, unscanned, scanResult);

                AtomicLong nextOpid = new AtomicLong();

                while (scanResult.more) {

                    timeoutTracker.check();

                    if (timer != null) {
                        log.trace("tid={} oid={} Continuing multi scan, scanid={}",
                                Thread.currentThread().getId(), nextOpid.get(), imsr.scanID);
                        timer.reset().start();
                    }

                    scanResult = client.continueMultiScan(TraceUtil.traceInfo(), imsr.scanID, Long.MAX_VALUE);

                    if (timer != null) {
                        timer.stop();
                        log.trace("tid={} oid={} Got more multi scan results, #results={} {} in {}",
                                Thread.currentThread().getId(), nextOpid.getAndIncrement(),
                                scanResult.results.size(), (scanResult.more ? " scanID=" + imsr.scanID : ""),
                                String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
                    }

                    if (scanResult.results.size() > 0)
                        receiver.receive(scanResult.results.parallelStream().map( x -> {
                            return DocumentKeyConversion.getDocument(returnType,docRawFields,x);
                        }).collect(Collectors.toList()));


                    trackScanning(failures, unscanned, scanResult);
                }

                client.closeMultiScan(TraceUtil.traceInfo(), imsr.scanID);

            } finally {
                ThriftUtil.returnClient(client,context);
            }
        } catch (TTransportException e) {
            log.debug("Server : {} msg : {}", server, e.getMessage());
            timeoutTracker.errorOccured();
            throw new IOException(e);
        } catch (ThriftSecurityException e) {
            log.debug("Server : {} msg : {}", server, e.getMessage(), e);
            throw new AccumuloSecurityException(e.user, e.code, e);
        } catch (TApplicationException e) {
            log.debug("Server : {} msg : {}", server, e.getMessage(), e);
            throw new AccumuloServerException(server, e);
        } catch (NoSuchScanIDException e) {
            log.debug("Server : {} msg : {}", server, e.getMessage(), e);
            throw new IOException(e);
        } catch (TSampleNotPresentException e) {
            log.debug("Server : " + server + " msg : " + e.getMessage(), e);
            String tableInfo = "?";
            String message = "Table " + tableInfo + " does not have sampling configured or built";
            throw new SampleNotPresentException(message, e);
        } catch (TException e) {
            log.debug("Server : {} msg : {}", server, e.getMessage(), e);
            timeoutTracker.errorOccured();
            throw new IOException(e);
        }
    }

    static void doSingleLookup(ClientContext context, String server, Map<KeyExtent,List<Range>> requested,
                         Map<KeyExtent,List<Range>> failures, Map<KeyExtent,List<Range>> unscanned,
                         ResultReceiver receiver, List<TColumn> columns, ScannerOptions options,
                         List<ByteBuffer> sharedByteBuffers,
                         Authorizations authorizations  , TimeoutTracker timeoutTracker , DocumentSerialization.ReturnType returnType, boolean docRawFields)
            throws IOException, AccumuloSecurityException, AccumuloServerException {

        if (requested.size() == 0) {
            return;
        }

        // copy requested to unscanned map. we will remove ranges as they are scanned in trackScanning()
        for (Entry<KeyExtent,List<Range>> entry : requested.entrySet()) {
            ArrayList<Range> ranges = new ArrayList<>();
            for (Range range : entry.getValue()) {
                ranges.add(new Range(range));
            }
            unscanned.put(KeyExtent.copyOf(entry.getKey()), ranges);
        }

        timeoutTracker.startingScan();
        TTransport transport = null;
        try {
            final HostAndPort parsedServer = HostAndPort.fromString(server);
            final TabletScanClientService.Client client;
            if (timeoutTracker.getTimeOut() < context.getClientTimeoutInMillis())
                client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SCAN,parsedServer, context, timeoutTracker.getTimeOut());
            else
                client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SCAN,parsedServer, context);
            MyScannerOptions opts = new MyScannerOptions(options);
            try {

                OpTimer timer = null;

                TabletType ttype = TabletType.type(requested.keySet());
                boolean waitForWrites = !ThriftScanner.serversWaitedForWrites.get(ttype).contains(server);

                Map<TKeyExtent, List<TRange>> thriftTabletRanges = requested.entrySet().stream().collect(Collectors.toMap(
                        (entry) -> entry.getKey().toThrift(),
                        (entry) -> entry.getValue().stream().map(Range::toThrift).collect(Collectors.toList())
                ));
                Map<String,String> execHints = null;

                Entry<TKeyExtent,List<TRange>> rng = thriftTabletRanges.entrySet().iterator().next();

                InitialScan imsr = client.startScan(TraceUtil.traceInfo(), context.rpcCreds(),
                        rng.getKey(), rng.getValue().get(0), columns,5000,
                        opts.getServerSideIteratorList(), opts.getServerSideIteratorOptions(),
                        sharedByteBuffers, waitForWrites,false,1000,
                        SamplerConfigurationImpl.toThrift(options.getSamplerConfiguration()),
                        Long.MAX_VALUE, options.getClassLoaderContext(), execHints, Long.MAX_VALUE);
                if (waitForWrites)
                    ThriftScanner.serversWaitedForWrites.get(ttype).add(server.toString());

                ScanResult scanResult = imsr.result;

                if (timer != null) {
                    timer.stop();
                    log.trace("tid={} Got 1st multi scan results, #results={} {} in {}",
                            Thread.currentThread().getId(), scanResult.results.size(),
                            (scanResult.more ? "scanID=" + imsr.scanID : ""),
                            String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
                }

                if (scanResult.results.size() > 0 )
                    timeoutTracker.madeProgress();

                if (scanResult.results.size() > 0)
                    receiver.receive(scanResult.results.parallelStream().map( x -> {
                        return DocumentKeyConversion.getDocument(returnType,docRawFields,x);
                    }).collect(Collectors.toList()));


                AtomicLong nextOpid = new AtomicLong();

                while (scanResult.more) {

                    timeoutTracker.check();

                    if (timer != null) {
                        log.trace("tid={} oid={} Continuing multi scan, scanid={}",
                                Thread.currentThread().getId(), nextOpid.get(), imsr.scanID);
                        timer.reset().start();
                    }

                    scanResult = client.continueScan(TraceUtil.traceInfo(), imsr.scanID, Long.MAX_VALUE);

                    if (timer != null) {
                        timer.stop();
                        log.trace("tid={} oid={} Got more multi scan results, #results={} {} in {}",
                                Thread.currentThread().getId(), nextOpid.getAndIncrement(),
                                scanResult.results.size(), (scanResult.more ? " scanID=" + imsr.scanID : ""),
                                String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
                    }

                    if (scanResult.results.size() > 0)
                        receiver.receive(scanResult.results.parallelStream().map( x -> {
                            return DocumentKeyConversion.getDocument(returnType,docRawFields,x);
                        }).collect(Collectors.toList()));

                }

                client.closeScan(TraceUtil.traceInfo(), imsr.scanID);

            } finally {
                ThriftUtil.returnClient(client,context);
            }
        } catch (TTransportException e) {
            log.debug("Server : {} msg : {}", server, e.getMessage());
            timeoutTracker.errorOccured();
            throw new IOException(e);
        } catch (ThriftSecurityException e) {
            log.debug("Server : {} msg : {}", server, e.getMessage(), e);
            throw new AccumuloSecurityException(e.user, e.code, e);
        } catch (TApplicationException e) {
            log.debug("Server : {} msg : {}", server, e.getMessage(), e);
            throw new AccumuloServerException(server, e);
        } catch (NoSuchScanIDException e) {
            log.debug("Server : {} msg : {}", server, e.getMessage(), e);
            throw new IOException(e);
        } catch (TSampleNotPresentException e) {
            log.debug("Server : " + server + " msg : " + e.getMessage(), e);
            String tableInfo = "?";
            String message = "Table " + tableInfo + " does not have sampling configured or built";
            throw new SampleNotPresentException(message, e);
        } catch (TException e) {
            log.debug("Server : {} msg : {}", server, e.getMessage(), e);
            timeoutTracker.errorOccured();
            throw new IOException(e);
        }
    }

    static int sumSizes(Collection<List<Range>> values) {
        int sum = 0;

        for (List<Range> list : values) {
            sum += list.size();
        }

        return sum;
    }

    public static class MyScannerOptions extends ScannerOptions{
        public MyScannerOptions(ScannerOptions opts){
            super(opts);
        }

        public List<IterInfo> getServerSideIteratorList(){
            return serverSideIteratorList;
        }

        public Map<String,Map<String,String>> getServerSideIteratorOptions(){
            return serverSideIteratorOptions;
        }
    }
}

