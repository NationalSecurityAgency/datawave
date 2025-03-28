package datawave.mr.bulk;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.Reader;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.IteratorBuilder;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.core.iteratorsImpl.system.DeletingIterator;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.iteratorsImpl.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import datawave.mr.bulk.BulkInputFormat.AccumuloIterator;
import datawave.mr.bulk.BulkInputFormat.AccumuloIteratorOption;
import datawave.mr.bulk.split.FileRangeSplit;
import datawave.mr.bulk.split.RangeSplit;
import datawave.mr.bulk.split.TabletSplitSplit;

public class RecordIterator extends RangeSplit implements SortedKeyValueIterator<Key,Value>, Closeable {

    private static final int READ_AHEAD_THREADS = 5;

    public static final long DEFAULT_FAILURE_SLEEP = 3000L;

    public static final int FAILURE_MAX_DEFAULT = 5;

    public static final String RECORDITER_FAILURE_COUNT_MAX = "recorditer.failure.count.max";

    public static final String RECORDITER_FAILURE_SLEEP_INTERVAL = "recorditer.failure.sleep.interval";

    protected static final CryptoService CRYPTO_SERVICE;

    static {
        // Use our default properties from the classpath, if necessary, as required by SiteConfiguration
        if (System.getProperty("accumulo.properties") == null) {
            System.setProperty("accumulo.properties", "accumulo-default.properties");
        }
        CRYPTO_SERVICE = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, SiteConfiguration.fromEnv().build().getAllCryptoProperties());
    }

    protected TabletSplitSplit fileSplit;

    protected Deque<Range> rangeQueue;

    protected Collection<FileRangeSplit> fileRangeSplits;

    protected Collection<FileSKVIterator> fileIterators;

    protected ConcurrentLinkedDeque<Future<SortedKeyValueIterator<Key,Value>>> futures = new ConcurrentLinkedDeque<>();

    protected Collection<RfileCloseable> rfileReferences;

    protected SortedKeyValueIterator<Key,Value> globalIter;

    protected volatile boolean isOpen = false;

    protected Key lastSeenKey = null;

    protected Configuration conf;

    private volatile int failureCount = 0;

    private final int failureMax;

    private final long failureSleep;

    protected Range currentRange;

    protected AccumuloConfiguration acuTableConf;

    protected Authorizations auths = null;

    private long MAX_COUNT = 1;

    protected long count = 0;

    private float lastProgress = 0.0f;

    protected AtomicBoolean callClosed = new AtomicBoolean(false);

    private class RecordIteratorFactory implements ThreadFactory {

        private ThreadFactory dtf = Executors.defaultThreadFactory();
        private int threadNum = 1;
        private StringBuilder threadIdentifier;

        public RecordIteratorFactory(String threadName)

        {
            this.threadIdentifier = new StringBuilder(threadName);
        }

        public Thread newThread(Runnable r) {
            Thread thread = dtf.newThread(r);
            thread.setName("Datawave BatchScanner Session " + threadIdentifier + " -" + threadNum++);
            thread.setDaemon(true);
            return thread;
        }

    }

    protected ExecutorService executor = null;

    protected volatile int numberFiles = 0;

    // our goal is a precision of 0.1%, so set the precision to half of that
    // given we double MAX_COUNT each time
    private static final float PROGRESS_PRECISION = 0.0005f;

    private static final Logger log = Logger.getLogger(RecordIterator.class);

    public static class RFileEnvironment implements IteratorEnvironment {

        AccumuloConfiguration conf;

        public RFileEnvironment(AccumuloConfiguration conf) {
            this.conf = conf;
        }

        public RFileEnvironment() {
            this.conf = DefaultConfiguration.getInstance();
        }

        @Override
        public AccumuloConfiguration getConfig() {
            return conf;
        }

        @Override
        public IteratorScope getIteratorScope() {
            return IteratorScope.scan;
        }

        @Override
        public boolean isFullMajorCompaction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isUserCompaction() {
            return false;
        }

        @Override
        public Authorizations getAuthorizations() {
            throw new UnsupportedOperationException();
        }

        @Override
        public IteratorEnvironment cloneWithSamplingEnabled() {
            throw new SampleNotPresentException();
        }

        @Override
        public boolean isSamplingEnabled() {
            return false;
        }

        @Override
        public SamplerConfiguration getSamplerConfiguration() {
            return null;
        }

        @Override
        public ServiceEnvironment getServiceEnv() {
            return null;
        }

        @Override
        public PluginEnvironment getPluginEnv() {
            return null;
        }
    }

    public RecordIterator(final TabletSplitSplit fileSplit, Configuration conf) {
        this(fileSplit, null, conf);

    }

    public RecordIterator(final TabletSplitSplit fileSplit, AccumuloConfiguration acuTableConf, Configuration conf) {
        super();

        this.conf = conf;

        this.conf.setInt("dfs.client.socket-timeout", 10 * 1000);

        failureMax = conf.getInt(RECORDITER_FAILURE_COUNT_MAX, FAILURE_MAX_DEFAULT);

        failureSleep = conf.getLong(RECORDITER_FAILURE_SLEEP_INTERVAL, DEFAULT_FAILURE_SLEEP);

        String[] authStrings = conf.getStrings("recorditer.auth.string");

        List<ByteBuffer> authBuffer = Lists.newArrayList();
        if (null != authStrings) {
            for (String authString : authStrings) {
                authBuffer.add(ByteBuffer.wrap(authString.getBytes()));
            }
        }

        auths = new Authorizations(authBuffer);

        this.fileSplit = fileSplit;

        this.acuTableConf = acuTableConf;

        executor = Executors.newFixedThreadPool(READ_AHEAD_THREADS, new RecordIteratorFactory("RecordIterator "));

        try {
            fileRangeSplits = buildRangeSplits(fileSplit);
            if (log.isTraceEnabled()) {
                log.trace("Iterator over the following files: " + fileRangeSplits);
            }

            initialize(conf, true);

            if (rangeQueue.isEmpty()) {
                throw new RuntimeException("Queue of ranges is empty");
            }

            seekToNextKey();

        } catch (Exception e) {
            try {
                close();
            } catch (IOException e1) {
                // ignore
            }
            throw new RuntimeException(e);
        }

    }

    protected Collection<FileRangeSplit> buildRangeSplits(TabletSplitSplit split) throws IOException {
        Collection<FileRangeSplit> splits = Lists.newArrayList();
        for (int i = 0; i < split.getLength(); i++) {
            FileRangeSplit fileSplit = (FileRangeSplit) split.get(i);

            splits.add(fileSplit);

        }

        return splits;
    }

    protected synchronized void initialize(Configuration conf, boolean initRanges) throws IOException {

        if (isOpen) {
            close();
            executor = Executors.newFixedThreadPool(READ_AHEAD_THREADS, new RecordIteratorFactory("RecordIterator "));
        }

        fileIterators = new ConcurrentLinkedDeque<>();

        rfileReferences = new ConcurrentLinkedDeque<>();

        List<SortedKeyValueIterator<Key,Value>> iterators = Lists.newArrayList();

        Set<Path> pathSet = Sets.newHashSet();

        for (FileRangeSplit split : fileRangeSplits) {
            List<Range> myRanges = split.getRanges();
            if (initRanges) {
                for (Range myRange : myRanges) {
                    addRange(myRange);
                }
            }

            pathSet.add(split.getPath());

        }

        FileOperations fops = RFileOperations.getInstance();

        futures.clear();
        numberFiles = pathSet.size();
        for (Path path : pathSet) {

            try {

                if (null != path)
                    futures.add(iterFromFile(fops, path, conf));
            } catch (Exception e) {

            }

        }

        for (Future<SortedKeyValueIterator<Key,Value>> future : futures) {

            SortedKeyValueIterator<Key,Value> rfileIter = null;
            try {
                rfileIter = future.get();
            } catch (ExecutionException e) {
                close();
                throw new RuntimeException(e.getCause());
            } catch (Exception e) {

            }
            if (null != rfileIter)
                iterators.add(rfileIter);

        }

        executor.shutdownNow();

        futures.clear();

        SortedKeyValueIterator<Key,Value> topIter = new MultiIterator(iterators, true);

        boolean applyDeletingIterator = conf.getBoolean("range.record.reader.apply.delete", true);
        if (applyDeletingIterator)
            topIter = DeletingIterator.wrap(topIter, false, DeletingIterator.Behavior.PROCESS);

        try {
            globalIter = applyTableIterators(topIter, conf);
            globalIter = buildTopIterators(globalIter, conf);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new IOException(e);
        }

        if (initRanges) {
            rangeQueue = new LinkedList<>();
            rangeQueue.addAll(ranges);
        }

        isOpen = true;
    }

    /**
     * Applies the table configuration if one is specified.
     *
     * @param topIter
     *            a iterator of key/values
     * @param conf
     *            a configuration
     * @return the loaded table iterator
     * @throws IOException
     *             for issues with read/write
     */
    protected SortedKeyValueIterator<Key,Value> applyTableIterators(SortedKeyValueIterator<Key,Value> topIter, Configuration conf) throws IOException {

        if (null != acuTableConf) {
            // don't need to be populated as we'll do this later.
            RFileEnvironment iterEnv = new RFileEnvironment();

            ColumnVisibility cv = new ColumnVisibility(acuTableConf.get(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY));
            byte[] defaultSecurityLabel = cv.getExpression();

            SortedKeyValueIterator<Key,Value> visFilter = VisibilityFilter.wrap(topIter, auths, defaultSecurityLabel);
            IteratorBuilder.IteratorBuilderEnv iterLoad = IteratorConfigUtil.loadIterConf(IteratorScope.scan, Collections.emptyList(), Collections.emptyMap(),
                            acuTableConf);
            return IteratorConfigUtil.loadIterators(visFilter, iterLoad.env(iterEnv).build());
        }

        return topIter;
    }

    /**
     * @param topIter
     *            the top iterator
     * @param conf
     *            the configuration
     * @return the loaded iterators
     * @throws ClassNotFoundException
     *             if there is an issue finding the class
     * @throws IllegalAccessException
     *             for issues instantiating the sub iterator
     * @throws InstantiationException
     *             for issues with instantiation
     * @throws IOException
     *             for read/write issues
     */
    protected SortedKeyValueIterator<Key,Value> buildTopIterators(SortedKeyValueIterator<Key,Value> topIter, Configuration conf)
                    throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {

        List<AccumuloIterator> iterators = BulkInputFormat.getIterators(conf);
        List<AccumuloIteratorOption> options = BulkInputFormat.getIteratorOptions(conf);

        Map<String,IteratorSetting> scanIterators = new HashMap<>();
        for (AccumuloIterator iterator : iterators) {
            scanIterators.put(iterator.getIteratorName(), new IteratorSetting(iterator.getPriority(), iterator.getIteratorName(), iterator.getIteratorClass()));
        }
        for (AccumuloIteratorOption option : options) {
            scanIterators.get(option.getIteratorName()).addOption(option.getKey(), option.getValue());
        }

        SortedKeyValueIterator<Key,Value> newIter = topIter;

        BulkIteratorEnvironment myData = new BulkIteratorEnvironment(IteratorScope.scan);
        // ensure we create the iterators in priority order
        Collections.sort(iterators, (o1, o2) -> {
            if (o1.getPriority() < o2.getPriority()) {
                return -1;
            } else if (o1.getPriority() > o2.getPriority()) {
                return 1;
            } else {
                return o1.getIteratorName().compareTo(o2.getIteratorName());
            }
        });
        for (AccumuloIterator iterator : iterators) {

            IteratorSetting settings = scanIterators.get(iterator.getIteratorName());

            Class<? extends SortedKeyValueIterator> iter = Class.forName(settings.getIteratorClass()).asSubclass(SortedKeyValueIterator.class);

            SortedKeyValueIterator<Key,Value> newInstance = iter.newInstance();

            newInstance.init(newIter, settings.getOptions(), myData);

            newIter = newInstance;
        }
        return newIter;
    }

    protected Future<SortedKeyValueIterator<Key,Value>> iterFromFile(final FileOperations ops, final Path path, final Configuration conf)
                    throws TableNotFoundException, IOException {

        return executor.submit(() -> {

            if (callClosed.get()) {
                throw new IOException("Shutdown in progress");
            }

            final FileSystem fs = FileSystem.newInstance(path.toUri(), conf);

            if (callClosed.get()) {
                fs.close();
                throw new IOException("Shutdown in progress");
            }

            AtomicBoolean isSuccess = new AtomicBoolean(false);

            RfileCloseable closeable = new RfileCloseable();

            FileSKVIterator fileIterator = null;

            try {

                rfileReferences.add(closeable);

                closeable.setFileSystemReference(fs);

                // Path path = new Path(file);

                closeable.setInputStream(fs.open(path));

                long length = fs.getFileStatus(path).getLen();

                //@formatter:off
                CachableBlockFile.CachableBuilder builder = new CachableBlockFile.CachableBuilder()
                        .input(closeable.getInputStream(), CachableBlockFile.pathToCacheId(path))
                        .cryptoService(CRYPTO_SERVICE)
                        .fsPath(fs, path)
                        .length(length)
                        .conf(conf);
                //@formatter:on

                closeable.setBlockFile(new Reader(builder));

                fileIterator = new RFile.Reader(closeable.getReader());

                closeable.setIterator(fileIterator);

                fileIterators.add(fileIterator);

                isSuccess.set(true);
            } catch (Exception e) {
                if (null != fileIterator) {
                    fileIterator.close();
                }
                closeable.close();
                throw e;
            } finally {
                if (!isSuccess.get() || Thread.interrupted() || callClosed.get()) {
                    closeable.close();
                }
            }

            return fileIterator;
        });
    }

    protected synchronized void fail(Throwable t) {

        if (++failureCount >= failureMax || callClosed.get()) {
            try {
                close();
            } catch (IOException e) {
                // do nothing
            }
            throw new RuntimeException("Failure count of " + failureCount + " + exceeded failureMax. Last known error: " + t);
        }

        try {
            close();
            executor = Executors.newFixedThreadPool(READ_AHEAD_THREADS, new RecordIteratorFactory("RecordIterator "));
            // now reset the callClosed to allow everything to restart.....
            callClosed.set(false);
        } catch (Throwable e) {
            try {
                close();
            } catch (IOException e1) {
                // do nothing
            }
            throw new RuntimeException("Failure while creating new fixed thread pool");
            // do nothing, but clean up resources anyway.
        }
        try {
            Thread.sleep(failureSleep);
        } catch (InterruptedException e) {
            try {
                close();
            } catch (IOException e1) {
                // do nothing
            }
            throw new RuntimeException(e);
        }

        final String tableName = BulkInputFormat.getTablename(conf);

        final List<Range> rangeList = Lists.newArrayList(ranges.iterator().next());

        try {

            MultiRfileInputformat.clearMetadataCache();

            Collection<InputSplit> splits = MultiRfileInputformat.computeSplitPoints(conf, tableName, rangeList);

            Preconditions.checkArgument(splits.size() == 1);

            TabletSplitSplit tabletSplit = (TabletSplitSplit) splits.iterator().next();

            fileRangeSplits = buildRangeSplits(tabletSplit);

            initialize(conf, false);

            seekLastSeen();

        } catch (Exception e) {
            if (!callClosed.get() && !Thread.interrupted())
                fail(e);
        }

    }

    protected void closeOnExit() {
        // close the underlying file system references
        if (null != rfileReferences) {
            for (Closeable fs : rfileReferences) {
                try {
                    fs.close();
                } catch (Exception e) {
                    log.debug(e);
                }
            }
        }
        if (null != fileIterators) {
            for (FileSKVIterator iter : fileIterators) {
                try {
                    iter.close();
                } catch (Exception e) {
                    log.warn(e);
                }
            }
        }
    }

    public synchronized void close() throws IOException {
        isOpen = false;
        globalIter = null;
        callClosed.set(true);
        if (null != executor) {
            executor.shutdownNow();

            closeOnExit();

            for (Future<SortedKeyValueIterator<Key,Value>> future : futures) {
                future.cancel(true);
            }

        }
        // try to close before we interrupt.
        // and then after. this is because HDFS may swallow errors
        // and re-try. If that does not close a thread we will
        // attempt to interrupt and then clean up those resources.
        closeOnExit();

        futures.clear();
        fileIterators.clear();

    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        throw new UnsupportedOperationException("Init is not supported in RecordIterator");

    }

    @Override
    public boolean hasTop() {
        if (!isOpen)
            return false;
        boolean hasTop = false;
        try {
            hasTop = globalIter.hasTop();
        } catch (Exception e) {
            if (!isOpen)
                return false;
            fail(e);
            hasTop = hasTop();
            failureCount = 0;
        }
        return hasTop;
    }

    @Override
    public void next() throws IOException {
        if (!isOpen) {
            throw new IllegalStateException("Close called before next allowed to be called");
        }
        try {
            globalIter.next();
        } catch (Exception e) {
            if (!isOpen)
                return;
            // don't need to call next() here as we'll see
            // or fail, depending on the situation
            fail(e);
        }
        if (!globalIter.hasTop()) {
            seekToNextKey();
        }
    }

    /**
     * @throws IOException
     *             for issues with read/write
     */
    private void seekLastSeen() throws IOException {

        try {
            if (lastSeenKey != null) {
                currentRange = new Range(lastSeenKey, false, currentRange.getEndKey(), currentRange.isEndKeyInclusive());
            }
        } catch (Exception e) {
            log.error(e);
        }

        globalIter.seek(currentRange, Collections.emptyList(), false);
    }

    /**
     * Seek to the next range containing a key.
     *
     * @throws IOException
     *             for issues with read/write
     */
    private void seekToNextKey() throws IOException {
        try {
            seekRange();
        } catch (Exception e) {
            fail(e);
        }
        while (!globalIter.hasTop() && !rangeQueue.isEmpty()) {
            try {
                seekRange();
            } catch (Exception e) {
                fail(e);
            }
        }
    }

    /**
     * @throws IOException
     *             for issues with read/write
     *
     */
    private void seekRange() throws IOException {

        if (!rangeQueue.isEmpty()) {
            currentRange = rangeQueue.removeFirst();
            if (currentRange != null) {
                // now that we have a completely new range, ensure that a
                // failure scenario (which calls seekLastSeen) does not put
                // us back into the previous range.
                lastSeenKey = null;

                globalIter.seek(currentRange, Collections.emptyList(), false);
            }

        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // now that we have a completely new range, ensure that a failure
        // scenario (which calls seekLastSeen) does not put
        // us back into the previous range.
        lastSeenKey = null;
        currentRange = range;
        globalIter.seek(range, columnFamilies, inclusive);
    }

    @Override
    public Key getTopKey() {

        Key topKey = null;
        try {
            topKey = globalIter.getTopKey();
        } catch (Exception e) {
            fail(e);
            topKey = getTopKey();
            failureCount = 0;
        }
        lastSeenKey = topKey;
        return topKey;
    }

    @Override
    public Value getTopValue() {
        Value topValue = null;
        try {
            topValue = globalIter.getTopValue();
        } catch (Exception e) {
            fail(e);
            topValue = getTopValue();
            failureCount = 0;
        }
        return topValue;
    }

    /**
     * Get the progress for the last key returned. This will try to minimize the underlying progress calculations by reusing the last progress until the
     * progress had changed significantly enough. This is done by estimating how many keys to cache the last value. If the progress does not change
     * significantly over a set of keys, then the number of keys for which to cache the value is doubled.
     *
     * @return the current progress
     */
    public float getProgress() {
        count++;
        if (count >= MAX_COUNT) {
            float progress;
            // There is bug in the HeapIterator that will cause a NPE when it is
            // out of iterators instead of simply
            // returning null.
            try {
                progress = getProgress(globalIter.getTopKey());
            } catch (NullPointerException npe) {
                progress = 1.0f;
            }
            if (progress >= lastProgress) {
                if ((progress - lastProgress) < PROGRESS_PRECISION) {
                    MAX_COUNT = MAX_COUNT * 2;
                }
                count = 0;
                lastProgress = progress;
            } else {
                // here we have progress that has gone backwards....keep the
                // last process and try again later
                count = 0;
            }
        }
        return lastProgress;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new RecordIterator(fileSplit, conf);
    }

    public class RfileCloseable implements Closeable {
        protected FileSystem fileSystemReference = null;

        protected FSDataInputStream inputStream = null;

        protected AtomicBoolean closed = new AtomicBoolean(false);

        protected Reader reader = null;

        protected FileSKVIterator fileIterator = null;

        public synchronized void setFileSystemReference(FileSystem fileSystemReference) throws InterruptedException {
            if (closed.get())
                throw new InterruptedException();
            this.fileSystemReference = fileSystemReference;
        }

        public synchronized void setIterator(FileSKVIterator fileIterator) throws InterruptedException {
            if (closed.get())
                throw new InterruptedException();
            this.fileIterator = fileIterator;
        }

        public CachableBlockFile.Reader getReader() {
            return reader;
        }

        public synchronized void setBlockFile(Reader reader) throws InterruptedException {
            if (closed.get())
                throw new InterruptedException();
            this.reader = reader;
        }

        public FSDataInputStream getInputStream() {
            return inputStream;
        }

        public synchronized void setInputStream(FSDataInputStream inputStream) throws InterruptedException {
            if (closed.get())
                throw new InterruptedException();
            this.inputStream = inputStream;
        }

        @Override
        public synchronized void close() {
            closed.set(true);
            try {
                if (null != fileIterator)
                    fileIterator.close();
            } catch (Exception e) {
                log.warn("close iterator " + e);
            }

            try {
                if (null != reader)
                    reader.close();

            } catch (Exception e) {
                log.warn("close reader " + e);
            }

            try {
                if (null != inputStream)
                    inputStream.close();
            } catch (Exception e) {
                log.warn("close input " + e);
            }

            try {
                if (null != fileSystemReference)
                    fileSystemReference.close();
            } catch (Exception e) {
                log.warn("close fs" + e);
            }

        }
    }

}
