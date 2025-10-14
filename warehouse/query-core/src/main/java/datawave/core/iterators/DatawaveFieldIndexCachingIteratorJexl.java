package datawave.core.iterators;

import static datawave.core.iterators.IvaratorRunnable.Status;
import static datawave.core.iterators.IvaratorRunnable.TotalResults;
import static datawave.core.iterators.IvaratorRunnable.Status.COMPLETED;
import static datawave.core.iterators.IvaratorRunnable.Status.CREATED;
import static datawave.core.iterators.IvaratorRunnable.Status.SUSPENDED;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import datawave.core.iterators.querylock.QueryLock;
import datawave.query.Constants;
import datawave.query.composite.CompositeMetadata;
import datawave.query.composite.CompositeSeeker.FieldIndexCompositeSeeker;
import datawave.query.exceptions.WaitWindowOverrunException;
import datawave.query.iterator.CachingIterator;
import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.QuerySpanCollector;
import datawave.query.iterator.profile.SourceTrackingIterator;
import datawave.query.iterator.waitwindow.WaitWindowObserver;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.TypeMetadata;
import datawave.query.util.sortedset.FileKeySortedSet;
import datawave.query.util.sortedset.FileSortedSet;
import datawave.query.util.sortedset.HdfsBackedSortedSet;

/**
 * The Ivarator base class
 * <p>
 * An iterator for the Datawave shard table, it searches FieldIndex keys and returns Event keys (its topKey must be an Event key).
 * <p>
 * This version will cache the values in an underlying HDFS file backed sorted set before returning the first top key.
 * <p>
 * FieldIndex keys: fi\0{fieldName}:{fieldValue}\0datatype\0uid
 * <p>
 * Event key: CF, {datatype}\0{UID}
 */
public abstract class DatawaveFieldIndexCachingIteratorJexl extends WrappingIterator {

    public static final Text ANY_FINAME = new Text("fi\0" + Constants.ANY_FIELD);
    public static final Text FI_START = new Text("fi\0");
    public static final Text FI_END = new Text("fi\0~");
    public static final Random RANDOM = new SecureRandom();

    public abstract static class Builder<B extends Builder<B>> {
        private String queryId;
        private String scanId;
        private WaitWindowObserver waitWindowObserver;
        private Text fieldName;
        protected Text fieldValue;
        private Predicate<Key> datatypeFilter;
        private TimeFilter timeFilter;
        private boolean negated;
        private PartialKey returnKeyType = DEFAULT_RETURN_KEY_TYPE;
        private int maxRangeSplit = 11;
        private List<IvaratorCacheDir> ivaratorCacheDirs;
        private int termNumber;
        private QueryLock queryLock;
        private boolean allowDirReuse;
        private long maxResults = -1;
        private long scanThreshold = 10000;
        private int hdfsBackedSetBufferSize = 10000;
        private int maxOpenFiles = 100;
        private int numRetries = 2;
        private FileSortedSet.PersistOptions persistOptions = new FileSortedSet.PersistOptions();
        private boolean sortedUIDs = true;
        protected QuerySpanCollector querySpanCollector = null;
        protected volatile boolean collectTimingDetails = false;
        private volatile long scanTimeout = 1000L * 60 * 60;
        protected TypeMetadata typeMetadata;
        private CompositeMetadata compositeMetadata;
        private int compositeSeekThreshold;
        private IteratorEnvironment env;
        private GenericObjectPool<SortedKeyValueIterator<Key,Value>> ivaratorSourcePool;

        @SuppressWarnings("unchecked")
        protected B self() {
            return (B) this;
        }

        public B withQueryId(String queryId) {
            this.queryId = queryId;
            return self();
        }

        public B withScanId(String scanId) {
            this.scanId = scanId;
            return self();
        }

        public B withWaitWindowObserver(WaitWindowObserver waitWindowObserver) {
            this.waitWindowObserver = waitWindowObserver;
            return self();
        }

        public B withFieldName(Text fieldName) {
            this.fieldName = fieldName;
            return self();
        }

        public B withFieldName(String fieldName) {
            return this.withFieldName(new Text(fieldName));
        }

        public B withFieldValue(Text fieldValue) {
            this.fieldValue = fieldValue;
            return self();
        }

        public B withFieldValue(String fieldValue) {
            return this.withFieldValue(new Text(fieldValue));
        }

        public B withTimeFilter(TimeFilter timeFilter) {
            this.timeFilter = timeFilter;
            return self();
        }

        public B withDatatypeFilter(Predicate datatypeFilter) {
            this.datatypeFilter = datatypeFilter;
            return self();
        }

        public B negated(boolean negated) {
            this.negated = negated;
            return self();
        }

        public B withScanThreshold(long scanThreshold) {
            this.scanThreshold = scanThreshold;
            return self();
        }

        public B withScanTimeout(long scanTimeout) {
            this.scanTimeout = scanTimeout;
            return self();
        }

        public B withHdfsBackedSetBufferSize(int hdfsBackedSetBufferSize) {
            this.hdfsBackedSetBufferSize = hdfsBackedSetBufferSize;
            return self();
        }

        public B withMaxRangeSplit(int maxRangeSplit) {
            this.maxRangeSplit = maxRangeSplit;
            return self();
        }

        public B withMaxOpenFiles(int maxOpenFiles) {
            this.maxOpenFiles = maxOpenFiles;
            return self();
        }

        public B withMaxResults(long maxResults) {
            this.maxResults = maxResults;
            return self();
        }

        public B withNumRetries(int numRetries) {
            this.numRetries = numRetries;
            return self();
        }

        public B withPersistOptions(FileSortedSet.PersistOptions persistOptions) {
            this.persistOptions = persistOptions;
            return self();
        }

        public B withIvaratorCacheDirs(List<IvaratorCacheDir> ivaratorCacheDirs) {
            this.ivaratorCacheDirs = ivaratorCacheDirs;
            return self();
        }

        public B withTermNumber(int termNumber) {
            this.termNumber = termNumber;
            return self();
        }

        public B withQueryLock(QueryLock queryLock) {
            this.queryLock = queryLock;
            return self();
        }

        public B allowDirResuse(boolean allowDirReuse) {
            this.allowDirReuse = allowDirReuse;
            return self();
        }

        public B withReturnKeyType(PartialKey returnKeyType) {
            this.returnKeyType = returnKeyType;
            return self();
        }

        public B withSortedUUIDs(boolean sortedUUIDs) {
            this.sortedUIDs = sortedUUIDs;
            return self();
        }

        public B withTypeMetadata(TypeMetadata typeMetadata) {
            this.typeMetadata = typeMetadata;
            return self();
        }

        public B withCompositeMetadata(CompositeMetadata compositeMetadata) {
            this.compositeMetadata = compositeMetadata;
            return self();
        }

        public B withCompositeSeekThreshold(int compositeSeekThreshold) {
            this.compositeSeekThreshold = compositeSeekThreshold;
            return self();
        }

        public B withIteratorEnv(IteratorEnvironment env) {
            this.env = env;
            return self();
        }

        public B withIvaratorSourcePool(GenericObjectPool<SortedKeyValueIterator<Key,Value>> ivaratorSourcePool) {
            this.ivaratorSourcePool = ivaratorSourcePool;
            return self();
        }

        public abstract DatawaveFieldIndexCachingIteratorJexl build();
    }

    public static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
    public static final Logger log = Logger.getLogger(DatawaveFieldIndexCachingIteratorJexl.class);
    public static final String NULL_BYTE = Constants.NULL_BYTE_STRING;
    public static final String ONE_BYTE = "\u0001";
    public static final PartialKey DEFAULT_RETURN_KEY_TYPE = PartialKey.ROW_COLFAM;
    // This iterator should have no seek column families. This is because all filtering is done by the bounding FI ranges,
    // the timefilter, and the datatype filters.
    // We do not want the underlying iterators to filter keys so that we can check the bounds in this iterator as quickly
    // as possible.
    @SuppressWarnings("unchecked")
    protected static final Collection<ByteSequence> EMPTY_CFS = Collections.emptyList();

    // These are the ranges to scan in the field index
    private final List<Range> boundingFiRanges = new ArrayList<>();
    protected Range currentFiRange = null;
    private Text fiRow = null;

    // This is the query id which is used for tracking purposes
    protected final String queryId;
    // This is the scan id which is used for tracking purposes
    protected final String scanId;
    // WaitWindowObserver to ensure that scan returns within a given timeframe
    protected final WaitWindowObserver waitWindowObserver;
    // This is the fieldname of interest
    private final Text fieldName;
    // part of the datawave shard structure: fi\0fieldname
    private final Text fiName;
    // part of the datawave shard structure (can be overridden by extended classes)
    private Text fieldValue;
    // a datatype filter
    private final Predicate<Key> datatypeFilter;
    // a time filter
    private final TimeFilter timeFilter;

    // Are we to negate the result of the "matches(key)" method
    private final boolean negated;

    // the number of underlying keys scanned (used by ivarators for example to determine when we should force persistence of the results)
    private final AtomicLong scannedKeys = new AtomicLong(0);

    // The parts of the event key to return (defaults to row and cf)
    private final PartialKey returnKeyType;

    // The max number of field index ranges to be executed individually by the ivarator thread pool
    private final int maxRangeSplit;

    // The configured ivarator cache paths
    private final List<IvaratorCacheDir> ivaratorCacheDirs;
    // The number in-order that this term was in the query when built
    private final int termNumber;
    // The control filesystem to use for this ivarator
    private final FileSystem controlFs;
    // The control directory to use for this ivarator
    private final Path controlDir;
    // A query lock to verify if the query is still running
    private final QueryLock queryLock;
    // are we allowing reuse of the hdfs directories
    private final boolean allowDirReuse;
    // the max number of scanned keys before we force persistance of the hdfs cache
    private final long scanThreshold;
    // the number of entries to cache in memory before flushing to hdfs
    private final int hdfsBackedSetBufferSize;
    // the max number of files to open simultaneously during a merge source
    private final int maxOpenFiles;
    // the max number of retries when attempting to persist a sorted set to a filesystem
    private final int numRetries;
    // the persistence options
    private final FileSortedSet.PersistOptions persistOptions;

    // the current top key
    private Key topKey = null;
    // the current top value
    private final Value topValue = new Value(new byte[0]);

    // must the returned UIDs be in sorted order? This is to allow for am optimization where the UIDs are not sorted which avoids the entire
    // caching and merge sorting that is done in the the initial seek. Note that the keys returned from this iterator will not be in sorted
    // order if sortedUIDs = false, and the calling iterator must handle that appropriately.
    private boolean sortedUIDs = true;

    // an fiSource used when not doing sorted UIDs
    private SortedKeyValueIterator<Key,Value> fiSource = null;

    // the hdfs backed sorted set
    private HdfsBackedSortedSet<Key> set = null;
    // a thread safe wrapper around the sorted set used by the scan threads
    private SortedSet<Key> threadSafeSet = null;
    // the iterator (merge sort) of key values once the sorted set has been filled
    private CachingIterator<Key> keys = null;
    // the current row covered by the hdfs set
    private String currentRow = null;
    // did we create the row directory
    private boolean createdRowDir = false;

    // The last range seeked used to filter the final results
    private Range lastRangeSeeked = null;

    // the initial env passed into init
    private IteratorEnvironment initEnv = null;

    // the hdfs back control object used to manipulate files in the hdfs set directory
    private HdfsBackedControl setControl = new HdfsBackedControl();

    // heavy optimization, for use by jump method only!
    protected StringBuilder jumpKeyStringBuilder = new StringBuilder();
    // heavy optimization, for use by buildBoundingFiRanges method only!
    protected StringBuilder boundingFiRangeStringBuilder = new StringBuilder();

    protected QuerySpanCollector querySpanCollector = null;

    protected volatile boolean collectTimingDetails = false;

    // the start time for this iterator
    private volatile long startTime = System.currentTimeMillis();

    // timeout for the building of the cache. Default 1 hour
    private volatile long scanTimeout = 1000L * 60 * 60;

    // have we timed out
    private volatile boolean timedOut = false;

    // The max number of results that can be returned from this iterator.
    private final long maxResults;

    protected CompositeMetadata compositeMetadata;
    protected FieldIndexCompositeSeeker compositeSeeker;
    protected int compositeSeekThreshold;

    protected GenericObjectPool<SortedKeyValueIterator<Key,Value>> ivaratorSourcePool = null;

    // -------------------------------------------------------------------------
    // ------------- Constructors

    public DatawaveFieldIndexCachingIteratorJexl() {
        super();
        this.queryId = null;
        this.scanId = null;
        this.waitWindowObserver = null;
        this.fieldName = null;
        this.fieldValue = null;
        this.fiName = null;

        this.negated = false;
        this.returnKeyType = DEFAULT_RETURN_KEY_TYPE;
        this.timeFilter = null;
        this.datatypeFilter = null;

        this.ivaratorCacheDirs = null;
        this.termNumber = 0;
        this.controlFs = null;
        this.controlDir = null;
        this.queryLock = null;
        this.allowDirReuse = false;
        this.scanThreshold = 10000;
        this.hdfsBackedSetBufferSize = 10000;
        this.maxOpenFiles = 100;
        this.numRetries = 2;
        this.maxRangeSplit = 11;
        this.maxResults = -1;
        this.persistOptions = new FileSortedSet.PersistOptions();

        this.sortedUIDs = true;
    }

    /**
     * Creates an ivarator using the specified builder.
     *
     * @param builder
     *            may be any builder which extends the abstract builder defined above. Specialized builders exist for regex, range, filter, and list ivarators.
     */
    protected DatawaveFieldIndexCachingIteratorJexl(Builder builder) {

        this.queryId = builder.queryId;
        this.scanId = builder.scanId;
        this.waitWindowObserver = builder.waitWindowObserver;
        this.ivaratorSourcePool = builder.ivaratorSourcePool;

        if (builder.fieldName.toString().startsWith("fi" + NULL_BYTE)) {
            this.fieldName = new Text(builder.fieldName.toString().substring(3));
            this.fiName = builder.fieldName;
        } else {
            this.fieldName = builder.fieldName;
            this.fiName = new Text("fi" + NULL_BYTE + builder.fieldName);
        }
        log.trace("fName : " + fiName.toString().replaceAll(NULL_BYTE, "%00"));

        this.fieldValue = builder.fieldValue;
        this.negated = builder.negated;
        this.returnKeyType = builder.returnKeyType;
        this.timeFilter = builder.timeFilter;
        this.datatypeFilter = builder.datatypeFilter;

        this.ivaratorCacheDirs = builder.ivaratorCacheDirs;
        this.termNumber = builder.termNumber;

        // Note: We have already selected the control directory at random in the DefaultQueryPlanner
        // @see DefaultQueryPlanner#getShuffledIvaratoCacheDirConfigs(ShardQueryConfiguration)
        if (ivaratorCacheDirs.size() > 0) {
            this.controlFs = ivaratorCacheDirs.get(0).getFs();
            this.controlDir = new Path(ivaratorCacheDirs.get(0).getPathURI());
        } else {
            throw new IllegalStateException("No ivarator cache dirs specified!");
        }

        this.queryLock = builder.queryLock;
        this.allowDirReuse = builder.allowDirReuse;
        this.scanThreshold = builder.scanThreshold;
        this.scanTimeout = builder.scanTimeout;
        this.maxResults = builder.maxResults;
        this.hdfsBackedSetBufferSize = builder.hdfsBackedSetBufferSize;
        this.maxOpenFiles = builder.maxOpenFiles;
        this.numRetries = builder.numRetries;
        this.persistOptions = builder.persistOptions;
        this.maxRangeSplit = builder.maxRangeSplit;

        this.sortedUIDs = builder.sortedUIDs;

        // setup composite logic if this is a composite field
        if (builder.compositeMetadata != null) {
            List<String> compositeFields = builder.compositeMetadata.getCompositeFieldMapByType().entrySet().stream()
                            .flatMap(x -> x.getValue().keySet().stream()).distinct().collect(Collectors.toList());
            if (compositeFields.contains(builder.fieldName.toString())) {
                this.compositeMetadata = builder.compositeMetadata;
                this.compositeSeeker = new FieldIndexCompositeSeeker(builder.typeMetadata.fold());
            }
        }

        this.compositeSeekThreshold = builder.compositeSeekThreshold;
        this.initEnv = builder.env;
    }

    public DatawaveFieldIndexCachingIteratorJexl(DatawaveFieldIndexCachingIteratorJexl other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
        this.queryId = other.queryId;
        this.scanId = other.scanId;
        this.waitWindowObserver = other.waitWindowObserver;
        this.fieldName = other.fieldName;
        this.fiName = other.fiName;
        this.returnKeyType = other.returnKeyType;
        this.timeFilter = other.timeFilter;
        this.datatypeFilter = other.datatypeFilter;
        this.fieldValue = other.fieldValue;
        this.boundingFiRanges.addAll(other.boundingFiRanges);
        this.negated = other.negated;

        this.ivaratorCacheDirs = other.ivaratorCacheDirs == null ? null : new ArrayList<>(other.ivaratorCacheDirs);
        this.termNumber = other.termNumber;
        this.controlFs = other.controlFs;
        this.controlDir = other.controlDir;
        this.queryLock = other.queryLock;
        this.allowDirReuse = other.allowDirReuse;
        this.scanThreshold = other.scanThreshold;
        this.scanTimeout = other.scanTimeout;
        this.maxResults = other.maxResults;
        this.hdfsBackedSetBufferSize = other.hdfsBackedSetBufferSize;
        this.maxOpenFiles = other.maxOpenFiles;
        this.numRetries = other.numRetries;
        this.persistOptions = other.persistOptions;

        this.set = other.set;
        this.keys = other.keys;
        this.currentRow = other.currentRow;
        this.createdRowDir = other.createdRowDir;
        this.maxRangeSplit = other.maxRangeSplit;

        this.sortedUIDs = other.sortedUIDs;

        try {
            this.setControl.takeOwnership(this.currentRow, this);
        } catch (IOException e) {
            log.error(controlDir + ": Could not take ownership of set", e);
            throw new IllegalStateException("Could not take ownership of set", e);
        }

        this.lastRangeSeeked = other.lastRangeSeeked;
        this.initEnv = env;
    }

    // -------------------------------------------------------------------------
    // ------------- Overrides

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        this.initEnv = env;
    }

    @Override
    protected void finalize() throws Throwable {
        clearRowBasedHdfsBackedSet();
        super.finalize();
    }

    @Override
    public abstract SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env);

    @Override
    public Key getTopKey() {
        return topKey;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

    @Override
    public boolean hasTop() {
        return (topKey != null);
    }

    @Override
    public void seek(Range r, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(controlDir + ": begin seek, range: " + r);
        }

        if (!lastRangeSeekedContains(r)) {
            // the start of this range is beyond the end of the last range seeked
            // we must reset keyValues to null and empty the underlying collection
            clearRowBasedHdfsBackedSet();
        } else {
            // inside the original range, so potentially need to reposition keyValues
            if (this.keys != null) {
                Key startKey = r.getStartKey();
                // decide if keyValues needs to be rebuilt or can be reused
                if (!this.keys.hasNext() || (this.keys.peek().compareTo(startKey) > 0)) {
                    this.keys = new CachingIterator<>(this.threadSafeSet.iterator());
                }
            }
        }

        // if we are not sorting UIDs, then determine whether we have a cq and capture the lastFiKey
        Key lastFiKey = null;
        Key startKey = r.getStartKey();
        if (!sortedUIDs) {
            String cq = WaitWindowObserver.removeMarkers(startKey.getColumnQualifier()).toString();
            if (r.getStartKey().getColumnFamily().getLength() > 0 && cq.length() > 0) {
                int fieldnameIndex = cq.indexOf('\0');
                if (fieldnameIndex >= 0) {
                    // If startKey colQual has YIELD_AT_BEGIN marker then we want to include keys with the
                    // fieldName / fieldValue in the key, otherwise we seek past this key by adding a \0
                    String cqSuffix = WaitWindowObserver.hasBeginMarker(startKey.getColumnQualifier()) ? "" : "\0";
                    String cf = startKey.getColumnFamily().toString();
                    lastFiKey = new Key(startKey.getRow().toString(), "fi\0" + cq.substring(0, fieldnameIndex),
                                    cq.substring(fieldnameIndex + 1) + '\0' + cf + cqSuffix);
                }
            }
        }

        QuerySpan querySpan = null;

        try {
            boolean hasTop = false;
            // this will block until an ivarator source becomes available
            final SortedKeyValueIterator<Key,Value> source = takePoolSource();

            try {

                if (collectTimingDetails && source instanceof SourceTrackingIterator) {
                    querySpan = ((SourceTrackingIterator) source).getQuerySpan();
                }

                // seek the source to the start of the field index that matches the specified field
                // this initial seek ensures that data for the field is actually present
                // if data is present, then bounding ranges are generated
                Range seekRange = buildInitialSeekRange(startKey, r);
                source.seek(seekRange, EMPTY_CFS, false);
                scannedKeys.incrementAndGet();
                if (log.isTraceEnabled()) {
                    try {
                        log.trace(controlDir + ": lastRangeSeeked: " + seekRange + "  source.getTopKey(): " + source.getTopKey());
                    } catch (Exception ex) {
                        log.trace(controlDir + ": Ignoring this while logging a trace message:", ex);
                        // let's not ruin everything when trace is on...
                    }
                }

                if (source.hasTop()) {
                    hasTop = true;
                    Text currentRow = source.getTopKey().getRow();
                    this.boundingFiRanges.clear();
                    this.boundingFiRanges.addAll(buildBoundingFiRanges(currentRow, this.fiName, this.fieldValue));
                    // this.fiRow should only be updated here and in moveToNextRow.
                    // fiRow is set to null in moveToNextRow when we reach the end of the last row in the range
                    if (this.sortedUIDs) {
                        // For sortedUIDs, moveToNextRow is called in fillSortedSets and fiRow == null is used
                        // as a stopping condition in findTop so we only call fillSortedSets once per row
                        if (this.lastRangeSeeked == null || (this.fiRow != null && !this.fiRow.equals(currentRow))) {
                            this.fiRow = currentRow;
                        }
                        // this.lastRangeSeeked is used in findTop to filter out values until we reach the seeked range
                        this.lastRangeSeeked = r;
                    } else {
                        // For !sortedUIDs, this.fiRow must remain set to the current row so that we can reach
                        // getNextUnsortedKey inside findTop until moveToNextRow sets this.fiRow == null
                        this.fiRow = currentRow;
                        this.lastRangeSeeked = r;
                        if (lastFiKey != null) {
                            // if we are not sorting uids and we have a starting value, then pop off the ranges until we have the one
                            // containing the last value returned. Then modify that range appropriately.
                            if (log.isTraceEnabled()) {
                                log.trace(controlDir + ": Reseeking fi to lastFiKey: " + lastFiKey);
                            }
                            while (!this.boundingFiRanges.isEmpty() && !this.boundingFiRanges.get(0).contains(lastFiKey)) {
                                if (log.isTraceEnabled()) {
                                    log.trace(controlDir + ": Skipping range: " + this.boundingFiRanges.get(0));
                                }
                                this.boundingFiRanges.remove(0);
                                if (this.boundingFiRanges.isEmpty()) {
                                    moveToNextRow();
                                }
                            }
                            if (!this.boundingFiRanges.isEmpty()) {
                                if (log.isTraceEnabled()) {
                                    log.trace(controlDir + ": Starting in range: " + this.boundingFiRanges.get(0));
                                }
                                Range boundingFiRange = this.boundingFiRanges.get(0);
                                // default to startKeyInclusive = false unless we have yielded with begin marker
                                boolean startKeyInclusive = WaitWindowObserver.hasBeginMarker(startKey.getColumnQualifier());
                                boundingFiRange = new Range(lastFiKey, startKeyInclusive, boundingFiRange.getEndKey(), boundingFiRange.isEndKeyInclusive());
                                this.boundingFiRanges.set(0, boundingFiRange);
                                if (log.isTraceEnabled()) {
                                    log.trace(controlDir + ": Reset range to: " + this.boundingFiRanges.get(0));
                                }
                            }
                        }
                    }
                } else {
                    this.topKey = null;
                }
            } finally {
                returnPoolSource(source);
            }

            // now lets find the top key
            if (hasTop) {
                findTop();
            }

            if (log.isTraceEnabled()) {
                log.trace(controlDir + ": seek, topKey : " + ((null == topKey) ? "null" : topKey));
            }
        } finally {
            if (collectTimingDetails && querySpanCollector != null && querySpan != null) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
    }

    /**
     * It is imperative to construct an initial seek range with a non-empty column qualifier. Seeking a range that consists of only a row will open sources and
     * read in keys that are unrelated to the field index, and other terms in the query may deep-copy this source which can lead to additional problems. In
     * summary: always build the most restrictive seek range possible.
     *
     * @param startKey
     *            the range stat key, possibly stripped of any yielding information
     * @param range
     *            the initial seek range
     * @return a better initial seek range
     */
    protected Range buildInitialSeekRange(Key startKey, Range range) {
        Preconditions.checkNotNull(startKey, "start key should be non-null");

        // If this seek is part of an iterator teardown/rebuild, do not modify the range. The column qualifier should already be non-null.
        if (!range.isStartKeyInclusive()) {
            return range;
        }

        if (fieldName.toString().equals(Constants.ANY_FIELD)) {
            // in the case of an ANYFIELD, which should only happen during unit tests, restrict the scan range
            // to just the fi prefix.
            Key start = new Key(startKey.getRow(), new Text("fi\0"));
            Key stop = new Key(startKey.getRow(), new Text("fi\0\uffff"));
            return new Range(start, range.isStartKeyInclusive(), stop, false);
        }

        // otherwise restrict the scan range to the full fi prefix and field name
        Key start = new Key(startKey.getRow(), this.fiName);
        Key stop = start.followingKey(PartialKey.ROW_COLFAM);
        return new Range(start, range.isStartKeyInclusive(), stop, false);
    }

    @Override
    public void next() throws IOException {
        log.trace(controlDir + ": next() called");

        findTop();

        if (topKey != null && log.isTraceEnabled()) {
            log.trace(controlDir + ": next() => " + topKey);
        }
    }

    // -------------------------------------------------------------------------
    // ------------- Public stuff

    public boolean isNegated() {
        return negated;
    }

    public Text getFieldName() {
        return fieldName;
    }

    public Text getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(Text fValue) {
        this.fieldValue = fValue;
    }

    /**
     * @return the field index column family (fi\0fieldname)
     */
    public Text getFiName() {
        return fiName;
    }

    public PartialKey getReturnKeyType() {
        return returnKeyType;
    }

    public int getMaxRangeSplit() {
        return maxRangeSplit;
    }

    /**
     * From a field index key, this builds row=shardId, cf=datatype\0UID, cq=fieldname\0fieldvalue Note: in the non-sorted case we need to include the COLQUAL
     * to maintain the position in the FI for reseeking purposes
     *
     * @param key
     *            a key
     * @param keyType
     *            the key type
     * @return Key(shardId, datatype\0UID)
     */
    public Key buildEventKey(Key key, PartialKey keyType) {
        // field index key is shardId : fi\0fieldName : fieldValue\0datatype\0uid
        // event key is shardId : dataType\0uid : fieldName\0fieldValue
        String cf = key.getColumnFamily().toString();
        String cq = key.getColumnQualifier().toString();
        // track backwards in the column qualifier to find the end of the value
        int cqNullIndex = cq.lastIndexOf('\0');
        cqNullIndex = cq.lastIndexOf('\0', cqNullIndex - 1);
        String cqStr = cq.substring(cqNullIndex + 1);
        if (!sortedUIDs) {
            // to enable repositioning appropriately in the field index, we need the other elements as well.
            keyType = PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME;
        }
        switch (keyType) {
            case ROW:
                // we really do not support ROW only, at least return the CF which contains the UID
            case ROW_COLFAM:
                return new Key(key.getRow(), new Text(cqStr));
            case ROW_COLFAM_COLQUAL:
                return new Key(key.getRow(), new Text(cqStr), new Text(cf.substring(3) + '\0' + cq.substring(0, cqNullIndex)));
            case ROW_COLFAM_COLQUAL_COLVIS:
                return new Key(key.getRow(), new Text(cqStr), new Text(cf.substring(3) + '\0' + cq.substring(0, cqNullIndex)), key.getColumnVisibility());
            default:
                return new Key(key.getRow(), new Text(cqStr), new Text(cf.substring(3) + '\0' + cq.substring(0, cqNullIndex)), key.getColumnVisibility(),
                                key.getTimestamp());
        }
    }

    // -------------------------------------------------------------------------
    // ------------- Other stuff

    /**
     * Since we are looking for a regular expression and not a specified value, we have to scan the entire range so that we can return the key/values in a
     * sorted order. We are using an Hdfs backed sorted set to this end.
     *
     * @throws IOException
     *             if there are issues with read/write
     */
    protected void findTop() throws IOException {

        this.topKey = null;

        // we are done if cancelled
        if (this.setControl.isCancelledQuery()) {
            return;
        }

        while (this.topKey == null) {

            // if we have key values, then exhaust them first
            if (this.keys != null) {
                // only pass through keys that fall within the range
                // this is required to handle cases where we start at a specific UID
                while (this.keys.hasNext()) {
                    Key key = this.keys.next();
                    if (this.sortedUIDs && log.isTraceEnabled()) {
                        log.trace(controlDir + ": Is " + key + " contained in " + this.lastRangeSeeked);
                    }
                    // no need to check containership if not returning sorted uids
                    if (!this.sortedUIDs || this.lastRangeSeeked.contains(key)) {
                        this.topKey = key;
                        if (log.isTraceEnabled()) {
                            log.trace(controlDir + ": setting as topKey " + this.topKey);
                        }
                        break;
                    }
                    // so the range does not contain the key. determine if we need to seek
                    else if (key.compareTo(this.lastRangeSeeked.getStartKey()) < 0) {
                        this.keys = new CachingIterator<>(this.threadSafeSet.tailSet(this.lastRangeSeeked.getStartKey()).iterator());
                        log.trace(controlDir + ": " + key + " is less than " + this.lastRangeSeeked.getStartKey() + " -> Tail set starts at "
                                        + this.keys.peek());
                    }
                }
            }

            if (this.topKey == null) {
                // start the timing
                startTiming();

                // if the current key values has no more, then clear out this row's set
                clearRowBasedHdfsBackedSet();

                // if we do not have a current fi row to scan, then we are done.
                if (this.fiRow == null) {
                    break;
                }

                // now get the keys. Get them all and sorted if needed, otherwise just get the next one.
                if (this.sortedUIDs) {
                    fillSortedSets();
                } else {
                    getNextUnsortedKey();
                }

                if (this.setControl.isCancelledQuery()) {
                    this.topKey = null;
                }

                if (isTimedOut()) {
                    log.error(controlDir + ": Ivarator query timed out");
                    throw new IvaratorException("Ivarator query timed out");
                }

                if (this.setControl.isCancelledQuery()) {
                    log.debug(controlDir + ": Ivarator query was cancelled");
                    throw new RuntimeException("Ivarator query was cancelled");
                }

                // persist set to disk so we can reuse it on a rebuild or yield
                if (this.set != null) {
                    forcePersistence();
                }

                if (this.keys == null) {
                    this.keys = new CachingIterator<>(this.threadSafeSet.iterator());
                }
            }

            if (this.setControl.isCancelledQuery()) {
                if (isTimedOut()) {
                    log.error(controlDir + ": Ivarator query timed out");
                    throw new IvaratorException("Ivarator query timed out");
                } else {
                    log.debug(controlDir + ": Ivarator query was cancelled");
                    throw new RuntimeException("Ivarator query was cancelled");
                }
            }

        }
    }

    private void fillSortedSets() throws IOException {
        String sourceRow = this.fiRow.toString();
        // If we are running fillSortedSets as part of a re-seek after yield, then the fillSet threads would not have
        // completed when a WaitWindowOverrunException was thrown. Therefore, the set will not have been marked as
        // complete when setupRowBasedHdfsBackedSet is called. We will try to copy the RowBasedHdfsBackedSet from
        // the previously used Ivarator and resume processing.
        boolean resumeFromIvaratorFutures = resumeFromIvaratorFutures();
        if (!resumeFromIvaratorFutures) {
            // If resuming from IvaratorFutures fails, then create and try to reuse a previous HDFS backed set.
            setupRowBasedHdfsBackedSet(sourceRow);
            // If this.keys != null, then we have a persisted and completed set from a previous Ivarator call.
            // We are done with fillSortedSet for this row and should advance this.fiRow or set it to null
            if (this.keys != null) {
                moveToNextRow();
                return;
            }
        }

        List<IvaratorFuture> futures = new ArrayList<>(this.boundingFiRanges.size());
        if (log.isDebugEnabled()) {
            log.debug(controlDir + ": Processing " + this.boundingFiRanges + " for " + this);
        }

        TotalResults totalResults = new TotalResults(this.maxResults);

        for (Range range : this.boundingFiRanges) {
            if (log.isTraceEnabled()) {
                log.trace(controlDir + ": range -> " + range);
            }
            // For each range, get either a new or pre-existing IvaratorFuture
            futures.add(fillSet(range, totalResults));
        }
        if (!resumeFromIvaratorFutures) {
            log.info(String.format("%s: Started Ivarator %s IvaratorRunnables created:%d", controlDir, getIvaratorInfo(fiRow.toString(), true),
                            futures.size()));
        }

        boolean failed = false;
        Exception exception = null;
        Object result = null;
        Pair<Key,String> yieldKey = null;
        long matched = 0;
        long scanned = 0;
        long firstIvaratorRunnableCreated = Long.MAX_VALUE;
        String ivaratorInfo = getIvaratorInfo(this.fiRow.toString(), true);
        try {
            // wait for all threads to complete
            for (IvaratorFuture future : futures) {
                checkTiming();

                if (!failed && !this.setControl.isCancelledQuery()) {
                    try {
                        result = future.get(this.waitWindowObserver.remainingTimeMs(), TimeUnit.MILLISECONDS);
                    } catch (TimeoutException e) {
                        // If the remaining time on the WaitWindowObserver has passed, then throw
                        // a WaitWindowOverrunException to yield at the startKey of the seekRange
                        yieldKey = this.waitWindowObserver.createYieldKey(this.lastRangeSeeked.getStartKey(), true,
                                        "DatawaveFieldIndexCachingIteratorJexl.fillSortedSets()");
                        throw new WaitWindowOverrunException(yieldKey);
                    } catch (InterruptedException e) {
                        exception = e;
                        result = e;
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        exception = e;
                        result = e;
                    }
                    if (result != null) {
                        failed = true;
                        this.setControl.setCancelled();
                    }
                }
                if (this.setControl.isCancelledQuery()) {
                    break;
                }
            }
        } finally {
            if (yieldKey == null) {
                // Whether we succeeded or failed, we should remove all Futures associated with this Ivarator
                // The only exception is if we interrupted the fillSortedSets with a WaitWindowOverrunException
                // in which case we want the IvaratorFutures to remain so that we can reconnect the HDFSBackedSortedSet
                // on the next call
                for (IvaratorFuture future : futures) {
                    matched += future.getIvaratorRunnable().getMatched();
                    scanned += future.getIvaratorRunnable().getScanned();
                    firstIvaratorRunnableCreated = Math.min(firstIvaratorRunnableCreated, future.getIvaratorRunnable().getCreatedTime());
                    IteratorThreadPoolManager.suspendIvarator(future, true, this.initEnv);
                }
            } else {
                // We can't use the source anymore since it is issued by Accumulo Tablet. We suspend any
                // unfinished IvaratorRunnable until a new call when it will be resumed with a new source
                for (IvaratorFuture future : futures) {
                    // COMPLETED : this call will have no effect and the results can be retrieved later
                    // RUNNING : save a restartKey and suspend the IvaratorRunnable
                    // CREATED: remove the Future from the executor's workQueue to prevent it from starting.
                    // The IvaratorFutures are left in IteratorThreadPoolManager so they can be retrieved in the next call
                    IteratorThreadPoolManager.suspendIvarator(future, false, this.initEnv);
                    // At this point, the IvaratorRunnable should be CREATED, SUSPENDED, or COMPLETED. If it is none of these,
                    // then we need to remove its reference from the IteratorThreadPoolManager and run a new one next time
                    Status status = future.getIvaratorRunnable().getStatus();
                    if (!status.equals(CREATED) && !status.equals(SUSPENDED) && !status.equals(COMPLETED)) {
                        IteratorThreadPoolManager.removeIvarator(future.getIvaratorRunnable().getTaskName(), this.initEnv);
                    }
                }
                log.info(String.format("%s: Suspended Ivarator %s fillSortedSets for %d ranges", controlDir, ivaratorInfo, boundingFiRanges.size()));
            }
        }

        long fillSetTiming = System.currentTimeMillis() - firstIvaratorRunnableCreated;
        log.info(String.format("%s: Completed Ivarator %s fillSortedSets for %d ranges, matched %d of %d keys in %dms", controlDir, ivaratorInfo,
                        boundingFiRanges.size(), matched, scanned, fillSetTiming));

        if (failed) {
            log.error(String.format("%s: Failed Ivarator %s fillSortedSets: %s", controlDir, ivaratorInfo, result), exception);
            throw new IvaratorException("Failed Ivarator fillSortedSets: " + result, exception);
        }

        // now reset the current source to the next viable range
        moveToNextRow();
    }

    private void getNextUnsortedKey() throws IOException {
        this.keys = null;

        // if we are in a row but bounding ranges is empty, then something has gone awry
        if (this.fiRow != null && this.boundingFiRanges.isEmpty()) {
            throw new IvaratorException("Ivarator found to be in an illegal state with empty bounding FiRanges: fiRow = " + this.fiRow
                            + " and lastRangeSeeked = " + this.lastRangeSeeked);
        }

        // create a set if needed (does not actually need to be thread safe as we are only using one thread in this case)
        if (this.threadSafeSet == null) {
            this.threadSafeSet = new TreeSet<>();
        } else {
            this.threadSafeSet.clear();
        }

        // if this is the first time through, then create a separate source, and seek
        if (this.fiSource == null) {
            this.fiSource = getSourceCopy();
            if (!this.boundingFiRanges.isEmpty()) {
                this.currentFiRange = new Range(this.boundingFiRanges.get(0));
                if (log.isTraceEnabled()) {
                    log.trace(controlDir + ": Seeking fiSource to " + this.currentFiRange);
                }
                this.fiSource.seek(this.currentFiRange, EMPTY_CFS, false);
            }
        }

        while (!this.boundingFiRanges.isEmpty() && this.threadSafeSet.isEmpty()) {
            // track through the ranges and rows until we have a hit
            while (!this.boundingFiRanges.isEmpty() && !this.fiSource.hasTop()) {
                // remove the top boundingFiRange since the fiSource seeked to that range is now exhausted
                this.boundingFiRanges.remove(0);
                if (this.boundingFiRanges.isEmpty()) {
                    moveToNextRow();
                }
                if (!this.boundingFiRanges.isEmpty()) {
                    this.currentFiRange = new Range(this.boundingFiRanges.get(0));
                    if (log.isTraceEnabled()) {
                        log.trace(controlDir + ": Seeking fiSource to " + this.currentFiRange);
                    }
                    this.fiSource.seek(this.currentFiRange, EMPTY_CFS, false);
                }
            }

            if (this.fiSource.hasTop()) {
                addKey(this.fiSource.getTopKey());
                this.fiSource.next();
            }
        }
    }

    /**
     * Start the timing of the ivarator.
     */
    protected void startTiming() {
        this.startTime = System.currentTimeMillis();
    }

    /**
     * Check if the scan timeout has been reached. Mark as timed out and cancel the query if so.
     */
    protected void checkTiming() {
        if (System.currentTimeMillis() > (this.startTime + this.scanTimeout)) {
            // mark as timed out
            this.timedOut = true;
            // and cancel the query
            this.setControl.setCancelled();
        }
    }

    /**
     * Was the timed out flag set.
     *
     * @return a boolean if timed out
     */
    protected boolean isTimedOut() {
        return this.timedOut;
    }

    /**
     * Set the timed out flag.
     */
    public void setTimedOut(boolean timedOut) {
        this.timedOut = timedOut;
    }

    /**
     * Get a source copy. This is only used when retrieving unsorted values.
     *
     * @return a source
     */
    protected SortedKeyValueIterator<Key,Value> getSourceCopy() {
        SortedKeyValueIterator<Key,Value> source = getSource();
        synchronized (source) {
            source = source.deepCopy(this.initEnv);
        }
        return source;
    }

    /**
     * Get a source copy from the source pool.
     *
     * @return a source
     */
    protected SortedKeyValueIterator<Key,Value> takePoolSource() {
        final SortedKeyValueIterator<Key,Value> source;
        try {
            source = this.ivaratorSourcePool.borrowObject();
        } catch (Exception e) {
            throw new RuntimeException("Unable to borrow object from ivarator source pool.  " + e.getMessage());
        }
        return source;
    }

    /**
     * Return a source copy to the source pool.
     *
     * @param source
     *            a source
     */
    protected void returnPoolSource(SortedKeyValueIterator<Key,Value> source) {
        try {
            this.ivaratorSourcePool.returnObject(source);
        } catch (Exception e) {
            throw new RuntimeException("Unable to return object to ivarator source pool.  " + e.getMessage());
        }
    }

    /**
     * Add the key to the underlying cached set if it passes the filters and the matches call.
     *
     * @param topFiKey
     *            the top index key
     * @return true if it matched
     * @throws IOException
     *             for issues with read/write
     */
    protected boolean addKey(Key topFiKey) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(controlDir + ": addKey evaluating " + topFiKey);
        }
        if ((this.timeFilter == null || this.timeFilter.apply(topFiKey)) && (this.datatypeFilter == null || this.datatypeFilter.apply(topFiKey))
                        && (matches(topFiKey) != negated)) {
            if (log.isTraceEnabled()) {
                log.trace(controlDir + ": addKey matched " + topFiKey);
            }
            Key topEventKey = buildEventKey(topFiKey, returnKeyType);
            // final check to ensure all keys are contained by initial seek
            if (this.sortedUIDs && log.isTraceEnabled()) {
                log.trace(controlDir + ": testing " + topEventKey + " against " + this.lastRangeSeeked);
            }
            // no need to check containership if not returning sorted uids
            if (!this.sortedUIDs || this.lastRangeSeeked.contains(topEventKey)) {
                // avoid writing to set if cancelled
                if (!DatawaveFieldIndexCachingIteratorJexl.this.setControl.isCancelledQuery()) {
                    if (log.isTraceEnabled()) {
                        log.trace(controlDir + ": Adding result: " + topEventKey);
                    }
                    DatawaveFieldIndexCachingIteratorJexl.this.threadSafeSet.add(topEventKey);
                    return true;
                }
            }
        }
        return false;
    }

    public long getScanTimeout() {
        return scanTimeout;
    }

    public long getStartTime() {
        return startTime;
    }

    /**
     * This method will asynchronously fill the set with matches from within the specified bounding FI range.
     *
     * @param boundingFiRange
     *            the bounding index range
     * @param totalResults
     *            total results
     * @return the Future
     */
    protected IvaratorFuture fillSet(final Range boundingFiRange, final TotalResults totalResults) {

        // create runnable
        String taskName = getTaskName(boundingFiRange);
        IvaratorFuture future = IteratorThreadPoolManager.getIvaratorFuture(taskName, this.initEnv);
        if (future == null) {
            log.debug(controlDir + ": Creating ivarator runnable for " + taskName);
            // no future exists, so get a source and create/execute a new IvaratorRunnable
            // this will block until an ivarator source becomes available
            SortedKeyValueIterator<Key,Value> source = takePoolSource();
            IvaratorRunnable ivaratorRunnable = new IvaratorRunnable(this, source, boundingFiRange, boundingFiRange, this.fiRow, this.queryId, totalResults);
            future = IteratorThreadPoolManager.executeIvarator(ivaratorRunnable, taskName, this.initEnv);
        } else {
            log.debug(controlDir + ": Found ivarator runnable for " + taskName);
        }
        return future;
    }

    public String getTaskName(Range boundingFiRange) {
        String fiRow = boundingFiRange.getStartKey().getRow().toString();
        StringBuilder sb = new StringBuilder();
        sb.append(toStringNoQueryId());
        sb.append(" scanId:").append(scanId);
        sb.append(" queryId:").append(queryId);
        sb.append(" fiRow:").append(fiRow);
        sb.append(" iHash:").append(getIHash(fiRow));
        sb.append(" directory:").append(controlDir);
        sb.append(" termNumber:").append(termNumber);
        sb.append(" range:").append(boundingFiRange);
        sb.append(" rangeHash:").append(Math.abs(boundingFiRange.hashCode() / 2));
        return sb.toString();
    }

    /**
     * Get the unique directory for a specific row
     *
     * @param uniqueDir
     *            the unique directory
     * @param row
     *            a row
     * @return the unique dir
     */
    protected Path getRowDir(Path uniqueDir, String row) {
        return new Path(uniqueDir, row);
    }

    /**
     * Clear out the current row-based hdfs backed set
     *
     */
    protected void clearRowBasedHdfsBackedSet() {
        this.keys = null;
        this.currentRow = null;
        this.set = null;
    }

    protected boolean resumeFromIvaratorFutures() {
        boolean canResume = true;
        String ivaratorInfo = getIvaratorInfo(this.fiRow.toString(), true);
        // Only copy previous Ivarator's RowBasedHdfsBackedSet if all futures are available
        // and the Ivarator in the IvaratorFuture is the same object
        DatawaveFieldIndexCachingIteratorJexl previousIvarator = null;
        List<IvaratorRunnable> runnables = new ArrayList<>();
        for (Range r : this.boundingFiRanges) {
            String taskName = getTaskName(r);
            IvaratorFuture f = IteratorThreadPoolManager.getIvaratorFuture(taskName, initEnv);
            if (f != null) {
                runnables.add(f.getIvaratorRunnable());
            }
        }

        if (runnables.isEmpty()) {
            canResume = false;
        } else {
            for (IvaratorRunnable ivaratorRunnable : runnables) {
                String taskName = ivaratorRunnable.getTaskName();
                Status status = ivaratorRunnable.getStatus();
                // All IvaratorRunnables from the previous execution must reference the same Ivarator or something is wrong
                if (previousIvarator == null) {
                    previousIvarator = ivaratorRunnable.getIvarator();
                } else if (previousIvarator != ivaratorRunnable.getIvarator()) {
                    log.info(String.format("%s: Resuming Ivarator %s failed - taskName:%s has inconsistent ivarator", controlDir, ivaratorInfo, taskName));
                    canResume = false;
                    break;
                }
            }
        }

        if (canResume) {
            int resumed = 0;
            int completed = 0;
            this.currentRow = previousIvarator.currentRow;
            this.threadSafeSet = previousIvarator.threadSafeSet;
            this.set = previousIvarator.set;
            this.setControl = previousIvarator.setControl;
            this.startTime = previousIvarator.startTime;
            // resume processing of each suspended IvaratorRunnable
            for (IvaratorRunnable ivaratorRunnable : runnables) {
                // all IvaratorRunnables must reference the new Ivarator
                ivaratorRunnable.setIvarator(this);
                Status status = ivaratorRunnable.getStatus();
                String taskName = ivaratorRunnable.getTaskName();
                if (status.equals(CREATED) || status.equals(SUSPENDED)) {
                    try {
                        // remove the previous IvaratorFuture from IteratorThreadPoolManager
                        IteratorThreadPoolManager.removeIvarator(taskName, this.initEnv);
                        ivaratorRunnable.prepareForResume(this);
                        // execute new IvaratorRunnable and add it to IteratorThreadPoolManager
                        IteratorThreadPoolManager.executeIvarator(ivaratorRunnable, taskName, this.initEnv);
                        resumed++;
                    } catch (IllegalStateException e) {
                        // unable to resume this IvaratorRunnable, it will get recreated later
                        // because its IvaratorFuture was removed a few lines above this
                        log.warn(e.getMessage());
                    }
                } else if (status.equals(COMPLETED)) {
                    completed++;
                } else {
                    // It's possible that there was a problem suspending a previous IvaratorRunnable, so we should
                    // ensure that suspendRequested is set and remove the previous IvaratorFuture from IteratorThreadPoolManager.
                    // A new IvaratorRunnable and IvaratorFuture will be created
                    IvaratorFuture future = IteratorThreadPoolManager.getIvaratorFuture(taskName, this.initEnv);
                    // No need to wait 60 seconds for the loop to exit; Very likely that it already has exited
                    IteratorThreadPoolManager.suspendIvarator(future, true, this.initEnv, 500, TimeUnit.MILLISECONDS);
                }
            }
            int recreated = this.boundingFiRanges.size() - resumed - completed;
            log.info(String.format("%s: Resumed Ivarator %s IvaratorRunnables completed:%d resumed:%d recreated:%d", controlDir, ivaratorInfo, completed,
                            resumed, recreated));
        } else {
            // can not resume from the previous Ivarator, so ensure that the previous IvaratorFutures are removed
            for (IvaratorRunnable ivaratorRunnable : runnables) {
                IteratorThreadPoolManager.removeIvarator(ivaratorRunnable.getTaskName(), this.initEnv);
            }
        }
        return canResume;
    }

    /**
     * This will setup the set for the specified range. This will attempt to reuse precomputed and persisted sets if we are allowed to.
     *
     * @param row
     *            a row
     * @throws IOException
     *             for issues with read/write
     */
    protected void setupRowBasedHdfsBackedSet(String row) throws IOException {
        // we are done if cancelled
        if (this.setControl.isCancelledQuery()) {
            return;
        }

        try {
            // for each of the ivarator cache dirs
            if (!this.allowDirReuse) {
                for (IvaratorCacheDir ivaratorCacheDir : this.ivaratorCacheDirs) {
                    // get the row specific dir
                    Path rowDir = getRowDir(new Path(ivaratorCacheDir.getPathURI()), row);
                    FileSystem fs = ivaratorCacheDir.getFs();

                    // if we are not allowing reuse of directories, then delete it
                    if (fs.exists(rowDir)) {
                        fs.delete(rowDir, true);
                    }
                }
            }

            // ensure the control directory is created
            Path controlRowDir = getRowDir(this.controlDir, row);
            if (!this.controlFs.exists(controlRowDir)) {
                this.controlFs.mkdirs(controlRowDir);
                this.createdRowDir = true;
            } else {
                this.createdRowDir = false;
            }

            // noinspection unchecked
            this.set = (HdfsBackedSortedSet<Key>) HdfsBackedSortedSet.builder().withBufferPersistThreshold(hdfsBackedSetBufferSize)
                            .withIvaratorCacheDirs(ivaratorCacheDirs).withUniqueSubPath(row).withMaxOpenFiles(maxOpenFiles).withNumRetries(numRetries)
                            .withPersistOptions(persistOptions).withSetFactory(new FileKeySortedSet.Factory()).build();

            this.threadSafeSet = Collections.synchronizedSortedSet(this.set);
            this.currentRow = row;
            this.setControl.takeOwnership(row, this);

            // if this set is not marked as complete (meaning completely filled AND persisted), then we cannot trust the contents and we need to recompute.
            if (!this.setControl.isCompleteAndPersisted(row)) {
                this.set.clear();
                this.keys = null;
                log.info(String.format("%s: Creating empty HdfsBackedSortedSet for Ivarator %s with ivaratorCacheDirs %s", controlDir,
                                getIvaratorInfo(row, false), ivaratorCacheDirs));
            } else {
                this.keys = new CachingIterator<>(this.set.iterator());
                log.info(String.format("%s: Reusing completed HdfsBackedSortedSet for Ivarator %s with ivaratorCacheDirs %s", controlDir,
                                getIvaratorInfo(row, false), ivaratorCacheDirs));
            }

            // reset the keyValues counter as we have a new set here
            this.scannedKeys.set(0);
        } catch (IOException ioe) {
            throw new IllegalStateException("Unable to create Hdfs backed sorted set", ioe);
        }
    }

    /**
     * Build the bounding FI ranges. Normally this returns only one range, but it could return multiple (@see DatawaveFieldIndexRegex/Range/ListIteratorJexl
     * superclasses). If multiple are returned, then they must be sorted. These ranges are expected to be exclusively in the field index!
     *
     * @param rowId
     *            a row id
     * @param fieldValue
     *            the field value
     * @param fiName
     *            the field index name
     * @return a list of ranges
     */
    @SuppressWarnings("hiding")
    protected abstract List<Range> buildBoundingFiRanges(Text rowId, Text fiName, Text fieldValue);

    /**
     * Does the last range seeked contain the passed in range
     *
     * @param r
     *            the range
     * @return true if there is a last seeked range and it contains the passed in range
     */
    protected boolean lastRangeSeekedContains(Range r) {
        boolean subRange = false;
        if (this.lastRangeSeeked != null) {
            Key beginOfThisRange = r.getStartKey();
            Key endOfThisRange = r.getEndKey();
            subRange = true;
            if (beginOfThisRange == null && this.lastRangeSeeked.getStartKey() != null) {
                subRange = false;
            } else if (!Objects.equal(beginOfThisRange, this.lastRangeSeeked.getStartKey()) && !this.lastRangeSeeked.contains(beginOfThisRange)) {
                subRange = false;
            } else if (endOfThisRange == null && this.lastRangeSeeked.getEndKey() != null) {
                subRange = false;
            } else if (!Objects.equal(endOfThisRange, this.lastRangeSeeked.getEndKey()) && !this.lastRangeSeeked.contains(endOfThisRange)) {
                subRange = false;
            }
        }

        return subRange;
    }

    // need to build a range starting at the end of current row (this.fiRow) and seek the
    // source to it. If we get an IOException, that means we hit the end of the tablet.
    protected Text moveToNextRow() throws IOException {
        log.trace(controlDir + ": moveToNextRow()");

        QuerySpan querySpan = null;

        try {
            // this will block until an ivarator source becomes available
            final SortedKeyValueIterator<Key,Value> source = takePoolSource();

            try {

                if (this.collectTimingDetails && source instanceof SourceTrackingIterator) {
                    querySpan = ((SourceTrackingIterator) source).getQuerySpan();
                }

                // Make sure the source iterator's key didn't seek past the end
                // of our starting row and get into the next row. It can happen if your
                // fi keys are on a row boundary.
                if (this.lastRangeSeeked.getEndKey() != null && !this.lastRangeSeeked.contains(new Key(this.fiRow).followingKey(PartialKey.ROW))) {
                    this.fiRow = null;
                } else {
                    Range followingRowRange = new Range(new Key(this.fiRow).followingKey(PartialKey.ROW), true, this.lastRangeSeeked.getEndKey(),
                                    this.lastRangeSeeked.isEndKeyInclusive());
                    if (log.isTraceEnabled()) {
                        log.trace(controlDir + ": moveToNextRow(Key k), followingRowRange: " + followingRowRange);
                    }
                    // do an initial seek to determine the next row (needed to calculate bounding FI ranges below)
                    source.seek(followingRowRange, EMPTY_CFS, false);
                    this.scannedKeys.incrementAndGet();
                    if (source.hasTop()) {
                        this.fiRow = source.getTopKey().getRow();
                    } else {
                        this.fiRow = null;
                    }
                }

            } finally {
                returnPoolSource(source);
            }

            if (log.isTraceEnabled()) {
                log.trace(controlDir + ": moveToNextRow, nextRow: " + this.fiRow);
            }

            // The boundingFiRange is used to test that we have the right fieldName->fieldValue pairing.
            this.boundingFiRanges.clear();
            if (this.fiRow != null) {
                this.boundingFiRanges.addAll(this.buildBoundingFiRanges(this.fiRow, this.fiName, this.fieldValue));

                if (log.isTraceEnabled()) {
                    log.trace(controlDir + ": moveToNextRow() boundingFiRange: " + this.boundingFiRanges);
                }
            }
        } finally {
            if (this.collectTimingDetails && this.querySpanCollector != null && querySpan != null) {
                this.querySpanCollector.addQuerySpan(querySpan);
            }
        }
        return this.fiRow;
    }

    /**
     * Does this key match. Note we are not overriding the super.isMatchingKey() as we need that to work as is NOTE: This method must be thread safe
     *
     * @param k
     *            the key
     * @return a boolean based on if the key matches
     * @throws IOException
     *             for issues with read/write
     */
    protected abstract boolean matches(Key k) throws IOException;

    /**
     * A protected method to force persistence of the set. This can be used by test cases to verify tear down and rebuilding with reuse of the previous results.
     *
     * @throws IOException
     *             for issues with read/write
     */
    protected void forcePersistence() throws IOException {
        if (this.set != null) {
            if (!this.set.isPersisted()) {
                this.set.persist();
            }
            // declare the persisted set complete
            this.setControl.setCompleteAndPersisted(this.currentRow);
        }
    }

    public class HdfsBackedControl {
        public static final String OWNERSHIP_FILE = "ownership";
        public static final String COMPLETE_FILE = "complete";

        // cancelled check interval is 1 minute
        public static final int CANCELLED_CHECK_INTERVAL = 1000 * 60;
        private volatile long lastCancelledCheck = System.currentTimeMillis() - RANDOM.nextInt(CANCELLED_CHECK_INTERVAL);
        private volatile boolean cancelled = false;

        private final int bufferSize = 128;

        protected Path getOwnershipFile(String row) {
            return new Path(getRowDir(controlDir, row), OWNERSHIP_FILE);
        }

        protected Path getCompleteFile(String row) {
            return new Path(getRowDir(controlDir, row), COMPLETE_FILE);
        }

        protected String getOwnerId(Object owner) {
            return DatawaveFieldIndexCachingIteratorJexl.getHostname() + "://" + Integer.toString(System.identityHashCode(owner));
        }

        public void takeOwnership(String row, Object owner) throws IOException {
            Path file = getOwnershipFile(row);
            writeFile(file, getOwnerId(owner).getBytes());
        }

        public boolean hasOwnership(String row, Object owner) throws IOException {
            byte[] ownerId = getOwnerId(owner).getBytes();

            Path file = getOwnershipFile(row);
            if (controlFs.exists(file)) {
                return hasContents(file, ownerId);
            }
            return false;
        }

        private boolean hasContents(Path file, byte[] contents) throws IOException {
            FSDataInputStream stream = controlFs.open(file, this.bufferSize);
            int len;
            byte[] buffer;
            try {
                buffer = new byte[this.bufferSize];
                len = stream.read(buffer);
            } finally {
                stream.close();
            }

            if (len == contents.length) {
                for (int i = 0; i < len; i++) {
                    if (contents[i] != buffer[i]) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        public boolean isCancelledQuery() {
            // if we have not determined we are cancelled yet, then check
            if (!cancelled && queryLock != null) {
                // but only if the last check was so long ago
                long now = System.currentTimeMillis();
                if ((now - lastCancelledCheck) > CANCELLED_CHECK_INTERVAL) {
                    synchronized (this) {
                        // now recheck the cancelled flag and timeout to ensure we really need to make the hdfs calls
                        if (!cancelled && ((now - lastCancelledCheck) > CANCELLED_CHECK_INTERVAL)) {
                            cancelled = !queryLock.isQueryRunning();
                            lastCancelledCheck = now;
                        }
                    }
                }
            }
            return cancelled;
        }

        public void setCancelled() {
            this.cancelled = true;
        }

        public void setCompleteAndPersisted(String row) throws IOException {
            Path file = getCompleteFile(row);
            writeFile(file, "complete".getBytes());
        }

        public boolean isCompleteAndPersisted(String row) throws IOException {
            Path file = getCompleteFile(row);
            return controlFs.exists(file);
        }

        private void writeFile(Path file, byte[] value) throws IOException {
            // if a cancelled query, then return immediately
            if (isCancelledQuery()) {
                return;
            }

            int count = 0;
            boolean done = false;
            boolean append = false;
            String reason = null;
            Exception exc = null;

            while (!done && count <= numRetries) {
                count++;
                try {
                    FSDataOutputStream stream = null;
                    if (append) {
                        try {
                            stream = controlFs.append(file, bufferSize);
                        } catch (IOException ioe) {
                            if (ioe.getMessage().equals("Not supported")) {
                                stream = controlFs.create(file, true, bufferSize);
                            } else {
                                throw ioe;
                            }
                        }
                    } else {
                        stream = controlFs.create(file, true, bufferSize);
                    }
                    try {
                        stream.write(value);
                    } finally {
                        stream.close();
                    }
                    exc = null;
                    done = true;
                } catch (Exception e) {
                    exc = e;
                    try {
                        // see if we can determine why
                        if (controlFs.exists(controlDir)) {
                            // so the directory exists, try the row dir
                            if (controlFs.exists(new Path(controlDir, currentRow))) {
                                // so the directory exists, how about the file
                                if (controlFs.exists(file)) {
                                    append = true;
                                    reason = "Failed to create file, but the file exists: " + file;
                                    // check if the contents actually got written
                                    if (hasContents(file, value)) {
                                        // we have a file with the correct contents, we are done...with success
                                        exc = null;
                                        done = true;
                                    }
                                } else {
                                    reason = "Failed to create file, the file does not exist: " + file;
                                }
                            } else {
                                reason = "Failed to create file, row dir does not exist: " + file;
                            }
                        } else {
                            reason = "Failed to create file, query dir does not exist: " + file;
                            // in this case, we really want to stop this iterator as we are cancelled
                            count = numRetries + 1;
                        }
                    } catch (Exception e2) {
                        reason = "Failed to create file: " + file;
                    }
                }
            }
            if (exc != null) {
                throw new IOException(reason, exc);
            }
        }

    }

    public static String getHostname() {
        String hostname = null;
        if (System.getProperty("os.name").startsWith("Windows")) { // probably unnecessary, but for completeness
            hostname = System.getenv("COMPUTERNAME");
        } else {
            hostname = System.getenv("HOSTNAME");
            if (null == hostname || hostname.isEmpty()) {
                try {
                    hostname = InetAddress.getLocalHost().getHostName();
                    // basic validation test, if hostname is not available, sometimes the ip may be returned
                    // if it is ipv6 there could be an issue using it as it contains a ':'
                    if (null == hostname || "localhost".equals(hostname) || "127.0.0.1".equals(hostname) || hostname.contains(":")) {
                        hostname = "";
                    }
                } catch (UnknownHostException e) {
                    hostname = "";
                }
            }
        }
        return hostname;
    }

    public void setCollectTimingDetails(boolean collectTimingDetails) {
        this.collectTimingDetails = collectTimingDetails;
    }

    public boolean getCollectTimingDetails() {
        return collectTimingDetails;
    }

    public void setQuerySpanCollector(QuerySpanCollector querySpanCollector) {
        this.querySpanCollector = querySpanCollector;
    }

    public String toStringNoQueryId() {
        return toStringImpl(false);
    }

    @Override
    public String toString() {
        return toStringImpl(true);
    }

    protected abstract String toStringImpl(boolean includeQueryId);

    public AtomicLong getScannedKeys() {
        return scannedKeys;
    }

    public QuerySpanCollector getQuerySpanCollector() {
        return querySpanCollector;
    }

    public HdfsBackedControl getSetControl() {
        return setControl;
    }

    public FieldIndexCompositeSeeker getCompositeSeeker() {
        return compositeSeeker;
    }

    public CompositeMetadata getCompositeMetadata() {
        return compositeMetadata;
    }

    public int getCompositeSeekThreshold() {
        return compositeSeekThreshold;
    }

    public String getIvaratorInfo(String row, boolean includeToString) {
        if (includeToString) {
            return String.format("queryId:%s fiRow:%s iHash:%s termNumber:%d %s", queryId, row, getIHash(row), termNumber, toStringNoQueryId());
        } else {
            return String.format("queryId:%s fiRow:%s iHash:%s termNumber:%d", queryId, row, getIHash(row), termNumber);
        }
    }

    public String getIHash(String row) {
        HashCodeBuilder builder = new HashCodeBuilder();
        int hashCode = builder.append(row).append(this.toString()).toHashCode();
        return String.valueOf(Math.abs(hashCode / 2));
    }
}
