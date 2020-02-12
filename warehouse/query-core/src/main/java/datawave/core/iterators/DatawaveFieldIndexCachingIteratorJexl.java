package datawave.core.iterators;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import datawave.query.composite.CompositeSeeker.FieldIndexCompositeSeeker;
import datawave.core.iterators.querylock.QueryLock;
import datawave.query.Constants;
import datawave.query.composite.CompositeMetadata;
import datawave.query.iterator.CachingIterator;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.QuerySpanCollector;
import datawave.query.iterator.profile.SourceTrackingIterator;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.TypeMetadata;
import datawave.query.util.sortedset.FileKeySortedSet;
import datawave.query.util.sortedset.HdfsBackedSortedSet;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The Ivarator base class
 *
 * An iterator for the Datawave shard table, it searches FieldIndex keys and returns Event keys (its topKey must be an Event key).
 * 
 * This version will cache the values in an underlying HDFS file backed sorted set before returning the first top key.
 * 
 * FieldIndex keys: fi\0{fieldName}:{fieldValue}\0datatype\0uid
 * 
 * Event key: CF, {datatype}\0{UID}
 * 
 */
public abstract class DatawaveFieldIndexCachingIteratorJexl extends WrappingIterator {
    
    public static final Text ANY_FINAME = new Text("fi\0" + Constants.ANY_FIELD);
    public static final Text FI_START = new Text("fi\0");
    public static final Text FI_END = new Text("fi\0~");
    
    public abstract static class Builder<B extends Builder<B>> {
        private Text fieldName;
        protected Text fieldValue;
        private Predicate<Key> datatypeFilter;
        private TimeFilter timeFilter;
        private boolean negated;
        private PartialKey returnKeyType = DEFAULT_RETURN_KEY_TYPE;
        private int maxRangeSplit = 11;
        private FileSystem fs;
        private Path uniqueDir;
        private QueryLock queryLock;
        private boolean allowDirReuse;
        private long scanThreshold = 10000;
        private int hdfsBackedSetBufferSize = 10000;
        private int maxOpenFiles = 100;
        private boolean sortedUIDs = true;
        protected QuerySpanCollector querySpanCollector = null;
        protected volatile boolean collectTimingDetails = false;
        private volatile long scanTimeout = 1000L * 60 * 60;
        protected TypeMetadata typeMetadata;
        private CompositeMetadata compositeMetadata;
        private int compositeSeekThreshold;
        private GenericObjectPool<SortedKeyValueIterator<Key,Value>> ivaratorSourcePool;
        
        @SuppressWarnings("unchecked")
        protected B self() {
            return (B) this;
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
        
        public B withFileSystem(FileSystem fs) {
            this.fs = fs;
            return self();
        }
        
        public B withUniqueDir(Path uniqueDir) {
            this.uniqueDir = uniqueDir;
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
    protected static final Collection<ByteSequence> EMPTY_CFS = Collections.EMPTY_LIST;
    
    // These are the ranges to scan in the field index
    private final List<Range> boundingFiRanges = new ArrayList<>();
    protected Range currentFiRange = null;
    private Text fiRow = null;
    
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
    
    // The hdfs fs
    private final FileSystem fs;
    // the directory for the hdfs cache
    private final Path uniqueDir;
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
    private PeekingIterator<Key> keys = null;
    // the current row covered by the hdfs set
    private String currentRow = null;
    // did we created the row directory
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
    
    protected CompositeMetadata compositeMetadata;
    protected FieldIndexCompositeSeeker compositeSeeker;
    protected int compositeSeekThreshold;
    
    protected GenericObjectPool<SortedKeyValueIterator<Key,Value>> ivaratorSourcePool = null;
    
    // -------------------------------------------------------------------------
    // ------------- Constructors
    
    public DatawaveFieldIndexCachingIteratorJexl() {
        super();
        this.fieldName = null;
        this.fieldValue = null;
        this.fiName = null;
        
        this.negated = false;
        this.returnKeyType = DEFAULT_RETURN_KEY_TYPE;
        this.timeFilter = null;
        this.datatypeFilter = null;
        
        this.fs = null;
        this.queryLock = null;
        this.uniqueDir = null;
        this.allowDirReuse = false;
        this.scanThreshold = 10000;
        this.hdfsBackedSetBufferSize = 10000;
        this.maxOpenFiles = 100;
        this.maxRangeSplit = 11;
        
        this.sortedUIDs = true;
    }
    
    protected DatawaveFieldIndexCachingIteratorJexl(Builder builder) {
        this(builder.fieldName, builder.fieldValue, builder.timeFilter, builder.datatypeFilter, builder.negated, builder.scanThreshold, builder.scanTimeout,
                        builder.hdfsBackedSetBufferSize, builder.maxRangeSplit, builder.maxOpenFiles, builder.fs, builder.uniqueDir, builder.queryLock,
                        builder.allowDirReuse, builder.returnKeyType, builder.sortedUIDs, builder.compositeMetadata, builder.compositeSeekThreshold,
                        builder.typeMetadata, builder.ivaratorSourcePool);
    }
    
    @SuppressWarnings("hiding")
    private DatawaveFieldIndexCachingIteratorJexl(Text fieldName, Text fieldValue, TimeFilter timeFilter, Predicate<Key> datatypeFilter, boolean neg,
                    long scanThreshold, long scanTimeout, int bufferSize, int maxRangeSplit, int maxOpenFiles, FileSystem fs, Path uniqueDir,
                    QueryLock queryLock, boolean allowDirReuse, PartialKey returnKeyType, boolean sortedUIDs, CompositeMetadata compositeMetadata,
                    int compositeSeekThreshold, TypeMetadata typeMetadata, GenericObjectPool<SortedKeyValueIterator<Key,Value>> ivaratorSourcePool) {
        
        this.ivaratorSourcePool = ivaratorSourcePool;
        
        if (fieldName.toString().startsWith("fi" + NULL_BYTE)) {
            this.fieldName = new Text(fieldName.toString().substring(3));
            this.fiName = fieldName;
        } else {
            this.fieldName = fieldName;
            this.fiName = new Text("fi" + NULL_BYTE + fieldName);
        }
        log.trace("fName : " + fiName.toString().replaceAll(NULL_BYTE, "%00"));
        this.fieldValue = fieldValue;
        this.negated = neg;
        this.returnKeyType = returnKeyType;
        this.timeFilter = timeFilter;
        this.datatypeFilter = datatypeFilter;
        
        this.fs = fs;
        this.queryLock = queryLock;
        this.uniqueDir = uniqueDir;
        this.allowDirReuse = allowDirReuse;
        this.scanThreshold = scanThreshold;
        this.scanTimeout = scanTimeout;
        this.hdfsBackedSetBufferSize = bufferSize;
        this.maxOpenFiles = maxOpenFiles;
        this.maxRangeSplit = maxRangeSplit;
        
        this.sortedUIDs = sortedUIDs;
        
        // setup composite logic if this is a composite field
        if (compositeMetadata != null) {
            List<String> compositeFields = compositeMetadata.getCompositeFieldMapByType().entrySet().stream().flatMap(x -> x.getValue().keySet().stream())
                            .distinct().collect(Collectors.toList());
            if (compositeFields.contains(fieldName.toString())) {
                this.compositeMetadata = compositeMetadata;
                this.compositeSeeker = new FieldIndexCompositeSeeker(typeMetadata.fold());
            }
        }
        
        this.compositeSeekThreshold = compositeSeekThreshold;
    }
    
    public DatawaveFieldIndexCachingIteratorJexl(DatawaveFieldIndexCachingIteratorJexl other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
        this.fieldName = other.fieldName;
        this.fiName = other.fiName;
        this.returnKeyType = other.returnKeyType;
        this.timeFilter = other.timeFilter;
        this.datatypeFilter = other.datatypeFilter;
        this.fieldValue = other.fieldValue;
        this.boundingFiRanges.addAll(other.boundingFiRanges);
        this.negated = other.negated;
        
        this.fs = other.fs;
        this.queryLock = other.queryLock;
        this.uniqueDir = other.uniqueDir;
        this.allowDirReuse = other.allowDirReuse;
        this.scanThreshold = other.scanThreshold;
        this.scanTimeout = other.scanTimeout;
        this.hdfsBackedSetBufferSize = other.hdfsBackedSetBufferSize;
        this.maxOpenFiles = other.maxOpenFiles;
        
        this.set = other.set;
        this.keys = other.keys;
        this.currentRow = other.currentRow;
        this.createdRowDir = other.createdRowDir;
        this.maxRangeSplit = other.maxRangeSplit;
        
        this.sortedUIDs = other.sortedUIDs;
        
        try {
            this.setControl.takeOwnership(this.currentRow, this);
        } catch (IOException e) {
            log.error("Could not take ownership of set", e);
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
            log.trace("begin seek, range: " + r);
        }
        
        if (!lastRangeSeekedContains(r)) {
            // the start of this range is beyond the end of the last range seeked
            // we must reset keyValues to null and empty the underlying collection
            clearRowBasedHdfsBackedSet();
        } else {
            // inside the original range, so potentially need to reposition keyValues
            if (keys != null) {
                Key startKey = r.getStartKey();
                // decide if keyValues needs to be rebuilt or can be reused
                if (!keys.hasNext() || (keys.peek().compareTo(startKey) > 0)) {
                    keys = new CachingIterator<>(threadSafeSet.iterator());
                }
            }
        }
        
        // if we are not sorting UIDs, then determine whether we have a cq and capture the lastFiKey
        Key lastFiKey = null;
        if (!sortedUIDs && r.getStartKey().getColumnFamily().getLength() > 0 && r.getStartKey().getColumnQualifier().getLength() > 0) {
            Key startKey = r.getStartKey();
            String cq = startKey.getColumnQualifier().toString();
            int fieldnameIndex = cq.indexOf('\0');
            if (fieldnameIndex >= 0) {
                String cf = startKey.getColumnFamily().toString();
                lastFiKey = new Key(startKey.getRow().toString(), "fi\0" + cq.substring(0, fieldnameIndex), cq.substring(fieldnameIndex + 1) + '\0' + cf + '\0');
            }
        }
        
        this.lastRangeSeeked = r;
        QuerySpan querySpan = null;
        
        try {
            this.fiRow = null;
            
            // this will block until an ivarator source becomes available
            final SortedKeyValueIterator<Key,Value> source = takePoolSource();
            
            try {
                
                if (collectTimingDetails && source instanceof SourceTrackingIterator) {
                    querySpan = ((SourceTrackingIterator) source).getQuerySpan();
                }
                
                // seek our underlying source to the start of the incoming range
                // expand the range as the underlying table may not actually contain the keys in this range as we are only returning keys
                // as specified by the returnKeyType
                Range seekRange = new Range(lastRangeSeeked.getStartKey(), lastRangeSeeked.isStartKeyInclusive(), (lastRangeSeeked.getEndKey() == null ? null
                                : new Key(lastRangeSeeked.getEndKey().getRow()).followingKey(PartialKey.ROW)), false);
                source.seek(seekRange, EMPTY_CFS, false);
                scannedKeys.incrementAndGet();
                if (log.isTraceEnabled()) {
                    try {
                        log.trace("lastRangeSeeked: " + lastRangeSeeked + "  source.getTopKey(): " + source != null ? source.getTopKey() : null);
                    } catch (Exception ex) {
                        log.trace("Ignoring this while logging a trace message:", ex);
                        // let's not ruin everything when trace is on...
                    }
                }
                
                // Determine the bounding FI ranges for the field index for this row
                this.boundingFiRanges.clear();
                if (source.hasTop()) {
                    this.fiRow = source.getTopKey().getRow();
                    this.boundingFiRanges.addAll(buildBoundingFiRanges(fiRow, fiName, fieldValue));
                    
                    // if we are not sorting uids and we have a starting value, then pop off the ranges until we have the one
                    // containing the last value returned. Then modify that range appropriately.
                    if (lastFiKey != null) {
                        if (log.isTraceEnabled()) {
                            log.trace("Reseeking fi to lastFiKey: " + lastFiKey);
                        }
                        while (!boundingFiRanges.isEmpty() && !boundingFiRanges.get(0).contains(lastFiKey)) {
                            if (log.isTraceEnabled()) {
                                log.trace("Skipping range: " + boundingFiRanges.get(0));
                            }
                            boundingFiRanges.remove(0);
                            if (this.boundingFiRanges.isEmpty()) {
                                moveToNextRow();
                            }
                        }
                        if (!boundingFiRanges.isEmpty()) {
                            if (log.isTraceEnabled()) {
                                log.trace("Starting in range: " + boundingFiRanges.get(0));
                            }
                            Range boundingFiRange = boundingFiRanges.get(0);
                            boundingFiRange = new Range(lastFiKey, false, boundingFiRange.getEndKey(), boundingFiRange.isEndKeyInclusive());
                            boundingFiRanges.set(0, boundingFiRange);
                            if (log.isTraceEnabled()) {
                                log.trace("Reset range to: " + boundingFiRanges.get(0));
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
            if (this.fiRow != null) {
                findTop();
            }
            
            if (log.isTraceEnabled()) {
                log.trace("seek, topKey : " + ((null == topKey) ? "null" : topKey));
            }
        } finally {
            if (collectTimingDetails && querySpanCollector != null && querySpan != null) {
                querySpanCollector.addQuerySpan(querySpan);
            }
        }
    }
    
    @Override
    public void next() throws IOException {
        log.trace("next() called");
        
        findTop();
        
        if (topKey != null && log.isTraceEnabled()) {
            log.trace("next() => " + topKey);
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
                    if (sortedUIDs && log.isTraceEnabled()) {
                        log.trace("Is " + key + " contained in " + this.lastRangeSeeked);
                    }
                    // no need to check containership if not returning sorted uids
                    if (!sortedUIDs || this.lastRangeSeeked.contains(key)) {
                        this.topKey = key;
                        if (log.isDebugEnabled()) {
                            log.debug("setting as topKey " + topKey);
                        }
                        break;
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
                if (sortedUIDs) {
                    fillSortedSets();
                } else {
                    getNextUnsortedKey();
                }
                
                if (this.setControl.isCancelledQuery()) {
                    this.topKey = null;
                }
                
                if (isTimedOut()) {
                    log.error("Ivarator query timed out");
                    throw new IvaratorException("Ivarator query timed out");
                }
                
                if (this.setControl.isCancelledQuery()) {
                    log.debug("Ivarator query was cancelled");
                    throw new IterationInterruptedException("Ivarator query was cancelled");
                }
                
                // if we have any persisted data or we have scanned a significant number of keys, then persist it completely
                if (this.set != null && (this.set.hasPersistedData() || (scanThreshold <= scannedKeys.get()))) {
                    forcePersistence();
                }
                
                if (this.keys == null) {
                    this.keys = new CachingIterator<>(this.threadSafeSet.iterator());
                }
            }
            
            if (this.setControl.isCancelledQuery()) {
                if (isTimedOut()) {
                    log.error("Ivarator query timed out");
                    throw new IvaratorException("Ivarator query timed out");
                } else {
                    log.debug("Ivarator query was cancelled");
                    throw new IterationInterruptedException("Ivarator query was cancelled");
                }
            }
            
        }
    }
    
    private void fillSortedSets() throws IOException {
        String sourceRow = this.fiRow.toString();
        setupRowBasedHdfsBackedSet(sourceRow);
        
        // for each range, fork off a runnable
        List<Future<?>> futures = new ArrayList<>(boundingFiRanges.size());
        if (log.isDebugEnabled()) {
            log.debug("Processing " + boundingFiRanges + " for " + this);
        }
        
        for (Range range : boundingFiRanges) {
            if (log.isTraceEnabled()) {
                log.trace("range -> " + range);
            }
            futures.add(fillSet(range));
        }
        
        boolean failed = false;
        Exception exception = null;
        Object result = null;
        
        // wait for all of the threads to complete
        for (Future<?> future : futures) {
            checkTiming();
            
            if (failed || this.setControl.isCancelledQuery()) {
                future.cancel(false);
            } else {
                try {
                    result = future.get();
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
        
        if (failed) {
            log.error("Failed to complete ivarator cache: " + result, exception);
            throw new IvaratorException("Failed to complete ivarator cache: " + result, exception);
        }
        
        // now reset the current source to the next viable range
        moveToNextRow();
    }
    
    private void getNextUnsortedKey() throws IOException {
        this.keys = null;
        
        // if we are in a row but bounding ranges is empty, then something has gone awry
        if (fiRow != null && boundingFiRanges.isEmpty()) {
            throw new IvaratorException("Ivarator found to be in an illegal state with empty bounding FiRanges: fiRow = " + fiRow + " and lastRangeSeeked = "
                            + lastRangeSeeked);
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
                if (log.isTraceEnabled()) {
                    log.trace("Seeking fisource to " + this.boundingFiRanges.get(0));
                }
                currentFiRange = new Range(this.boundingFiRanges.get(0));
                this.fiSource.seek(this.boundingFiRanges.get(0), EMPTY_CFS, false);
            }
        }
        
        while (!this.boundingFiRanges.isEmpty() && this.threadSafeSet.isEmpty()) {
            // track through the ranges and rows until we have a hit
            while (!this.boundingFiRanges.isEmpty() && !this.fiSource.hasTop()) {
                this.boundingFiRanges.remove(0);
                if (this.boundingFiRanges.isEmpty()) {
                    moveToNextRow();
                }
                if (!this.boundingFiRanges.isEmpty()) {
                    if (log.isTraceEnabled()) {
                        log.trace("Seeking fisource to " + this.boundingFiRanges.get(0));
                    }
                    currentFiRange = new Range(this.boundingFiRanges.get(0));
                    this.fiSource.seek(this.boundingFiRanges.get(0), EMPTY_CFS, false);
                }
            }
            
            if (this.fiSource.hasTop()) {
                addKey(this.fiSource.getTopKey(), this.fiSource.getTopValue());
                this.fiSource.next();
            }
        }
    }
    
    /**
     * Start the timing of the ivarator.
     */
    protected void startTiming() {
        startTime = System.currentTimeMillis();
    }
    
    /**
     * Check if the scan timeout has been reached. Mark as timed out and cancel the query if so.
     */
    protected void checkTiming() {
        if (System.currentTimeMillis() > (startTime + scanTimeout)) {
            // mark as timed out
            this.timedOut = true;
            // and cancel the query
            this.setControl.setCancelled();
        }
    }
    
    /**
     * Was the timed out flag set.
     */
    protected boolean isTimedOut() {
        return this.timedOut;
    }
    
    /**
     * Get a source copy. This is only used when retrieving unsorted values.
     *
     * @return a source
     */
    protected SortedKeyValueIterator<Key,Value> getSourceCopy() {
        SortedKeyValueIterator<Key,Value> source = getSource();
        synchronized (source) {
            source = source.deepCopy(initEnv);
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
            source = ivaratorSourcePool.borrowObject();
        } catch (Exception e) {
            throw new IterationInterruptedException("Unable to borrow object from ivarator source pool.  " + e.getMessage());
        }
        return source;
    }
    
    /**
     * Return a source copy to the source pool.
     */
    protected void returnPoolSource(SortedKeyValueIterator<Key,Value> source) {
        try {
            ivaratorSourcePool.returnObject(source);
        } catch (Exception e) {
            throw new IterationInterruptedException("Unable to return object to ivarator source pool.  " + e.getMessage());
        }
    }
    
    /**
     * Add the key to the underlying cached set if it passes the filters and the matches call.
     * 
     * @param topFiKey
     * @param value
     * @return true if it matched
     * @throws IOException
     */
    protected boolean addKey(Key topFiKey, Value value) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("addKey evaluating " + topFiKey);
        }
        if ((timeFilter == null || timeFilter.apply(topFiKey)) && (datatypeFilter == null || datatypeFilter.apply(topFiKey)) && (matches(topFiKey) != negated)) {
            if (log.isTraceEnabled()) {
                log.trace("addKey matched " + topFiKey);
            }
            Key topEventKey = buildEventKey(topFiKey, returnKeyType);
            // final check to ensure all keys are contained by initial seek
            if (sortedUIDs && log.isTraceEnabled()) {
                log.trace("testing " + topEventKey + " against " + lastRangeSeeked);
            }
            // no need to check containership if not returning sorted uids
            if (!sortedUIDs || lastRangeSeeked.contains(topEventKey)) {
                // avoid writing to set if cancelled
                if (!DatawaveFieldIndexCachingIteratorJexl.this.setControl.isCancelledQuery()) {
                    if (log.isTraceEnabled()) {
                        log.trace("Adding result: " + topEventKey);
                    }
                    DatawaveFieldIndexCachingIteratorJexl.this.threadSafeSet.add(topEventKey);
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * This method will asynchronously fill the set with matches from within the specified bounding FI range.
     * 
     * @param boundingFiRange
     * @return the Future
     */
    protected Future<?> fillSet(final Range boundingFiRange) {
        
        // this will block until an ivarator source becomes available
        final SortedKeyValueIterator<Key,Value> source = takePoolSource();
        
        // create runnable
        Runnable runnable = () -> {
            if (log.isDebugEnabled()) {
                log.debug("Starting fillSet(" + boundingFiRange + ')');
            }
            int scanned = 0;
            int matched = 0;
            QuerySpan querySpan = null;
            Key nextSeekKey = null;
            int nextCount = 0;
            try {
                if (collectTimingDetails && source instanceof SourceTrackingIterator) {
                    querySpan = ((SourceTrackingIterator) source).getQuerySpan();
                }
                
                // seek the source to a range covering the entire row....the bounding box will dictate the actual scan
                source.seek(boundingFiRange, EMPTY_CFS, false);
                scanned++;
                DatawaveFieldIndexCachingIteratorJexl.this.scannedKeys.incrementAndGet();
                
                // if this is a range iterator, build the composite-safe Fi range
                Range compositeSafeFiRange = (this instanceof DatawaveFieldIndexRangeIteratorJexl) ? ((DatawaveFieldIndexRangeIteratorJexl) this)
                                .buildCompositeSafeFiRange(fiRow, fiName, fieldValue) : null;
                
                while (source.hasTop()) {
                    checkTiming();
                    
                    Key top = source.getTopKey();
                    
                    // if we are setup for composite seeking, seek if we are out of range
                    if (compositeSeeker != null && compositeSafeFiRange != null) {
                        String colQual = top.getColumnQualifier().toString();
                        String ingestType = colQual.substring(colQual.indexOf('\0') + 1, colQual.lastIndexOf('\0'));
                        String colFam = top.getColumnFamily().toString();
                        String fieldName = colFam.substring(colFam.indexOf('\0') + 1);
                        
                        Collection<String> componentFields = null;
                        String separator = null;
                        Multimap<String,String> compositeToFieldMap = compositeMetadata.getCompositeFieldMapByType().get(ingestType);
                        Map<String,String> compositeSeparatorMap = compositeMetadata.getCompositeFieldSeparatorsByType().get(ingestType);
                        if (compositeToFieldMap != null && compositeSeparatorMap != null) {
                            componentFields = compositeToFieldMap.get(fieldName);
                            separator = compositeSeparatorMap.get(fieldName);
                        }
                        
                        if (componentFields != null && separator != null && !compositeSeeker.isKeyInRange(top, compositeSafeFiRange, separator)) {
                            boolean shouldSeek = false;
                            
                            // top key precedes nextSeekKey
                            if (nextSeekKey != null && top.compareTo(nextSeekKey) < 0) {
                                // if we hit the seek threshold, seek
                                if (nextCount >= compositeSeekThreshold)
                                    shouldSeek = true;
                            }
                            // top key exceeds nextSeekKey, or nextSeekKey unset
                            else {
                                nextCount = 0;
                                nextSeekKey = null;
                                
                                // get a new seek key
                                Key newStartKey = compositeSeeker.nextSeekKey(new ArrayList<>(componentFields), top, compositeSafeFiRange, separator);
                                if (newStartKey != boundingFiRange.getStartKey() && newStartKey.compareTo(boundingFiRange.getStartKey()) > 0
                                                && newStartKey.compareTo(boundingFiRange.getEndKey()) <= 0) {
                                    nextSeekKey = newStartKey;
                                    
                                    // if we hit the seek threshold (i.e. if it is set to 0), seek
                                    if (nextCount >= compositeSeekThreshold)
                                        shouldSeek = true;
                                }
                            }
                            
                            if (shouldSeek) {
                                source.seek(new Range(nextSeekKey, boundingFiRange.isStartKeyInclusive(), boundingFiRange.getEndKey(), boundingFiRange
                                                .isEndKeyInclusive()), EMPTY_CFS, false);
                                
                                // reset next count and seek key
                                nextSeekKey = null;
                                nextCount = 0;
                            } else {
                                nextCount++;
                                source.next();
                            }
                            
                            scanned++;
                            continue;
                        }
                    }
                    
                    // terminate if timed out or cancelled
                    if (DatawaveFieldIndexCachingIteratorJexl.this.setControl.isCancelledQuery()) {
                        break;
                    }
                    
                    if (addKey(top, source.getTopValue())) {
                        matched++;
                    }
                    
                    source.next();
                    scanned++;
                    DatawaveFieldIndexCachingIteratorJexl.this.scannedKeys.incrementAndGet();
                }
            } catch (Exception e) {
                // throw the exception up which will be available via the Future
                log.error("Failed to complete fillSet(" + boundingFiRange + ")", e);
                throw new RuntimeException(e);
            } finally {
                // return the ivarator source back to the pool.
                returnPoolSource(source);
                if (log.isDebugEnabled()) {
                    StringBuilder builder = new StringBuilder();
                    builder.append("Matched ").append(matched).append(" out of ").append(scanned).append(" for ").append(boundingFiRange).append(": ")
                                    .append(DatawaveFieldIndexCachingIteratorJexl.this);
                    log.debug(builder.toString());
                }
                if (collectTimingDetails && querySpanCollector != null && querySpan != null) {
                    querySpanCollector.addQuerySpan(querySpan);
                }
            }
        };
        
        return IteratorThreadPoolManager.executeIvarator(runnable, DatawaveFieldIndexCachingIteratorJexl.this + " in " + boundingFiRange);
        
    }
    
    /**
     * Get the unique directory for a specific row
     * 
     * @param row
     * @return the unique dir
     */
    protected Path getRowDir(String row) {
        return new Path(this.uniqueDir, row);
    }
    
    /**
     * Clear out the current row based hdfs backed set
     * 
     * @throws IOException
     */
    protected void clearRowBasedHdfsBackedSet() throws IOException {
        this.keys = null;
        this.currentRow = null;
        this.set = null;
    }
    
    /**
     * This will setup the set for the specified range. This will attempt to reuse precomputed and persisted sets if we are allowed to.
     * 
     * @param row
     * @throws IOException
     */
    protected void setupRowBasedHdfsBackedSet(String row) throws IOException {
        // we are done if cancelled
        if (this.setControl.isCancelledQuery()) {
            return;
        }
        
        try {
            // get the row specific dir
            Path rowDir = getRowDir(row);
            
            // if we are not allowing reuse of directories, then delete it
            if (!allowDirReuse && this.fs.exists(rowDir)) {
                this.fs.delete(rowDir, true);
            }
            
            // ensure the directory is created
            if (!this.fs.exists(rowDir)) {
                this.fs.mkdirs(rowDir);
                this.createdRowDir = true;
            } else {
                this.createdRowDir = false;
            }
            
            this.set = new HdfsBackedSortedSet<Key>(null, hdfsBackedSetBufferSize, fs, rowDir, maxOpenFiles, new FileKeySortedSet.Factory());
            this.threadSafeSet = Collections.synchronizedSortedSet(this.set);
            this.currentRow = row;
            this.setControl.takeOwnership(row, this);
            
            // if this set is not marked as complete (meaning completely filled AND persisted), then we cannot trust the contents and we need to recompute.
            if (!this.setControl.isCompleteAndPersisted(row)) {
                this.set.clear();
                this.keys = null;
            } else {
                this.keys = new CachingIterator<>(this.set.iterator());
            }
            
            // reset the keyValues counter as we have a new set here
            scannedKeys.set(0);
        } catch (IOException ioe) {
            throw new IllegalStateException("Unable to create Hdfs backed sorted set", ioe);
        }
    }
    
    /**
     * Build the bounding FI ranges. Normally this returns only one range, but it could return multiple (@see DatawaveFieldIndexRegex/Range/ListIteratorJexl
     * superclasses). If multiple are returned, then they must be sorted. These ranges are expected to be exclusively in the field index!
     * 
     * @param rowId
     * @return
     */
    @SuppressWarnings("hiding")
    protected abstract List<Range> buildBoundingFiRanges(Text rowId, Text fiName, Text fieldValue);
    
    /**
     * Does the last range seeked contain the passed in range
     * 
     * @param r
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
        log.trace("moveToNextRow()");
        
        QuerySpan querySpan = null;
        
        try {
            // this will block until an ivarator source becomes available
            final SortedKeyValueIterator<Key,Value> source = takePoolSource();
            
            try {
                
                if (collectTimingDetails && source instanceof SourceTrackingIterator) {
                    querySpan = ((SourceTrackingIterator) source).getQuerySpan();
                }
                
                // Make sure the source iterator's key didn't seek past the end
                // of our starting row and get into the next row. It can happen if your
                // fi keys are on a row boundary.
                if (lastRangeSeeked.getEndKey() != null && !lastRangeSeeked.contains(new Key(this.fiRow).followingKey(PartialKey.ROW))) {
                    fiRow = null;
                } else {
                    Range followingRowRange = new Range(new Key(this.fiRow).followingKey(PartialKey.ROW), true, lastRangeSeeked.getEndKey(),
                                    lastRangeSeeked.isEndKeyInclusive());
                    if (log.isTraceEnabled()) {
                        log.trace("moveToNextRow(Key k), followingRowRange: " + followingRowRange);
                    }
                    // do an initial seek to determine the next row (needed to calculate bounding FI ranges below)
                    source.seek(followingRowRange, EMPTY_CFS, false);
                    scannedKeys.incrementAndGet();
                    if (source.hasTop()) {
                        fiRow = source.getTopKey().getRow();
                    } else {
                        fiRow = null;
                    }
                }
                
            } finally {
                returnPoolSource(source);
            }
            
            if (log.isTraceEnabled()) {
                log.trace("moveToNextRow, nextRow: " + fiRow);
            }
            
            // The boundingFiRange is used to test that we have the right fieldName->fieldValue pairing.
            boundingFiRanges.clear();
            if (fiRow != null) {
                boundingFiRanges.addAll(this.buildBoundingFiRanges(fiRow, fiName, fieldValue));
                
                if (log.isTraceEnabled()) {
                    log.trace("findTop() boundingFiRange: " + boundingFiRanges);
                }
            }
        } finally {
            if (collectTimingDetails && querySpanCollector != null && querySpan != null) {
                this.querySpanCollector.addQuerySpan(querySpan);
            }
        }
        return fiRow;
    }
    
    /**
     * Does this key match. Note we are not overriding the super.isMatchingKey() as we need that to work as is NOTE: This method must be thread safe
     * 
     * @param k
     * @return
     */
    protected abstract boolean matches(Key k) throws IOException;
    
    /**
     * A protected method to force persistence of the set. This can be used by test cases to verify tear down and rebuilding with reuse of the previous results.
     * 
     * @throws IOException
     */
    protected void forcePersistence() throws IOException {
        if (this.set != null && !this.set.isPersisted()) {
            this.set.persist();
            // declare the persisted set complete
            this.setControl.setCompleteAndPersisted(this.currentRow);
        }
    }
    
    public class HdfsBackedControl {
        public static final String OWNERSHIP_FILE = "ownership";
        public static final String COMPLETE_FILE = "complete";
        
        // cancelled check interval is 1 minute
        public static final int CANCELLED_CHECK_INTERVAL = 1000 * 60;
        private volatile long lastCancelledCheck = System.currentTimeMillis() - new Random().nextInt(CANCELLED_CHECK_INTERVAL);
        private volatile boolean cancelled = false;
        
        private final int bufferSize = 128;
        
        protected Path getOwnershipFile(String row) {
            return new Path(getRowDir(row), OWNERSHIP_FILE);
        }
        
        protected Path getCompleteFile(String row) {
            return new Path(getRowDir(row), COMPLETE_FILE);
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
            if (fs.exists(file)) {
                return hasContents(file, ownerId);
            }
            return false;
        }
        
        private boolean hasContents(Path file, byte[] contents) throws IOException {
            FSDataInputStream stream = fs.open(file, bufferSize);
            int len;
            byte[] buffer;
            try {
                buffer = new byte[bufferSize];
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
            return fs.exists(file);
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
            
            while (!done && count < 3) {
                count++;
                try {
                    FSDataOutputStream stream = null;
                    if (append) {
                        try {
                            stream = fs.append(file, bufferSize);
                        } catch (IOException ioe) {
                            if (ioe.getMessage().equals("Not supported")) {
                                stream = fs.create(file, true, bufferSize);
                            } else {
                                throw ioe;
                            }
                        }
                    } else {
                        stream = fs.create(file, true, bufferSize);
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
                        if (fs.exists(DatawaveFieldIndexCachingIteratorJexl.this.uniqueDir)) {
                            // so the directory exists, try the row dir
                            if (fs.exists(getRowDir(currentRow))) {
                                // so the directory exists, how about the file
                                if (fs.exists(file)) {
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
                            count = 3;
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
    
    public void setQuerySpanCollector(QuerySpanCollector querySpanCollector) {
        this.querySpanCollector = querySpanCollector;
    }
}
