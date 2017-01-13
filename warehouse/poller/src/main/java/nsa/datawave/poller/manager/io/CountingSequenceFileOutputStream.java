package nsa.datawave.poller.manager.io;

import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class does nothing but look like a CountingOutputStream. It's used by the parent class for the getByteCount method. This hides all inherited versions of
 * the write method.
 */
public class CountingSequenceFileOutputStream extends CountingOutputStream {
    private static final Logger log = LoggerFactory.getLogger(CountingSequenceFileOutputStream.class);
    
    private static final CompressionCodec cc = new GzipCodec();
    private static final CompressionType ct = CompressionType.BLOCK;
    
    private final ReentrantReadWriteLock _writeSyncLock = new ReentrantReadWriteLock();
    private final AtomicBoolean _isSynched = new AtomicBoolean(true);
    private final AtomicBoolean _isClosed = new AtomicBoolean(false);
    
    private SequenceFile.Writer writer = null;
    private boolean haveEntries = false;
    private long byteCount = 0;
    private long records = 0;
    
    private long unsyncedByteCount = 0;
    private long unsyncedRecords = 0;
    
    public CountingSequenceFileOutputStream(final FileSystem fs, final JobContext job, final Path p, final File f, final Class<?> kClass, final Class<?> vClass)
                    throws IOException {
        super(new FileOutputStream(f));
        if (p == null)
            throw new IllegalArgumentException("Path cannot be null");
        
        writer = SequenceFile.createWriter(job.getConfiguration(), Writer.file(fs.makeQualified(p)), Writer.keyClass(kClass), Writer.valueClass(vClass),
                        Writer.compression(ct, cc));
        log.info("Created a writer of type: {}", writer.getClass().getName());
        log.info("Created a writer with compression codec: {}", writer.getCompressionCodec().getClass().getName());
        // record the header length
        byteCount = writer.getLength();
    }
    
    // used to create test instance
    protected CountingSequenceFileOutputStream(OutputStream out) {
        super(out);
    }
    
    @Override
    public long getByteCount() {
        return haveEntries ? (byteCount + unsyncedByteCount) : 0;
    }
    
    /**
     * Writes the given key/value pair to the SequenceFile.Writer.
     *
     * @param ctx
     *            Task Context
     * @param key
     *            Object key
     * @param val
     *            Object value
     * @throws IOException
     */
    /* Manages the Thread-Saftey and parameter checking. */
    public void write(final TaskInputOutputContext<?,?,?,?> ctx, final Object key, final Object val) throws IOException {
        if (key == null || val == null) {
            ctx.getCounter("Output", "Skipped").increment(1);
            return;
        }
        
        if (_isClosed.get())
            return;
        
        _writeSyncLock.readLock().lock();
        try {
            if (!_isClosed.get())
                handleWrite(ctx, key, val);
        } finally {
            _writeSyncLock.readLock().unlock();
        }
    }
    
    /**
     * This method will sync the underlying writer which results in flushing the data to the underlying stream. After a sync, the byteCount should be accurate
     * even if using block compression.
     *
     * @throws IOException
     */
    public void sync() throws IOException {
        if (_isClosed.get() || _isSynched.get())
            return;
        
        _writeSyncLock.writeLock().lock();
        try {
            if (!_isClosed.get() && _isSynched.compareAndSet(false, true))
                handleSync();
        } finally {
            _writeSyncLock.writeLock().unlock();
        }
    }
    
    /** Performs a sync and closes the delegate {@link SequenceFile.Writer} */
    @Override
    public void close() throws IOException {
        loggingSync();
        if (!_isClosed.compareAndSet(false, true))
            return;
        _writeSyncLock.writeLock().lock();
        
        try {
            log.trace("Closing writer");
            writer.close();
            super.close();
        } finally {
            _writeSyncLock.writeLock().unlock();
        }
    }
    
    /** Wraps the sync() method with a Try-Catch, logging a warning on any errors. */
    private void loggingSync() {
        try {
            sync();
        } catch (Exception ex) {
            log.warn("Error synchronizing the OutputStream", ex);
        }
    }
    
    /** Actual Write Implementation */
    protected void handleWrite(final TaskInputOutputContext<?,?,?,?> ctx, final Object key, final Object val) throws IOException {
        _isSynched.set(false);
        
        unsyncedRecords++;
        haveEntries = true;
        
        if (val instanceof Iterable<?>) {
            for (final Object v : (Iterable<?>) val) {
                log.trace("Appending record to writer");
                writer.append(key, v);
                ctx.getCounter("Output", "Records").increment(1);
            }
        } else {
            log.trace("Appending record to writer");
            writer.append(key, val);
            ctx.getCounter("Output", "Records").increment(1);
        }
        
        updateByteCount();
    }
    
    protected void updateByteCount() throws IOException {
        long newByteCount = writer.getLength();
        if (byteCount != newByteCount) {
            if (log.isTraceEnabled()) {
                log.info("New writer byte count: " + newByteCount);
            }
            byteCount = newByteCount;
            records += unsyncedRecords;
            unsyncedByteCount = 0;
            unsyncedRecords = 0;
        } else if (records > 0) {
            double bytesPerRecord = (double) byteCount / (double) records;
            unsyncedByteCount = (long) (bytesPerRecord * unsyncedRecords);
            if (log.isTraceEnabled()) {
                log.info("Estimating unsynced as " + bytesPerRecord + " input records: " + unsyncedByteCount);
            }
        } else {
            unsyncedByteCount = unsyncedRecords * 256;
            if (log.isTraceEnabled()) {
                log.info("Estimating unsynced as 256 byte input records: " + unsyncedByteCount);
            }
        }
    }
    
    protected void handleSync() throws IOException {
        log.trace("Syncing writer");
        writer.sync();
        updateByteCount();
    }
    
    @Override
    public int getCount() {
        return unsupported();
    }
    
    @Override
    public long resetByteCount() {
        return unsupported();
    }
    
    @Override
    public int resetCount() {
        return unsupported();
    }
    
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        unsupported();
    }
    
    @Override
    public void write(byte[] b) throws IOException {
        unsupported();
    }
    
    @Override
    public void write(int b) throws IOException {
        unsupported();
    }
    
    public int unsupported() {
        throw new UnsupportedOperationException("Not supported.");
    }
}
