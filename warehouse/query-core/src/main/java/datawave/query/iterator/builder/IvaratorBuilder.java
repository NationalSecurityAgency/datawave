package datawave.query.iterator.builder;

import datawave.core.iterators.querylock.QueryLock;
import datawave.query.composite.CompositeMetadata;
import datawave.query.iterator.profile.QuerySpanCollector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.fs.FileSystem;

/**
 * A base class used to build ivarators
 */
public abstract class IvaratorBuilder extends IndexIteratorBuilder {
    
    protected FileSystem hdfsFileSystem;
    protected String hdfsFileCompressionCodec;
    protected QueryLock queryLock;
    protected String ivaratorCacheDirURI;
    protected long ivaratorCacheScanPersistThreshold = 100000L;
    protected long ivaratorCacheScanTimeout = 1000L * 60 * 60;
    protected int ivaratorCacheBufferSize = 10000;
    protected int maxRangeSplit = 11;
    protected int ivaratorMaxOpenFiles = 100;
    protected boolean collectTimingDetails = false;
    protected QuerySpanCollector querySpanCollector = null;
    protected CompositeMetadata compositeMetadata;
    protected int compositeSeekThreshold;
    protected GenericObjectPool<SortedKeyValueIterator<Key,Value>> ivaratorSourcePool;
    
    public FileSystem getHdfsFileSystem() {
        return hdfsFileSystem;
    }
    
    public void setHdfsFileSystem(FileSystem hdfsFileSystem) {
        this.hdfsFileSystem = hdfsFileSystem;
    }
    
    public String getHdfsFileCompressionCodec() {
        return hdfsFileCompressionCodec;
    }
    
    public void setHdfsFileCompressionCodec(String hdfsFileCompressionCodec) {
        this.hdfsFileCompressionCodec = hdfsFileCompressionCodec;
    }
    
    public QueryLock getQueryLock() {
        return queryLock;
    }
    
    public void setQueryLock(QueryLock queryLock) {
        this.queryLock = queryLock;
    }
    
    public String getIvaratorCacheDirURI() {
        return ivaratorCacheDirURI;
    }
    
    public void setIvaratorCacheDirURI(String ivaratorCacheDirURI) {
        this.ivaratorCacheDirURI = ivaratorCacheDirURI;
    }
    
    public int getIvaratorCacheBufferSize() {
        return ivaratorCacheBufferSize;
    }
    
    public void setIvaratorCacheBufferSize(int ivaratorCacheBufferSize) {
        this.ivaratorCacheBufferSize = ivaratorCacheBufferSize;
    }
    
    public long getIvaratorCacheScanPersistThreshold() {
        return ivaratorCacheScanPersistThreshold;
    }
    
    public void setIvaratorCacheScanPersistThreshold(long ivaratorCacheScanPersistThreshold) {
        this.ivaratorCacheScanPersistThreshold = ivaratorCacheScanPersistThreshold;
    }
    
    public long getIvaratorCacheScanTimeout() {
        return ivaratorCacheScanTimeout;
    }
    
    public void setIvaratorCacheScanTimeout(long ivaratorCacheScanTimeout) {
        this.ivaratorCacheScanTimeout = ivaratorCacheScanTimeout;
    }
    
    public int getMaxRangeSplit() {
        return maxRangeSplit;
    }
    
    public void setMaxRangeSplit(int maxRangeSplit) {
        this.maxRangeSplit = maxRangeSplit;
    }
    
    public int getIvaratorMaxOpenFiles() {
        return ivaratorMaxOpenFiles;
    }
    
    public void setIvaratorMaxOpenFiles(int ivaratorMaxOpenFiles) {
        this.ivaratorMaxOpenFiles = ivaratorMaxOpenFiles;
    }
    
    public void setCollectTimingDetails(boolean collectTimingDetails) {
        this.collectTimingDetails = collectTimingDetails;
    }
    
    public void setQuerySpanCollector(QuerySpanCollector querySpanCollector) {
        this.querySpanCollector = querySpanCollector;
    }
    
    public CompositeMetadata getCompositeMetadata() {
        return compositeMetadata;
    }
    
    public void setCompositeMetadata(CompositeMetadata compositeMetadata) {
        this.compositeMetadata = compositeMetadata;
    }
    
    public int getCompositeSeekThreshold() {
        return compositeSeekThreshold;
    }
    
    public void setCompositeSeekThreshold(int compositeSeekThreshold) {
        this.compositeSeekThreshold = compositeSeekThreshold;
    }
    
    public GenericObjectPool<SortedKeyValueIterator<Key,Value>> getIvaratorSourcePool() {
        return ivaratorSourcePool;
    }
    
    public void setIvaratorSourcePool(GenericObjectPool<SortedKeyValueIterator<Key,Value>> ivaratorSourcePool) {
        this.ivaratorSourcePool = ivaratorSourcePool;
    }
}
