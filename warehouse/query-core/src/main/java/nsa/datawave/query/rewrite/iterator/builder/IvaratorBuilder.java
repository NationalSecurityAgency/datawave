package nsa.datawave.query.rewrite.iterator.builder;

import nsa.datawave.query.rewrite.iterator.profile.QuerySpanCollector;
import org.apache.hadoop.fs.FileSystem;

/**
 * A base class used to build ivarators
 */
public abstract class IvaratorBuilder extends IndexIteratorBuilder {
    
    protected FileSystem hdfsFileSystem;
    protected String hdfsCacheDirURI;
    protected long hdfsCacheScanPersistThreshold = 100000L;
    protected long hdfsCacheScanTimeout = 1000L * 60 * 60;
    protected int hdfsCacheBufferSize = 10000;
    protected boolean hdfsCacheReused = true;
    protected String hdfsFileCompressionCodec;
    protected int maxRangeSplit = 11;
    protected boolean collectTimingDetails = false;
    protected QuerySpanCollector querySpanCollector = null;
    
    public FileSystem getHdfsFileSystem() {
        return hdfsFileSystem;
    }
    
    public void setHdfsFileSystem(FileSystem hdfsFileSystem) {
        this.hdfsFileSystem = hdfsFileSystem;
    }
    
    public String getHdfsCacheDirURI() {
        return hdfsCacheDirURI;
    }
    
    public void setHdfsCacheDirURI(String hdfsCacheDirURI) {
        this.hdfsCacheDirURI = hdfsCacheDirURI;
    }
    
    public int getHdfsCacheBufferSize() {
        return hdfsCacheBufferSize;
    }
    
    public void setHdfsCacheBufferSize(int hdfsCacheBufferSize) {
        this.hdfsCacheBufferSize = hdfsCacheBufferSize;
    }
    
    public long getHdfsCacheScanPersistThreshold() {
        return hdfsCacheScanPersistThreshold;
    }
    
    public void setHdfsCacheScanPersistThreshold(long hdfsCacheScanPersistThreshold) {
        this.hdfsCacheScanPersistThreshold = hdfsCacheScanPersistThreshold;
    }
    
    public long getHdfsCacheScanTimeout() {
        return hdfsCacheScanTimeout;
    }
    
    public void setHdfsCacheScanTimeout(long hdfsCacheScanTimeout) {
        this.hdfsCacheScanTimeout = hdfsCacheScanTimeout;
    }
    
    public String getHdfsFileCompressionCodec() {
        return hdfsFileCompressionCodec;
    }
    
    public void setHdfsFileCompressionCodec(String hdfsFileCompressionCodec) {
        this.hdfsFileCompressionCodec = hdfsFileCompressionCodec;
    }
    
    public boolean isHdfsCacheReused() {
        return hdfsCacheReused;
    }
    
    public void setHdfsCacheReused(boolean hdfsCacheReused) {
        this.hdfsCacheReused = hdfsCacheReused;
    }
    
    public int getMaxRangeSplit() {
        return maxRangeSplit;
    }
    
    public void setMaxRangeSplit(int maxRangeSplit) {
        this.maxRangeSplit = maxRangeSplit;
    }
    
    public void setCollectTimingDetails(boolean collectTimingDetails) {
        this.collectTimingDetails = collectTimingDetails;
    }
    
    public void setQuerySpanCollector(QuerySpanCollector querySpanCollector) {
        this.querySpanCollector = querySpanCollector;
    }
}
