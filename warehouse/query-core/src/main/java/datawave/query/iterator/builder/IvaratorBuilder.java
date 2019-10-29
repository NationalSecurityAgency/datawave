package datawave.query.iterator.builder;

import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.core.iterators.querylock.QueryLock;
import datawave.query.composite.CompositeMetadata;
import datawave.query.iterator.profile.QuerySpanCollector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * A base class used to build ivarators
 */
public abstract class IvaratorBuilder extends IndexIteratorBuilder {
    
    protected List<IvaratorCacheDir> ivaratorCacheDirs;
    protected String hdfsFileCompressionCodec;
    protected QueryLock queryLock;
    protected long ivaratorCacheScanPersistThreshold = 100000L;
    protected long ivaratorCacheScanTimeout = 1000L * 60 * 60;
    protected int ivaratorCacheBufferSize = 10000;
    protected int maxRangeSplit = 11;
    protected int ivaratorMaxOpenFiles = 100;
    protected long maxIvaratorResults = -1;
    protected int ivaratorNumRetries = 2;
    protected boolean collectTimingDetails = false;
    protected QuerySpanCollector querySpanCollector = null;
    protected CompositeMetadata compositeMetadata;
    protected int compositeSeekThreshold;
    
    protected void validateIvaratorCacheDirs(List<IvaratorCacheDir> ivaratorCacheDirs) {
        if (ivaratorCacheDirs.isEmpty())
            throw new IllegalStateException("No ivarator cache dirs defined");
        
        IvaratorCacheDir ivaratorCacheDir = ivaratorCacheDirs.get(0);
        
        String ivaratorCacheDirURI = ivaratorCacheDir.getPathURI();
        FileSystem hdfsFileSystem = ivaratorCacheDir.getFs();
        
        final URI hdfsCacheURI;
        try {
            hdfsCacheURI = new URI(ivaratorCacheDirURI);
            hdfsFileSystem.mkdirs(new Path(hdfsCacheURI));
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Unable to load hadoop configuration", e);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create hadoop file system", e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Invalid hdfs cache dir URI: " + ivaratorCacheDirURI, e);
        }
    }
    
    public List<IvaratorCacheDir> getIvaratorCacheDirs() {
        return ivaratorCacheDirs;
    }
    
    public void setIvaratorCacheDirs(List<IvaratorCacheDir> ivaratorCacheDirs) {
        this.ivaratorCacheDirs = ivaratorCacheDirs;
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
    
    public long getMaxIvaratorResults() {
        return maxIvaratorResults;
    }
    
    public void setMaxIvaratorResults(long maxIvaratorResults) {
        this.maxIvaratorResults = maxIvaratorResults;
    }
    
    public int getIvaratorNumRetries() {
        return ivaratorNumRetries;
    }
    
    public void setIvaratorNumRetries(int ivaratorNumRetries) {
        this.ivaratorNumRetries = ivaratorNumRetries;
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
}
