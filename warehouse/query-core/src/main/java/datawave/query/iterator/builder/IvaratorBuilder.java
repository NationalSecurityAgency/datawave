package datawave.query.iterator.builder;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import datawave.core.iterators.querylock.QueryLock;
import datawave.query.composite.CompositeMetadata;
import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.profile.QuerySpanCollector;
import datawave.query.util.sortedset.FileSortedSet;

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
    protected FileSortedSet.PersistOptions ivaratorPersistOptions = new FileSortedSet.PersistOptions();
    protected boolean collectTimingDetails = false;
    protected QuerySpanCollector querySpanCollector = null;
    protected CompositeMetadata compositeMetadata;
    protected int compositeSeekThreshold;
    protected GenericObjectPool<SortedKeyValueIterator<Key,Value>> ivaratorSourcePool;

    protected void validateIvaratorControlDir(IvaratorCacheDir ivaratorCacheDir) {
        String ivaratorCacheDirURI = ivaratorCacheDir.getPathURI();
        FileSystem fileSystem = ivaratorCacheDir.getFs();

        try {
            final Path cachePath = new Path(new URI(ivaratorCacheDirURI));
            // get the parent directory
            final Path parentCachePath = cachePath.getParent();
            final URI parentURI = parentCachePath.toUri();
            if (!fileSystem.exists(parentCachePath)) {
                // being able to make the parent directory is proof enough
                fileSystem.mkdirs(parentCachePath);
            } else if (fileSystem.getFileStatus(parentCachePath).isFile()) {
                throw new IOException(parentCachePath + " exists but is a file.  Expecting directory");
            } else if (parentURI.getScheme().equals("file")) {
                File parent = new File(parentURI.getPath());
                if (!parent.canWrite() || !parent.canRead()) {
                    throw new IllegalStateException("Invalid permissions to directory " + parentCachePath);
                }
            }
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Invalid ivarator configuration: " + ivaratorCacheDirURI, e);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create file system", e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Invalid cache dir URI: " + ivaratorCacheDirURI, e);
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

    public FileSortedSet.PersistOptions getIvaratorPersistOptions() {
        return ivaratorPersistOptions;
    }

    public void setIvaratorPersistOptions(FileSortedSet.PersistOptions ivaratorPersistOptions) {
        this.ivaratorPersistOptions = ivaratorPersistOptions;
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
