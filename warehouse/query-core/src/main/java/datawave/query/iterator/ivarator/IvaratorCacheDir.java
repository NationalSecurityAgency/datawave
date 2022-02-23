package datawave.query.iterator.ivarator;

import org.apache.hadoop.fs.FileSystem;

public class IvaratorCacheDir {
    
    final protected IvaratorCacheDirConfig config;
    
    // the filesystem to use for this ivarator cache dir
    final protected FileSystem fs;
    
    // the path for caching ivarator output for this query
    final protected String pathURI;
    
    public IvaratorCacheDir(IvaratorCacheDirConfig config, FileSystem fs, String pathURI) {
        this.config = config;
        this.fs = fs;
        this.pathURI = pathURI;
    }
    
    public IvaratorCacheDirConfig getConfig() {
        return config;
    }
    
    public FileSystem getFs() {
        return fs;
    }
    
    public String getPathURI() {
        return pathURI;
    }
}
