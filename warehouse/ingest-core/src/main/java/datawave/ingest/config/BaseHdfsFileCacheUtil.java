package datawave.ingest.config;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public abstract class BaseHdfsFileCacheUtil {
    
    protected Path cacheFilePath;
    protected final Configuration conf;
    protected AccumuloHelper accumuloHelper;
    private static String delimiter = "\t";
    
    private static final Logger log = Logger.getLogger(BaseHdfsFileCacheUtil.class);
    
    public BaseHdfsFileCacheUtil(Configuration conf) {
        Validate.notNull(conf, "Configuration object passed in null");
        this.conf = conf;
        this.cacheFilePath = getCacheFilePath(conf);
    }
    
    protected abstract Path getCacheFilePath(Configuration conf);
    
    public void read() throws Exception {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(FileSystem.get(this.cacheFilePath.toUri(), conf).open(this.cacheFilePath)))) {
            readCache(in, "\t");
        } catch (IOException ex) {
            
        }
        
    }
    
    public void write() {
        
    }
    
    public void update() {
        
    }
    
    protected void initAccumuloHelper() {
        if (accumuloHelper == null) {
            accumuloHelper = new AccumuloHelper();
            accumuloHelper.setup(conf);
        }
    }
    
    public void createCacheFile(FileSystem fs, Path tmpCacheFile) {
        try {
            fs.delete(this.cacheFilePath, false);
            if (!fs.rename(tmpCacheFile, this.cacheFilePath)) {
                throw new IOException("Failed to rename temporary cache file");
            }
            
        } catch (Exception e) {
            log.warn("Unable to rename " + tmpCacheFile + " to " + this.cacheFilePath + "probably because somebody else replaced it ", e);
            try {
                fs.delete(tmpCacheFile, false);
            } catch (Exception e2) {
                log.error("Unable to clean up " + tmpCacheFile, e2);
            }
        }
        log.info("Updated " + cacheFilePath);
        
    }
    
    protected void readCache(BufferedReader in, String delimiter) throws IOException {
        String line;
        while ((line = in.readLine()) != null) {
            String[] parts = StringUtils.split(line, delimiter);
            if (parts.length == 2) {
                conf.set(parts[0], parts[1]);
            }
        }
        in.close();
    }
    
    public Path createTempFile(FileSystem fs) throws IOException {
        int count = 1;
        Path tmpCacheFile = null;
        try {
            do {
                Path parentDirectory = this.cacheFilePath.getParent();
                String fileName = this.cacheFilePath.getName() + "." + count;
                log.info("Attempting to create " + fileName + "under " + parentDirectory);
                tmpCacheFile = new Path(parentDirectory, fileName);
                count++;
            } while (!fs.createNewFile(tmpCacheFile));
        } catch (IOException ex) {
            throw new IOException("Could not create temp cache file", ex);
        }
        return tmpCacheFile;
    }
    
    protected abstract boolean shouldRefreshCache(Configuration conf);
}
