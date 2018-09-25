package datawave.ingest.mapreduce.job;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class MetadataTableSplitsCacheStatus {
    // default timeout is 6 hours
    private static final long DEFAULT_CACHE_TIMEOUT = 21600000L;
    public static final String SPLITS_CACHE_TIMEOUT_MS = "datawave.ingest.splits.cache.timeout.ms";
    public static final Logger log = Logger.getLogger(MetadataTableSplitsCacheStatus.class);
    
    /**
     * 
     * @param conf
     * @return
     */
    public static boolean isCacheValid(Configuration conf) throws IOException {
        Path splitsPath = MetadataTableSplits.getSplitsPath(conf);
        FileStatus fileStatus = null;
        try {
            fileStatus = FileSystem.get(conf).getFileStatus(splitsPath);
        } catch (IOException ex) {
            log.warn("Could not get the FileStatus of the splits file " + splitsPath.toString());
        }
        long expirationTime = System.currentTimeMillis() - conf.getLong(SPLITS_CACHE_TIMEOUT_MS, DEFAULT_CACHE_TIMEOUT);
        boolean isFileAgeValid = fileStatus.getModificationTime() >= expirationTime;
        if (!isFileAgeValid) {
            log.warn("SplitsCache has expired " + splitsPath.toString() + " age: " + fileStatus.getModificationTime() + " expiration: " + expirationTime);
        }
        return null != fileStatus && isFileAgeValid;
    }
    
}
