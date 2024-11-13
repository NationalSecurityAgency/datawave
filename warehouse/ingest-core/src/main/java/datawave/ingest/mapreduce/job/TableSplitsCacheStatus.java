package datawave.ingest.mapreduce.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class TableSplitsCacheStatus {
    // default timeout is 6 hours
    private static final long DEFAULT_CACHE_TIMEOUT = 21600000L;
    public static final String SPLITS_CACHE_TIMEOUT_MS = "datawave.ingest.splits.cache.timeout.ms";
    public static final Logger log = Logger.getLogger(TableSplitsCacheStatus.class);

    /**
     *
     * @param conf
     *            the configuration
     * @return if the cache is valid
     * @throws IOException
     *             for issues with read or write
     */
    public static boolean isCacheValid(Configuration conf) throws IOException {
        Path splitsPath = TableSplitsCache.getSplitsPath(conf);
        FileStatus fileStatus = null;
        try {
            fileStatus = FileSystem.get(splitsPath.toUri(), conf).getFileStatus(splitsPath);
        } catch (IOException ex) {
            log.warn("Could not get the FileStatus of the splits file " + splitsPath);
            return false;
        }
        long expirationTime = System.currentTimeMillis() - conf.getLong(SPLITS_CACHE_TIMEOUT_MS, DEFAULT_CACHE_TIMEOUT);
        boolean isFileAgeValid = fileStatus.getModificationTime() >= expirationTime;
        if (!isFileAgeValid) {
            log.warn("SplitsCache has expired " + splitsPath + " age: " + fileStatus.getModificationTime() + " expiration: " + expirationTime);
        }
        return null != fileStatus && isFileAgeValid;
    }

}
