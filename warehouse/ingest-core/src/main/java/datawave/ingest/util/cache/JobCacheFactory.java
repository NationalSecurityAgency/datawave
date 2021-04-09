package datawave.ingest.util.cache;

import com.google.common.collect.Maps;
import datawave.common.io.FilesFinder;
import datawave.ingest.util.cache.converter.CacheLockConverter;
import datawave.ingest.util.cache.delete.DeleteJobCache;
import datawave.ingest.util.cache.lease.JobCacheLockFactory;
import datawave.ingest.util.cache.load.LoadJobCache;
import datawave.ingest.util.cache.path.FileSystemPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.util.Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** The JobCacheFactory will expose the basic functionality to interact and use the job cache */
public class JobCacheFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobCacheFactory.class);
    public static final Comparator<Path> PATH_COMPARATOR = Comparator.comparing(x -> x.toUri().getPath());
    public static final String DW_JOB_CACHE_TIMESTAMP_DIR_PREFIX = "datawave.job.cache.timestamp.dir.prefix";
    public static final String DW_JOB_CACHE_TIMESTAMP_PATTERN = "datawave.job.cache.timestamp.pattern";
    public static final String DW_JOB_CACHE_BASE_PATH = "datawave.job.cache.base.path";
    public static final String DW_JOB_CACHE_LOCK_FACTORY_MODE = "datawave.job.cache.lock.factory.mode";
    
    private final FileSystemPath jobCacheBasePath;
    private final Pattern timestampPattern;
    private final JobCacheLockFactory lockFactory;
    private final String timestampDirPrefix;
    private final Map<String,String> jobIdToCacheId = Maps.newHashMap();
    
    public JobCacheFactory(Configuration conf) throws IOException {
        Path basePath = new Path(Args.notNull(conf.get(DW_JOB_CACHE_BASE_PATH), DW_JOB_CACHE_BASE_PATH));
        jobCacheBasePath = new FileSystemPath(basePath.getFileSystem(conf), basePath);
        
        timestampPattern = Pattern.compile(Args.notNull(conf.get(DW_JOB_CACHE_TIMESTAMP_PATTERN), DW_JOB_CACHE_TIMESTAMP_PATTERN));
        timestampDirPrefix = Args.notNull(conf.get(DW_JOB_CACHE_TIMESTAMP_DIR_PREFIX), DW_JOB_CACHE_TIMESTAMP_DIR_PREFIX);
        
        String lockFactoryMode = Args.notNull(conf.get(DW_JOB_CACHE_LOCK_FACTORY_MODE), DW_JOB_CACHE_LOCK_FACTORY_MODE);
        lockFactory = new CacheLockConverter().convert(lockFactoryMode);
        lockFactory.init(conf);
    }
    
    /**
     * Will attempt to find the most recent unlocked job cache.
     *
     * @param jobId
     *            The unique identifier for the job.
     * @return The path of the most recent active job cache.
     * @throws Exception
     *             if no unlocked cache is found.
     */
    public Path getJobCache(String jobId) throws Exception {
        for (FileSystemPath jobCache : getCacheCandidates(jobCacheBasePath, timestampPattern, 0)) {
            String lockIdPath = getLockIdPath(jobCache.getOutputPath().getName(), jobId);
            if (lockFactory.acquireLock(lockIdPath)) {
                jobIdToCacheId.put(jobId, lockIdPath);
                LOGGER.info("{} will be assigned to cache {}", jobId, lockIdPath);
                return jobCache.getOutputPath();
            }
        }
        throw new Exception("Unable to find an active cache in " + jobCacheBasePath.getOutputPath());
    }
    
    /**
     * Will attempt to release the lock associated with this job id.
     *
     * @param jobId
     *            The unique identifier for the job.
     * @return Whether the release of the lease was successful.
     */
    public boolean releaseId(String jobId) {
        return jobIdToCacheId.containsKey(jobId) && lockFactory.releaseLock(jobIdToCacheId.get(jobId));
    }
    
    /**
     * Will load a new job cache in the specified paths.
     *
     * @param baseCachePaths
     *            The base cache paths for each active system.
     * @param filesToLoad
     *            The files to load to the cache.
     * @param cacheReplicationCnt
     *            The replication factor.
     * @param executorThreads
     *            The number of executor threads.
     */
    public void loadNewJobCache(Collection<FileSystemPath> baseCachePaths, Collection<String> filesToLoad, short cacheReplicationCnt, int executorThreads) {
        LoadJobCache.load(baseCachePaths, filesToLoad, true, cacheReplicationCnt, executorThreads, LoadJobCache.getJobCacheTimestampDir(timestampDirPrefix), "");
    }
    
    /**
     * Will attempt to delete the cachePath specified if it is not active or locked.
     *
     * @param cachePath
     *            An object that holds the cache path and its associated file system.
     */
    public void deleteJobCache(FileSystemPath cachePath) {
        DeleteJobCache.deleteCacheIfNotActive(Collections.singleton(cachePath), lockFactory);
    }
    
    /**
     * Find all the job cache's status under the system's base cache path
     *
     * @param baseCachePath
     *            An object that contains the job cache base path and its associated file system.
     * @return A collection objects representing the status for each job cache.
     */
    public Collection<JobCacheStatus> getJobCacheStatuses(FileSystemPath baseCachePath) {
        // @formatter:off
        return getCacheCandidates(baseCachePath, timestampPattern, 0)
                .stream()
                .map(this::getJobCacheStatus)
                .collect(Collectors.toList());
        // @formatter:on
    }
    
    /**
     * Will attempt to release all locks for each cache id stored.
     */
    public void close() {
        jobIdToCacheId.clear();
        lockFactory.close();
    }
    
    /**
     * Find job cache paths that pass the input criteria.
     *
     * @param cacheBasePath
     *            An object that contains the job cache base path and its associated file system.
     * @param pattern
     *            A pattern that will be used to filter job cache paths.
     * @param keepNumVersions
     *            The number of versions to skip(this is used primarily when deleting)
     * @return A collection of job cache paths sorted in reverse order.
     */
    public static Collection<FileSystemPath> getCacheCandidates(FileSystemPath cacheBasePath, Pattern pattern, int keepNumVersions) {
        Path cachePath = cacheBasePath.getOutputPath();
        FileSystem fileSystem = cacheBasePath.getFileSystem();
        
        FileStatus[] pathStatus = FilesFinder.listPath(fileSystem, cachePath, pattern);
        
        // @formatter:off
        return Arrays
                .stream(pathStatus)
                .filter(FileStatus::isDirectory)
                .map(FileStatus::getPath)
                .sorted(PATH_COMPARATOR.reversed())
                .skip(keepNumVersions)
                .map(deleteCachePath -> new FileSystemPath(fileSystem, deleteCachePath))
                .collect(Collectors.toList());
        // @formatter:on
    }
    
    JobCacheLockFactory getLockFactory() {
        return lockFactory;
    }
    
    /**
     * Find the job cache status for this job cache path
     *
     * @param cachePath
     *            An object that contains the job cache path and its associated file system.
     * @return An object that represents the status of the job cache.
     */
    private JobCacheStatus getJobCacheStatus(FileSystemPath cachePath) {
        String lockIdPath = getLockIdPath(cachePath.getOutputPath().getName());
        return new JobCacheStatus(cachePath, lockFactory.getCacheStatus(lockIdPath));
    }
    
    /**
     * Form the lock id by joining the passed in array with file.separator as the delimiter.
     *
     * @param lockIdElements
     *            An array of lock id components that when joined form the lock id.
     * @return A String representing the lock id.
     */
    String getLockIdPath(String... lockIdElements) {
        StringJoiner joiner = new StringJoiner(File.separator);
        Arrays.stream(lockIdElements).forEach(joiner::add);
        return joiner.toString();
    }
}
