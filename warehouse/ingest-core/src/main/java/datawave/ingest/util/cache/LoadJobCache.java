package datawave.ingest.util.cache;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static datawave.common.io.HadoopFileSystemUtils.getCopyFromLocalFileRunnable;

/** Loads hdfs cache from a collection of local files */
public class LoadJobCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadJobCache.class);
    private static final String LOADING_PREFIX = "LOADING_";
    
    /**
     * Create a timestamped hdfs directory and copy local files to it.
     *
     * @param pathToFileSystem
     *            A map of output paths to their file system
     * @param filesToLoad
     *            A collection of files to load cache
     * @param finalizeLoad
     *            Controls when to finalize cache by setting the replication and moving to its final path
     * @param cacheReplicationCnt
     *            The number of replicas to create for each cache file
     * @param executorThreads
     *            The number of threads to use to load the cache
     * @param timestampDir
     *            The timestamped directory for the cache
     * @param optionalSubDir
     *            Optional subdirectory to provide a more granular cache structure
     */
    public static void load(Map<Path,FileSystem> pathToFileSystem, Collection<String> filesToLoad, boolean finalizeLoad, short cacheReplicationCnt,
                    int executorThreads, String timestampDir, String optionalSubDir) {
        ExecutorService executorService = Executors.newFixedThreadPool(executorThreads);
        
        Consumer<Map.Entry<Path,FileSystem>> loadCache = getLoadCacheConsumer(filesToLoad, executorService, timestampDir, optionalSubDir, cacheReplicationCnt);
        Consumer<Map.Entry<Path,FileSystem>> finalizeCache = getFinalizeCacheConsumer(timestampDir);
        Consumer<Map.Entry<Path,FileSystem>> deleteCache = getDeleteCacheConsumer(timestampDir);
        
        try {
            load(pathToFileSystem, finalizeLoad, loadCache, finalizeCache, deleteCache);
        } finally {
            executorService.shutdown();
        }
        
    }
    
    /**
     * Create a timestamped hdfs directory and copy local files to it.
     *
     * @param pathToFileSystem
     *            A map of output paths to their file system
     * @param finalizeLoad
     *            A boolean to determine if the cache should be finalized
     * @param loadCache
     *            A Consumer method that will load the cache
     * @param finalizeCache
     *            A Consumer method that will finalize the cache
     * @param deleteCache
     *            A Consumer method that will delete the cache on error
     */
    public static void load(Map<Path,FileSystem> pathToFileSystem, boolean finalizeLoad, Consumer<Map.Entry<Path,FileSystem>> loadCache,
                    Consumer<Map.Entry<Path,FileSystem>> finalizeCache, Consumer<Map.Entry<Path,FileSystem>> deleteCache) {
        try {
            pathToFileSystem.entrySet().forEach(loadCache);
            LOGGER.info("Finished loading local files to cachhe for {}", pathToFileSystem.keySet());
            if (finalizeLoad) {
                pathToFileSystem.entrySet().forEach(finalizeCache);
                LOGGER.info("Finished finalizing {}", pathToFileSystem.keySet());
            }
        } catch (Exception e) {
            LOGGER.error("Unable to load job cache. Deleting temporary directories. ", e);
            pathToFileSystem.entrySet().forEach(deleteCache);
        }
    }
    
    /**
     * Create a consumer that will delete temporary cache artifacts upon error.
     *
     * @param timestampDir
     *            A timestamp directory that will reflect when the cache was loaded
     * @return A consumer that will delete the cache.
     */
    static Consumer<Map.Entry<Path,FileSystem>> getDeleteCacheConsumer(String timestampDir) {
        return (Map.Entry<Path,FileSystem> entrySet) -> {
            Path uploadWorkingDir = new Path(entrySet.getKey(), LOADING_PREFIX + timestampDir);
            FileSystem cacheFs = entrySet.getValue();
            
            try {
                cacheFs.delete(uploadWorkingDir, true);
            } catch (IOException e) {
                LOGGER.error("Unable to delete temporary directory {} ", uploadWorkingDir, e);
            }
        };
    }
    
    /**
     * Create a consumer that will finalize the cache directory.
     *
     * @param timestampDir
     *            A timestamp directory that will reflect when the cache was loaded
     * @return A consumer that will finalize the cache
     */
    static Consumer<Map.Entry<Path,FileSystem>> getFinalizeCacheConsumer(String timestampDir) {
        return (Map.Entry<Path,FileSystem> entrySet) -> {
            Path jobCacheDir = new Path(entrySet.getKey(), timestampDir);
            Path uploadWorkingDir = new Path(jobCacheDir.getParent(), LOADING_PREFIX + jobCacheDir.getName());
            FileSystem cacheFs = entrySet.getValue();
            
            try {
                cacheFs.rename(uploadWorkingDir, jobCacheDir);
            } catch (IOException e) {
                throw new RuntimeException("Unable to rename " + uploadWorkingDir + "  to " + jobCacheDir, e);
            }
        };
    }
    
    /**
     * Create a consumer that will load local files to the hdfs cache.
     *
     * @param filesToLoad
     *            A collection of local files to load.
     * @param executorService
     *            A executor service to help parallelize the upload
     * @param optionalSubDir
     *            An optional sub directory timestamp directory
     * @param cacheReplicationCnt
     *            The replication count for each file in the cache
     * @return A consumer that will load the cache.
     */
    static Consumer<Map.Entry<Path,FileSystem>> getLoadCacheConsumer(Collection<String> filesToLoad, ExecutorService executorService, String timestampDir,
                    String optionalSubDir, short cacheReplicationCnt) {
        return (Map.Entry<Path,FileSystem> entrySet) -> {
            Path jobCacheDir = new Path(entrySet.getKey(), timestampDir);
            Path uploadWorkingDir = new Path(jobCacheDir.getParent(), LOADING_PREFIX + jobCacheDir.getName());
            Path loadingDir = StringUtils.isBlank(optionalSubDir) ? uploadWorkingDir : new Path(uploadWorkingDir, optionalSubDir);
            FileSystem cacheFs = entrySet.getValue();
            
            try {
                cacheFs.mkdirs(loadingDir);
            } catch (IOException e) {
                throw new RuntimeException("Unable to make directory " + loadingDir, e);
            }
            
            // @formatter:off
            filesToLoad
                    .stream()
                    .map(path -> getCopyFromLocalFileRunnable(cacheFs, new Path(path), loadingDir, cacheReplicationCnt))
                    .map(runnable -> CompletableFuture.runAsync(runnable, executorService))
                    .forEach(CompletableFuture::join);
            // @formatter:on
        };
    }
}
