package datawave.ingest.util.cache.load;

import datawave.ingest.util.cache.path.FileSystemPath;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static datawave.common.io.HadoopFileSystemUtils.getCopyFromLocalFileRunnable;

/** Loads hdfs cache from a collection of local files */
public class LoadJobCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadJobCache.class);
    private static final String LOADING_PREFIX = "LOADING_";
    public static final String JOB_CACHE_TIMESTAMP_FORMAT = "yyyyMMddHHmmss";
    public static final DateTimeFormatter JOB_CACHE_FORMATER = DateTimeFormatter.ofPattern(JOB_CACHE_TIMESTAMP_FORMAT).withZone(ZoneOffset.UTC);
    
    public static String getJobCacheTimestampDir(String prefix) {
        return prefix + LoadJobCache.JOB_CACHE_FORMATER.format(LocalDateTime.now());
    }
    
    /**
     * Create a timestamped hdfs directory and copy local files to it.
     *
     * @param fileSystemPaths
     *            An object that contains the path and its associated file system
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
    public static void load(Collection<FileSystemPath> fileSystemPaths, Collection<String> filesToLoad, boolean finalizeLoad, short cacheReplicationCnt,
                    int executorThreads, String timestampDir, String optionalSubDir) {
        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(filesToLoad.size(), executorThreads));
        
        Consumer<FileSystemPath> loadCache = getLoadCacheConsumer(filesToLoad, executorService, timestampDir, optionalSubDir, cacheReplicationCnt);
        Consumer<FileSystemPath> finalizeCache = getFinalizeCacheConsumer(timestampDir);
        Consumer<FileSystemPath> deleteCache = getDeleteCacheConsumer(timestampDir);
        
        try {
            load(fileSystemPaths, finalizeLoad, loadCache, finalizeCache, deleteCache);
        } finally {
            executorService.shutdown();
        }
        
    }
    
    /**
     * Create a timestamped hdfs directory and copy local files to it.
     *
     * @param fileSystemPaths
     *            An object that contains the path and its associated file system
     * @param finalizeLoad
     *            A boolean to determine if the cache should be finalized
     * @param loadCache
     *            A Consumer method that will load the cache
     * @param finalizeCache
     *            A Consumer method that will finalize the cache
     * @param deleteCache
     *            A Consumer method that will delete the cache on error
     */
    public static void load(Collection<FileSystemPath> fileSystemPaths, boolean finalizeLoad, Consumer<FileSystemPath> loadCache,
                    Consumer<FileSystemPath> finalizeCache, Consumer<FileSystemPath> deleteCache) {
        try {
            fileSystemPaths.forEach(loadCache);
            if (finalizeLoad) {
                fileSystemPaths.forEach(finalizeCache);
            }
        } catch (Exception e) {
            LOGGER.error("Unable to load job cache. Deleting temporary directories. ", e);
            fileSystemPaths.forEach(deleteCache);
        }
    }
    
    /**
     * Create a consumer that will delete temporary cache artifacts upon error.
     *
     * @param timestampDir
     *            A timestamp directory that will reflect when the cache was loaded
     * @return A consumer that will delete the cache.
     */
    static Consumer<FileSystemPath> getDeleteCacheConsumer(String timestampDir) {
        return (FileSystemPath fsPath) -> {
            Path uploadWorkingDir = new Path(fsPath.getOutputPath(), LOADING_PREFIX + timestampDir);
            FileSystem cacheFs = fsPath.getFileSystem();
            
            try {
                cacheFs.delete(uploadWorkingDir, true);
            } catch (IOException e) {
                LOGGER.error("Unable to delete temporary directory " + uploadWorkingDir, e);
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
    static Consumer<FileSystemPath> getFinalizeCacheConsumer(String timestampDir) {
        return (FileSystemPath fsPath) -> {
            Path jobCacheDir = new Path(fsPath.getOutputPath(), timestampDir);
            Path uploadWorkingDir = new Path(jobCacheDir.getParent(), LOADING_PREFIX + jobCacheDir.getName());
            FileSystem cacheFs = fsPath.getFileSystem();
            
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
    static Consumer<FileSystemPath> getLoadCacheConsumer(Collection<String> filesToLoad, ExecutorService executorService, String timestampDir,
                    String optionalSubDir, short cacheReplicationCnt) {
        return (FileSystemPath fsPath) -> {
            Path jobCacheDir = new Path(fsPath.getOutputPath(), timestampDir);
            Path uploadWorkingDir = new Path(jobCacheDir.getParent(), LOADING_PREFIX + jobCacheDir.getName());
            Path loadingDir = StringUtils.isBlank(optionalSubDir) ? uploadWorkingDir : new Path(uploadWorkingDir, optionalSubDir);
            FileSystem cacheFs = fsPath.getFileSystem();
            
            try {
                cacheFs.mkdirs(loadingDir);
            } catch (IOException e) {
                throw new RuntimeException("Unable to make directory " + loadingDir, e);
            }
            
            // @formatter:off
            Collection<CompletableFuture<Void>> loadFileFutures = filesToLoad
                    .stream()
                    .map(path -> getCopyFromLocalFileRunnable(cacheFs, new Path(path), loadingDir, cacheReplicationCnt))
                    .map(runnable -> CompletableFuture.runAsync(runnable, executorService))
                    .collect(Collectors.toList());
            // @formatter:on
            loadFileFutures.forEach(CompletableFuture::join);
            LOGGER.info("Loaded files to {}", loadingDir);
        };
    }
}
