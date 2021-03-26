package datawave.ingest.util.cache.delete;

import com.google.common.base.Predicates;
import datawave.common.io.FilesFinder;
import datawave.ingest.util.cache.lease.LockFactory;
import datawave.ingest.util.cache.path.FileSystemPath;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** This class will be responsible for deleting job caches */
public class DeleteJobCache {
    public static final Comparator<Path> PATH_COMPARATOR = Comparator.comparing(x -> x.toUri().getPath());
    
    /**
     * Find job cache directories that are candidates for deletion.
     *
     * @param jobCachePath
     *            An object that contains the job cache path and its associated file system.
     * @param timestampPattern
     *            A pattern that matches job cache directories.
     * @param keepNumVersions
     *            The number of versions to keep if possible.
     * @return A collection of job cache directories to attempt to delete.
     */
    public static Collection<FileSystemPath> getDeletionCandidates(FileSystemPath jobCachePath, Pattern timestampPattern, int keepNumVersions) {
        Path cachePath = jobCachePath.getOutputPath();
        FileSystem fileSystem = jobCachePath.getFileSystem();
        
        FileStatus[] pathStatus = FilesFinder.listPath(fileSystem, cachePath, timestampPattern);
        
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
    
    /**
     * Find job cache directories that are candidates for deletion.
     *
     * @param deletePaths
     *            Job cache paths that should be deleted.
     * @param keepPaths
     *            Job cache paths that should be kept.
     * @return A collection of job cache directories to attempt to delete.
     */
    public static Collection<Path> getDeletionCandidates(Collection<Path> deletePaths, Collection<Path> keepPaths) {
        // @formatter:off
        return deletePaths
                .stream()
                .filter(Predicates.not(keepPaths::contains))
                .collect(Collectors.toList());
        // @formatter:on
    }
    
    /**
     * Attempts to delete inactive job cache directories.
     *
     * @param jobCachePaths
     *            A collection of job cache directories where deletion will be attempted.
     * @param lockFactory
     *            A factory to determine caches that are still active or locked.
     */
    public static void deleteCacheIfNotActive(Collection<FileSystemPath> jobCachePaths, LockFactory lockFactory) {
        // @formatter:off
        jobCachePaths
                .stream()
                .filter(jobCache -> lockFactory.acquireLock(jobCache.getOutputPath().getName(), lockFactory.getCacheAvailablePredicate()))
                .filter(DeleteJobCache::deleteCache)
                .forEach(jobCache -> lockFactory.releaseLock(jobCache.getOutputPath().getName()));
        // @formatter:on
    }
    
    /**
     * Deletes the job cache directory.
     *
     * @param jobCachePath
     *            An object that contains the job cache path and its associated file system.
     * @return A boolean representing if the cache was deleted successfully.
     */
    private static boolean deleteCache(FileSystemPath jobCachePath) {
        FileSystem fileSystem = jobCachePath.getFileSystem();
        Path cachePath = jobCachePath.getOutputPath();
        
        try {
            return fileSystem.delete(cachePath, true);
        } catch (IOException e) {
            throw new RuntimeException("Unable to delete " + cachePath, e);
        }
    }
}
