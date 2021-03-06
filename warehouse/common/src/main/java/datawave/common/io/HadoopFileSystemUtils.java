package datawave.common.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/* Helper class for hadoop file system operations */
public class HadoopFileSystemUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopFileSystemUtils.class);
    
    private HadoopFileSystemUtils() {}
    
    /**
     * Makes an attempt to close the file system
     *
     * @param fileSystems
     *            A collection of hadoop file system objects to close
     */
    public static void close(Collection<FileSystem> fileSystems) {
        for (FileSystem fs : fileSystems) {
            try {
                fs.close();
            } catch (IOException e) {
                LOGGER.warn("Unable to close {}", fs, e);
            }
        }
    }
    
    /**
     * Find the hadoop file system associated with the file path
     *
     * @param hadoopConfs
     *            A collection of hadoop configurations
     * @param filePath
     *            A file path
     * @return An optional that contains a file system for the file path or an empty optional
     */
    public static Optional<FileSystem> getFileSystem(Collection<Configuration> hadoopConfs, Path filePath) {
        for (Configuration conf : hadoopConfs) {
            try {
                return Optional.of(filePath.getFileSystem(conf));
            } catch (IOException e) {
                LOGGER.debug("Unable to create filesystem for path {} and configuration {} ", filePath, conf, e);
            }
        }
        return Optional.empty();
    }
    
    /**
     * Find the hadoop file system associated with each file path
     *
     * @param hadoopConfs
     *            A collection of hadoop configurations
     * @param filePaths
     *            A collection of file paths
     * @return A map of a path to its file system.
     */
    public static Map<Path,FileSystem> getPathToFileSystemMapping(Collection<Configuration> hadoopConfs, Collection<Path> filePaths) {
        // @formatter:off
        Map<Path, FileSystem> pathToFileSystem = filePaths
                .stream()
                .map(path -> new AbstractMap.SimpleEntry<>(path, HadoopFileSystemUtils.getFileSystem(hadoopConfs, path)))
                .filter(entry -> entry.getValue().isPresent())
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, entry -> entry.getValue().get()));
        // @formatter:on
        
        if (pathToFileSystem.keySet().size() != filePaths.size()) {
            // @formatter:off
            Collection<Path> pathsWithNoFileSystem = filePaths
                    .stream()
                    .filter(parentPath -> !pathToFileSystem.containsKey(parentPath))
                    .collect(Collectors.toList());
            // @formatter:on
            throw new IllegalArgumentException("Unable to create file systems for " + pathsWithNoFileSystem);
        }
        
        return pathToFileSystem;
    }


    //TODO: NEEDS CLASSPATH
    /**
     * Returns a runnable method for copying files to hdfs
     *
     * @param fileSystem
     *            The file system to copy files
     * @param srcPath
     *            The source path to copy
     * @param dstPath
     *            The destination path
     * @param replicationCnt
     *            The number of replications
     * @return A runnable method that will copy files.
     */
    public static Runnable getCopyFromLocalFileRunnable(FileSystem fileSystem, Path srcPath, Path dstPath, short replicationCnt) {
        return () -> {
            try {
                fileSystem.copyFromLocalFile(false, true, srcPath, dstPath);
                fileSystem.setReplication(dstPath, replicationCnt);
            } catch (IOException e) {
                throw new RuntimeException("Unable to upload " + srcPath + " to " + dstPath, e);
            }
        };
    }
}
