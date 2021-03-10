package datawave.common.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

/* Helper class for hadoop file system operations */
public class HadoopFileSystemUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopFileSystemUtils.class);
    
    private HadoopFileSystemUtils() {}

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
    
    // TODO: NEEDS CLASSPATH
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
