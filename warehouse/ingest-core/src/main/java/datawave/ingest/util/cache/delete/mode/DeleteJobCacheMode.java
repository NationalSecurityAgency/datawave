package datawave.ingest.util.cache.delete.mode;

import datawave.ingest.util.cache.path.FileSystemPath;
import org.apache.hadoop.conf.Configuration;

import java.util.Collection;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

/** Interface that will be used to determine method to inactive job caches */
public interface DeleteJobCacheMode {
    ServiceLoader<DeleteJobCacheMode> serviceLoader = ServiceLoader.load(DeleteJobCacheMode.class);
    
    /**
     * Find mode that will determine which timestamp cache directories to delete.
     *
     * @param mode
     *            Enum that specifies method to find cache candidates for deletion.
     * @return An optional for DeleteJobCacheMode object
     */
    static Optional<DeleteJobCacheMode> getDeleteCacheMode(Mode mode) {
        // @formatter:off
        return StreamSupport.stream(serviceLoader.spliterator(), false)
                .filter(deleteMode -> deleteMode.getMode().equals(mode))
                .findFirst();
        // @formatter:on
    }
    
    /**
     * Get enum representing mode specified
     *
     * @return A mode to find cache directories to delete
     */
    Mode getMode();
    
    /**
     * Get job cache candidates for deletion
     *
     * @param hadoopConfs
     *            Hadoop configuration used to search for correct file system
     * @param options
     *            Mode options necessary for finding deletion candidates
     * @return A collection of candidates for deletion
     */
    Collection<FileSystemPath> getDeletionCandidates(Collection<Configuration> hadoopConfs, DeleteModeOptions options);
    
    /**
     * Mode enum for specifying the method to find files to delete
     */
    enum Mode {
        DELETES_SPECIFIED, OLD_INACTIVE
    }
}
