package datawave.ingest.util.cache.load.mode;

import java.util.Collection;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

/** Interface to support the different modes available for loading the job cache. */
public interface LoadJobCacheMode {
    ServiceLoader<LoadJobCacheMode> serviceLoader = ServiceLoader.load(LoadJobCacheMode.class);
    
    /**
     * Find mode that will determine how to find files to load cache
     *
     * @param mode
     *            Enum that specifies method to find files to load cache
     * @return An optional of LoadJobCacheMode
     */
    static Optional<LoadJobCacheMode> getLoadCacheMode(Mode mode) {
        // @formatter:off
        return StreamSupport
                .stream(serviceLoader.spliterator(), false)
                .filter(loaderMode -> loaderMode.getMode().equals(mode))
                .findFirst();
        // @formatter:on
    }
    
    /**
     * Get enum representing mode specified
     *
     * @return A mode to find cache files to load
     */
    Mode getMode();
    
    /**
     * Get files to load to cache
     *
     * @param options
     *            Mode options necessary for finding local files
     * @return A collection of files to load
     */
    Collection<String> getFilesToLoad(ModeOptions options);
    
    /** Mode enum for specifying the method to find files to load */
    enum Mode {
        CLASSPATH, FILE_PATTERN
    }
}
