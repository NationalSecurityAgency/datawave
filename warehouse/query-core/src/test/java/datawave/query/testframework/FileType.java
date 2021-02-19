package datawave.query.testframework;

import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.util.function.BiFunction;

/**
 * Defines the types of files that are supported by the test framework.
 */
public enum FileType {
    /**
     * Indicates data stored in CSV files.
     */
    CSV(CSVTestFileLoader::new),
    
    /**
     * Indicates data stored as JSON files.
     */
    JSON(JsonTestFileLoader::new),
    
    /**
     * Indicates data for grouping tests.
     */
    GROUPING(null);
    
    private final BiFunction<URI,Configuration,TestFileLoader> fileLoader;
    
    FileType(BiFunction<URI,Configuration,TestFileLoader> fileLoader) {
        this.fileLoader = fileLoader;
    }
    
    /**
     * Returns a {@link TestFileLoader} configured with the given URI and Hadoop configuration if one exists for this {@link FileType}, or null otherwise.
     * 
     * @param uri
     *            the URI
     * @param configuration
     *            the Hadoop configuration
     * @return a new {@link TestFileLoader} if there is one applicable for this {@link FileType}, or null otherwise
     */
    public TestFileLoader getFileLoader(URI uri, Configuration configuration) {
        return fileLoader != null ? fileLoader.apply(uri, configuration) : null;
    }
    
    /**
     * Returns whether or not this {@link FileType} has an applicable {@link TestFileLoader}.
     * 
     * @return true if there is an applicable {@link TestFileLoader}, or false otherwise.
     */
    public boolean hasFileLoader() {
        return fileLoader != null;
    }
}
