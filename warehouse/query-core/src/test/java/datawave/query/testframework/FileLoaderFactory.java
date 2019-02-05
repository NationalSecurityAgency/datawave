package datawave.query.testframework;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Factory class for creation of a file loader instance. Supported file types are defined by {@link FileType}.
 */
public class FileLoaderFactory {
    
    /**
     * Defines the types of files that are supported by the test framework.
     */
    public enum FileType {
        // define the different types of files that are loaded for testing
        CSV,
        JSON,
        // data for grouping tests
        GROUPING;
    }
    
    // create private default constructor
    private FileLoaderFactory() {}
    
    static TestFileLoader getLoader(FileType fileType, final DataTypeHadoopConfig dataType, final Configuration conf) throws IOException, URISyntaxException {
        switch (fileType) {
            case CSV:
                return new CSVTestFileLoader(dataType.getIngestFile(), conf);
            case JSON:
                return new JsonTestFileLoader(dataType.getIngestFile(), conf);
            default:
                throw new AssertionError("file type not configured: " + fileType.name());
        }
    }
}
