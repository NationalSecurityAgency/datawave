package datawave.query.testframework;

import org.apache.commons.math.analysis.integration.UnivariateRealIntegrator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Loads data from an external source.
 */
public interface TestFileLoader {
    
    /**
     * Loads test data from an external source into Accumulo.
     * 
     * @param seqFile
     *            hadoop sequence file
     */
    void loadTestData(SequenceFile.Writer seqFile) throws IOException;
    
}
