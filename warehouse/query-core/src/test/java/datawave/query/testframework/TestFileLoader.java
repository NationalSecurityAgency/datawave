package datawave.query.testframework;

import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

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
