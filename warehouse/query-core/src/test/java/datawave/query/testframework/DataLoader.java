package datawave.query.testframework;

import com.google.common.collect.Multimap;
import datawave.ingest.data.config.NormalizedContentInterface;

import java.io.IOException;
import java.util.Collection;

public interface DataLoader {
    
    /**
     * Loads raw test data into a {@link Multimap}, which can then be processed for expected results.
     * 
     * @return raw data
     * @throws IOException
     *             error reading data from input file
     */
    Collection<Multimap<String,NormalizedContentInterface>> getRawData() throws IOException;
}
