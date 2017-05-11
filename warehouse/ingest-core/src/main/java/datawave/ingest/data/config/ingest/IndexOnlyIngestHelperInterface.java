package datawave.ingest.data.config.ingest;

import java.util.Set;

/**
 * Circumventing the annoyance of not following one of the preexisting methods for processing data and getting the correct records in the metadata table
 */
public interface IndexOnlyIngestHelperInterface {
    public Set<String> getIndexOnlyFields();
}
