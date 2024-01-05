package datawave.experimental.scanner.tf;

import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl2.parser.ASTJexlScript;

import datawave.query.attributes.Document;

/**
 * Interface for scanners dedicated to scanning TFs
 */
public interface TermFrequencyScanner {

    Map<String,Object> fetchOffsets(ASTJexlScript script, Document d, String shard, String uid);

    void setTermFrequencyFields(Set<String> termFrequencyFields);

    /**
     * Should this scanner log timing information
     *
     * @param logStats
     *            logging flag
     */
    void setLogStats(boolean logStats);
}
