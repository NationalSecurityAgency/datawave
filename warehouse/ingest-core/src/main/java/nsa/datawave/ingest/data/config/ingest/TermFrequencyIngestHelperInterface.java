package nsa.datawave.ingest.data.config.ingest;

/**
 * For ingest helpers that support recording term freqencies
 * 
 * 
 * 
 */
public interface TermFrequencyIngestHelperInterface {
    
    public abstract boolean isTermFrequencyField(String fieldName);
}
