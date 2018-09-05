package datawave.ingest.mapreduce.job;

/**
 * Splits Files created will have the form:
 * 
 * <pre>
 * tableA split1 [tablet]
 * tableA split2 [tablet]
 * ...
 * tableB split1 [tablet]
 * tableB split2 [tablet]
 * ...
 * </pre>
 */
public enum SplitsFileType {
    // @formatter:off
    /*
     * Indicates that the splits are sampled to a constrained interval
     * 
     * For example if there are 10 splits: 
     * tableA 0ab 
     * tableA 1cd 
     * tableA 2ef 
     * tableA 3gh 
     * tableA 4ij 
     * tableA 5kl 
     * tableA 6mn 
     * tableA 7op 
     * tableA 8qr
     * tableA 9st
     * 
     * And the number wanted is 4 then the resulting splits would be 
     * tableA 2ef 
     * tableA 4ij 
     * tableA 6mn 
     * tableA 8qr
     * 
     */
    // @formatter:on
    TRIMMEDBYNUMBER,
    
    /* This is the full version of the splits */
    UNTRIMMED,
    
    /* This will contain both the splits as well as the tablet locations for them */
    SPLITSANDLOCATIONS
}
