package datawave.ingest.util;

import datawave.ingest.data.config.NormalizedContentInterface;

import org.apache.log4j.Logger;

import com.google.common.hash.BloomFilter;

/**
 * Tokenizes and applies n-grams to a BloomFilter until memory or disk space has been exhausted to a minimum threshold (expressed as a percentage of the total
 * available). Once a resource starvation condition is detected, n-grams are parsed and counted but no longer applied to the filter.
 */
public class MemoryStarvationStrategy extends AbstractNGramTokenizationStrategy {

    public static final String DEFAULT_PATH_FOR_DISK_SPACE_VALIDATION = ResourceAvailabilityUtil.ROOT_PATH;

    private final Logger log = Logger.getLogger(MemoryStarvationStrategy.class);
    private TokenizationException lowMemoryException;
    private final float minMemoryThreshold;
    private int ngramCount;

    /**
     * Constructor
     *
     * @param minMemoryThreshold
     *            Minimum amount of available memory, expressed as a percentage, needed to create NGrams
     */
    public MemoryStarvationStrategy(float minMemoryThreshold) {
        this.minMemoryThreshold = minMemoryThreshold;
    }

    /**
     * Constructor
     *
     * @param filter
     *            a bloom filter
     * @param minMemoryThreshold
     *            Minimum amount of available memory, expressed as a percentage, needed to create NGrams
     */
    public MemoryStarvationStrategy(final BloomFilter<String> filter, float minMemoryThreshold) {
        super(filter);
        this.minMemoryThreshold = minMemoryThreshold;
    }

    /**
     * Constructor
     *
     * @param source
     *            a source tokenization strategy
     * @param minMemoryThreshold
     *            Minimum amount of available memory, expressed as a percentage, needed to create NGrams
     */
    public MemoryStarvationStrategy(final AbstractNGramTokenizationStrategy source, float minMemoryThreshold) {
        super(source);
        this.minMemoryThreshold = minMemoryThreshold;
    }

    @Override
    public int tokenize(final NormalizedContentInterface content, int maxNGramLength) throws TokenizationException {
        // Check for and handle low memory condition
        if (!ResourceAvailabilityUtil.isMemoryAvailable(this.minMemoryThreshold)) {
            if (null == this.lowMemoryException) {
                final String fieldName;
                if (null != content) {
                    fieldName = content.getIndexedFieldName();
                } else {
                    fieldName = null;
                }

                final String message = "Available memory is less than " + (this.minMemoryThreshold * 100) + "% of capacity. "
                                + "NGrams will be created and counted for field " + fieldName + " but not included in " + "the bloom filter.";
                this.lowMemoryException = new LowMemoryException(message);
                this.log.warn("Problem creating n-grams", this.lowMemoryException);
            }
        } else if (null != this.lowMemoryException) {
            this.lowMemoryException = null;
            this.ngramCount = 0;
        }

        // Tokenize as normal
        return super.tokenize(content, maxNGramLength);
    }

    @Override
    protected boolean updateFilter(final String ngram, final NormalizedContentInterface content) throws TokenizationException {
        // Allow other sources to execute first
        boolean updated = super.updateFilter(ngram, content);

        // If a resource exception is active, prevent updates from occurring by returning true
        if (null != this.lowMemoryException) {
            updated = true;
        }

        // Increment the count regardless
        this.ngramCount++;

        return updated;
    }

    /**
     * Logged if disk space reaches a minimum available threshold
     */
    public class LowMemoryException extends TokenizationException {
        private static final long serialVersionUID = -4872575732603058606L;

        public LowMemoryException(final String message) {
            super(message);
            this.setNGramCount(MemoryStarvationStrategy.this.ngramCount);
            MemoryStarvationStrategy.this.ngramCount = 0;
        }
    }
}
