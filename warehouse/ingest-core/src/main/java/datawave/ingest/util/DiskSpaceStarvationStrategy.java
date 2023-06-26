package datawave.ingest.util;

import datawave.ingest.data.config.NormalizedContentInterface;

import org.apache.log4j.Logger;

import com.google.common.hash.BloomFilter;

/**
 * Tokenizes and applies n-grams to a BloomFilter until disk space has been exhausted to a minimum threshold (expressed as a percentage of the total available).
 * Once a resource starvation condition is detected, n-grams are parsed and counted but no longer applied to the filter.
 */
public class DiskSpaceStarvationStrategy extends AbstractNGramTokenizationStrategy {

    public static final String DEFAULT_PATH_FOR_DISK_SPACE_VALIDATION = ResourceAvailabilityUtil.ROOT_PATH;

    private final Logger log = Logger.getLogger(DiskSpaceStarvationStrategy.class);
    private TokenizationException lowDiskSpaceException;
    private final float minDiskSpaceThreshold;
    private final String minDiskSpacePath;
    private int ngramCount;

    /**
     * Constructor
     *
     * @param minDiskSpaceThreshold
     *            Minimum amount of available disk space, expressed as a percentage, needed to create NGrams
     * @param minDiskSpacePath
     *            Path to check for available disk space
     */
    public DiskSpaceStarvationStrategy(float minDiskSpaceThreshold, final String minDiskSpacePath) {
        this.minDiskSpaceThreshold = minDiskSpaceThreshold;
        this.minDiskSpacePath = (null != minDiskSpacePath) ? minDiskSpacePath : DEFAULT_PATH_FOR_DISK_SPACE_VALIDATION;
    }

    /**
     * Constructor
     *
     * @param filter
     *            a bloom filter
     * @param minDiskSpaceThreshold
     *            Minimum amount of available disk space, expressed as a percentage, needed to create NGrams
     * @param minDiskSpacePath
     *            Path to check for available disk space
     */
    public DiskSpaceStarvationStrategy(final BloomFilter<String> filter, float minDiskSpaceThreshold, final String minDiskSpacePath) {
        super(filter);
        this.minDiskSpaceThreshold = minDiskSpaceThreshold;
        this.minDiskSpacePath = (null != minDiskSpacePath) ? minDiskSpacePath : DEFAULT_PATH_FOR_DISK_SPACE_VALIDATION;
    }

    /**
     * Constructor
     *
     * @param source
     *            a source tokenization strategy
     * @param minDiskSpaceThreshold
     *            Minimum amount of available disk space, expressed as a percentage, needed to create NGrams
     * @param minDiskSpacePath
     *            Path to check for available disk space
     */
    public DiskSpaceStarvationStrategy(final AbstractNGramTokenizationStrategy source, float minDiskSpaceThreshold, final String minDiskSpacePath) {
        super(source);
        this.minDiskSpaceThreshold = minDiskSpaceThreshold;
        this.minDiskSpacePath = (null != minDiskSpacePath) ? minDiskSpacePath : DEFAULT_PATH_FOR_DISK_SPACE_VALIDATION;
    }

    @Override
    public int tokenize(final NormalizedContentInterface content, int maxNGramLength) throws TokenizationException {
        // Check for and handle low disk space condition
        if (!ResourceAvailabilityUtil.isDiskAvailable(this.minDiskSpacePath, this.minDiskSpaceThreshold)) {
            if (null == this.lowDiskSpaceException) {
                final String fieldName;
                if (null != content) {
                    fieldName = content.getIndexedFieldName();
                } else {
                    fieldName = null;
                }

                final String message = "Available disk space is less than " + (this.minDiskSpaceThreshold * 100) + "% of capacity. "
                                + "NGrams will be created and counted for field " + fieldName + " but not included in " + "the bloom filter.";
                this.lowDiskSpaceException = new LowDiskSpaceException(message);
                this.log.warn("Problem creating n-grams", this.lowDiskSpaceException);
            }
        } else if (null != this.lowDiskSpaceException) {
            this.lowDiskSpaceException = null;
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
        if (null != this.lowDiskSpaceException) {
            updated = true;
        }

        // Increment the count regardless
        this.ngramCount++;

        return updated;
    }

    /**
     * Logged if disk space reaches a minimum available threshold
     */
    public class LowDiskSpaceException extends TokenizationException {
        private static final long serialVersionUID = -4872575732603058606L;

        public LowDiskSpaceException(final String message) {
            super(message);
            this.setNGramCount(DiskSpaceStarvationStrategy.this.ngramCount);
            DiskSpaceStarvationStrategy.this.ngramCount = 0;
        }
    }
}
