package datawave.ingest.util;

import com.google.common.hash.BloomFilter;

import datawave.ingest.data.config.NormalizedContentInterface;

/**
 * Tokenizes n-grams until an allowed number of milliseconds has elapsed since a specified start time, at which point a TimeoutException is thrown
 */
public class TimeoutStrategy extends AbstractNGramTokenizationStrategy {
    public static final String MAPRED_TASK_TIMEOUT = "mapred.task.timeout";

    private final long startTimeMillis;
    private final int maxAllowedExecutionTime;
    private int ngramCount;

    /**
     * Constructor
     *
     * @param startTime
     *            Start time of the overall tokenization operation
     * @param maxAllowedExecutionTime
     *            Maximum number of milliseconds elapsed from the specified start time before a TimeoutException is thrown
     *
     */
    public TimeoutStrategy(long startTime, int maxAllowedExecutionTime) {
        this.startTimeMillis = startTime;
        this.maxAllowedExecutionTime = maxAllowedExecutionTime;
    }

    /**
     * Constructor
     *
     * @param filter
     *            a bloom filter
     * @param startTime
     *            Start time of the overall tokenization operation
     * @param maxAllowedExecutionTime
     *            Maximum number of milliseconds elapsed from the specified start time before a TimeoutException is thrown
     */
    public TimeoutStrategy(final BloomFilter<String> filter, long startTime, int maxAllowedExecutionTime) {
        super(filter);
        this.startTimeMillis = startTime;
        this.maxAllowedExecutionTime = maxAllowedExecutionTime;
    }

    /**
     * Constructor
     *
     * @param substrategy
     *            a child tokenization strategy
     * @param startTime
     *            Start time of the overall tokenization operation
     * @param maxAllowedExecutionTime
     *            Maximum number of milliseconds elapsed from the specified start time before a TimeoutException is thrown
     */
    public TimeoutStrategy(final AbstractNGramTokenizationStrategy substrategy, long startTime, int maxAllowedExecutionTime) {
        super(substrategy);
        this.startTimeMillis = startTime;
        this.maxAllowedExecutionTime = maxAllowedExecutionTime;
    }

    @Override
    public int tokenize(final NormalizedContentInterface content, int maxNGramLength) throws TokenizationException {
        // Reset n-gram count
        this.ngramCount = 0;

        // Tokenize as normal
        return super.tokenize(content, maxNGramLength);
    }

    @Override
    protected boolean updateFilter(final String ngram, final NormalizedContentInterface content) throws TokenizationException {
        // Trigger a timeout exception if the maximum allowed execution time has passed
        if (this.maxAllowedExecutionTime > 0) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - startTimeMillis > this.maxAllowedExecutionTime) {
                final String fieldName;
                if (null != content) {
                    fieldName = content.getIndexedFieldName();
                } else {
                    fieldName = null;
                }

                final String message = "Exceeded the maximum allowed time of " + this.maxAllowedExecutionTime + " milliseconds to create NGrams "
                                + "while handling field " + fieldName + ". No additional NGrams will be created or counted for this or any "
                                + "proceeding fields in the given set. Only the previously processed tokens and remaining field values will "
                                + "be included in the bloom filter.";
                throw new TimeoutException(message, this.ngramCount);
            }
        }

        // Otherwise, update the filter using the superclass
        boolean updated = super.updateFilter(ngram, content);
        if (updated) {
            this.ngramCount++;
        }

        return updated;
    }

    /**
     * Thrown and logged if n-gram generation takes too long relative to the allowed number of milliseconds, which is presumably based on the overall
     * mapred.task.timeout
     */
    public class TimeoutException extends TokenizationException {
        private static final long serialVersionUID = -4872575732603058606L;

        private int ngramCount;

        public TimeoutException(final String message, int ngramCount) {
            super(message);

            this.ngramCount = ngramCount;
        }

        public int getNgramCount() {
            return this.ngramCount;
        }

        public void setNGramCount(int ngramCount) {
            this.ngramCount = ngramCount;
        }
    }
}
