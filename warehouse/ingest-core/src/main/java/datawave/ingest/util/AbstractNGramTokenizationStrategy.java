package datawave.ingest.util;

import com.google.common.hash.BloomFilter;
import datawave.ingest.data.config.NormalizedContentInterface;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.ngram.NGramTokenizer;

/**
 * Base class for generating n-grams (a.k.a. "tokens," "terms," or "shingles") from normalized content and applying them to a BloomFilter. Subclasses may
 * override methods to create a stack of strategies for validating state, generating n-grams, "pruning" the number of generated n-grams, and otherwise limiting
 * the application of n-grams to the specified BloomFilter.
 *
 * @see com.google.common.hash.BloomFilter
 * @see BloomFilterUtil
 * @see NGramTokenizationStrategy
 */
public abstract class AbstractNGramTokenizationStrategy {
    protected static final int DEFAULT_MAX_NGRAM_LENGTH = 25;

    private BloomFilter<String> filter;
    private final Logger log = Logger.getLogger(AbstractNGramTokenizationStrategy.class);
    private AbstractNGramTokenizationStrategy source;

    /**
     * Constructor
     */
    public AbstractNGramTokenizationStrategy() {
        this.filter = null;
    }

    /**
     * Constructor
     *
     * @param filter
     *            Updated with n-grams tokenized from normalized content
     */
    public AbstractNGramTokenizationStrategy(final BloomFilter<String> filter) {
        if (null == filter) {
            this.log.warn("Cannot create n-grams for bloom filter", new IllegalArgumentException("BloomFilter is null"));
        }
        this.setFilter(filter);
    }

    /**
     * Constructor
     *
     * @param source
     *            strategy with which to delegate tokenization operations
     */
    public AbstractNGramTokenizationStrategy(final AbstractNGramTokenizationStrategy source) {
        this.setSourceStrategy(source);
    }

    /**
     * Gets the BloomFilter, if any, specified at construction time
     *
     * @return the bloom filter
     */
    public BloomFilter<String> getFilter() {
        return this.filter;
    }

    /**
     * Increments the tokenizer and returns the next n-gram in the stream, or null if no n-gram was generated.
     *
     * @param tokenizer
     *            The tokenizer responsible for generating the next available n-gram
     * @return the next n-gram in the stream, or null if no n-gram was generated
     * @throws TokenizationException
     *             for issues with tokenization
     */
    protected String increment(final NGramTokenizer tokenizer) throws TokenizationException {
        final AbstractNGramTokenizationStrategy source = this.getSourceStrategy();
        final String ngram;
        if (null != source) {
            ngram = source.increment(tokenizer);
        } else {
            ngram = null;
        }
        return ngram;
    }

    /**
     * Returns the source strategy, if defined
     *
     * @return the source strategy, if defined
     */
    protected AbstractNGramTokenizationStrategy getSourceStrategy() {
        return this.source;
    }

    /**
     * Sets the filter, which is also applied to the source strategy, if defined
     *
     * @param filter
     *            a bloom filter with which to apply n-grams
     */
    protected void setFilter(final BloomFilter<String> filter) {
        if (filter != this.filter) {
            this.filter = filter;
            final AbstractNGramTokenizationStrategy source = this.getSourceStrategy();
            if (null != source) {
                source.setFilter(filter);
            }
        }
    }

    /**
     * Specifies a strategy intended to be invoked before the current instance. Sources can be referenced with each other into a stack of strategies to be
     * executed in prioritized order, beginning with the top-level strategy that has no assigned source.
     *
     * @param source
     *            a higher-order strategy
     */
    public void setSourceStrategy(final AbstractNGramTokenizationStrategy source) {
        // Get filter from source
        if (null != source) {
            this.setFilter(source.getFilter());
        }

        // Assign new source strategy
        if (this.source != source) {
            this.source = source;
        }
    }

    /**
     * Creates n-grams based on normalized content. N-gram strings will be no longer than the specified length.
     *
     * @param content
     *            Normalized field name and value
     * @param maxNGramLength
     *            Maximum length of tokenized n-grams
     * @return The number of tokenized n-grams, or a negative integer indicating that tokenization did not occur
     *
     * @throws TokenizationException
     *             for issues with tokenization
     */
    public int tokenize(final NormalizedContentInterface content, int maxNGramLength) throws TokenizationException {
        final AbstractNGramTokenizationStrategy source = this.getSourceStrategy();
        int ngrams;
        if (null != source) {
            ngrams = source.tokenize(content, maxNGramLength);
        } else {
            ngrams = -1;
        }

        return ngrams;
    }

    /**
     * Applies a tokenized n-gram to the BloomFilter based on the specified normalized content
     *
     * @param ngram
     *            An n-gram generated from the specified normalized content
     * @param content
     *            A normalized field name and value
     * @return true, if the n-gram was applied to the strategy's BloomFilter
     * @throws TokenizationException
     *             for issues with tokenization
     */
    protected boolean updateFilter(final String ngram, final NormalizedContentInterface content) throws TokenizationException {
        final AbstractNGramTokenizationStrategy source = this.getSourceStrategy();
        boolean updated;
        if (null != source) {
            updated = source.updateFilter(ngram, content);
        } else {
            updated = false;
        }
        return updated;
    }

    /**
     * Thrown and/or logged if a problem occurs generating n-grams
     */
    public class TokenizationException extends Exception {
        private static final long serialVersionUID = -9128123011801172958L;

        private int ngramCount;

        public TokenizationException(final String message) {
            super(message);
        }

        public TokenizationException(final Throwable cause) {
            super(cause);
        }

        public TokenizationException(final String message, Throwable cause) {
            super(message, cause);
        }

        /**
         * Returns the number of n-grams tokenized prior to the exception being thrown
         *
         * @return the number of n-grams tokenized prior to the exception being thrown
         */
        public int getNgramCount() {
            return this.ngramCount;
        }

        /**
         * Sets the number of n-grams tokenized prior to the exception being thrown
         *
         * @param ngramCount
         *            number of n-grams to set
         */
        protected void setNGramCount(int ngramCount) {
            this.ngramCount = ngramCount;
        }
    }
}
