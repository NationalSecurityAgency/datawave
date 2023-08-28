package datawave.ingest.util;

import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.ngram.NGramTokenizer;

import com.google.common.hash.BloomFilter;

import datawave.ingest.data.config.NormalizedContentInterface;

/**
 * Prevents excessive n-gram tokenization without sacrificing small field values in relation to a handful of very large field values. Tokenization is limited
 * (i.e., n-grams are pruned) based on a logarithmic "flattening" algorithm in which the n-gram counts of smaller length fields are reduced at a smaller rate
 * than the n-gram lengths of larger length fields. Pruning only occurs, however, if the originally expected number of n-grams exceeds the maximum allowed
 * number of n-grams.
 */
public class WeightedValuePruningStrategy extends NGramTokenizationStrategy {

    private int incrementCount;
    private int maxIncrementCount;
    private final int maxNGramLength;
    private final List<Integer> originalStringLengths;
    private final float totalAllowedNgramsCount;
    private final BloomFilterUtil util = BloomFilterUtil.newInstance();
    private final Map<Integer,Integer> weightedNGramCounts;
    private boolean weightingsCalculated = false;

    /**
     * Constructor
     *
     * @param maxAllowedNgrams
     *            The maximum allowed number of n-grams to be tokenized for all field values applied to this strategy instance. A negative value effectively
     *            turns off pruning logic and allows for an unlimited number of n-grams.
     */
    public WeightedValuePruningStrategy(int maxAllowedNgrams) {
        this(maxAllowedNgrams, -1);
    }

    /**
     * Constructor
     *
     * @param maxAllowedNgrams
     *            The maximum allowed number of n-grams to be tokenized for all field values applied to this strategy instance. A negative value effectively
     *            turns off pruning logic and allows for an unlimited number of n-grams.
     * @param maxNGramLength
     *            The maximum size of any generated n-gram. Any non-positive integer results in the value defined by DEFAULT_MAX_NGRAM_LENGTH.
     */
    public WeightedValuePruningStrategy(int maxAllowedNgrams, int maxNGramLength) {
        this.maxNGramLength = (maxNGramLength > 0) ? maxNGramLength : DEFAULT_MAX_NGRAM_LENGTH;
        this.totalAllowedNgramsCount = maxAllowedNgrams;
        this.originalStringLengths = new LinkedList<>();
        this.weightedNGramCounts = new Hashtable<>();
    }

    /**
     * Adds a new weighting to the strategy based on the specified field value's string length and the predicted number of generated n-grams
     *
     * @param value
     *            a field value
     */
    public void applyFieldValue(final String value) {
        this.applyFieldValue(value, false);
    }

    /**
     * Adds a new weighting to the strategy based on the specified field value's string length and the predicted number of generated n-grams
     *
     * @param value
     *            a field value
     * @param calculateWeightings
     *            if true, immediately calculate pruned n-gram counts for this and all previously applied field values
     */
    public void applyFieldValue(final String value, boolean calculateWeightings) {
        synchronized (this.originalStringLengths) {
            this.weightingsCalculated = false;
            if (null != value) {
                this.originalStringLengths.add(value.trim().length());
                if (calculateWeightings) {
                    final Integer[] lengths = new Integer[this.originalStringLengths.size()];
                    this.calculateWeightings(this.originalStringLengths.toArray(lengths));
                }
            }
        }
    }

    private int calculateWeightings(final Integer[] originalStringLengths) {
        // Clear out the map
        this.weightedNGramCounts.clear();

        // Fill up the n-gram counts map with the original string value lengths and corresponding n-gram
        // counts. Also fill an array with the original string value lengths, which will be used to scale
        // down n-gram counts if the latter's total exceeds the allowed maximum.
        double[] scalableOriginalStringLengths = new double[originalStringLengths.length];
        double totalNgramCounts = 0;
        for (int i = 0; i < originalStringLengths.length; i++) {
            int length = originalStringLengths[i];
            int ngramCount = BloomFilterUtil.predictNGramCount(length, 25);
            scalableOriginalStringLengths[i] = length;
            totalNgramCounts += ngramCount;
            this.weightedNGramCounts.put(length, ngramCount);
        }

        // Conditionally scale down n-gram counts based on the inverse log ratios of original string lengths to
        // total n-gram counts. The n-gram counts of smaller original string values will decrease in scale
        // at a smaller rate than the n-gram counts of larger original string values. Scaling will stop once
        // the total n-gram counts are less than the maximum allowed, or until the loop control variable reaches
        // its limit.
        boolean scaledDown = false;
        for (int i = 0; (i < 15) && (this.totalAllowedNgramsCount > 0) && (totalNgramCounts > this.totalAllowedNgramsCount); i++) {
            double logOfTotalNgramCounts = Math.log(totalNgramCounts);
            totalNgramCounts = 0;
            scaledDown = true;
            for (int j = 0; j < scalableOriginalStringLengths.length; j++) {
                double logOfOriginalStringLength = 0;
                if (scalableOriginalStringLengths[j] > 0) {
                    logOfOriginalStringLength = Math.log(scalableOriginalStringLengths[j]);
                }
                double inverseRatioOfLogs = (1 - (logOfOriginalStringLength / logOfTotalNgramCounts));
                double reducedScaleStringLength = scalableOriginalStringLengths[j] * inverseRatioOfLogs;
                int reducedScaleNgramCount = BloomFilterUtil.predictNGramCount((float) reducedScaleStringLength, this.maxNGramLength);
                scalableOriginalStringLengths[j] = reducedScaleStringLength;
                totalNgramCounts += reducedScaleNgramCount;
            }
        }

        // Conditionally scale up the individual n-gram counts to proportionally fill the maximum allowed counts
        if (scaledDown && (this.totalAllowedNgramsCount > 0)) {
            float ratioOfTotalNgramCountsToMaxAllowed = (float) totalNgramCounts / this.totalAllowedNgramsCount;
            totalNgramCounts = 0;
            for (int i = 0; i < scalableOriginalStringLengths.length; i++) {
                int reducedScaleNgramCount = BloomFilterUtil.predictNGramCount((float) scalableOriginalStringLengths[i], this.maxNGramLength);
                if (ratioOfTotalNgramCountsToMaxAllowed > 1.0f) {
                    reducedScaleNgramCount = Math.round(((float) reducedScaleNgramCount) * ratioOfTotalNgramCountsToMaxAllowed);
                } else {
                    reducedScaleNgramCount = Math.round(((float) reducedScaleNgramCount) / ratioOfTotalNgramCountsToMaxAllowed);
                }

                totalNgramCounts += reducedScaleNgramCount;
                this.weightedNGramCounts.put(originalStringLengths[i], reducedScaleNgramCount);
            }
        }

        // Set the flag
        this.weightingsCalculated = true;

        // Return the weighted total count
        return (int) Math.round(totalNgramCounts);
    }

    /**
     * Creates a new BloomFilter based on a size determined by the pruning strategy applied by this instance. Whatever bloom filter may have already been
     * assigned to this instance remains unchanged.
     *
     * @param numberOfFields
     *            An untokenized count of fields to use as a mimumum size of the constructed filter. The number of currently applied field values will be used
     *            if this parameter is not specified with a positive integer.
     * @return a new bloom filter
     */
    public BloomFilter<String> newFilter(int numberOfFields) {
        int prunedNGramCount;
        int totalTokenCount;
        synchronized (this.originalStringLengths) {
            prunedNGramCount = this.getExpectedNGramCount();
            totalTokenCount = prunedNGramCount + ((numberOfFields > 0) ? numberOfFields : this.originalStringLengths.size());
        }

        return this.util.newDefaultFilter(totalTokenCount).getFilter();
    }

    /**
     * Calculates and returns the total number of n-grams expected to be tokenized by this strategy. This number may be much less than the total number of
     * predicted n-grams if limited by the maximum allowed.
     *
     * @return the total number of n-grams predicted to be tokenized by this strategy
     */
    public int getExpectedNGramCount() {
        synchronized (this.originalStringLengths) {
            final Integer[] lengths = new Integer[this.originalStringLengths.size()];
            return this.calculateWeightings(this.originalStringLengths.toArray(lengths));
        }
    }

    @Override
    protected String increment(final NGramTokenizer tokenizer) throws TokenizationException {
        this.incrementCount++;
        final String ngram;
        if (this.incrementCount <= this.maxIncrementCount) {
            ngram = super.increment(tokenizer);
        } else {
            ngram = null;
        }

        return ngram;
    }

    @Override
    public int tokenize(final NormalizedContentInterface content, int maxNGramLength) throws TokenizationException {
        // Get the field's value
        final String fieldValue;
        if (null != content) {
            fieldValue = content.getIndexedFieldValue();
        } else {
            fieldValue = null;
        }

        // Reset the increment count variables
        this.maxIncrementCount = 0;
        this.incrementCount = 0;

        // If defined, determine the maximum number of times to call the increment(..) method
        // (i.e., the max number of times to generate an n-gram)
        int tokenized = 0;
        if (null != fieldValue) {
            // Ensure the weightings have been calculated at least once
            if (!this.weightingsCalculated) {
                synchronized (this.originalStringLengths) {
                    final Integer[] lengths = new Integer[this.originalStringLengths.size()];
                    this.calculateWeightings(this.originalStringLengths.toArray(lengths));
                }
            }

            // Tokenize the n-grams count based on the logarithmic weightings, as applicable
            Integer maxNGramsCount = this.weightedNGramCounts.get(fieldValue.trim().length());
            if (null == maxNGramsCount) {
                maxNGramsCount = 0;
            }

            this.maxIncrementCount = maxNGramsCount;

            // Proceed with tokenization
            if (this.maxIncrementCount > 0) {
                tokenized = super.tokenize(content, maxNGramLength);
            }
        }

        return tokenized;
    }
}
