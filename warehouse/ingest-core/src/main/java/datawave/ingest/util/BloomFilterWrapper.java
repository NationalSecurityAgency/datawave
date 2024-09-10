package datawave.ingest.util;

import com.google.common.hash.BloomFilter;

/**
 * A container for referencing a finalized instance of {@link BloomFilter}. Also contains information about values applied to the filter, such as n-grams and
 * field values.
 */
public class BloomFilterWrapper {
    private int fieldValuesApplied;
    private final BloomFilter<String> filter;
    private int ngramsApplied;
    private int ngramsPruned;

    /**
     * Constructor
     *
     * @param filter
     *            a bloom filter to wrap
     */
    public BloomFilterWrapper(final BloomFilter<String> filter) {
        this(filter, 0);
    }

    /**
     * Constructor
     *
     * @param filter
     *            a bloom filter to wrap
     * @param numberOfFieldValueInsertions
     *            the number of field values inserted (or expected to be inserted) into the specified filter
     */
    public BloomFilterWrapper(final BloomFilter<String> filter, int numberOfFieldValueInsertions) {
        if (null == filter) {
            throw new IllegalArgumentException("Bloom filter cannot be null");
        }
        this.filter = filter;
    }

    /**
     * Returns the number of field values, if any, applied to the filter
     *
     * @return the number of field values applied to the filter
     */
    public int getFieldValuesAppliedToFilter() {
        return this.fieldValuesApplied;
    }

    /**
     * Returns a non-null bloom filter
     *
     * @return a non-null bloom filter
     */
    public BloomFilter<String> getFilter() {
        return this.filter;
    }

    /**
     * Returns the number of n-grams, if any, applied to the wrapper's filter
     *
     * @return the number of n-grams applied to the wrapper's filter
     */
    public int getNGramsAppliedToFilter() {
        return this.ngramsApplied;
    }

    /**
     * Returns the number of n-grams, if any, excluded from being applied to the filter wrapped by this instance. Although no n-grams would be excluded from a
     * filter in an ideal world, some of them may be excluded if the filter is deliberately limited in size, JVM memory is scarce, processing time is running
     * too long, or some other less-than-ideal situation occurs.
     *
     * @return the number of n-grams excluded from the wrapper's filter
     *
     * @see WeightedValuePruningStrategy
     * @see DiskSpaceStarvationStrategy
     * @see MemoryStarvationStrategy
     * @see TimeoutStrategy
     * @see BloomFilterUtil
     */
    public int getNGramsPrunedFromFilter() {
        return this.ngramsPruned;
    }

    /**
     * Sets the number of field values, if any, applied to the filter referenced by this instance
     *
     * @param numberOfAppliedFieldValues
     *            the number of field values applied to the filter
     */
    public void setFieldValuesAppliedToFilter(int numberOfAppliedFieldValues) {
        this.fieldValuesApplied = numberOfAppliedFieldValues;
    }

    /**
     * Sets the number of n-grams, if any, applied to the wrapper's filter
     *
     * @param numberOfAppliedNGrams
     *            the number of n-grams applied to the wrapper's filter
     */
    public void setNGramsAppliedToFilter(int numberOfAppliedNGrams) {
        this.ngramsApplied = numberOfAppliedNGrams;
    }

    /**
     * Sets the number of n-grams, if any, excluded from being applied to the filter wrapped by this instance. Although n-grams would not be excluded from a
     * filter in an ideal world, some of them may be excluded if the filter is deliberately limited in size, JVM memory is scarce, processing time is running
     * too long, or some other less-than-ideal situation occurs.
     *
     * @param numberOfExcludedNGrams
     *            the number of n-grams excluded from the wrapped filter
     */
    public void setNGramsPrunedFromFilter(int numberOfExcludedNGrams) {
        this.ngramsPruned = numberOfExcludedNGrams;
    }
}
