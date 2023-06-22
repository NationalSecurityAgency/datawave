package datawave.ingest.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Multimap;
import com.google.common.hash.BloomFilter;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.mapreduce.MemberShipTest;
import datawave.ingest.util.AbstractNGramTokenizationStrategy.TokenizationException;
import datawave.ingest.util.TimeoutStrategy.TimeoutException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.ngram.NGramTokenizer;

/**
 * Utility class for creating bloom filters, such as those built from {@link Multimap}s and n-gram tokenenization
 */
public class BloomFilterUtil {

    /**
     * The number of bytes in an empty bloom filter. At the time this variable was first calculated, the value was 401 bytes.
     */
    public static final int EMPTY_FILTER_SIZE = MemberShipTest.toValue(BloomFilterUtil.newInstance().newDefaultFilter(0).getFilter()).getSize();
    private static final float FILTER_SIZE_TO_NGRAM_COUNT_FACTOR = 1.1f;

    private final AbstractContentIngestHelper helper;
    private final Logger log = Logger.getLogger(BloomFilterUtil.class);
    private final int maxAllowedExecutionTime;
    private int maxNGramLength = AbstractNGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH;
    private final String minDiskSpacePath;
    private final float minDiskSpaceThreshold;
    private final float minMemoryThreshold;
    private boolean missingHelperLogged;
    private int optimumFilterSize;

    /**
     * Protected constructor to discourage instantiation outside of package
     *
     * @param helper
     *            helper instance for content ingest
     * @param minMemoryThreshold
     *            Minimum amount of available memory, expressed as a percentage, needed to create NGrams
     * @param minDiskSpaceThreshold
     *            Minimum amount of available disk space, expressed as a percentage, needed to create NGrams
     * @param minDiskSpacePath
     *            Path to check for available disk space
     * @param timeoutInMilliseconds
     *            maximum allowed time in milliseconds for NGrams to be created. Unlimited time will allowed if specified with a value equal to or less than
     *            zero (0)
     */
    protected BloomFilterUtil(final AbstractContentIngestHelper helper, float minMemoryThreshold, float minDiskSpaceThreshold, final String minDiskSpacePath,
                    int timeoutInMilliseconds) {
        this.helper = helper;

        this.minDiskSpaceThreshold = minDiskSpaceThreshold;
        if ((null != minDiskSpacePath) && !minDiskSpacePath.isEmpty()) {
            this.minDiskSpacePath = minDiskSpacePath;
        } else {
            this.minDiskSpacePath = DiskSpaceStarvationStrategy.DEFAULT_PATH_FOR_DISK_SPACE_VALIDATION;
        }
        this.minMemoryThreshold = minMemoryThreshold;
        this.maxAllowedExecutionTime = timeoutInMilliseconds;
    }

    /**
     * Tokenize the collection of normalized content and apply the resulting n-grams to the specified filter.
     *
     * @param fieldName
     *            The name of the field the value came from
     * @param ncis
     *            collection of normalized content to tokenize
     * @param strategy
     *            the n-gram tokenization strategy
     * @return The number of generated n-grams
     * @throws TimeoutException
     *             if the tokenization operation takes too long in relation to the overall mapred.task.timeout
     */
    private int applyNgrams(final String fieldName, final Collection<NormalizedContentInterface> ncis, final AbstractNGramTokenizationStrategy strategy)
                    throws TokenizationException {
        // Perform n-gram tokenization for all normalized content based on the given field
        int generatedNGramCount = 0;
        for (final NormalizedContentInterface nci : ncis) {
            try {
                generatedNGramCount += strategy.tokenize(nci, this.maxNGramLength);
            } catch (final TokenizationException e) {
                // Apply and rethrow the exception with an incremented n-gram count
                generatedNGramCount += e.getNgramCount();
                e.setNGramCount(generatedNGramCount);
                throw e;
            }
        }

        return generatedNGramCount;
    }

    /*
     * Determine the maximum percentage of n-grams based on an optimum desired filter size. A negative value indicates an indeterminate number of allowed
     * n-grams.
     *
     * @return max allowed n-grams expressed as a percentage of the total predicted n-grams
     */
    private int calculateMaxAllowedNgrams(int numberOfFields) {
        int maxAllowedNgrams = -1;
        if (this.optimumFilterSize > EMPTY_FILTER_SIZE) {
            maxAllowedNgrams = predictMaxFilterAdditions(this.optimumFilterSize);
            maxAllowedNgrams -= numberOfFields;
            if (maxAllowedNgrams < 0) {
                maxAllowedNgrams = 0;
            }
        }

        return maxAllowedNgrams;
    }

    /**
     * Returns the maximum number of characters allowed for an n-gram created or predicted by the utility.
     *
     * @return the maximum number of characters allowed for an n-gram created or predicted by the utility
     */
    public int getMaxNGramLength() {
        return this.maxNGramLength;
    }

    /**
     * Returns the desired filter size to output from the applyNGrams(..) method. This value is meant as an approximation to help limit and optimize the number
     * of n-grams applied to a generated filter. A value less than or equal to the EMPTY_FILTER_SIZE effectively turns off pruning optimizations based on filter
     * size, which could result in unexpectedly large bloom filters.
     *
     * @return desired filter size, in bytes
     */
    public int getOptimumFilterSize() {
        return this.optimumFilterSize;
    }

    /**
     * Create a BloomFilter based on the number of expected insertions
     *
     * @param expectedInsertions
     *            the number of expected insertions
     * @return a wrapped BloomFilter based on the number of expected insertions
     */
    public BloomFilterWrapper newDefaultFilter(int expectedInsertions) {
        int count = expectedInsertions;
        if (count < 0) {
            count = 0;
        }

        final BloomFilter<String> filter = MemberShipTest.create(count);
        return new BloomFilterWrapper(filter, count);
    }

    /**
     * Create a BloomFilter based on a multi-map of fields
     *
     * @param fields
     *            The fields and their values with which to create a bloom filter
     * @return a wrapped BloomFilter based on a multi-map of fields
     */
    public BloomFilterWrapper newMultimapBasedFilter(final Multimap<String,NormalizedContentInterface> fields) {
        // Declare the return value
        final BloomFilter<String> filter;

        // Handle a non-null map of fields
        int fieldsApplied = 0;
        if (null != fields) {
            filter = MemberShipTest.create(fields.size());
            for (final Entry<String,NormalizedContentInterface> e : fields.entries()) {
                MemberShipTest.update(filter, e.getValue().getIndexedFieldValue());
                fieldsApplied++;
            }
        }
        // Handle a null set of fields
        else {
            filter = MemberShipTest.create(fieldsApplied);
        }

        final BloomFilterWrapper wrapper = new BloomFilterWrapper(filter);
        wrapper.setFieldValuesAppliedToFilter(fieldsApplied);
        return wrapper;
    }

    /**
     * Create a new factory instance with a default configuration.
     *
     * @return a bloomfilterutil instance
     */
    public static BloomFilterUtil newInstance() {
        return new BloomFilterUtil(null, 0f, 0f, DiskSpaceStarvationStrategy.DEFAULT_PATH_FOR_DISK_SPACE_VALIDATION, -1);
    }

    /**
     * Creates a new factory instance based on an ingest helper and minimum resource thresholds.
     *
     * @param helper
     *            helper instance for content ingest
     * @param minMemoryThreshold
     *            Minimum amount of available memory, expressed as a percentage, needed to create NGrams
     * @param minDiskSpaceThreshold
     *            Minimum amount of available disk space, expressed as a percentage, needed to create NGrams
     * @param timeoutMillis
     *            maximum allowed time in milliseconds for NGrams to be created. Unlimited time will allowed if specified with a value equal to or less than
     *            zero (0)
     * @param minDiskSpacePath
     *            the min disk space path
     * @return a new factory instance
     */
    public static BloomFilterUtil newInstance(final AbstractContentIngestHelper helper, float minMemoryThreshold, float minDiskSpaceThreshold,
                    final String minDiskSpacePath, int timeoutMillis) {
        return new BloomFilterUtil(helper, minMemoryThreshold, minDiskSpaceThreshold, minDiskSpacePath, timeoutMillis);
    }

    /**
     * Create a BloomFilter based on tokenized n-grams
     *
     * @param fields
     *            The original fields with which to generate n-grams (a.k.a. shingles)
     * @return a wrapped BloomFilter based on tokenized n-grams
     */
    public BloomFilterWrapper newNGramBasedFilter(final Multimap<String,NormalizedContentInterface> fields) {
        // Check for handler and log warning (only once) if missing
        if (!this.missingHelperLogged && (null == this.helper)) {
            final String message = "Unable to create NGrams due to null ContentIngestHelperInterface";
            this.log.warn(message, new IllegalArgumentException());
            this.missingHelperLogged = true;
        }

        // Initialize local variables based on validated fields and internal state
        final BloomFilterWrapper result;
        if ((null != fields) && (null != this.helper)) {
            // Create a top-level tokenization strategy
            int maxAllowedNGrams = this.calculateMaxAllowedNgrams(fields.size());
            final Map<String,String> fieldsToTokenize = new HashMap<>();
            final WeightedValuePruningStrategy pruningStrategy = new WeightedValuePruningStrategy(maxAllowedNGrams, this.maxNGramLength);

            // Initialize basic variables for n-gram creation
            long startTime = System.currentTimeMillis();
            final String tokenFieldNameDesignator = this.helper.getTokenFieldNameDesignator();

            // Add tokenize-able fields to the pruning strategy
            int totalPredictedNgrams = 0;
            if (maxAllowedNGrams != 0) {
                for (final Entry<String,NormalizedContentInterface> entry : fields.entries()) {
                    // Extract the field name and a modified tokenization field name
                    final String fieldName = entry.getValue().getIndexedFieldName();
                    final String modifiedFieldName = fieldName + tokenFieldNameDesignator;

                    // Validate the tokenize-able fields and predict the expected number of n-grams
                    if ((helper.isContentIndexField(fieldName)) || (helper.isReverseContentIndexField(fieldName))) {
                        for (final NormalizedContentInterface nci : fields.get(modifiedFieldName)) {
                            final String fieldValue = nci.getIndexedFieldValue();
                            int count = predictNGramCount(fieldValue, this.maxNGramLength);
                            if (count >= 0) {
                                totalPredictedNgrams += count;
                                pruningStrategy.applyFieldValue(fieldValue);
                                fieldsToTokenize.put(fieldName, modifiedFieldName);
                            }
                        }
                    }
                }
            }

            // Create a bloom filter based on the total expected filter additions, which
            // includes the expected number of n-grams plus field values
            int totalExpectedFilterAdditions = fields.size() + pruningStrategy.getExpectedNGramCount();
            final BloomFilter<String> filter = this.newDefaultFilter(totalExpectedFilterAdditions).getFilter();

            // Create and stack additional layers of tokenization strategies
            final TimeoutStrategy timeoutStrategy = new TimeoutStrategy(startTime, this.maxAllowedExecutionTime);
            final MemoryStarvationStrategy memoryStrategy = new MemoryStarvationStrategy(timeoutStrategy, this.minMemoryThreshold);
            final DiskSpaceStarvationStrategy diskStrategy = new DiskSpaceStarvationStrategy(memoryStrategy, this.minDiskSpaceThreshold, this.minDiskSpacePath);
            pruningStrategy.setSourceStrategy(diskStrategy);
            pruningStrategy.setFilter(filter);

            // Apply all field values to the newly created BloomFilter, plus the n-grams of
            // any identified subset of tokenize-able fields
            int totalAppliedValues = 0;
            int totalAppliedNGrams = 0;
            TimeoutException timeout = null;
            for (final Entry<String,NormalizedContentInterface> entry : fields.entries()) {
                // Get the field name
                final String fieldName = entry.getValue().getIndexedFieldName();

                // Get the field value and apply it to the BloomFilter
                final String fieldValue = entry.getValue().getIndexedFieldValue();
                MemberShipTest.update(filter, fieldValue);
                totalAppliedValues++;

                // If the field is tokenize-able, get the modified field name
                // and look up its normalized content
                final String modifiedFieldName = fieldsToTokenize.get(fieldName);
                final Collection<NormalizedContentInterface> ncis;
                if (null != modifiedFieldName) {
                    ncis = fields.get(modifiedFieldName);
                } else {
                    ncis = Collections.emptyList();
                }

                // If defined as a non-empty collection, tokenize the normalized content
                // into n-grams and apply to the filter
                if ((null != ncis) && !ncis.isEmpty() && (null == timeout)) {
                    try {
                        totalAppliedNGrams += this.applyNgrams(fieldName, ncis, pruningStrategy);
                    } catch (final TokenizationException e) {
                        if (e.getCause() instanceof IOException) {
                            this.log.error(e);
                        } else if (e.getCause() instanceof TimeoutException) {
                            timeout = (TimeoutException) e.getCause();
                        }

                        this.log.warn("Problem creating n-grams", e);
                        totalAppliedNGrams += e.getNgramCount();
                    }
                }
            }

            // Determine the numbers of applied and pruned n-grams
            totalAppliedValues += totalAppliedNGrams;

            // Create the result and add the tokenization/pruning information
            result = new BloomFilterWrapper(filter);
            result.setFieldValuesAppliedToFilter(totalAppliedValues - totalAppliedNGrams);
            result.setNGramsAppliedToFilter(totalAppliedNGrams);
            result.setNGramsPrunedFromFilter(totalPredictedNgrams - totalAppliedNGrams);
        } else {
            result = this.newMultimapBasedFilter(fields);
        }

        return result;
    }

    /**
     * Approximates the maximum number of tokens that can be applied to a bloom filter based on a maximum optimum filter size. This value is assumed to include
     * n-grams and original field values.
     *
     * @param desiredFilterSizeInBytes
     *            the maximum desired filter size (measured in bytes)
     * @return the approximate number of tokens that will be applied to a bloom filter based on the specified byte size
     */
    public static int predictMaxFilterAdditions(int desiredFilterSizeInBytes) {
        return Math.round(FILTER_SIZE_TO_NGRAM_COUNT_FACTOR * ((float) (desiredFilterSizeInBytes - EMPTY_FILTER_SIZE)));
    }

    /**
     * Predicts the total number of n-grams created by the Apache {@link NGramTokenizer} based on a given field value and maximum token length.
     * <p>
     * Note that this total count does NOT include the field value itself, even though such a field value is applied to and increases the total size of a bloom
     * filter when created through the newNGramBasedFilter(..) method. In other words, a predicted n-gram count calculated by itself will be 1 count less that
     * the overall number of n-grams actually created through the factory method.
     *
     * @param fieldValue
     *            The field value
     * @return the predicted number of n-grams
     */
    public static int predictNGramCount(final String fieldValue) {
        return predictNGramCount(fieldValue, AbstractNGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
    }

    /**
     * Predicts the total number of n-grams created by the Apache {@link NGramTokenizer} based on a given field value and maximum token length based to the
     * following formula, where f = the number of characters in the field value, and n = the number of created n-grams: <br>
     *
     * <pre>
     * n = (f(f - 1)) / 2
     * </pre>
     *
     * <br>
     * Note that this total count DOES include the field value itself, even though such a value isn't actually output by the {@link NGramTokenizer}.
     *
     * @param fieldValue
     *            The field value
     * @param maxNGramLength
     *            The maximum length of a generated n-gram, which overrides the value otherwise specified by setMaxNGramLength(int);
     * @return the predicted number of n-grams
     */
    public static int predictNGramCount(final String fieldValue, int maxNGramLength) {
        // Declare the return value
        int predictedNGramCount;

        // Validate against null and empty strings
        if ((null != fieldValue) && !fieldValue.isEmpty()) {
            // Predict the number of n-grams based purely on a summation of the number of characters
            int fieldValueLength = fieldValue.trim().length();
            predictedNGramCount = predictNGramCount(fieldValueLength, maxNGramLength);
        } else {
            predictedNGramCount = 0;
        }

        // Ensure only a zero or positive integer is returned
        if (predictedNGramCount < 0) {
            predictedNGramCount = 0;
        }

        return predictedNGramCount;
    }

    /**
     * Predicts the total number of n-grams created by the Apache {@link NGramTokenizer} based on a given field value and maximum token length based to the
     * following formula, where f = the number of characters in the field value, and n = the number of created n-grams: <br>
     *
     * <pre>
     * n = (f(f - 1)) / 2
     * </pre>
     *
     * <br>
     * Note that this total count DOES include the field value itself, even though such a value isn't actually output by the {@link NGramTokenizer}.
     *
     * @param fieldValueLength
     *            The field value's length
     * @param maxNGramLength
     *            The maximum length of a generated n-gram, which overrides the value otherwise specified by setMaxNGramLength(int);
     * @return the predicted number of n-grams
     */
    public static int predictNGramCount(float fieldValueLength, int maxNGramLength) {
        // Declare the return value
        float predictedNGramCount;

        // Validate against null and empty strings
        if (fieldValueLength > 0) {
            // Predict the number of n-grams based purely on a summation of the number of characters
            predictedNGramCount = (((fieldValueLength - 1.0f) * fieldValueLength) / 2.0f);

            // Calculate an adjustment if the max n-gram length is exceeded
            float adjustmentDueToMaxLength = 0;
            if (fieldValueLength > maxNGramLength) {
                adjustmentDueToMaxLength = ((fieldValueLength - maxNGramLength) * (fieldValueLength - maxNGramLength + 1)) / 2.0f;
            }

            // Adjust the predicted count based on the max length adjustment
            predictedNGramCount = predictedNGramCount - adjustmentDueToMaxLength;
        } else {
            predictedNGramCount = 0;
        }

        // Ensure only a zero or positive integer is returned
        if (predictedNGramCount < 0) {
            predictedNGramCount = 0;
        }

        return Math.round(predictedNGramCount);
    }

    /**
     * Sets the maximum number of characters allowed for an n-gram created or predicted by the utility.
     *
     * @param maxNGramLength
     *            the maximum number of characters allowed for an n-gram created or predicted by the utility
     */
    public void setMaxNGramLength(int maxNGramLength) {
        this.maxNGramLength = maxNGramLength;
    }

    /**
     * Sets the desired filter size to output from the applyNGrams(..) method. This value is meant as an approximation to help limit and optimize the number of
     * n-grams applied to a generated filter. A value less than or equal to the EMPTY_FILTER_SIZE effectively turns off pruning optimizations based on filter
     * size, which could result in unexpectedly large bloom filters.
     *
     * @param sizeInBytes
     *            desired filter size
     */
    public void setOptimumFilterSize(int sizeInBytes) {
        this.optimumFilterSize = sizeInBytes;
    }
}
