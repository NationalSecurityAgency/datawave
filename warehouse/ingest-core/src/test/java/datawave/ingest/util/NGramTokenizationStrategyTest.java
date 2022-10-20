package datawave.ingest.util;

import com.google.common.hash.BloomFilter;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.util.DiskSpaceStarvationStrategy.LowDiskSpaceException;
import datawave.ingest.util.MemoryStarvationStrategy.LowMemoryException;
import datawave.ingest.util.TimeoutStrategy.TimeoutException;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.when;

public class NGramTokenizationStrategyTest {

    BloomFilter<String> filter;

    Logger logger;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setup() throws Exception {
        filter = Mockito.mock(BloomFilter.class);
        logger = Mockito.mock(Logger.class);
    }

    private NormalizedFieldAndValue createNormalizedFieldAndValue(int counter, int length) {
        StringBuilder builder = new StringBuilder();
        char character = 'a';
        for (int i = 0; i < length; i++) {
            builder.append(character);
            character++;
            if (character > 'Z') {
                character = 'a';
            }
        }

        return new NormalizedFieldAndValue("TEST_" + counter, builder.toString());
    }

    @Test
    public void testTokenize_NoPruning() throws Exception {
        // Create test input
        final NormalizedContentInterface nci = new NormalizedFieldAndValue("TEST", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");

        // Calculate the expected number of n-grams
        final String fieldValue = nci.getIndexedFieldValue();
        int expectedNGramCount = BloomFilterUtil.predictNGramCount(fieldValue, NGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);

        // Set expectations
        when(this.filter.apply(isA(String.class))).thenReturn(true);

        // Run the test
        NGramTokenizationStrategy subject = new NGramTokenizationStrategy(this.filter);
        int result1 = subject.tokenize(nci, NGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);

        // Verify results
        assertEquals(expectedNGramCount, result1, "Should have tokenized and applied " + expectedNGramCount + " n-grams to the bloom filter");
    }

    @Test
    public void testTokenize_FilterSizeBasedPruning() throws Exception {
        // Create test input
        final NormalizedContentInterface nci = new NormalizedFieldAndValue("TEST", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");

        // Calculate the expected number of n-grams
        final String fieldValue = nci.getIndexedFieldValue();
        int expectedNGramCount = BloomFilterUtil.predictNGramCount(fieldValue, AbstractNGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);

        // Set expectations
        when(this.filter.apply(isA(String.class))).thenReturn(true);

        // Run the test
        NGramTokenizationStrategy subject = new NGramTokenizationStrategy(this.filter);
        int result1 = subject.tokenize(nci, AbstractNGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);

        // Verify results
        assertEquals(expectedNGramCount, result1, "Should have tokenized and applied " + expectedNGramCount + " n-grams to the bloom filter");
    }

    @Test
    public void testTokenize_LowMemory() throws Exception {
        // Create test input
        final NormalizedContentInterface nci = new NormalizedFieldAndValue("TEST", "test");

        // Calculate the expected number of n-grams
        final String fieldValue = nci.getIndexedFieldValue();
        int expectedNGramCount = BloomFilterUtil.predictNGramCount(fieldValue, MemoryStarvationStrategy.DEFAULT_MAX_NGRAM_LENGTH);


        try (MockedStatic<Logger> l = Mockito.mockStatic(Logger.class)) {
            l.when(() -> Logger.getLogger(isA(Class.class))).thenReturn(this.logger);
            try (MockedStatic<ResourceAvailabilityUtil> util = Mockito.mockStatic(ResourceAvailabilityUtil.class)) {
                util.when(() -> ResourceAvailabilityUtil.isMemoryAvailable(.05f)).thenReturn(true);
                util.when(() -> ResourceAvailabilityUtil.isMemoryAvailable(.05f)).thenReturn(false);
                 this.logger.warn(isA(String.class), isA(LowMemoryException.class));

                // Run the test
                MemoryStarvationStrategy subject = new MemoryStarvationStrategy(this.filter, .05f);
                NGramTokenizationStrategy strategy = new NGramTokenizationStrategy(subject);

                int result1 = strategy.tokenize(nci, MemoryStarvationStrategy.DEFAULT_MAX_NGRAM_LENGTH);

                // Verify results
                assertEquals(expectedNGramCount, result1, "Strategy should have logged a low memory exception but still tokenized n-grams");
            }
        }
    }

    @Test
    public void testTokenize_LowDiskSpace() throws Exception {
        // Create test input
        final NormalizedContentInterface nci = new NormalizedFieldAndValue("TEST", "test");

        // Calculate the expected number of n-grams
        final String fieldValue = nci.getIndexedFieldValue();
        int expectedNGramCount = BloomFilterUtil.predictNGramCount(fieldValue, DiskSpaceStarvationStrategy.DEFAULT_MAX_NGRAM_LENGTH);

        // Run the test
        try (MockedStatic<Logger> l = Mockito.mockStatic(Logger.class)) {
            l.when(() -> Logger.getLogger(isA(Class.class))).thenReturn(this.logger);
            try (MockedStatic<ResourceAvailabilityUtil> util = Mockito.mockStatic(ResourceAvailabilityUtil.class)) {
                util.when(() -> ResourceAvailabilityUtil.isDiskAvailable("/", .05f)).thenReturn(true);
                util.when(() -> ResourceAvailabilityUtil.isDiskAvailable("/", .05f)).thenReturn(false);
                this.logger.warn(isA(String.class), isA(LowDiskSpaceException.class));
                DiskSpaceStarvationStrategy subject = new DiskSpaceStarvationStrategy(this.filter, .05f, "/");
                NGramTokenizationStrategy strategy = new NGramTokenizationStrategy(subject);
                int result1 = strategy.tokenize(nci, DiskSpaceStarvationStrategy.DEFAULT_MAX_NGRAM_LENGTH);

                // Verify results
                assertEquals(expectedNGramCount, result1, "Strategy should have logged a low disk exception but still tokenized n-grams");
            }
        }
    }

    @Test
    public void testTokenize_Timeout() throws Exception {
        // Create test input
        final NormalizedContentInterface nci = new NormalizedFieldAndValue("TEST", "test");

        // Set expectations
        when(this.filter.apply(isA(String.class))).thenReturn(true); // Only first token is applied before a thread sleep
        // in the substrategy triggers a TimeoutException to be
        // thrown by the parent TimeoutTokenizationStrategy

        // Run the test
        SimulatedProcessingDelayStrategy delayStrategy = new SimulatedProcessingDelayStrategy(this.filter, 0);
        TimeoutStrategy subject = new TimeoutStrategy(delayStrategy, System.currentTimeMillis(), 200);
        NGramTokenizationStrategy strategy = new NGramTokenizationStrategy(subject);
        TimeoutException result1 = null;
        try {
            strategy.tokenize(nci, TimeoutStrategy.DEFAULT_MAX_NGRAM_LENGTH);
        } catch (TimeoutException e) {
            result1 = e;
        }

        // Verify results
        assertNotNull(result1, "Strategy should have thrown a timeout exception");
    }

    @Test
    public void testTokenize_WeightedLengthPruningWithUndefinedMaxNGramCount() throws Exception {
        // Create test input
        final Vector<NormalizedContentInterface> ncis = new Vector<>();
        int fieldNameExtension = 1;
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));

        // Determine max allowed n-grams based on desired filter size
        int maxAllowedNgrams = -1;

        // Calculate the expected number of n-grams
        int expectedNGramCount = 0;
        for (final NormalizedContentInterface nci : ncis) {
            expectedNGramCount += BloomFilterUtil.predictNGramCount(nci.getIndexedFieldValue());
        }

        // Set expectations
        when(this.filter.apply(isA(String.class))).thenReturn(true);

        // Run the test
        WeightedValuePruningStrategy subject = new WeightedValuePruningStrategy(maxAllowedNgrams);
        for (final NormalizedContentInterface nci : ncis) {
            if (nci.getIndexedFieldValue().length() == 2) {
                subject.applyFieldValue(nci.getIndexedFieldValue(), true); // Apply with weighting recalculation
            } else {
                subject.applyFieldValue(nci.getIndexedFieldValue()); // Apply without weighting recalculation
            }
        }

        BloomFilter<String> result1 = subject.newFilter(ncis.size()); // Create filter, which forces weighting calculation
        subject.setFilter(this.filter); // Set the new filter

        Map<String,Integer> result3 = new HashMap<>();
        int result2 = 0;
        for (final NormalizedContentInterface nci : ncis) { // Tokenize each field value
            int tokenized = subject.tokenize(nci, AbstractNGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
            result2 += tokenized;
            result3.put(nci.getIndexedFieldName(), tokenized);
        }

        // Verify results
        assertNotNull(result1, "Should have created a non-null filter");

        assertEquals(expectedNGramCount, result2, "Should have applied " + expectedNGramCount + " n-grams to the bloom filter");

        String fieldName = ncis.lastElement().getIndexedFieldName();
        int expectedCount = BloomFilterUtil.predictNGramCount(ncis.lastElement().getIndexedFieldValue());
        assertTrue(
                (null != result3.get(fieldName)) && (result3.get(fieldName) == expectedCount), "Should NOT have pruned the n-grams tokenized for field " + fieldName + " but got " + result3.get(fieldName));

    }

    @Test
    public void testTokenize_WeightedLengthPruningWithAllUnderweightValues() throws Exception {
        // Create test input
        final Vector<NormalizedContentInterface> ncis = new Vector<>();
        int fieldNameExtension = 1;
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 5));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 4));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 5));

        // Determine max allowed n-grams based on desired filter size
        int idealFilterSize = 2500;
        int maxAllowedNgrams = BloomFilterUtil.predictMaxFilterAdditions(idealFilterSize) - ncis.size();

        // Calculate the expected number of n-grams
        int expectedNGramCount = 0;
        for (final NormalizedContentInterface nci : ncis) {
            expectedNGramCount += BloomFilterUtil.predictNGramCount(nci.getIndexedFieldValue());
        }
        expectedNGramCount = (expectedNGramCount > maxAllowedNgrams) ? maxAllowedNgrams : expectedNGramCount;

        // Set expectations
        when(this.filter.apply(isA(String.class))).thenReturn(true);

        // Run the test
        WeightedValuePruningStrategy subject = new WeightedValuePruningStrategy(maxAllowedNgrams);
        for (final NormalizedContentInterface nci : ncis) {
            if (nci.getIndexedFieldValue().length() == 2) {
                subject.applyFieldValue(nci.getIndexedFieldValue(), true); // Apply with weighting recalculation
            } else {
                subject.applyFieldValue(nci.getIndexedFieldValue()); // Apply without weighting recalculation
            }
        }

        BloomFilter<String> result1 = subject.newFilter(ncis.size()); // Create filter, which forces weighting calculation
        subject.setFilter(this.filter); // Set the new filter

        Map<String,Integer> result3 = new HashMap<>();
        int result2 = 0;
        for (final NormalizedContentInterface nci : ncis) { // Tokenize each field value
            int tokenized = subject.tokenize(nci, AbstractNGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
            result2 += tokenized;
            result3.put(nci.getIndexedFieldName(), tokenized);
        }

        // Verify results
        assertNotNull(result1, "Should have created a non-null filter");

        assertTrue((result2 > expectedNGramCount - 3)
                && (result2 < idealFilterSize), "Should have applied approximately " + expectedNGramCount + " n-grams to the bloom filter");

        String fieldName = ncis.lastElement().getIndexedFieldName();
        int expectedCount = BloomFilterUtil.predictNGramCount(ncis.lastElement().getIndexedFieldValue());
        assertTrue(
                (null != result3.get(fieldName)) && (result3.get(fieldName) == expectedCount), "Should NOT have pruned the n-grams tokenized for field " + fieldName + " but got " + result3.get(fieldName));

    }

    @Test
    public void testTokenize_WeightedLengthPruningWithMinorityOfOverweightValues() throws Exception {
        // Create test input
        final Vector<NormalizedContentInterface> ncis = new Vector<>();
        int fieldNameExtension = 1;
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 5));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 4));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 5));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 2));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 4));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 7));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 5));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 4));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 2));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 6));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 4));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 23));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 23));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 23));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 23));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 23));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 23));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 10));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 23));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 80));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 80));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 80));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 80));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 80));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 674));

        // Determine max allowed n-grams based on desired filter size
        int idealFilterSize = 2500;
        int maxAllowedNgrams = BloomFilterUtil.predictMaxFilterAdditions(idealFilterSize) - ncis.size();

        // Calculate the expected number of n-grams
        int expectedNGramCount = 0;
        for (final NormalizedContentInterface nci : ncis) {
            expectedNGramCount += BloomFilterUtil.predictNGramCount(nci.getIndexedFieldValue());
        }
        expectedNGramCount = (expectedNGramCount > maxAllowedNgrams) ? maxAllowedNgrams : expectedNGramCount;

        // Set expectations
        when(this.filter.apply(isA(String.class))).thenReturn(true);

        // Run the test
        WeightedValuePruningStrategy subject = new WeightedValuePruningStrategy(maxAllowedNgrams);
        for (final NormalizedContentInterface nci : ncis) {
            if (nci.getIndexedFieldValue().length() == 2) {
                subject.applyFieldValue(nci.getIndexedFieldValue(), true); // Apply with weighting recalculation
            } else {
                subject.applyFieldValue(nci.getIndexedFieldValue()); // Apply without weighting recalculation
            }
        }

        BloomFilter<String> result1 = subject.newFilter(ncis.size()); // Create filter, which forces weighting calculation
        subject.setFilter(this.filter); // Set the new filter

        Map<String,Integer> result3 = new HashMap<>();
        int result2 = 0;
        for (final NormalizedContentInterface nci : ncis) { // Tokenize each field value
            int tokenized = subject.tokenize(nci, AbstractNGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
            result2 += tokenized;
            result3.put(nci.getIndexedFieldName(), tokenized);
        }

        // Verify results
        assertNotNull(result1, "Should have created a non-null filter");

        /*
         * Debugging note: Recent changes to Java package names in this project mysteriously and unfortunately had the side effect of breaking the original
         * lower-bound assertion below, ie "(result2 > expectedNGramCount - 3)"
         *
         * Turns out, the value of 'expectedNGramCount' depends on BloomFilter object serialization within BloomFilterUtil. The purpose of that serialization is
         * to calculate BloomFilterUtil.EMPTY_FILTER_SIZE (in bytes), thus, EMPTY_FILTER_SIZE and 'expectedNGramCount' are directly affected by the length of
         * BloomFilter's fully-qualified name. In this case, a 4-character difference in the length of the new name was enough to break the test. Hopefully, the
         * revision here will make debugging this issue easier in the future.
         */
        final int result2LowerBound = expectedNGramCount - 5;
        assertTrue(result2 > result2LowerBound, "result2 (" + result2 + ") should have been greater than " + result2LowerBound);
        assertTrue(result2 < idealFilterSize, "result2 (" + result2 + ") should have been less than " + idealFilterSize);

        String fieldName = ncis.lastElement().getIndexedFieldName();
        int expectedCount = BloomFilterUtil.predictNGramCount(ncis.lastElement().getIndexedFieldValue());
        assertTrue((null != result3.get(fieldName))
                && (result3.get(fieldName) < expectedCount), "Should have pruned the n-grams tokenized for field " + fieldName + " but got " + result3.get(fieldName));
    }

    @Test
    public void testTokenize_WeightedLengthPruningWithMajorityOfOverweightValues() throws Exception {
        // Create test input
        final Vector<NormalizedContentInterface> ncis = new Vector<>();
        int fieldNameExtension = 1;
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 3));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 4));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 67));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 68));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 69));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 70));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 71));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 72));

        // Determine max allowed n-grams based on desired filter size
        int idealFilterSize = 2500;
        int maxAllowedNgrams = BloomFilterUtil.predictMaxFilterAdditions(idealFilterSize) - ncis.size();

        // Calculate the expected number of n-grams
        int expectedNGramCount = 0;
        for (final NormalizedContentInterface nci : ncis) {
            expectedNGramCount += BloomFilterUtil.predictNGramCount(nci.getIndexedFieldValue());
        }
        expectedNGramCount = (expectedNGramCount > maxAllowedNgrams) ? maxAllowedNgrams : expectedNGramCount;

        // Set expectations
        when(this.filter.apply(isA(String.class))).thenReturn(true);

        // Run the test
        WeightedValuePruningStrategy subject = new WeightedValuePruningStrategy(maxAllowedNgrams);
        for (final NormalizedContentInterface nci : ncis) {
            if (nci.getIndexedFieldValue().length() == 2) {
                subject.applyFieldValue(nci.getIndexedFieldValue(), true); // Apply with weighting recalculation
            } else {
                subject.applyFieldValue(nci.getIndexedFieldValue()); // Apply without weighting recalculation
            }
        }

        BloomFilter<String> result1 = subject.newFilter(ncis.size()); // Create filter, which forces weighting calculation
        subject.setFilter(this.filter); // Set the new filter

        Map<String,Integer> result3 = new HashMap<>();
        int result2 = 0;
        for (final NormalizedContentInterface nci : ncis) { // Tokenize each field value
            int tokenized = subject.tokenize(nci, AbstractNGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
            result2 += tokenized;
            result3.put(nci.getIndexedFieldName(), tokenized);
        }

        // Verify results
        assertNotNull(result1, "Should have created a non-null filter");

        assertTrue((result2 > expectedNGramCount - 20)
                && (result2 < idealFilterSize), "Should have applied approximately " + expectedNGramCount + " n-grams to the bloom filter");

        String fieldName = ncis.lastElement().getIndexedFieldName();
        int expectedCount = BloomFilterUtil.predictNGramCount(ncis.lastElement().getIndexedFieldValue());
        assertTrue((null != result3.get(fieldName))
                && (result3.get(fieldName) < expectedCount), "Should have pruned the n-grams tokenized for field " + fieldName + " but got " + result3.get(fieldName));

    }

    @Test
    public void testTokenize_StrategyStack() throws Exception {
        // Create test input
        final Vector<NormalizedContentInterface> ncis = new Vector<>();
        int fieldNameExtension = 1;
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 5));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 4));
        ncis.add(this.createNormalizedFieldAndValue(fieldNameExtension++, 5));

        // Determine max allowed n-grams based on desired filter size
        int idealFilterSize = 2000;
        int maxAllowedNgrams = BloomFilterUtil.predictMaxFilterAdditions(idealFilterSize) - ncis.size();

        // Calculate the expected number of n-grams
        int expectedNGramCount = 0;
        for (final NormalizedContentInterface nci : ncis) {
            expectedNGramCount += BloomFilterUtil.predictNGramCount(nci.getIndexedFieldValue());
        }
        expectedNGramCount = (expectedNGramCount > maxAllowedNgrams) ? maxAllowedNgrams : expectedNGramCount;

        // Determine n-gram at which to introduce timeout
        int timeoutAfterNGramCount = BloomFilterUtil.predictNGramCount(ncis.iterator().next().getIndexedFieldValue());

        try (MockedStatic<Logger> l = Mockito.mockStatic(Logger.class)) {
            l.when(() -> Logger.getLogger(isA(Class.class))).thenReturn(this.logger);
            try (MockedStatic<ResourceAvailabilityUtil> util = Mockito.mockStatic(ResourceAvailabilityUtil.class)) {
                util.when(() -> ResourceAvailabilityUtil.isDiskAvailable("/", .05f)).thenReturn(true);
                util.when(() -> ResourceAvailabilityUtil.isDiskAvailable("/", .05f)).thenReturn(false);
                when(this.filter.apply(isA(String.class))).thenReturn(true);
                this.logger.warn(isA(String.class), isA(LowDiskSpaceException.class));


                // Run the test
                WeightedValuePruningStrategy subject = new WeightedValuePruningStrategy(maxAllowedNgrams);
                for (final NormalizedContentInterface nci : ncis) {
                    if (nci.getIndexedFieldValue().length() == 2) {
                        subject.applyFieldValue(nci.getIndexedFieldValue(), true); // Apply with weighting recalculation
                    } else {
                        subject.applyFieldValue(nci.getIndexedFieldValue()); // Apply without weighting recalculation
                    }
                }

                int result1 = subject.getExpectedNGramCount(); // Create filter, which forces weighting calculation

                SimulatedProcessingDelayStrategy delayStrategy = new SimulatedProcessingDelayStrategy(this.filter, timeoutAfterNGramCount);
                TimeoutStrategy timeoutStrategy = new TimeoutStrategy(delayStrategy, System.currentTimeMillis(), 200);
                MemoryStarvationStrategy memoryStrategy = new MemoryStarvationStrategy(timeoutStrategy, .05f);
                DiskSpaceStarvationStrategy diskSpaceStrategy = new DiskSpaceStarvationStrategy(memoryStrategy, .05f, "/");
                subject.setSourceStrategy(diskSpaceStrategy);
                subject.setFilter(this.filter); // Set the new filter

                int result2 = 0;
                Map<String, Integer> result3 = new HashMap<>();
                Exception result4 = null;
                try {
                    for (final NormalizedContentInterface nci : ncis) { // Tokenize each field value
                        int tokenized = subject.tokenize(nci, AbstractNGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
                        result2 += tokenized;
                        result3.put(nci.getIndexedFieldName(), tokenized);
                    }
                } catch (TimeoutException e) {
                    result4 = e;
                }

                // Verify results
                assertEquals(expectedNGramCount, result1, "Predicted n-grams should not have been pruned");

                assertEquals(timeoutAfterNGramCount, result2, "Should have applied " + timeoutAfterNGramCount + " n-grams to the bloom filter");

                assertNotNull(result4, "Should have caught a timeout exception");
            }
        }
    }

    private class SimulatedProcessingDelayStrategy extends AbstractNGramTokenizationStrategy {
        int sleepAtUpdate = 0;
        int updateCount = 0;

        public SimulatedProcessingDelayStrategy(final BloomFilter<String> filter, int sleepAtUpdate) {
            super(filter);
            this.sleepAtUpdate = sleepAtUpdate;
        }

        @Override
        public boolean updateFilter(String ngram, NormalizedContentInterface content) throws TokenizationException {
            if (this.updateCount >= this.sleepAtUpdate) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Thread interrupted unexpectedly during test execution", e);
                }
            }

            this.updateCount++;

            return false;
        }

    }
}