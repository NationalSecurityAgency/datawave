package datawave.ingest.util;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.mapreduce.MemberShipTest;
import datawave.ingest.util.DiskSpaceStarvationStrategy.LowDiskSpaceException;
import datawave.ingest.util.MemoryStarvationStrategy.LowMemoryException;
import datawave.ingest.util.TimeoutStrategy.TimeoutException;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.hash.BloomFilter;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ResourceAvailabilityUtil.class, Logger.class, MemberShipTest.class, BloomFilter.class})
public class NGramTokenizationStrategyTest {
    
    BloomFilter<String> filter;
    
    Logger logger;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {
        filter = PowerMock.createMock(BloomFilter.class);
        logger = PowerMock.createMock(Logger.class);
    }
    
    /**
     * Unverified expectations were bleeding over from other tests, such as the MainServletTest or MultiThreadedEventSequenceFileCombiningPollManagerTest. Since
     * the developer was not familiar with the unrelated, yet interfering test classes, this test method was added here to handle the other tests' "unfulfilled"
     * expectations, swallow their misleading errors, and prevent this test class from blowing up.
     */
    @Test
    public void testNothingReally_ButPreventMisleadingVerificationErrorsFromInterferingTestClasses() {
        try {
            PowerMock.replayAll();
        } catch (Throwable e) {
            // No op
        }
        
        try {
            PowerMock.verifyAll();
        } catch (Throwable e) {
            // No op
        }
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
        expect(this.filter.apply(isA(String.class))).andReturn(true).times(expectedNGramCount);
        
        // Run the test
        PowerMock.replayAll();
        NGramTokenizationStrategy subject = new NGramTokenizationStrategy(this.filter);
        int result1 = subject.tokenize(nci, NGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
        PowerMock.verifyAll();
        
        // Verify results
        assertEquals("Should have tokenized and applied " + expectedNGramCount + " n-grams to the bloom filter", expectedNGramCount, result1);
    }
    
    @Test
    public void testTokenize_FilterSizeBasedPruning() throws Exception {
        // Create test input
        final NormalizedContentInterface nci = new NormalizedFieldAndValue("TEST", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
        
        // Calculate the expected number of n-grams
        final String fieldValue = nci.getIndexedFieldValue();
        int expectedNGramCount = BloomFilterUtil.predictNGramCount(fieldValue, AbstractNGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
        
        // Set expectations
        expect(this.filter.apply(isA(String.class))).andReturn(true).times(expectedNGramCount);
        
        // Run the test
        PowerMock.replayAll();
        NGramTokenizationStrategy subject = new NGramTokenizationStrategy(this.filter);
        int result1 = subject.tokenize(nci, AbstractNGramTokenizationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
        PowerMock.verifyAll();
        
        // Verify results
        assertEquals("Should have tokenized and applied " + expectedNGramCount + " n-grams to the bloom filter", expectedNGramCount, result1);
    }
    
    @Test
    public void testTokenize_LowMemory() throws Exception {
        // Create test input
        final NormalizedContentInterface nci = new NormalizedFieldAndValue("TEST", "test");
        
        // Calculate the expected number of n-grams
        final String fieldValue = nci.getIndexedFieldValue();
        int expectedNGramCount = BloomFilterUtil.predictNGramCount(fieldValue, MemoryStarvationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
        
        // Set expectations
        PowerMock.mockStaticPartial(Logger.class, "getLogger");
        expect(Logger.getLogger(isA(Class.class))).andReturn(this.logger).anyTimes();
        PowerMock.mockStaticPartial(ResourceAvailabilityUtil.class, "isMemoryAvailable");
        expect(ResourceAvailabilityUtil.isMemoryAvailable(.05f)).andReturn(true);
        PowerMock.mockStaticPartial(ResourceAvailabilityUtil.class, "isMemoryAvailable");
        expect(ResourceAvailabilityUtil.isMemoryAvailable(.05f)).andReturn(false);
        this.logger.warn(isA(String.class), isA(LowMemoryException.class));
        
        // Run the test
        PowerMock.replayAll();
        MemoryStarvationStrategy subject = new MemoryStarvationStrategy(this.filter, .05f);
        NGramTokenizationStrategy strategy = new NGramTokenizationStrategy(subject);
        
        int result1 = strategy.tokenize(nci, MemoryStarvationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
        PowerMock.verifyAll();
        
        // Verify results
        assertEquals("Strategy should have logged a low memory exception but still tokenized n-grams", expectedNGramCount, result1);
    }
    
    @Test
    public void testTokenize_LowDiskSpace() throws Exception {
        // Create test input
        final NormalizedContentInterface nci = new NormalizedFieldAndValue("TEST", "test");
        
        // Calculate the expected number of n-grams
        final String fieldValue = nci.getIndexedFieldValue();
        int expectedNGramCount = BloomFilterUtil.predictNGramCount(fieldValue, DiskSpaceStarvationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
        
        // Set expectations
        PowerMock.mockStaticPartial(Logger.class, "getLogger");
        expect(Logger.getLogger(isA(Class.class))).andReturn(this.logger).anyTimes();
        PowerMock.mockStaticPartial(ResourceAvailabilityUtil.class, "isDiskAvailable");
        expect(ResourceAvailabilityUtil.isDiskAvailable("/", .05f)).andReturn(true);
        PowerMock.mockStaticPartial(ResourceAvailabilityUtil.class, "isDiskAvailable");
        expect(ResourceAvailabilityUtil.isDiskAvailable("/", .05f)).andReturn(false);
        this.logger.warn(isA(String.class), isA(LowDiskSpaceException.class));
        
        // Run the test
        PowerMock.replayAll();
        DiskSpaceStarvationStrategy subject = new DiskSpaceStarvationStrategy(this.filter, .05f, "/");
        NGramTokenizationStrategy strategy = new NGramTokenizationStrategy(subject);
        int result1 = strategy.tokenize(nci, DiskSpaceStarvationStrategy.DEFAULT_MAX_NGRAM_LENGTH);
        PowerMock.verifyAll();
        
        // Verify results
        assertEquals("Strategy should have logged a low disk exception but still tokenized n-grams", expectedNGramCount, result1);
    }
    
    @Test
    public void testTokenize_Timeout() throws Exception {
        // Create test input
        final NormalizedContentInterface nci = new NormalizedFieldAndValue("TEST", "test");
        
        // Set expectations
        expect(this.filter.apply(isA(String.class))).andReturn(true); // Only first token is applied before a thread sleep
                                                                      // in the substrategy triggers a TimeoutException to be
                                                                      // thrown by the parent TimeoutTokenizationStrategy
        
        // Run the test
        PowerMock.replayAll();
        SimulatedProcessingDelayStrategy delayStrategy = new SimulatedProcessingDelayStrategy(this.filter, 0);
        TimeoutStrategy subject = new TimeoutStrategy(delayStrategy, System.currentTimeMillis(), 200);
        NGramTokenizationStrategy strategy = new NGramTokenizationStrategy(subject);
        TimeoutException result1 = null;
        try {
            strategy.tokenize(nci, TimeoutStrategy.DEFAULT_MAX_NGRAM_LENGTH);
        } catch (TimeoutException e) {
            result1 = e;
        }
        PowerMock.verifyAll();
        
        // Verify results
        assertNotNull("Strategy should have thrown a timeout exception", result1);
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
        expect(this.filter.apply(isA(String.class))).andReturn(true).times(expectedNGramCount);
        
        // Run the test
        PowerMock.replayAll();
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
        PowerMock.verifyAll();
        
        // Verify results
        assertNotNull("Should have created a non-null filter", result1);
        
        assertEquals("Should have applied " + expectedNGramCount + " n-grams to the bloom filter", expectedNGramCount, result2);
        
        String fieldName = ncis.lastElement().getIndexedFieldName();
        int expectedCount = BloomFilterUtil.predictNGramCount(ncis.lastElement().getIndexedFieldValue());
        assertTrue("Should NOT have pruned the n-grams tokenized for field " + fieldName + " but got " + result3.get(fieldName),
                        (null != result3.get(fieldName)) && (result3.get(fieldName) == expectedCount));
        
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
        expect(this.filter.apply(isA(String.class))).andReturn(true).times(expectedNGramCount - 4, idealFilterSize); // Allow for rounding errors
        
        // Run the test
        PowerMock.replayAll();
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
        PowerMock.verifyAll();
        
        // Verify results
        assertNotNull("Should have created a non-null filter", result1);
        
        assertTrue("Should have applied approximately " + expectedNGramCount + " n-grams to the bloom filter", (result2 > expectedNGramCount - 3)
                        && (result2 < idealFilterSize));
        
        String fieldName = ncis.lastElement().getIndexedFieldName();
        int expectedCount = BloomFilterUtil.predictNGramCount(ncis.lastElement().getIndexedFieldValue());
        assertTrue("Should NOT have pruned the n-grams tokenized for field " + fieldName + " but got " + result3.get(fieldName),
                        (null != result3.get(fieldName)) && (result3.get(fieldName) == expectedCount));
        
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
        expect(this.filter.apply(isA(String.class))).andReturn(true).times(expectedNGramCount - 4, idealFilterSize); // Allow for rounding errors
        
        // Run the test
        PowerMock.replayAll();
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
        PowerMock.verifyAll();
        
        // Verify results
        assertNotNull("Should have created a non-null filter", result1);
        
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
        assertTrue("result2 (" + result2 + ") should have been greater than " + result2LowerBound, result2 > result2LowerBound);
        assertTrue("result2 (" + result2 + ") should have been less than " + idealFilterSize, result2 < idealFilterSize);
        
        String fieldName = ncis.lastElement().getIndexedFieldName();
        int expectedCount = BloomFilterUtil.predictNGramCount(ncis.lastElement().getIndexedFieldValue());
        assertTrue("Should have pruned the n-grams tokenized for field " + fieldName + " but got " + result3.get(fieldName), (null != result3.get(fieldName))
                        && (result3.get(fieldName) < expectedCount));
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
        expect(this.filter.apply(isA(String.class))).andReturn(true).times(expectedNGramCount - 10, idealFilterSize); // Allow for rounding errors
        
        // Run the test
        PowerMock.replayAll();
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
        PowerMock.verifyAll();
        
        // Verify results
        assertNotNull("Should have created a non-null filter", result1);
        
        assertTrue("Should have applied approximately " + expectedNGramCount + " n-grams to the bloom filter", (result2 > expectedNGramCount - 20)
                        && (result2 < idealFilterSize));
        
        String fieldName = ncis.lastElement().getIndexedFieldName();
        int expectedCount = BloomFilterUtil.predictNGramCount(ncis.lastElement().getIndexedFieldValue());
        assertTrue("Should have pruned the n-grams tokenized for field " + fieldName + " but got " + result3.get(fieldName), (null != result3.get(fieldName))
                        && (result3.get(fieldName) < expectedCount));
        
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
        
        // Set expectations
        PowerMock.mockStaticPartial(Logger.class, "getLogger");
        expect(Logger.getLogger(isA(Class.class))).andReturn(this.logger).anyTimes();
        PowerMock.mockStaticPartial(ResourceAvailabilityUtil.class, "isDiskAvailable");
        expect(ResourceAvailabilityUtil.isDiskAvailable("/", .05f)).andReturn(true).times(1);
        expect(ResourceAvailabilityUtil.isDiskAvailable("/", .05f)).andReturn(false).times(1);
        expect(this.filter.apply(isA(String.class))).andReturn(true).anyTimes(); // Allow for timeout
        this.logger.warn(isA(String.class), isA(LowDiskSpaceException.class));
        
        // Run the test
        PowerMock.replayAll();
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
        Map<String,Integer> result3 = new HashMap<>();
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
        PowerMock.verifyAll();
        
        // Verify results
        assertEquals("Predicted n-grams should not have been pruned", expectedNGramCount, result1);
        
        assertEquals("Should have applied " + timeoutAfterNGramCount + " n-grams to the bloom filter", timeoutAfterNGramCount, result2);
        
        assertNotNull("Should have caught a timeout exception", result4);
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
