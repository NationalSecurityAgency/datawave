package datawave.ingest.mapreduce.handler.shard.content;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.mapreduce.StatusReporter;

/**
 * A convenience wrapper around Hadoop MapReduce counters. Holds a set of "expected" counter names for static access. When a counter increment is submitted,
 * it's stored into a map with the counter name as the key. When a counter increment causes its value to exceed the {@link #bufferSize} parameter, the counter
 * is flushed to Hadoop.
 * 
 * The user *must* call {@link #flush(StatusReporter)} to ensure that all counters are written to the context.
 * 
 * 
 * 
 */
public class ContentIndexCounters {
    public static final String MISSING_ZONE_COUNTER = "Tokens missing a zone", MISSING_ZONE_SYNONYM_COUNTER = "Tokens (including synonyms) missing a zone",
                    EXCESSIVE_LENGTH_COUNTER = "Tokens with excessive length", ORIGINAL_PROCESSED_COUNTER = "Document tokens processed",
                    ORIGINAL_PROCESSED_TYPE_SUFFIX = " tokens processed", SYNONYMS_PROCESSED_COUNTER = "Document synonyms processed",
                    SYNONYMS_PROCESSED_TYPE_SUFFIX = " synonyms processed", PHRASES_PROCESSED_COUNTER = "Document phrases processed",
                    ALL_PROCESSED_COUNTER = "All document tokens/phrases processed", INVALID_UTF8_COUNTER = "Tokens with invalid UTF-8 characters",
                    TOO_SHORT_COUNTER = "Tokens whose length is too small", BLOOM_FILTER_ADDED = "Tokens that were added to the mutation bloom filter.",
                    BLOOM_FILTER_EXISTS = "Tokenizer Bloom Filter Hits", TOKEN_OFFSET_CACHE_EXISTS = "Tokenizer Offset Cache Hits",
                    TOKENIZER_TIME_PREFIX = "Payload Tokenization Time ", TOKENIZER_TIME_WARNINGS = "Tokenization Time Warnings",
                    TOKENIZER_TIME_ERRORS = "Tokenization Time Errors", TOKENIZER_OFFSET_CACHE_OVERFLOWS = "Tokenizer Offset Cache Overflows",
                    TOKENIZER_OFFSET_CACHE_POSITIONS_OVERFLOWED = "Tokenizer Offset Cache Positions Overflowed",
                    CONTENT_RECORDS_CREATED = "Content Records Created", TRUNCATION_COUNTER = "Truncated Tokens",
                    LENGTH_WARNING_COUNTER = "Term Length Warnings", CONTENT_RECORDS_LIVE = "Content Records Live Ingest",
                    CONTENT_RECORDS_BULK = "Content Records Bulk Ingest";
    
    public static final String COUNTER_GROUP_NAME = "Content Index Counters";
    public static final String TOKENIZER_TIME_GROUP_NAME = "Tokenizer Time Counters";
    public static final String TERM_TYPE_GROUP_NAME = "Term Type Counters";
    public static final String SYNONYM_TYPE_GROUP_NAME = "Synonym Type Counters";
    public static final String TERM_SIZE_GROUP_NAME = "Term Size Counters";
    
    private int bufferSize = 100;
    private final Map<String,Map<String,AtomicInteger>> counts;
    
    public ContentIndexCounters() {
        counts = new HashMap<>();
        
        HashMap<String,AtomicInteger> group = new HashMap<>();
        
        counts.put(COUNTER_GROUP_NAME, group);
        
        // Load the map up with the expected counters
        group.put(MISSING_ZONE_COUNTER, new AtomicInteger(0));
        group.put(MISSING_ZONE_SYNONYM_COUNTER, new AtomicInteger(0));
        group.put(EXCESSIVE_LENGTH_COUNTER, new AtomicInteger(0));
        group.put(ORIGINAL_PROCESSED_COUNTER, new AtomicInteger(0));
        group.put(SYNONYMS_PROCESSED_COUNTER, new AtomicInteger(0));
        group.put(PHRASES_PROCESSED_COUNTER, new AtomicInteger(0));
        group.put(ALL_PROCESSED_COUNTER, new AtomicInteger(0));
        group.put(INVALID_UTF8_COUNTER, new AtomicInteger(0));
        group.put(TOO_SHORT_COUNTER, new AtomicInteger(0));
        group.put(BLOOM_FILTER_ADDED, new AtomicInteger(0));
        group.put(BLOOM_FILTER_EXISTS, new AtomicInteger(0));
        group.put(TOKEN_OFFSET_CACHE_EXISTS, new AtomicInteger(0));
        group.put(TRUNCATION_COUNTER, new AtomicInteger(0));
        group.put(LENGTH_WARNING_COUNTER, new AtomicInteger(0));
    }
    
    /**
     * Increments the counter denoted by counterName by one. The counter's value will only be written to the context if it exceeds bufferSize
     * 
     * @param counterName
     *            The name of the counter to increment
     * @param reporter
     *            The current task's context
     */
    public void increment(String counterName, StatusReporter reporter) {
        this.increment(COUNTER_GROUP_NAME, counterName, reporter);
    }
    
    /**
     * Increments the counter denoted by counterName by one. The counter's value will only be written to the context if it exceeds bufferSize
     * 
     * @param groupName
     *            The name of the counter's group
     * @param counterName
     *            The name of the counter to increment
     * @param reporter
     *            The current task's context
     */
    public void increment(String groupName, String counterName, StatusReporter reporter) {
        Map<String,AtomicInteger> group = counts.get(groupName);
        if (group == null) {
            group = new HashMap<>();
            counts.put(groupName, group);
        }
        
        if (group.containsKey(counterName)) {
            AtomicInteger val = group.get(counterName);
            
            if (val.get() > bufferSize && reporter != null) {
                reporter.getCounter(groupName, counterName).increment(val.getAndSet(0));
            }
            
            val.incrementAndGet();
        } else {
            group.put(counterName, new AtomicInteger(1));
        }
    }
    
    /**
     * Increments the counter denoted by counterName by the given value. The counter's value will only be written to the context if it exceeds bufferSize
     * 
     * @param counterName
     *            The name of the counter to increment
     * @param value
     *            The amount to increment the counter by
     * @param reporter
     *            The current task's context
     */
    public void incrementValue(String counterName, int value, StatusReporter reporter) {
        this.incrementValue(COUNTER_GROUP_NAME, counterName, value, reporter);
    }
    
    /**
     * Increments the counter denoted by counterName by the given value. The counter's value will only be written to the context if it exceeds bufferSize
     * 
     * @param groupName
     *            The name of the counter's group
     * @param counterName
     *            The name of the counter to increment
     * @param value
     *            The amount to increment the counter by
     * @param reporter
     *            The current task's context
     */
    public void incrementValue(String groupName, String counterName, int value, StatusReporter reporter) {
        Map<String,AtomicInteger> group = counts.get(groupName);
        if (group == null) {
            group = new HashMap<>();
            counts.put(groupName, group);
        }
        
        if (group.containsKey(counterName)) {
            AtomicInteger val = group.get(counterName);
            
            if (val.get() > bufferSize && reporter != null) {
                reporter.getCounter(groupName, counterName).increment(val.getAndSet(0));
            }
            
            val.addAndGet(value);
        } else {
            group.put(counterName, new AtomicInteger(1));
        }
    }
    
    /**
     * Returns the value a counter should be written to the context.
     * 
     * @return The specified size to wait befor writing a counter
     */
    public int getBufferSize() {
        return bufferSize;
    }
    
    /**
     * Sets the value which determines at what value a counter should be written to the context.
     * 
     * @param bufferSize
     *            the buffer size to set
     */
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
    
    /**
     * Flushes all counter values to the context and sets the values to zero.
     * 
     * @param reporter
     *            the reporter holding the entries
     */
    public void flush(StatusReporter reporter) {
        if (reporter != null) {
            for (Entry<String,Map<String,AtomicInteger>> countEntry : counts.entrySet()) {
                String groupName = countEntry.getKey();
                for (Entry<String,AtomicInteger> groupEntry : countEntry.getValue().entrySet()) {
                    if (groupEntry.getValue().get() > 0) {
                        reporter.getCounter(groupName, groupEntry.getKey()).increment(groupEntry.getValue().getAndSet(0));
                    }
                }
            }
        }
    }
}
