package datawave.query.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Range;

import com.google.common.collect.Multimap;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.microservice.query.QueryImpl;
import datawave.util.ssdeep.BucketAccumuloKeyGenerator;
import datawave.util.ssdeep.ChunkSizeEncoding;
import datawave.util.ssdeep.IntegerEncoding;
import datawave.util.ssdeep.NGramGenerator;
import datawave.util.ssdeep.NGramTuple;
import datawave.util.ssdeep.SSDeepHash;

public class SSDeepSimilarityQueryConfiguration extends GenericQueryConfiguration {

    int queryThreads = 100;

    int ngramSize = NGramGenerator.DEFAULT_NGRAM_SIZE;
    int maxRepeatedCharacters = SSDeepHash.DEFAULT_MAX_REPEATED_CHARACTERS;
    int minHashSize = NGramGenerator.DEFAULT_MIN_HASH_SIZE;
    int indexBuckets = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_COUNT;
    int bucketEncodingBase = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_BASE;
    int bucketEncodingLength = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_LENGTH;

    /** Used to encode buckets as characters which are prepended to the ranges used to retrieve ngram tuples */
    private IntegerEncoding bucketEncoder;
    /** Used to encode the chunk size as a character which is included in the ranges used to retrieve ngram tuples */
    private ChunkSizeEncoding chunkSizeEncoder;

    /**
     * The number of hashes the query logic will accept, -1 indicates unlimited
     */
    private int maxHashes = -1;

    /**
     * Dedupe matching hashes matched inside the SSDeepScoringFunction. When true only process a matching hash one regardless of how many times an ngram may
     * match it
     */
    private boolean dedupeSimilarityHashes = true;

    /**
     * The max number of hashes to be retrieved per ngram, -1 indicates unlimited
     */
    private int maxHashesPerNGram = -1;

    // start of query state objections
    private Collection<Range> ranges;

    /**
     * The query map, which relates SSDeep hash ngrams with the original query hashes, so that the SSDeep hashes from Accumulo can be married up with the
     * original queries that caused them to be retrieved.
     */
    private Multimap<NGramTuple,SSDeepHash> queryMap;

    private Set<Integer> seenHashes = new HashSet<>();

    private Map<String,Long> ngramCountMap = new HashMap<>();
    // end of query state objects

    public SSDeepSimilarityQueryConfiguration() {
        super();
        setQuery(new QueryImpl());
    }

    public SSDeepSimilarityQueryConfiguration(SSDeepSimilarityQueryConfiguration other) {
        copyFrom(other);
    }

    public SSDeepSimilarityQueryConfiguration(BaseQueryLogic<?> configuredLogic) {
        super(configuredLogic);
    }

    public static SSDeepSimilarityQueryConfiguration create() {
        return new SSDeepSimilarityQueryConfiguration();
    }

    public void copyFrom(SSDeepSimilarityQueryConfiguration other) {
        super.copyFrom(other);

        this.bucketEncoder = other.bucketEncoder;
        this.chunkSizeEncoder = other.chunkSizeEncoder;
        setBucketEncodingBase(other.getBucketEncodingBase());
        setBucketEncodingLength(other.getBucketEncodingLength());
        setIndexBuckets(other.getIndexBuckets());
        setMaxRepeatedCharacters(other.getMaxRepeatedCharacters());
        setMinHashSize(other.getMinHashSize());
        setNGramSize(other.getNGramSize());
        setQueryMap(other.getQueryMap());
        setQueryThreads(other.getQueryThreads());
        setRanges(other.getRanges());
        setDedupeSimilarityHashes(other.isDedupeSimilarityHashes());
        setMaxHashes(other.getMaxHashes());
        setMaxHashesPerNGram(other.getMaxHashesPerNGram());
        setSeenHashes(other.getSeenHashes());
        setNgramCountMap(other.getNgramCountMap());
    }

    public Collection<Range> getRanges() {
        return ranges;
    }

    public void setRanges(Collection<Range> ranges) {
        this.ranges = ranges;
    }

    public Multimap<NGramTuple,SSDeepHash> getQueryMap() {
        return queryMap;
    }

    public void setQueryMap(Multimap<NGramTuple,SSDeepHash> queryMap) {
        this.queryMap = queryMap;
    }

    public int getIndexBuckets() {
        return indexBuckets;
    }

    public void setIndexBuckets(int indexBuckets) {
        this.indexBuckets = indexBuckets;
    }

    public int getQueryThreads() {
        return queryThreads;
    }

    public void setQueryThreads(int queryThreads) {
        this.queryThreads = queryThreads;
    }

    public int getNGramSize() {
        return ngramSize;
    }

    public void setNGramSize(int ngramSize) {
        this.ngramSize = ngramSize;
    }

    public int getMaxRepeatedCharacters() {
        return maxRepeatedCharacters;
    }

    public void setMaxRepeatedCharacters(int maxRepeatedCharacters) {
        this.maxRepeatedCharacters = maxRepeatedCharacters;
    }

    public int getMinHashSize() {
        return minHashSize;
    }

    public void setMinHashSize(int minHashSize) {
        this.minHashSize = minHashSize;
    }

    public int getBucketEncodingBase() {
        return bucketEncodingBase;
    }

    public void setBucketEncodingBase(int bucketEncodingBase) {
        this.bucketEncodingBase = bucketEncodingBase;
    }

    public int getBucketEncodingLength() {
        return bucketEncodingLength;
    }

    public void setBucketEncodingLength(int bucketEncodingLength) {
        this.bucketEncodingLength = bucketEncodingLength;
    }

    public boolean isDedupeSimilarityHashes() {
        return this.dedupeSimilarityHashes;
    }

    public void setDedupeSimilarityHashes(boolean dedupeSimilarityHashes) {
        this.dedupeSimilarityHashes = dedupeSimilarityHashes;
    }

    public int getMaxHashes() {
        return maxHashes;
    }

    public void setMaxHashes(int maxHashes) {
        this.maxHashes = maxHashes;
    }

    public int getMaxHashesPerNGram() {
        return maxHashesPerNGram;
    }

    public void setMaxHashesPerNGram(int maxHashesPerNGram) {
        this.maxHashesPerNGram = maxHashesPerNGram;
    }

    public Set<Integer> getSeenHashes() {
        return seenHashes;
    }

    public void setSeenHashes(Set<Integer> seenHashes) {
        this.seenHashes = seenHashes;
    }

    public Map<String,Long> getNgramCountMap() {
        return ngramCountMap;
    }

    public void setNgramCountMap(Map<String,Long> ngramCountMap) {
        this.ngramCountMap = ngramCountMap;
    }
}
