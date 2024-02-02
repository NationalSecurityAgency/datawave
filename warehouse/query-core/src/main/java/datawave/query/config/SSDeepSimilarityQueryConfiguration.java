package datawave.query.config;

import java.util.Collection;

import org.apache.accumulo.core.data.Range;

import com.google.common.collect.Multimap;

import datawave.util.ssdeep.BucketAccumuloKeyGenerator;
import datawave.util.ssdeep.ChunkSizeEncoding;
import datawave.util.ssdeep.IntegerEncoding;
import datawave.util.ssdeep.NGramTuple;
import datawave.util.ssdeep.SSDeepHash;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.BaseQueryLogic;

public class SSDeepSimilarityQueryConfiguration extends GenericQueryConfiguration {

    int queryThreads = 100;
    int maxRepeatedCharacters = 3;

    int indexBuckets = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_COUNT;
    int bucketEncodingBase = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_BASE;
    int bucketEncodingLength = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_LENGTH;

    /** Used to encode buckets as characters which are prepended to the ranges used to retrieve ngram tuples */
    private IntegerEncoding bucketEncoder;
    /** Used to encode the chunk size as a character which is included in the ranges used to retrieve ngram tuples */
    private ChunkSizeEncoding chunkSizeEncoder;

    private Query query;

    private Collection<Range> ranges;

    private Multimap<NGramTuple,SSDeepHash> queryMap;

    public SSDeepSimilarityQueryConfiguration() {
        super();
        query = new QueryImpl();
    }

    public SSDeepSimilarityQueryConfiguration(BaseQueryLogic<?> configuredLogic) {
        super(configuredLogic);
    }

    public static SSDeepSimilarityQueryConfiguration create() {
        return new SSDeepSimilarityQueryConfiguration();
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
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

    public int getMaxRepeatedCharacters() {
        return maxRepeatedCharacters;
    }

    public void setMaxRepeatedCharacters(int maxRepeatedCharacters) {
        this.maxRepeatedCharacters = maxRepeatedCharacters;
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
}
