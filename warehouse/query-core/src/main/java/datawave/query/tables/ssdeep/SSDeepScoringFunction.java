package datawave.query.tables.ssdeep;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.util.ssdeep.ChunkSizeEncoding;
import datawave.util.ssdeep.IntegerEncoding;
import datawave.util.ssdeep.NGramTuple;
import datawave.util.ssdeep.SSDeepHash;
import datawave.util.ssdeep.SSDeepHashEditDistanceScorer;
import datawave.util.ssdeep.SSDeepHashScorer;
import datawave.util.ssdeep.SSDeepNGramOverlapScorer;

/** A function that transforms entries retrieved from Accumulo into Scored SSDeep hash matches */
public class SSDeepScoringFunction implements Function<Map.Entry<Key,Value>,Stream<ScoredSSDeepPair>> {

    public static final String MIN_SSDEEP_SCORE_PARAMETER = "minScore";
    private static final Logger log = Logger.getLogger(SSDeepScoringFunction.class);

    /** Used to encode the chunk size as a character which is included in the ranges used to retrieve ngram tuples */
    private final ChunkSizeEncoding chunkSizeEncoding;

    /** Used to encode buckets as characters which are prepended to the ranges used to retrieve ngram tuples */
    private final IntegerEncoding bucketEncoder;

    /**
     * the position where the ngram will start in the generated ranges, determined at construction time based on the bucketEncoder parameters
     */
    private final int chunkStart;
    /**
     * the position where the query ngram will end in the generate ranges, determined based on the bucketEncoder and chunkSizeEncoding parameters
     */

    private final int chunkEnd;

    /** The maximum number of repeated characters allowed in a ssdeep hash - used to perform normalization for scoring */
    private final int maxRepeatedCharacters;

    /**
     * The query map, which relates SSDeep hash ngrams with the original query hashes, so that the SSDeep hashes from Accumulo can be married up with the
     * original queries that caused them to be retrieved.
     */
    private final Multimap<NGramTuple,SSDeepHash> queryMap;

    /** We'll toss out any matches that have scores less than this value. If set to 0 or less we'll keep all hashes */
    private final int minScoreThreshold;

    private final SSDeepHashScorer<Integer> editDistanceScorer;

    private final SSDeepHashScorer<Set<NGramTuple>> ngramOverlapScorer;

    public SSDeepScoringFunction(SSDeepSimilarityQueryConfiguration config) {
        this.queryMap = config.getQueryMap();
        this.maxRepeatedCharacters = config.getMaxRepeatedCharacters();

        this.bucketEncoder = new IntegerEncoding(config.getBucketEncodingBase(), config.getBucketEncodingLength());
        this.chunkSizeEncoding = new ChunkSizeEncoding();

        this.chunkStart = bucketEncoder.getLength();
        this.chunkEnd = chunkStart + chunkSizeEncoding.getLength();

        this.minScoreThreshold = readOptionalMinScoreThreshold(config.getQuery());

        this.editDistanceScorer = new SSDeepHashEditDistanceScorer(maxRepeatedCharacters);
        this.ngramOverlapScorer = new SSDeepNGramOverlapScorer(config.getNGramSize(), maxRepeatedCharacters, config.getMinHashSize());
    }

    /**
     * Extract the minimum score threshold from the query parameters, if present.
     *
     * @param query
     *            the query that has the parameters we want to read.
     * @return the minimum score threshold specified in the query parameter, 0 if none is set or the value of the parameter is outside of the bounds 0 &lt;= n
     *         &lt;= 100.
     */
    private int readOptionalMinScoreThreshold(Query query) {
        QueryImpl.Parameter minScoreParameter = query.findParameter(MIN_SSDEEP_SCORE_PARAMETER);
        if (minScoreParameter != null) {
            String minScoreString = minScoreParameter.getParameterValue();
            try {
                int minScore = Integer.parseInt(minScoreString);
                if (minScore < 0 || minScore > 100) {
                    log.warn("Ssdeep score threshold must be between 0-100, but was " + minScoreString + ", ignoring " + MIN_SSDEEP_SCORE_PARAMETER
                                    + " parameter.");
                } else {
                    return minScore;
                }
            } catch (NumberFormatException e) {
                log.warn("Number format exception encountered when parsing score threshold of '" + minScoreString + "' ignoring " + MIN_SSDEEP_SCORE_PARAMETER
                                + " parameter.");
            }
        }
        return 0;
    }

    /**
     * Extract matching SSDeep hashes from the Keys/Values returned by Accumulo. Each element from Accumulo will yield zero to many scored SSDeep pairs of
     * query/matching hashes.
     *
     * @param entry
     *            the function argument
     * @return A Stream of scored SSDeep pairs related to the row returned by Accumulo.
     */
    @Override
    public Stream<ScoredSSDeepPair> apply(Map.Entry<Key,Value> entry) {
        // We will receive entries like the following that follow the SSDeep bucket index format:
        // +++//thPkK 3:3:yionv//thPkKlDtn/rXScG2/uDlhl2UE9FQEul/lldDpZflsup:6v/lhPkKlDtt/6TIPFQEqRDpZ+up []
        // see: https://github.com/NationalSecurityAgency/datawave/wiki/SSDeep-In-Datawave#ssdeep-table-structure-bucketed
        // generally, the rowid consists of a:
        // - bucket; first two characters based on a hash of the original ssdeep
        // - chunk; third character,
        // - ngram; the rest of the rowid.
        // the column family holds the chunk size.
        // the column qualifier holds the original ssdeep hash from which the ngram was extracted.
        final Key k = entry.getKey();
        final String row = k.getRow().toString();

        // strip off the bucketing to extract the matching ngram and chunk size from the rowId.
        int chunkSize = chunkSizeEncoding.decode(row.substring(chunkStart, chunkEnd));
        String ngram = row.substring(chunkEnd);

        // extract the matching ssdeep hash from the column qualifier
        final String matchingHashString = k.getColumnQualifier().toString();
        final SSDeepHash matchingHash = SSDeepHash.parse(matchingHashString);

        // extract the query ssdeeps that contained this ngram from the query map.
        final NGramTuple matchingNgram = new NGramTuple(chunkSize, ngram);
        Collection<SSDeepHash> queryHashes = queryMap.get(matchingNgram);

        // score the match between each query ssdeep and matching hash, keep those that exceed the match
        // threshold.
        return queryHashes.stream().flatMap(queryHash -> {
            Set<NGramTuple> overlappingNGrams = ngramOverlapScorer.apply(queryHash, matchingHash);
            int weightedScore = editDistanceScorer.apply(queryHash, matchingHash);
            if (minScoreThreshold <= 0 || weightedScore > minScoreThreshold) {
                return Stream.of(new ScoredSSDeepPair(queryHash, matchingHash, overlappingNGrams, weightedScore));
            } else {
                return Stream.empty();
            }
        });
    }

}
