package datawave.query.tables.ssdeep;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.util.ssdeep.NGramTuple;
import datawave.util.ssdeep.SSDeepHash;
import datawave.util.ssdeep.SSDeepHashEditDistanceScorer;
import datawave.util.ssdeep.SSDeepHashScorer;
import datawave.util.ssdeep.SSDeepNGramOverlapScorer;

/** A function that transforms entries retrieved from Accumulo into Scored SSDeep hash matches */
public class SSDeepScoringFunction implements Function<Map.Entry<NGramTuple,SSDeepHash>,Stream<ScoredSSDeepPair>> {

    public static final String MIN_SSDEEP_SCORE_PARAMETER = "minScore";
    private static final Logger log = Logger.getLogger(SSDeepScoringFunction.class);

    /** The maximum number of repeated characters allowed in a ssdeep hash - used to perform normalization for scoring */
    private final int maxRepeatedCharacters;

    /** We'll toss out any matches that have scores less than this value. If set to 0 or less we'll keep all hashes */
    private final int minScoreThreshold;

    private final SSDeepHashScorer<Integer> editDistanceScorer;

    private final SSDeepHashScorer<Set<NGramTuple>> ngramOverlapScorer;

    private final SSDeepSimilarityQueryState queryState;

    public SSDeepScoringFunction(SSDeepSimilarityQueryConfiguration config) {
        this.queryState = config.getState();
        this.maxRepeatedCharacters = config.getMaxRepeatedCharacters();

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
    public Stream<ScoredSSDeepPair> apply(Map.Entry<NGramTuple,SSDeepHash> entry) {
        NGramTuple ngram = entry.getKey();
        SSDeepHash matchingHash = entry.getValue();

        // extract the query ssdeeps that contained this ngram from the query map.
        Collection<SSDeepHash> queryHashes = queryState.getQueryMap().get(ngram);

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
