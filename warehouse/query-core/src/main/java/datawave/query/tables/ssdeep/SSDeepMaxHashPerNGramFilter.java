package datawave.query.tables.ssdeep;

import java.util.Map;
import java.util.function.Predicate;

import org.apache.log4j.Logger;

import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.util.ssdeep.NGramTuple;
import datawave.util.ssdeep.SSDeepHash;

public class SSDeepMaxHashPerNGramFilter implements Predicate<Map.Entry<NGramTuple,SSDeepHash>> {
    private static final Logger log = Logger.getLogger(SSDeepMaxHashPerNGramFilter.class);
    private final int maxHashesPerNGram;
    private final Map<String,Long> countMap;

    public SSDeepMaxHashPerNGramFilter(SSDeepSimilarityQueryConfiguration config) {
        this.maxHashesPerNGram = config.getMaxHashesPerNGram();
        this.countMap = config.getState().getNgramCountMap();
    }

    @Override
    public boolean test(Map.Entry<NGramTuple,SSDeepHash> entry) {
        String ngram = entry.getKey().getChunk();
        Long count = countMap.get(ngram);
        if (count == null) {
            // first seen
            count = 0L;
        }

        if (count + 1 > maxHashesPerNGram) {
            // exceeded count the first time
            log.warn("Exceeded " + maxHashesPerNGram + " hashes for " + ngram + " ignoring remaining hashes");
            countMap.put(ngram, -1L);
            return false;
        } else if (count < 0) {
            // exceeded count beyond the first time, go negative for tracking purposes
            countMap.put(ngram, count - 1);
            return false;
        }
        // increment count
        countMap.put(ngram, count + 1);

        return true;
    }

    public Map<String,Long> getCountMap() {
        return this.countMap;
    }
}
