package datawave.query.tables.ssdeep;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Range;

import com.google.common.collect.Multimap;

import datawave.util.ssdeep.NGramTuple;
import datawave.util.ssdeep.SSDeepHash;

public class SSDeepSimilarityQueryState {
    private Collection<Range> ranges;

    /**
     * The query map, which relates SSDeep hash ngrams with the original query hashes, so that the SSDeep hashes from Accumulo can be married up with the
     * original queries that caused them to be retrieved.
     */
    private Multimap<NGramTuple,SSDeepHash> queryMap;

    private Set<Integer> seenHashes = new HashSet<>();

    private Map<String,Long> ngramCountMap = new HashMap<>();

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
