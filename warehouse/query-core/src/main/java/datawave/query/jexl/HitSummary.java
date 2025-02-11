package datawave.query.jexl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import datawave.query.attributes.ValueTuple;

public class HitSummary {

    private final Object evaluation;
    private final Map<String,Object> resultCache;
    private final Set<ValueTuple> hits;

    public HitSummary(Object result, Map<String,Object> cache, Set<ValueTuple> hits) {
        this.evaluation = result;
        this.resultCache = cache;
        this.hits = hits;
    }

    public Object getEvaluation() {
        return evaluation;
    }

    public Map<String,Object> getResultCache() {
        return resultCache;
    }

    public Set<ValueTuple> getHits() {
        return hits;
    }

    public Set<String> getMatchedNodeStrings() {
        Set<String> nodeStrings = new HashSet<>();
        for (Map.Entry<String,Object> result : resultCache.entrySet()) {
            if (ArithmeticJexlEngines.isMatched(result.getValue())) {
                nodeStrings.add(result.getKey());
            }
        }
        return nodeStrings;
    }

}
