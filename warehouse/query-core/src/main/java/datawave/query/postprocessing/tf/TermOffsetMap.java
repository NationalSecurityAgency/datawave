package datawave.query.postprocessing.tf;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.jexl.functions.TermFrequencyList;
import org.javatuples.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

public class TermOffsetMap {
    
    private final Map<String,TermFrequencyList> termFrequencies = new HashMap<>();
    
    private final Multimap<String,Pair<Integer,Integer>> phraseOffsetMap = HashMultimap.create();
    
    public TermOffsetMap() {
    }
    
    public TermOffsetMap(Map<String,TermFrequencyList> termFrequencies) {
        if (termFrequencies != null) {
            this.termFrequencies.putAll(termFrequencies);
        }
    }
    
    public void putTermFrequencyList(String field, TermFrequencyList termFrequencyList) {
        termFrequencies.put(field, termFrequencyList);
    }
    
    public void putAllTermFrequencyLists(Map<String,TermFrequencyList> termOffsetMap) {
        this.termFrequencies.putAll(termOffsetMap);
    }
    
    public TermFrequencyList getTermFrequencyList(String term) {
        return termFrequencies.get(term);
    }
    
    public Map<String,TermFrequencyList> getTermFrequencies() {
        return termFrequencies;
    }
    
    public void putPhraseOffset(String field, int start, int end) {
        phraseOffsetMap.put(field, Pair.with(start, end));
    }
    
    public Collection<Pair<Integer,Integer>> getPhraseOffsets(String field) {
        return phraseOffsetMap.get(field);
    }
    
    public Multimap<String,Pair<Integer,Integer>> getPhraseOffsets() {
        return phraseOffsetMap;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TermOffsetMap that = (TermOffsetMap) o;
        return Objects.equals(termFrequencies, that.termFrequencies) && Objects.equals(phraseOffsetMap, that.phraseOffsetMap);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(termFrequencies, phraseOffsetMap);
    }
    
    @Override
    public String toString() {
        return new StringJoiner(", ", TermOffsetMap.class.getSimpleName() + "[", "]").add("termFrequencies=" + termFrequencies)
                        .add("phraseOffsetMap=" + phraseOffsetMap).toString();
    }
}
