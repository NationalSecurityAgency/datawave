package datawave.query.jexl.functions;

import java.util.*;

import datawave.ingest.protobuf.TermWeightPosition;
import org.apache.log4j.Logger;

/**
 * <p>
 * To support within() queries on all unicode terms, the terms are presented as an array of Strings and the integer offset lists are stored in the
 * <code>Map&lt;String, List&lt;Integer&gt;&gt;</code>. There should be an entry in the map for each term provided in the function call
 * </p>
 *
 * <b>Functions</b>
 * <ul>
 * <li>content:within(int, map, term1, term2, ...)
 * <ul>
 * <li>Returns true if the terms occur within the specified distance of each other</li>
 * <li>The distance parameter is the maximum acceptable distance (term offset) between the terms provided</li>
 * <li>For example, for the phrase "the quick brown fox" content:within(2, 'quick', 'brown', 'fox') will return true because the difference in word offsets one
 * and three is less than or equal to two {@code (3 - 1 <= 2)}. Searching for {@code content:within(1, 'quick', 'brown', 'fox')} will fail because it is
 * impossible for three terms to have a minimum distance of two.</li>
 * </ul>
 * </li>
 * </ul>
 * 
 * 
 * 
 *
 */
public class ContentUnorderedEvaluator extends ContentFunctionEvaluator {
    private static final Logger log = Logger.getLogger(ContentUnorderedEvaluator.class);
    
    public ContentUnorderedEvaluator(Set<String> fields, int distance, float maxScore, Map<String,TermFrequencyList> termOffsetMap, String... terms) {
        super(fields, distance, maxScore, termOffsetMap, terms);
    }
    
    /**
     * Evaluate a list of offsets in an unordered kind of way.
     * 
     * @param offsets
     * @return true if we found an unordered list within the specified distance for the specified set of offsets.
     */
    @Override
    public List<Integer> evaluate(List<List<TermWeightPosition>> offsets) {
        MultiOffsetMatcher mlIter = new MultiOffsetMatcher(distance, terms, offsets);
        int match = mlIter.findMatch();
        if (match > -1) {
            List<Integer> results = new ArrayList<>(1);
            results.add(match);
            return results;
        }
        
        return null;
    }
    
    private void filterOffsets(List<List<TermWeightPosition>> offsets) {
        // if max score is maximum possible value short circuit
        if (maxScore == DEFAULT_MAX_SCORE) {
            return;
        }
        
        for (List<TermWeightPosition> offset : offsets) {
            Iterator<TermWeightPosition> twpIter = offset.iterator();
            while (twpIter.hasNext()) {
                Integer score = twpIter.next().getScore();
                if (null == score || score > maxScore) {
                    twpIter.remove();
                }
            }
        }
    }
    
    private static class OffsetList implements Comparable<OffsetList> {
        private final String term;
        private final List<TermWeightPosition> offsets;
        private int minOffset;
        private int maxOffset = -1;
        
        public OffsetList(String term, List<TermWeightPosition> o) {
            this.term = term;
            this.offsets = o;
            
            // as long as there is at least one term, grab the last item for a max
            if (!o.isEmpty()) {
                // offsets with skip words will sort based on min so for max we need to loop over all offsets
                maxOffset = Collections.max(offsets, new TermWeightPosition.MaxOffsetComparator()).getOffset();
            }
            
            nextOffset();
        }
        
        public int getMinOffset() {
            return minOffset;
        }
        
        /**
         * 
         * @return the highest value in this list
         */
        public int getMaxOffset() {
            return maxOffset;
        }
        
        public int nextOffset() {
            if (offsets.isEmpty()) {
                this.minOffset = -1;
            } else {
                TermWeightPosition position = offsets.remove(0);
                minOffset = position.getLowOffset();
            }
            return minOffset;
        }
        
        @Override
        public int compareTo(OffsetList o) {
            return this.getMinOffset() - o.getMinOffset();
        }
        
        @Override
        public boolean equals(Object o) {
            return minOffset == ((OffsetList) o).minOffset;
        }
        
        @Override
        public String toString() {
            return term + ";" + minOffset + ":" + offsets.toString();
        }
    }
    
    private static class MultiOffsetMatcher {
        int distance = 0;
        
        final String[] terms;
        
        final PriorityQueue<OffsetList> offsetQueue = new PriorityQueue<OffsetList>();
        int maxOffset = -1;
        
        /**
         * At the end of this method, terms will contain the query terms. currentOffsets will contain the minimum offset for each term and offsetLists will
         * contain the remaining offsets for each term.
         * 
         * The indexes of terms, offsetLists and currentOffsets are parallel in that the i'th item in currentOffsets corresponds to the i'th item in offsetLists
         * and term[i].
         *
         * @param distance
         *            the maximum acceptable distance between terms.
         * @param terms
         *            the query terms.
         * @param termOffsets
         *            the offsets for the specified terms, these lists will not be modified in any way.
         * @throws IllegalArgumentException
         *             if the number of terms does not match the number of offset lists.
         */
        public MultiOffsetMatcher(int distance, String[] terms, Collection<List<TermWeightPosition>> termOffsets) {
            this.distance = distance;
            this.terms = terms;
            
            if (terms.length > termOffsets.size()) {
                // more terms than offsets, no match, falls through to quick short-circuit in findMatch.
                return;
            } else if (terms.length < termOffsets.size()) {
                throw new IllegalArgumentException("Less terms than the number of offset lists received");
            }
            
            int termPos = 0;
            
            // holds the (canonical) offset list for of each term
            final Map<String,List<TermWeightPosition>> termsSeen = new HashMap<>();
            
            for (List<TermWeightPosition> offsetList : termOffsets) {
                String term = terms[termPos++];
                
                if (offsetList != null) {
                    if (!termsSeen.containsKey(term)) {
                        // new term, create a defensive copy that's safe to modify.
                        offsetList = new LinkedList<>(offsetList);
                        termsSeen.put(term, offsetList);
                    } else {
                        // already seen term, all matching terms should reference the same list.
                        offsetList = termsSeen.get(term);
                    }
                }
                
                if (offsetList == null || offsetList.isEmpty()) {
                    if (log.isTraceEnabled()) {
                        log.trace("The offset list for " + term + " is null or has no elements: " + offsetList + ". Exiting");
                    }
                    
                    offsetQueue.clear();
                    return;
                }
                
                OffsetList entry = new OffsetList(term, offsetList);
                if (entry.getMinOffset() > maxOffset) {
                    maxOffset = entry.getMinOffset();
                }
                offsetQueue.add(entry);
            }
        }
        
        public int findMatch() {
            // Quick short-circuit -- if we have fewer offsets than terms in the phrase/adjacency/within
            // we're evaluating, we know there are no results
            if (terms.length > offsetQueue.size()) {
                return -1;
            }
            
            while (true) {
                OffsetList o = offsetQueue.remove();
                if (maxOffset - o.getMinOffset() <= distance) {
                    return o.getMinOffset();
                }
                
                // if the maxOffset is more than distance from the largest value in this list, there is no way to satisfy
                if (maxOffset - o.getMaxOffset() > distance) {
                    return -1;
                }
                
                int nextOffset = o.nextOffset();
                if (nextOffset < 0) { // no more offsets from this list
                    return -1;
                }
                
                if (nextOffset > maxOffset) {
                    maxOffset = nextOffset;
                }
                
                offsetQueue.add(o);
            }
        }
        
        @Override
        public String toString() {
            return "MultiOffsetMatcher; dis:" + distance + " max: " + maxOffset + " queue: " + offsetQueue;
        }
    }
}
