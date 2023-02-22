package datawave.query.jexl.functions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.postprocessing.tf.TermOffsetMap;
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
 * <li>Returns the first position of the matching set if the terms occur within the specified distance of each other</li>
 * <li>The distance parameter is the maximum acceptable distance (term offset) between the terms provided</li>
 * <li>For example, for the phrase "the quick brown fox" content:within(2, 'quick', 'brown', 'fox') will return true because the difference in word offsets one
 * and three is less than or equal to two {@code (3 - 1 <= 2)}. Searching for {@code content:within(1, 'quick', 'brown', 'fox')} will fail because it is
 * impossible for three terms to have a minimum distance of two.</li>
 * <li>TermWeightPosition adds the idea of a skip word to the evaluation. So a term with a position of 3 and 2 previous skips will evaluate true for all
 * positions between 1 and 3{@code (3 - 2 = 1)}. This forces the comparison to be the distance of the lowest position with out skips and the highest position
 * with skips applied.</li>
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
    
    public ContentUnorderedEvaluator(Set<String> fields, int distance, float maxScore, TermOffsetMap termOffsetMap, String... terms) {
        super(fields, distance, maxScore, termOffsetMap, terms);
    }
    
    /**
     * Evaluate a list of offsets in an unordered kind of way.
     * 
     * @param offsets
     *            a list of offsets
     * @param field
     *            the field string
     * @param eventId
     *            the eventid string
     * @return true if we found an unordered list within the specified distance for the specified set of offsets.
     */
    @Override
    public boolean evaluate(String field, String eventId, List<List<TermWeightPosition>> offsets) {
        filterOffsets(offsets);
        MultiOffsetMatcher mlIter = new MultiOffsetMatcher(distance, terms, offsets, field, eventId, termOffsetMap);
        return mlIter.findMatch();
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
        private TermWeightPosition minOffset;
        private TermWeightPosition maxOffset = null;
        
        public OffsetList(String term, List<TermWeightPosition> o) {
            this.term = term;
            this.offsets = o;
            
            // as long as there is at least one term, grab the last item for a max
            if (!o.isEmpty()) {
                // offsets with skip words will sort based on min so for max we need to loop over all offsets
                maxOffset = Collections.max(offsets, new TermWeightPosition.MaxOffsetComparator());
            }
            
            nextOffset();
        }
        
        public TermWeightPosition getMinOffset() {
            return minOffset;
        }
        
        /**
         * 
         * @return the highest value in this list
         */
        public TermWeightPosition getMaxOffset() {
            return maxOffset;
        }
        
        public Optional<TermWeightPosition> nextOffset() {
            if (offsets.isEmpty()) {
                return Optional.empty();
            } else {
                minOffset = offsets.remove(0);
                return Optional.of(minOffset);
            }
        }
        
        @Override
        public int compareTo(OffsetList o) {
            return this.getMinOffset().compareTo(o.getMinOffset());
        }
        
        @Override
        public boolean equals(Object o) {
            return minOffset == ((OffsetList) o).minOffset;
        }
        
        @Override
        public String toString() {
            return term + ";" + minOffset + ":" + maxOffset;
        }
    }
    
    private static class MultiOffsetMatcher {
        int distance = 0;
        
        final String[] terms;
        
        final PriorityQueue<OffsetList> offsetQueue = new PriorityQueue<>();
        Optional<TermWeightPosition> maxOffset = Optional.empty();
        final String field;
        final String eventId;
        final TermOffsetMap termOffsetMap;
        
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
         * @param field
         *            the field
         * @param eventId
         *            the event id (see @TermFrequencyList.getEventId(Key))
         * @param termOffsets
         *            the offsets for the specified terms, these lists will not be modified in any way.
         * @throws IllegalArgumentException
         *             if the number of terms does not match the number of offset lists.
         */
        public MultiOffsetMatcher(int distance, String[] terms, Collection<List<TermWeightPosition>> termOffsets, String field, String eventId,
                        TermOffsetMap termOffsetMap) {
            this.distance = distance;
            this.terms = terms;
            this.field = field;
            this.eventId = eventId;
            this.termOffsetMap = termOffsetMap;
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
                if ((!maxOffset.isPresent()) || (entry.getMinOffset().compareTo(maxOffset.get()) > 0)) {
                    maxOffset = Optional.of(entry.getMinOffset());
                }
                offsetQueue.add(entry);
            }
        }
        
        public boolean findMatch() {
            // Quick short-circuit -- if we have fewer offsets than terms in the phrase/adjacency/within
            // we're evaluating, we know there are no results
            if (terms.length > offsetQueue.size() || (!maxOffset.isPresent())) {
                return false;
            }
            
            while (true) {
                OffsetList o = offsetQueue.remove();
                
                if (maxOffset.get().getLowOffset() - o.getMinOffset().getOffset() <= distance) {
                    // Track the start and end offset for the phrase.
                    int startOffset = o.getMinOffset().getOffset();
                    int endOffset = maxOffset.get().getLowOffset();
                    termOffsetMap.addPhraseIndexTriplet(field, eventId, startOffset, endOffset);
                    if (log.isTraceEnabled()) {
                        log.trace("Adding phrase indexes [" + startOffset + "," + endOffset + "] for field " + field + " to jexl context");
                    }
                    return true;
                }
                
                // if the maxOffset is more than distance from the largest value in this list, there is no way to satisfy
                if (maxOffset.get().getLowOffset() - o.getMaxOffset().getOffset() > distance) {
                    return false;
                }
                
                Optional<TermWeightPosition> nextOffset = o.nextOffset();
                if (!nextOffset.isPresent()) { // no more offsets from this list
                    return false;
                }
                
                if (nextOffset.get().compareTo(maxOffset.get()) > 0) {
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
