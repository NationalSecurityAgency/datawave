package datawave.query.jexl.functions;

import java.util.*;

import datawave.ingest.protobuf.TermWeightPosition;
import org.apache.log4j.Logger;

/**
 * <p>
 * To support phrase() queries on all unicode terms, the terms are presented as an array of Strings and the integer offset lists are stored in the
 * <code>Map&lt;String, List&lt;Integer&gt;&gt;</code>. There should be an entry in the map for each term provided in the function call
 * </p>
 *
 * <b>Functions</b>
 * <ul>
 * <li>content:phrase(map, term1, term2, ...)
 * <ul>
 * <li>Only matches true on documents that contain the terms adjacent to each other in the order provided. Synonyms at the same position are considered
 * adjacent.</li>
 * </ul>
 * </li>
 * <li>content:phrase(zone, map, term1, term2, ...)
 * <ul>
 * <li>Same as content:phrase() but with a zone specified</li>
 * </ul>
 * </li>
 * </ul>
 *
 * @deprecated This class was replaced by ContentOrderEvaluatorTreeSet, it adds in functionality to properly evaluate the previous skips.
 */
@Deprecated
public class ContentOrderedEvaluator extends ContentFunctionEvaluator {
    private static final Logger log = Logger.getLogger(ContentOrderedEvaluator.class);
    
    public ContentOrderedEvaluator(Set<String> fields, int distance, float maxScore, Map<String,TermFrequencyList> termOffsetMap, String... terms) {
        super(fields, distance, maxScore, termOffsetMap, terms);
    }
    
    /**
     * Evaluate a list of offset lists to find an ordered sequence of offsets with a slot of this.distance. This will find the minimum length list of offsets,
     * and then search forward and backward for ascending and descending offsets.
     */
    @Override
    protected List<Integer> evaluate(List<List<TermWeightPosition>> offsets) {
        // Quick short-circuit -- if we have fewer offsets than terms in the phrase/adjacency/within
        // we're evaluating, we know there are no results
        if (this.terms.length > offsets.size()) {
            return null;
        }
        
        // first lets prune the lists by the maximum first offset and the minimum last offset
        offsets = prune(offsets);
        if (offsets == null) {
            return null;
        }
        
        // find the minimum length list
        int minLength = offsets.get(0).size();
        int minLengthIndex = 0;
        int leftDensity = minLength;
        int rightDensity = 0;
        for (int i = 1; i < offsets.size(); i++) {
            final int sz = offsets.get(i).size();
            rightDensity += sz;
            
            if (sz < minLength) {
                minLength = sz;
                minLengthIndex = i;
                leftDensity += rightDensity;
                rightDensity = 0;
            }
        }
        leftDensity -= minLength;
        
        // Quick short-circuit -- if we have an empty offset list then no results
        if (minLength == 0) {
            return null;
        }
        
        final String[] terms = this.terms;
        
        if (rightDensity > leftDensity) {
            // now evaluate the list of lists in both directions to find an appropriate sequence of offsets
            for (int i = 0; i < minLength; i++) {
                // if we can find a ascending sequence going forward
                if (traverseAndPrune(terms, offsets, minLengthIndex, i, 0, distance, 1, minLengthIndex + 1)) {
                    // if we can find a descending sequence going backward
                    if (traverseAndPrune(terms, offsets, minLengthIndex, i, 0, distance, -1, -1)) {
                        // then we have a matching sequence!
                        return new ArrayList<>();
                    }
                }
            }
        } else {
            // now evaluate the list of lists in both directions to find an appropriate sequence of offsets
            for (int i = minLength - 1; i >= 0; i--) {
                // if we can find a ascending sequence going forward
                if (traverseAndPrune(terms, offsets, minLengthIndex, i, 0, distance, -1, minLengthIndex - 1)) {
                    // if we can find a descending sequence going backward
                    if (traverseAndPrune(terms, offsets, minLengthIndex, i, 0, distance, 1, offsets.size())) {
                        // then we have a matching sequence!
                        return new ArrayList<>();
                    }
                }
            }
        }
        
        return null;
    }
    
    /**
     * This method will start at the offset list indexed by termIndex, and the offset indexed by offsetIndex, and will navigate the other offset lists in the
     * direction specified for preceding or following offsets within the range specified. The offset lists will be replace (pruned) as offsets are eliminated
     * from being possible candidates.
     * 
     * @param terms
     *            an array of terms, parallel with the top level list from the <code>offsets</code> parameter.
     * @param offsets
     *            matrix of term positions, the outer list lis a list of terms, the inner lists are term positions for that term. The top level of this list is
     *            parallel with the array of terms[].
     * @param termIndex
     *            index of the current term in the terms array and offsets list.
     * @param offsetIndex
     *            starting term position in the current term's offset list.
     * @param rangeMin
     *            minimum matching distance: accept adjacent term positions that are no less than this distance away from the current term.
     * @param rangeMax
     *            maximum matching distance: accept adjacent terms positions that are no greater than this distance away from the current term.
     * @param direction
     * @return true if a sequence if found
     */
    private static final boolean traverseAndPrune(final String[] terms, final List<List<TermWeightPosition>> offsets, final int termIndex,
                    final int offsetIndex, final int rangeMin, final int rangeMax, final int direction, final int pruneIndex) {
        // if already at the end, then we have success
        if (termIndex == (direction == 1 ? offsets.size() - 1 : 0)) {
            return true;
        }
        
        // get the offset in the current list
        TermWeightPosition position = offsets.get(termIndex).get(offsetIndex);
        Integer offset = position.getOffset();
        
        // find the range of offsets that meet our criteria in the next list
        List<TermWeightPosition> nextList = offsets.get(termIndex + direction);
        
        // If we are not accepting multiple of the same term matching at the same offset,
        // then adjust rangeMin from 0 to 1 if terms[termIndex] and terms[termIndex+direction]
        // are equal
        int startOffset = position.getOffset() + (rangeMin * direction);
        int endOffset = offset + (rangeMax * direction);
        if (rangeMin == 0 && terms[termIndex].equals(terms[termIndex + direction])) {
            startOffset = offset + direction;
        }
        
        int start, end;
        
        if (startOffset <= endOffset) {
            start = findFirst(nextList, startOffset);
            end = scanForLast(nextList, start, endOffset);
        } else {
            start = findLast(nextList, startOffset);
            end = scanForFirst(nextList, start, endOffset);
        }
        
        // for each offset that falls within the specified range
        for (int i = start; (direction == 1 ? i <= end : i >= end); i += direction) {
            // now recursively traverse and prune the next term.
            int nextListSize = nextList.size();
            if (traverseAndPrune(terms, offsets, termIndex + direction, i, rangeMin, rangeMax, direction, pruneIndex)) {
                return true;
            } else if (direction == 1) {
                // Note that this call may have pruned the next term list
                // invalidating our index range, so we will need to adjust accordingly
                nextList = offsets.get(termIndex + direction); // offsets.get(..)
                int adjustment = nextListSize - nextList.size();
                i -= adjustment;
                end -= adjustment;
                nextListSize = nextList.size();
            }
        }
        
        // prune if requested
        if (direction == 1 ? termIndex >= pruneIndex : termIndex <= pruneIndex) {
            offsets.set(termIndex, pruneByIndex(offsets.get(termIndex), offsetIndex + direction, direction));
        }
        
        // no traversal found
        return false;
    }
    
    /**
     * Prune the lists by the maximum first offset and the minimum last offset
     * 
     * @param offsets
     *            , null if this results in
     */
    private List<List<TermWeightPosition>> prune(List<List<TermWeightPosition>> offsets) {
        // first find the max offset of the initial offset in each list and the min offset of the last offset in each list (O(n))
        List<TermWeightPosition> list = offsets.get(0);
        int maxFirstOffset = list.get(0).getOffset();
        int minLastOffset = list.get(list.size() - 1).getOffset();
        for (int i = 1; i < offsets.size(); i++) {
            list = offsets.get(i);
            int first = list.get(0).getOffset();
            if (first > maxFirstOffset) {
                maxFirstOffset = first;
            }
            int last = list.get(list.size() - 1).getOffset();
            if (last < minLastOffset) {
                minLastOffset = last;
            }
        }
        // fail fast if the max first offset is greater than the min last offset so much that
        // a consecutive sequence cannot be found between the two
        int maxOverallDistance = (distance * offsets.size());
        
        // TODO: take the maxFirstOffsetIndex and the minLastOffsetIndex into account when computing the distance to compare against
        if ((maxFirstOffset - minLastOffset) > maxOverallDistance) {
            return null;
        }
        
        // now prune the offsets from each end (O(n))
        // TODO: take the maxFirstOffsetIndex and the minLastOffsetIndex into account when computing the prune points
        List<List<TermWeightPosition>> newOffsets = new ArrayList<>(offsets.size());
        for (int i = 0; i < offsets.size(); i++) {
            // defensive copy because we will be modifying these later.
            list = new ArrayList<>(offsets.get(i));
            
            newOffsets.add(pruneByValue(list, maxFirstOffset - maxOverallDistance, minLastOffset + maxOverallDistance));
        }
        return newOffsets;
    }
    
    /**
     * Given a list of offsets, return a sublist that contains nothing less that the max first offset, and nothing more than the min last offset
     * 
     * @param offsets
     * @param maxFirstOffset
     * @param minLastOffset
     * @return the sublist
     */
    private List<TermWeightPosition> pruneByValue(List<TermWeightPosition> offsets, int maxFirstOffset, int minLastOffset) {
        final int start, end;
        
        if (maxFirstOffset <= minLastOffset) {
            start = findFirst(offsets, maxFirstOffset);
            end = findLast(offsets, minLastOffset);
        } else {
            start = findLast(offsets, maxFirstOffset);
            end = findFirst(offsets, minLastOffset);
        }
        
        return offsets.subList(start, end + 1);
    }
    
    private static final List<TermWeightPosition> pruneByIndex(final List<TermWeightPosition> offsets, final int fromIndex, final int direction) {
        if (direction == 1) {
            return offsets.subList(fromIndex, offsets.size());
        } else {
            return offsets.subList(0, fromIndex + 1);
        }
    }
    
    private static final int findFirst(final List<TermWeightPosition> offsets, final int offset) {
        int index = Collections.binarySearch(offsets, new TermWeightPosition.Builder().setOffset(offset).build());
        if (index < 0) { // invert the index from negative to find the appropriate starting postion for a missing offset value.
            index = (index + 1) * -1;
        }
        return index;
    }
    
    private static final int findLast(final List<TermWeightPosition> offsets, final int offset) {
        int index = Collections.binarySearch(offsets, new TermWeightPosition.Builder().setOffset(offset).build());
        if (index < 0) { // invert the index from negative to find the appropriate starting postion for a missing offset value.
            index = (index + 2) * -1;
        }
        return index;
    }
    
    /** Scan for index of the term position that is not greater than toOffset */
    private static final int scanForLast(final List<TermWeightPosition> offsets, final int startIndex, final int toOffset) {
        for (int i = startIndex; i < offsets.size(); i++) {
            if (offsets.get(i).getOffset() > toOffset) {
                return i - 1;
            }
        }
        return offsets.size() - 1;
    }
    
    private static final int scanForFirst(final List<TermWeightPosition> offsets, final int startIndex, final int toOffset) {
        for (int i = startIndex; i >= 0; i--) {
            if (offsets.get(i).getOffset() < toOffset) {
                return i + 1;
            }
        }
        return 0;
    }
}
