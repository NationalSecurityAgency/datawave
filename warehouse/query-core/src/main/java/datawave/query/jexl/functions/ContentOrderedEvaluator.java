package datawave.query.jexl.functions;

import datawave.ingest.protobuf.TermWeightPosition;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * <p>
 * To support phrase() queries on all unicode terms, the terms are presented as an array of Strings and the TermWeightPosition offset lists are stored in the
 * <code>Map&lt;String, List&lt;TermFrequencyList&gt;&gt;</code>. There should be an entry in the map for each term provided in the function call
 * </p>
 *
 * <b>Functions</b>
 * <ul>
 * <li>content:phrase(map, term1, term2, ...)
 * <ul>
 * <li>Only matches true, a list of positions &gt; 0, on documents that contain the terms adjacent to each other in the order provided. Synonyms at the same
 * position are considered adjacent.</li>
 * </ul>
 * </li>
 * <li>content:phrase(zone, map, term1, term2, ...)
 * <ul>
 * <li>Same as content:phrase() but with a zone specified</li>
 * </ul>
 * </li>
 * <li>content:phrase(zone, score, map, term1, term2, ...)
 * <ul>
 * <li>Same as content:phrase() but with a zone and max score filter specified</li>
 * </ul>
 * </li>
 * </ul>
 */
public class ContentOrderedEvaluator extends ContentFunctionEvaluator {
    
    private static final Logger log = Logger.getLogger(ContentOrderedEvaluator.class);
    
    private static final int FORWARD = 1;
    private static final int REVERSE = -1;
    
    public ContentOrderedEvaluator(Set<String> fields, int distance, float maxScore, Map<String,TermFrequencyList> termOffsetMap, String... terms) {
        super(fields, distance, maxScore, termOffsetMap, terms);
        if (log.isTraceEnabled()) {
            log.trace("ContentOrderedEvaluatorTreeSet constructor");
        }
    }
    
    @Override
    protected boolean evaluate(List<List<TermWeightPosition>> offsets) {
        if (offsets.isEmpty() || offsets.size() < terms.length) {
            return false;
        }
        
        List<NavigableSet<EvaluateTermPosition>> termPositions = buildTermPositions(offsets);
        
        // apply a trim
        termPositions = trim(termPositions);
        
        // if a trim resulted in no more term positions we are done
        if (termPositions == null) {
            return false;
        }
        
        while (!isConverged(termPositions, distance)) {
            // look for alternatives that also satisfy convergence within each term before rolling forward. Move at most one term one position until there are
            // no alternatives that satisfy the distance left
            List<NavigableSet<EvaluateTermPosition>> alternativeTermPositions = trimAlternatives(termPositions, distance);
            boolean alternativeConverged = false;
            while (alternativeTermPositions != null && !(alternativeConverged = isConverged(alternativeTermPositions, distance))) {
                alternativeTermPositions = trimAlternatives(alternativeTermPositions, distance);
            }
            
            // found a valid alternative
            if (alternativeConverged) {
                return true;
            }
            
            // if no alternatives, move to the next first term and start again
            termPositions.get(0).pollFirst();
            
            // ensure that wasn't the last one
            if (termPositions.get(0).isEmpty()) {
                return false;
            }
            
            // trim whatever is left
            termPositions = trim(termPositions);
            
            // if term positions are null a trim resulted in an impossible match
            if (termPositions == null) {
                return false;
            }
        }
        
        // converged on the phrase
        return true;
    }
    
    /**
     * Convert a List of offsets into a NavigableSet of EvaluateTermPositions
     * 
     * @param offsets
     * @return null if a phrase match is not possible, or the term positions
     */
    private List<NavigableSet<EvaluateTermPosition>> buildTermPositions(List<List<TermWeightPosition>> offsets) {
        // build sorted sets for each term
        int index = 0;
        List<NavigableSet<EvaluateTermPosition>> termPositions = new ArrayList<>(terms.length);
        for (List<TermWeightPosition> termOffsets : offsets) {
            termPositions.add(new TreeSet<>());
            for (TermWeightPosition twp : termOffsets) {
                // Skip terms greater then the max score if it score is set
                if (twp.getScore() > maxScore) {
                    if (log.isTraceEnabled()) {
                        log.trace("[" + terms[index] + "] Skip score => " + twp);
                    }
                    continue;
                }
                
                EvaluateTermPosition etp = new EvaluateTermPosition(terms[index], index, twp);
                termPositions.get(index).add(etp);
            }
            
            // if any term positions were never populated a match is not possible
            if (termPositions.get(index).isEmpty()) {
                return null;
            }
            
            // increment the index
            index++;
        }
        
        return termPositions;
    }
    
    /**
     * Trim impossible offsets from the arrays by removing any terms which are less than the first term walking through the term lists
     * 
     * @param offsets
     * @return null if a terms offsets are empty, otherwise the trimmed list
     */
    private List<NavigableSet<EvaluateTermPosition>> trim(List<NavigableSet<EvaluateTermPosition>> offsets) {
        // sanity check
        if (offsets == null) {
            return null;
        }
        
        // advance each first/second pair so that second is always >= first
        for (int i = 0; i + 1 < offsets.size(); i++) {
            NavigableSet<EvaluateTermPosition> first = offsets.get(i);
            NavigableSet<EvaluateTermPosition> second = offsets.get(i + 1);
            
            int termCompare = first.first().termWeightPosition.compareTo(second.first().termWeightPosition);
            
            // advance second until less than first or while they are the same term and position
            while (termCompare > 0 || (termCompare == 0 && first.first().isSameTerm(second.first()))) {
                // advance second
                second.pollFirst();
                
                // test for end condition
                if (second.isEmpty()) {
                    return null;
                }
                
                // update compare
                termCompare = first.first().termWeightPosition.compareTo(second.first().termWeightPosition);
            }
        }
        
        return offsets;
    }
    
    /**
     * Taking an existing set of offsets, look for alternatives starting with the second term that also satisfy the distance requirement. Move at most one term
     * one position. Do not modify offsets, but return a copy
     * 
     * @param offsets
     *            original offsets
     * @param distance
     *            distance to accept for alternatives
     * @return alternatives or null if no alternatives exist
     */
    private List<NavigableSet<EvaluateTermPosition>> trimAlternatives(List<NavigableSet<EvaluateTermPosition>> offsets, int distance) {
        List<NavigableSet<EvaluateTermPosition>> alternatives = new ArrayList<>(offsets.size());
        
        alternatives.add(offsets.get(0));
        
        for (int i = 1; i < offsets.size(); i++) {
            // strip the first offset for this term and make a copy
            NavigableSet<EvaluateTermPosition> candidateSet = offsets.get(i).tailSet(offsets.get(i).first(), false);
            
            // if the candidate set has something in it, and it is within the constraints of the distance this is a valid alternative
            if (!candidateSet.isEmpty()
                            && (alternatives.get(i - 1).first().isWithIn(candidateSet.first(), distance) && !alternatives.get(i - 1).first()
                                            .isSameTerm(candidateSet.first()))) {
                alternatives.add(candidateSet);
                
                // once there is a new alternative, leave all other terms alone to test it fully
                for (int j = i + 1; j < offsets.size(); j++) {
                    alternatives.add(offsets.get(j));
                }
                
                // send these back for evaluation
                return alternatives;
            } else {
                // nothing to change for this term, add it as-is
                alternatives.add(offsets.get(i));
            }
        }
        
        // no good candidate exists, there are no alternatives
        return null;
    }
    
    /**
     * Test if a set of offsets satisfy a distance requirement
     * 
     * @param offsets
     * @param distance
     * @return true if satisfied, false otherwise
     */
    private boolean isConverged(List<NavigableSet<EvaluateTermPosition>> offsets, int distance) {
        if (offsets.size() == 1) {
            return true;
        }
        
        int secondIndex = 1;
        NavigableSet<EvaluateTermPosition> first = offsets.get(0);
        NavigableSet<EvaluateTermPosition> second = offsets.get(secondIndex);
        
        // test that these terms are within distance
        while (first.first().isWithIn(second.first(), distance)) {
            // are there more terms?
            if (secondIndex + 1 < offsets.size()) {
                // advance and test the next pair
                secondIndex++;
                first = second;
                second = offsets.get(secondIndex);
            } else {
                // nope
                return true;
            }
        }
        
        // terms not within distance
        return false;
    }
    
    /**
     * A class that holds the results of a traversal. If we have a NavigableSet then we can restart the traversal with that set. Otherwise we will have a result
     * to return.
     */
    private static class TraverseResult {
        private static final TraverseResult FINAL_NULL_RESULT = new TraverseResult((List<EvaluateTermPosition>) null);
        private final boolean finalResult;
        private final List<EvaluateTermPosition> result;
        private final NavigableSet<EvaluateTermPosition> sub;
        
        public TraverseResult(List<EvaluateTermPosition> result) {
            this.finalResult = true;
            this.result = result;
            this.sub = null;
        }
        
        public TraverseResult(NavigableSet<EvaluateTermPosition> sub) {
            this.finalResult = false;
            this.result = null;
            this.sub = sub;
        }
        
        private boolean hasFinalResult() {
            return finalResult;
        }
        
        private List<EvaluateTermPosition> getResult() {
            return result;
        }
        
        private NavigableSet<EvaluateTermPosition> getNextTraversal() {
            return sub;
        }
    }
    
    /**
     *
     * The traverse function descend the tree until a term is out of reach. When the next erm is out of reach it will fall back to the last "first term" and
     * start over.
     *
     * skipped = is a reserved list of terms, reserved for matching at the same offset
     *
     * found = represent the largest match for that term and position still within the distance
     *
     * @param sub
     *            NavigableSet sorted by EvaluateTermPosition comparable
     * @param direction
     *            1 == FORWARD, -1 == REVERSE, use static finals
     * @return
     */
    protected List<EvaluateTermPosition> traverse(NavigableSet<EvaluateTermPosition> sub, int direction) {
        int targetIndex = (direction == FORWARD) ? 0 : (terms.length - 1);
        
        // start with an initial non-final result
        TraverseResult result = new TraverseResult(sub);
        
        // while we do not have a final result, process
        while (!result.hasFinalResult()) {
            List<EvaluateTermPosition> skipped = new ArrayList<>();
            List<EvaluateTermPosition> found = new ArrayList<>();
            
            // use the navigable set from the last non-final result
            sub = result.getNextTraversal();
            
            // set result to the null list in case we never get to traverse
            result = TraverseResult.FINAL_NULL_RESULT;
            
            // Find first root node
            for (EvaluateTermPosition b : sub) {
                if (b.phraseIndex == targetIndex) {
                    found.add(0, b);
                    
                    if (!skipped.isEmpty()) {
                        // Add the skipped values that are at the same offset or within the distance of teh first term
                        evaluateSkipped(b, skipped, found, direction);
                        
                        // Test for completion
                        if (found.size() == terms.length) {
                            return found;
                        }
                    }
                    
                    // Search based on the largest found term index
                    result = traverse(found.get(found.size() - 1), sub.tailSet(b, false), found, direction);
                    break;
                }
                skipped.add(b);
            }
        }
        
        // return the final result
        return result.getResult();
    }
    
    /**
     *
     * @param root
     *            the last term and position matched
     * @param sub
     *            sorted navigableset of the remaining tree to match
     * @param found
     *            represents the largest match for that term and position still within the distance
     * @param direction
     *            1 == forward, -1 reverse
     * @return number of matched terms
     */
    protected TraverseResult traverse(EvaluateTermPosition root, NavigableSet<EvaluateTermPosition> sub, List<EvaluateTermPosition> found, int direction) {
        // Success, why keep going.
        if (found.size() == terms.length) {
            return new TraverseResult(found);
        }
        
        List<EvaluateTermPosition> skipped = new ArrayList<>();
        for (EvaluateTermPosition termPosition : sub) {
            // Same term position
            if (root.phraseIndex == termPosition.phraseIndex) {
                boolean first = (direction == FORWARD) ? (termPosition.phraseIndex - 1 < 0) : (termPosition.phraseIndex + 1 >= terms.length);
                if (first || (!first && found.get(getFoundIndex(termPosition, direction)).isWithIn(termPosition, distance, direction))) {
                    // First or Not first, and with in distance
                    updateFound(termPosition, found, direction);
                    root = termPosition;
                    continue;
                    
                } else {
                    // Not with in distance
                    if (found.size() == terms.length) {
                        return new TraverseResult(found);
                    }
                    
                    // The term was to large for the currant found terms
                    // We will grab the last skipped value, set it as the new root
                    // and roll back to the last found term and try again.
                    return traverseFailure(sub, skipped, termPosition);
                }
            }
            
            // Next term phrase
            if ((root.phraseIndex + direction) == termPosition.phraseIndex) {
                if (root.isWithIn(termPosition, distance, direction)) {
                    // Same term and position, drop it
                    if (root.isSameTerm(termPosition) && root.termWeightPosition.equals(termPosition.termWeightPosition)) {
                        continue;
                    }
                    
                    updateFound(termPosition, found, direction);
                    
                    if (found.size() == terms.length) {
                        return new TraverseResult(found);
                    }
                    
                    NavigableSet<EvaluateTermPosition> subB = sub.tailSet(termPosition, false);
                    
                    TraverseResult result = traverse(termPosition, subB, found, direction);
                    if (result.getResult() == null || result.getResult().size() != terms.length) {
                        if (!skipped.isEmpty()) {
                            evaluateSkipped(found.get(found.size() - 1), skipped, found, direction);
                            result = new TraverseResult(found);
                        }
                    }
                    
                    return result;
                }
                
                // Failure for current root node find next
                return traverseFailure(sub, skipped, termPosition);
            }
            
            if (log.isTraceEnabled()) {
                log.trace("term is out of position, likely on same offset, add to skip: " + termPosition);
            }
            skipped.add(termPosition);
        }
        
        // empty sub
        return new TraverseResult(found);
    }
    
    /**
     *
     * @param sub
     *            current sored set being evaluated
     * @param skipped
     *            list of terms that were skipped for being out of order, sorted in the same order as the set
     * @param term
     *            current term that failed against the current found terms
     * @return number of found terms for the given criteria
     */
    protected TraverseResult traverseFailure(NavigableSet<EvaluateTermPosition> sub, List<EvaluateTermPosition> skipped, EvaluateTermPosition term) {
        
        // Failure for current root node find next
        NavigableSet<EvaluateTermPosition> subB;
        if (!skipped.isEmpty()) {
            subB = sub.tailSet(skipped.get(0), true);
        } else {
            subB = sub.tailSet(term, true);
        }
        
        if (!subB.isEmpty()) { // retry with this new tail set
            return new TraverseResult(subB);
        }
        
        // we have a final result, and it is null
        return TraverseResult.FINAL_NULL_RESULT;
    }
    
    /**
     *
     * @param root
     *            term normally the last found and highest index
     * @param skipped
     *            list of terms sorted by offset or EvaluateTermPosition comparable
     * @param found
     *            list of matched terms for the given distance value
     * @param direction
     *            1 == FORWARD, -1 == REVERSE
     */
    protected void evaluateSkipped(EvaluateTermPosition root, List<EvaluateTermPosition> skipped, List<EvaluateTermPosition> found, int direction) {
        
        while (!skipped.isEmpty() && found.size() < terms.length) {
            EvaluateTermPosition skip = skipped.remove(skipped.size() - 1);
            if (root.isZeroOffset(skip)) {
                // Fail fast, same position
                return;
            }
            
            // Same term and position skip
            if (root.isSameTerm(skip) && root.termWeightPosition.equals(skip.termWeightPosition)) {
                continue;
            }
            
            if (root.phraseIndex + direction == skip.phraseIndex && root.isWithIn(skip, distance, direction)) {
                updateFound(skip, found, direction);
                root = skip;
            } else {
                // Fail fast, should be an ordered list
                return;
            }
        }
    }
    
    /**
     *
     * @param termPosition
     *            current term
     * @param direction
     *            1 == FORWARD, -1 == REVERSE
     * @return index for reference into the "found" list, it is always filled in 0 to terms length
     */
    protected int getFoundIndex(EvaluateTermPosition termPosition, int direction) {
        if (direction == FORWARD) {
            return termPosition.phraseIndex;
        } else {
            return (terms.length - termPosition.phraseIndex) + direction;
        }
    }
    
    /**
     * Updates the term into the found list, add or replace
     *
     * @param termPosition
     *            current term
     * @param found
     *            current list of found nodes, the terms getFoundIndex will be added or replaced
     * @param direction
     *            1 == FORWARD, -1 == REVERSE
     */
    protected void updateFound(EvaluateTermPosition termPosition, List<EvaluateTermPosition> found, int direction) {
        int index = getFoundIndex(termPosition, direction);
        if (found.size() <= index) {
            found.add(index, termPosition);
        } else {
            found.set(index, termPosition);
        }
    }
    
    /**
     * <p>
     * EvaluateTermPosition: Is a utility class for sorting terms and their positions
     * </p>
     *
     * <ul>
     * <li>isZeroOffset() two terms have the same offset, if it isn't allowed</li>
     * <li>isWithIn() are two terms within a certain distance, with respect to direction</li>
     * <li>comparable =&gt; prioritizes position, then phraseIndex, and finally alphanumeric on term</li>
     * </ul>
     */
    private static class EvaluateTermPosition implements Comparable<EvaluateTermPosition> {
        String term;
        int phraseIndex;
        TermWeightPosition termWeightPosition;
        
        public EvaluateTermPosition(String term, Integer phraseIndex, TermWeightPosition termWeightPosition) {
            this.term = term;
            this.phraseIndex = phraseIndex;
            this.termWeightPosition = termWeightPosition;
        }
        
        /**
         *
         * Conditional if to allow these positions based on offset True if zeroOffset is not allowed, and offsets are equal. False if zeroOffsets are allowed.
         *
         * @param o
         *            Other position
         * @return True if zeroOffset is not allowed, and offsets are equal
         */
        public boolean isZeroOffset(EvaluateTermPosition o) {
            if ((!this.termWeightPosition.getZeroOffsetMatch()) || (!o.termWeightPosition.getZeroOffsetMatch())) {
                
                if (this.termWeightPosition.getOffset() == o.termWeightPosition.getOffset()) {
                    if (log.isTraceEnabled()) {
                        log.trace("EvaluateTermPosition.isZeroOffset: " + this.termWeightPosition.getOffset() + " == " + o.termWeightPosition.getOffset());
                    }
                    return true;
                }
            }
            return false;
        }
        
        public boolean isWithIn(EvaluateTermPosition o, int distance) {
            return isWithIn(o, distance, FORWARD);
        }
        
        public boolean isWithIn(EvaluateTermPosition o, int distance, int direction) {
            
            // Instructed to not match at the same position
            if (isZeroOffset(o)) {
                return false;
            }
            
            int low, high = -1;
            EvaluateTermPosition eval;
            switch (direction) {
                case FORWARD:
                    low = termWeightPosition.getLowOffset();
                    high = termWeightPosition.getOffset() + distance;
                    eval = o;
                    break;
                case REVERSE:
                    low = o.termWeightPosition.getLowOffset();
                    high = o.termWeightPosition.getOffset() + distance;
                    eval = this;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid direction option.");
            }
            
            if (log.isTraceEnabled()) {
                log.trace("EvaluateTermPosition.isWithIn: " + low + "<=" + eval.termWeightPosition.getOffset() + " && "
                                + eval.termWeightPosition.getLowOffset() + "<=" + high);
            }
            
            return (low <= eval.termWeightPosition.getOffset() && eval.termWeightPosition.getLowOffset() <= high);
        }
        
        public boolean isSameTerm(EvaluateTermPosition o) {
            // Fail on same term at same position
            if (term.equals(o.term)) {
                return true;
            }
            
            return false;
        }
        
        @Override
        public int compareTo(EvaluateTermPosition o) {
            int result = termWeightPosition.compareTo(o.termWeightPosition);
            if (result != 0) {
                return result;
            }
            
            // Reverse the phrase index so you hit the other phrases before hitting current phrase index
            // This helps with end match scenarios
            result = Integer.compare(o.phraseIndex, phraseIndex);
            if (result != 0) {
                return result;
            }
            
            // todo: comparing the term value makes no sense given how this class is used to subset a list of ordered term positions
            // perhaps we are never supposed to get here?
            return term.compareTo(o.term);
        }
        
        @Override
        public String toString() {
            return "{" + "term='" + term + '\'' + ", index=" + phraseIndex + ", position=" + termWeightPosition + '}';
        }
    }
}
