package datawave.query.jexl.functions;

import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.postprocessing.tf.TermOffsetMap;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
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
    
    public ContentOrderedEvaluator(Set<String> fields, int distance, float maxScore, TermOffsetMap termOffsetMap, String... terms) {
        super(fields, distance, maxScore, termOffsetMap, terms);
        if (log.isTraceEnabled()) {
            log.trace("ContentOrderedEvaluatorTreeSet constructor");
        }
    }
    
    @Override
    protected boolean evaluate(String field, String eventId, List<List<TermWeightPosition>> offsets) {
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
        
        while (!isConverged(field, eventId, termPositions, distance)) {
            // look for alternatives that also satisfy convergence within each term before rolling forward. Move at most one term one position until there are
            // no alternatives that satisfy the distance left
            List<NavigableSet<EvaluateTermPosition>> alternativeTermPositions = trimAlternatives(termPositions, distance);
            boolean alternativeConverged = false;
            while (alternativeTermPositions != null && !(alternativeConverged = isConverged(field, eventId, alternativeTermPositions, distance))) {
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
     * the list of offsets
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
     * the list of offsets
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
     * @param field
     *            the field where the offsets were found
     * @param eventId
     *            the event id (see @TermFrequencyList.getEventId(Key))
     * @param offsets
     * a list of offsets
     * @param distance
     * the distance
     * @return true if satisfied, false otherwise
     */
    private boolean isConverged(String field, String eventId, List<NavigableSet<EvaluateTermPosition>> offsets, int distance) {
        if (offsets.size() == 1) {
            return true;
        }
        
        int secondIndex = 1;
        NavigableSet<EvaluateTermPosition> first = offsets.get(0);
        NavigableSet<EvaluateTermPosition> second = offsets.get(secondIndex);
        
        // The start offset of the phrase if satisfied.
        int startOffset = first.first().termWeightPosition.getOffset();
        int endOffset;
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
                
                // Establish the end offset of the phrase.
                endOffset = second.first().termWeightPosition.getOffset();
                // Record the phrase offsets to fetch excerpts later if desired.
                termOffsetMap.addPhraseIndexTriplet(field, eventId, startOffset, endOffset);
                if (log.isTraceEnabled()) {
                    log.trace("Adding phrase indexes [" + startOffset + "," + endOffset + "] for field " + field + " for event " + eventId + " to jexl context");
                }
                return true;
            }
        }
        
        // terms not within distance
        return false;
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
