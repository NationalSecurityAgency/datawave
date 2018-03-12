package datawave.query.jexl.functions;

import datawave.ingest.protobuf.TermWeightPosition;
import org.apache.log4j.Logger;

import java.util.*;

public class ContentOrderedEvaluatorTreeSet extends ContentFunctionEvaluator {
    
    private static final Logger log = Logger.getLogger(ContentOrderedEvaluatorTreeSet.class);
    
    private static final int FORWARD = 1;
    private static final int REVERSE = -1;
    
    public ContentOrderedEvaluatorTreeSet(Set<String> fields, int distance, float maxScore, Map<String,TermFrequencyList> termOffsetMap, String... terms) {
        super(fields, distance, maxScore, termOffsetMap, terms);
        if (log.isTraceEnabled()) {
            log.trace("ContentOrderedEvaluatorTreeSet constructor");
        }
    }
    
    @Override
    protected List<Integer> evaluate(List<List<TermWeightPosition>> offsets) {
        if (offsets.isEmpty()) {
            return null;
        }
        
        NavigableSet<EvaluateTermPosition> termPositions = new TreeSet<>();
        int direction = FORWARD;
        
        // This is a little hokey, but because when the travers fails it falls back to looking for the first node
        // A short "first" or "last" list should make it fast.
        if (offsets.get(0).size() > offsets.get(offsets.size() - 1).size()) {
            direction = REVERSE;
        }
        
        int[] maxSkips = new int[terms.length];
        for (int i = 0; i < terms.length; i++) {
            int maxSkip = 0;
            for (TermWeightPosition twp : offsets.get(i)) {
                if (null != twp.getPrevSkips() && twp.getPrevSkips() > maxSkip) {
                    maxSkip = twp.getPrevSkips();
                }
                
                // Skip terms greater then the max score if it score is set
                if (null != twp.getScore() && twp.getScore() > maxScore) {
                    if (log.isTraceEnabled()) {
                        log.trace("[" + terms[i] + "] Skip score => " + twp);
                    }
                    continue;
                }
                
                EvaluateTermPosition etp = new EvaluateTermPosition(terms[i], i, twp);
                termPositions.add(etp);
            }
            maxSkips[i] = maxSkip;
        }
        
        // Low/High now needs to consider the distance of the skips
        EvaluateTermPosition[] lowHigh = getLowHigh(offsets, maxSkips);
        if (null == lowHigh) {
            return null;
        }
        termPositions = termPositions.subSet(lowHigh[0], true, lowHigh[1], true);
        
        if (log.isTraceEnabled()) {
            log.trace("Term Positions: " + termPositions);
        }
        
        // Number of matched terms
        termPositions = (direction == FORWARD) ? termPositions : termPositions.descendingSet();
        List<EvaluateTermPosition> found = traverse(termPositions, direction);
        
        if (null != found && found.size() == terms.length) {
            List<Integer> result = new ArrayList<>();
            result.add(found.get(0).termWeightPosition.getOffset());
            return result;
        }
        
        // fail
        return null;
    }
    
    /**
     * Prune the lists by the maximum first offset and the min last offset
     *
     * @param offsets
     */
    private EvaluateTermPosition[] getLowHigh(List<List<TermWeightPosition>> offsets, int[] maxSkips) {
        
        // first find the max offset of the initial offset in each list and the min offset of the last offset in each list (O(n))
        List<TermWeightPosition> list = offsets.get(0);
        int maxFirstTermIndex = 0;
        TermWeightPosition maxFirstOffset = list.get(0);
        int minLastTermIndex = 0;
        TermWeightPosition minLastOffset = list.get(list.size() - 1);
        
        for (int i = 1; i < offsets.size(); i++) {
            list = offsets.get(i);
            TermWeightPosition first = list.get(0);
            
            if (first.compareTo(maxFirstOffset) > 0) {
                maxFirstTermIndex = i;
                maxFirstOffset = first;
            }
            
            TermWeightPosition last = list.get(list.size() - 1);
            if (last.compareTo(minLastOffset) <= 0) {
                minLastTermIndex = i;
                minLastOffset = last;
            }
        }
        
        int maxFirstTWP = maxFirstOffset.getLowOffset() - (maxFirstTermIndex * distance);
        for (int i = maxFirstTermIndex; i >= 0; i--) {
            maxFirstTWP -= maxSkips[i];
        }
        TermWeightPosition lowTWP = new TermWeightPosition.Builder().setOffset(maxFirstTWP).build();
        
        int minLastTWP = minLastOffset.getOffset() + (terms.length - minLastTermIndex - 1) * distance;
        for (int i = minLastTermIndex; i < maxSkips.length; i++) {
            minLastTWP += maxSkips[i];
        }
        TermWeightPosition highTWP = new TermWeightPosition.Builder().setOffset(minLastTWP).build();
        
        // min/max have already been adjusted for distance,
        // if the first is larger the last they are out of order and no match is possible
        if ((maxFirstTWP - minLastTWP) > 0) {
            return null;
        }
        
        // Because sort is termPosition declining put low as the highest term position and hte high as the lowest
        EvaluateTermPosition low = new EvaluateTermPosition(terms[terms.length - 1], terms.length - 1, lowTWP);
        EvaluateTermPosition high = new EvaluateTermPosition(terms[0], 0, highTWP);
        
        return new EvaluateTermPosition[] {low, high};
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
        List<EvaluateTermPosition> skipped = new ArrayList<>();
        List<EvaluateTermPosition> found = new ArrayList<>();
        int targetIndex = (direction == FORWARD) ? 0 : (terms.length - 1);
        
        // Find first root node
        for (EvaluateTermPosition b : sub) {
            if (b.phraseIndex == targetIndex) {
                found.add(0, b);
                
                if (skipped.size() > 0) {
                    // Add the skipped values that are at the same offset or within the distance of teh first term
                    evaluateSkipped(b, skipped, found, direction);
                    
                    // Test for completion
                    if (found.size() == terms.length) {
                        return found;
                    }
                }
                
                // Start the search based on the largest found term index
                List<EvaluateTermPosition> result = traverse(found.get(found.size() - 1), sub.tailSet(b, false), found, direction);
                return result;
            }
            skipped.add(b);
        }
        
        // No root node fail
        return null;
    }
    
    /**
     *
     * @param root
     *            the last term and position matched
     * @param sub
     *            sorted navigable set of the remaining tree to match
     * @param direction
     *            1 == forward, -1 reverse
     * @return number of matched terms
     */
    protected List<EvaluateTermPosition> traverse(EvaluateTermPosition root, NavigableSet<EvaluateTermPosition> sub, int direction) {
        List<EvaluateTermPosition> etpa = new ArrayList<>(terms.length);
        etpa.add(0, root);
        return traverse(root, sub, etpa, direction);
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
    protected List<EvaluateTermPosition> traverse(EvaluateTermPosition root, NavigableSet<EvaluateTermPosition> sub, List<EvaluateTermPosition> found,
                    int direction) {
        // Success, why keep going.
        if (found.size() == terms.length) {
            return found;
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
                        return found;
                    }
                    
                    // The term was to large for the currant found terms
                    // We will grab the last skipped value, set it as the new root
                    // and roll back to the last found term and try again.
                    return traverseFailure(sub, skipped, termPosition, direction);
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
                        return found;
                    }
                    
                    NavigableSet<EvaluateTermPosition> subB = sub.tailSet(termPosition, false);
                    
                    List<EvaluateTermPosition> results = traverse(termPosition, subB, found, direction);
                    if (null == results || results.size() != terms.length) {
                        if (skipped.size() > 0) {
                            evaluateSkipped(found.get(found.size() - 1), skipped, found, direction);
                        }
                    }
                    return found;
                }
                
                // Failure for current root node find next
                return traverseFailure(sub, skipped, termPosition, direction);
            }
            
            if (log.isTraceEnabled()) {
                log.trace("term is out of position, likely on same offset, add to skip: " + termPosition);
            }
            skipped.add(termPosition);
        }
        
        // empty sub
        return found;
    }
    
    /**
     *
     * @param sub
     *            current sored set being evaluated
     * @param skipped
     *            list of terms that were skipped for being out of order, sorted in the same order as the set
     * @param term
     *            current term that failed against the current found terms
     * @param direction
     *            1 == FORWARD, -1 == REVERSE
     * @return number of found terms for the given criteria
     */
    protected List<EvaluateTermPosition> traverseFailure(NavigableSet<EvaluateTermPosition> sub, List<EvaluateTermPosition> skipped, EvaluateTermPosition term,
                    int direction) {
        
        // Failure for current root node find next
        NavigableSet<EvaluateTermPosition> subB;
        if (skipped.size() > 0) {
            subB = sub.tailSet(skipped.get(0), true);
        } else {
            subB = sub.tailSet(term, true);
        }
        
        if (subB.size() > 0) { // Nothing after a, so it will fall out of the loop
            return traverse(subB, direction);
        }
        
        return null;
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
        
        while (skipped.size() > 0 && found.size() < terms.length) {
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
    
    public static class EvaluateTermPosition implements Comparable<EvaluateTermPosition> {
        String term;
        Integer phraseIndex;
        TermWeightPosition termWeightPosition;
        
        public EvaluateTermPosition(String term, Integer phraseIndex, TermWeightPosition termWeightPosition) {
            this.term = term;
            this.phraseIndex = phraseIndex;
            this.termWeightPosition = termWeightPosition;
        }
        
        public boolean isZeroOffset(EvaluateTermPosition o) {
            if ((null != this.termWeightPosition.getZeroOffsetMatch() && !this.termWeightPosition.getZeroOffsetMatch())
                            || (null != o.termWeightPosition.getZeroOffsetMatch() && !o.termWeightPosition.getZeroOffsetMatch())) {
                
                if (this.termWeightPosition.getOffset().equals(o.termWeightPosition.getOffset())) {
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
        
        public int distance(EvaluateTermPosition o) {
            // Distance between the highest offset and lowest offset for this entry
            if (phraseIndex >= o.phraseIndex)
                return termWeightPosition.getLowOffset() - o.termWeightPosition.getOffset();
            
            return o.termWeightPosition.getLowOffset() - termWeightPosition.getOffset();
        }
        
        @Override
        public int compareTo(EvaluateTermPosition o) {
            int result = termWeightPosition.compareTo(o.termWeightPosition);
            if (result != 0) {
                return result;
            }
            
            // Reverse the phrase index so you hit the other phrases before hitting current phrase index
            // This helps wiht end match scenarios
            result = o.phraseIndex.compareTo(phraseIndex);
            if (result != 0) {
                return result;
            }
            
            return term.compareTo(o.term);
        }
        
        @Override
        public String toString() {
            return "{" + "term='" + term + '\'' + ", index=" + phraseIndex + ", position=" + termWeightPosition + '}';
        }
    }
}
