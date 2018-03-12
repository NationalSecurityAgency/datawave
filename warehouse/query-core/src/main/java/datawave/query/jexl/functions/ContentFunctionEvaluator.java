package datawave.query.jexl.functions;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.ingest.protobuf.TermWeightPosition;
import org.apache.log4j.Logger;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

/**
 * An abstract class to for the ordered and unordered content evaluators.
 */
public abstract class ContentFunctionEvaluator {
    private static final Logger log = Logger.getLogger(ContentFunctionEvaluator.class);
    protected static final int DEFAULT_MAX_SCORE = TermWeightPosition.PositionScoreToTermWeightScore(Float.NEGATIVE_INFINITY);
    
    final protected Set<String> fields;
    final protected int distance;
    final protected String[] terms;
    final protected Map<String,TermFrequencyList> termOffsetMap;
    final protected boolean canProcess;
    final protected int maxScore;
    protected Set<String> eventIds;
    
    public ContentFunctionEvaluator(Set<String> fields, int distance, float maxScore, Map<String,TermFrequencyList> termOffsetMap, String... terms) {
        this.fields = fields;
        this.distance = distance;
        this.maxScore = TermWeightPosition.PositionScoreToTermWeightScore(maxScore);
        this.termOffsetMap = termOffsetMap;
        this.terms = terms;
        
        this.canProcess = initialize();
    }
    
    /**
     * Is a path computable. This is used at various stages to fail-fast.
     * 
     * @return true if computable, false otherwise
     */
    protected boolean computable() {
        return canProcess;
    }
    
    /**
     * Validate the arguments.
     * 
     * @param distance
     * @param termOffsetMap
     * @param terms
     */
    protected boolean isValidArguments(int distance, Map<String,TermFrequencyList> termOffsetMap, String[] terms) {
        if (termOffsetMap == null || (distance < 0) || terms.length < 2) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Evaluate the function based on the list of offset lists. The lists are expected to be ordered, and there is one offset list per term.
     * 
     * @param offsets
     * @return true if true
     */
    protected abstract List<Integer> evaluate(List<List<TermWeightPosition>> offsets);
    
    /**
     * Validate and initialize this class. This will validate the arguments and setup other members.
     * 
     * @return true if valid, false if not valid.
     */
    protected boolean initialize() {
        if (log.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Distance: ").append(distance);
            sb.append(", termOffsetMap: ").append((termOffsetMap == null) ? "null" : termOffsetMap.toString());
            sb.append(", terms: ").append(Arrays.toString(terms));
            
            log.trace(sb.toString());
        }
        
        // Make sure we have at least two terms and that our distance is capable of occurring
        // i.e. impossible to find a phrase where 3 terms exist within 1 position, must be at least 2 in this case.
        if (!isValidArguments(distance, termOffsetMap, terms)) {
            if (log.isTraceEnabled()) {
                log.trace("Failing within() because of bad arguments");
                log.trace(distance + " " + ((termOffsetMap == null) ? "null" : termOffsetMap.toString()) + " " + Arrays.toString(terms));
            }
            
            return false;
        }
        
        // generate an intersection of event ids that cover all of the terms
        for (String term : terms) {
            if (term == null) {
                if (log.isTraceEnabled()) {
                    log.trace("Failing process() because of a null term");
                }
                
                return false;
            }
            
            TermFrequencyList tfList = termOffsetMap.get(term);
            
            if (tfList == null) {
                if (log.isTraceEnabled()) {
                    log.trace("Failing process() because of a null offset list for " + term);
                }
                
                return false;
            }
            if (tfList.fetchOffsets().size() == 0) {
                if (log.isTraceEnabled()) {
                    log.trace("Failing process() because of an empty offset list for " + term);
                }
                
                return false;
            }
            
            if (eventIds == null) {
                eventIds = new HashSet<>(tfList.eventIds());
            } else {
                eventIds.retainAll(tfList.eventIds());
            }
        }
        
        if (eventIds == null || eventIds.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace("Failing process() because of an empty event id intersection across the terms");
            }
            
            return false;
        }
        
        return true;
    }
    
    /**
     * Evaluate whether there is an unordered set of terms that are within the defined distance.
     * 
     * @return true if found, false otherwise
     */
    public List<Integer> evaluate() {
        if (computable()) {
            // now for each event, lets process the terms
            for (String eventId : eventIds) {
                ListMultimap<String,List<TermWeightPosition>> offsetsByField = LinkedListMultimap.create();
                for (String term : terms) {
                    TermFrequencyList tfList = termOffsetMap.get(term);
                    
                    // Invert the map to take all of the offsets for a term within a field
                    // and group the lists together
                    for (String field : tfList.fields()) {
                        TermFrequencyList.Zone zone = new TermFrequencyList.Zone(field, true, eventId);
                        Collection<TermWeightPosition> offsets = tfList.fetchOffsets().get(zone);
                        // if no offsets, but we are explicitly looking for this field (i.e. not unfielded), then check for a non-content expansion zone
                        if (offsets.isEmpty() && (fields != null && fields.contains(field))) {
                            zone = new TermFrequencyList.Zone(field, false, eventId);
                            offsets = tfList.fetchOffsets().get(zone);
                        }
                        // not all field/event pairs will have offsets
                        if (offsets != null && !offsets.isEmpty()) {
                            offsetsByField.put(field, Lists.newArrayList(offsets));
                        }
                    }
                }
                
                // If we have no offset lists, we can't match anything for this event
                // (shouldn't happen because we are using an intersection of event ids...but just in case)
                if (offsetsByField.isEmpty()) {
                    continue;
                }
                
                // Iterate over each collection of offsets (grouped by field) and try to find one that satisfies the phrase/adjacency
                for (String field : offsetsByField.keySet()) {
                    List<List<TermWeightPosition>> offsets = offsetsByField.get(field);
                    if (offsets == null || offsets.isEmpty()) {
                        continue;
                    }
                    
                    String logPrefix = "";
                    if (log.isTraceEnabled()) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Testing content evaluation in ").append(field).append('(').append(eventId).append(") over terms: [");
                        int length = terms.length;
                        for (int i = 0; i < length; i++) {
                            String term = terms[i];
                            String offset = i < offsets.size() ? offsets.get(i).toString() : "[]";
                            
                            sb.append(term).append(":").append(offset);
                            if (i < length - 1) {
                                sb.append(", ");
                            }
                        }
                        sb.append("]");
                        
                        logPrefix = sb.toString();
                    }
                    
                    // fail quick if we did not find enough offsets
                    if (offsets.size() < terms.length) {
                        continue;
                    }
                    
                    // evaluate the offsets
                    List<Integer> match = evaluate(offsets);
                    if (match != null && match.size() > 0) {
                        if (log.isTraceEnabled()) {
                            log.trace(logPrefix + " satisfied the content function");
                        }
                        
                        return match;
                    } else if (log.isTraceEnabled()) {
                        log.trace(logPrefix + " did not satisfy the content function");
                    }
                }
            }
        }
        
        return null;
    }
    
    @Override
    public String toString() {
        return "ContentFunctionEvaluator{fields=" + fields + ", distance=" + distance + ", terms=" + Arrays.toString(terms) + ", termOffsetMap="
                        + termOffsetMap + ", canProcess=" + canProcess + ", eventIds=" + eventIds + '}';
    }
    
}
