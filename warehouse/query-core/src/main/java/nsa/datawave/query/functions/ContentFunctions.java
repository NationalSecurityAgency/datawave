package nsa.datawave.query.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * <p>
 * Jexl functions that determine if a set of offset lists occur within some arbitrary distance. The functions must be given at least two terms. If the function
 * encounters any errors in the arguments, it will return false. Thus, improper function calls will appear to return no results to the user.
 * </p>
 *
 * <p>
 * To support within() queries on all unicode terms, the terms are presented as an array of Strings and the integer offset lists are stored in the
 * <code>Map&lt;String, List&lt;Integer&gt;&gt;</code>. There should be an entry in the map for each term provided in the function call
 * </p>
 *
 * <b>Functions</b>
 * <ul>
 * <li>content:within(int, map, term1, term2, ...)</li>
 * <ul>
 * <li>Returns true if the terms occur within the specified distance of each other</li>
 * <li>The distance parameter is the maximum acceptable distance (term offset) between the terms provided</li>
 * <li>For example, for the phrase "the quick brown fox" content:within(2, 'quick', 'brown', 'fox') will return true because the difference in word offsets one
 * and three is less than or equal to two (3 - 1 <= 2). Searching for content:within(1, 'quick', 'brown', 'fox') will fail because it is impossible for three
 * terms to have a minimum distance of two.</li>
 * </ul>
 * <li>content:within(zone, int, map, term1, term2, ...)</li>
 * <ul>
 * <li>Same as content:within() but with a zone specified</li>
 * </ul>
 * <li>content:adjacent(map, term1, term2, ...)</li>
 * <ul>
 * <li>Calls within() with a distance of num(terms) - 1, meaning that all of the terms occur next to each other</li>
 * <li>e.g. content:adjacent(map, term1, term2, term3) will call content:within(2, map, term1, term2, term3)</li>
 * </ul>
 * <li>content:adjacent(zone, map, term1, term2, ...)</li>
 * <ul>
 * <li>Same as content:adjacent() but with a zone specified</li>
 * </ul>
 * <li>content:phrase(map, term1, term2, ...)</li>
 * <ul>
 * <li>Only matches true on documents that contain the terms adjacent to each other in the order provided. Synonyms at the same position are considered
 * adjacent.</li>
 * </ul>
 * <li>content:phrase(zone, map, term1, term2, ...)</li>
 * <ul>
 * <li>Same as content:phrase() but with a zone specified</li>
 * </ul>
 * </ul>
 *
 * NOTE: The JexlFunctionArgumentDescriptorFactory is implemented by ContentFunctionsDescriptor. This is kept as a separate class to reduce accumulo
 * dependencies on other jars.
 *
 * 
 *
 */
@Deprecated
@JexlFunctions(descriptorFactory = "nsa.datawave.query.functions.ContentFunctionsDescriptor")
public class ContentFunctions {
    private static final Logger log = Logger.getLogger(ContentFunctions.class);
    
    private static class MultiOffsetMatcher {
        boolean orderMatters = false;
        int distance = 0;
        List<List<Integer>> offsetLists = new ArrayList<>();
        List<Integer> currentOffsets = new ArrayList<>();
        
        public MultiOffsetMatcher(int distance, List<List<Integer>> offsetLists, boolean orderMatters) {
            this.distance = distance;
            this.orderMatters = orderMatters;
            
            for (List<Integer> list : offsetLists) {
                if (list == null || list.size() == 0) {
                    if (log.isTraceEnabled()) {
                        log.trace("An offset list is null or has no elements: " + list + ". Exiting");
                    }
                    
                    this.offsetLists = null;
                    this.currentOffsets = null;
                    return;
                }
                
                // defensive copy because we will be modifying these later.
                list = new ArrayList<>(list);
                
                this.currentOffsets.add(list.get(0));
                
                list.remove(0);
                
                this.offsetLists.add(list);
            }
        }
        
        private boolean finished() {
            if (offsetLists == null || currentOffsets == null || offsetLists.size() <= 1 || currentOffsets.size() <= 1) {
                return true;
            }
            
            for (List<Integer> offsetList : this.offsetLists) {
                if (offsetList.size() > 0) {
                    return false;
                }
            }
            
            return true;
        }
        
        public boolean findMatch() {
            // While none of our offset lists are empty
            while (!finished()) {
                int minOffset = -1, minOffsetIndex = -1, maxOffset = -1;
                
                // Find the minimum and maximum offsets for the current list
                for (int i = 0; i < this.currentOffsets.size(); ++i) {
                    Integer offset = this.currentOffsets.get(i);
                    if (minOffset == -1 || offset < minOffset) {
                        minOffset = offset;
                        minOffsetIndex = i;
                    }
                    
                    if (maxOffset == -1 || offset > maxOffset) {
                        maxOffset = offset;
                    }
                }
                
                // If our range of offsets is less than our desired distance,
                // these terms match the requirement
                if (orderMatters) {
                    boolean matched = true;
                    
                    // Ensure that each offset is less than the previous (since the order matters)
                    for (int i = 1; i < this.currentOffsets.size() && matched; i++) {
                        if (this.currentOffsets.get(i) - this.currentOffsets.get(i - 1) != 1) {
                            matched = false;
                        }
                    }
                    
                    // If the offsets match, make sure the phrase distance also matches
                    if (matched) {
                        return true;
                    }
                } else {
                    if (maxOffset - minOffset <= distance) {
                        return true;
                    }
                }
                
                // Need to update the minimum offset with the next offset from the same list
                List<Integer> minList = offsetLists.get(minOffsetIndex);
                
                // If the list we want to pull the next offset from is empty, we found no match
                if (minList.size() == 0) {
                    return false;
                } else {
                    // Update the currentOffset with the next offset from the same list
                    currentOffsets.set(minOffsetIndex, minList.get(0));
                    minList.remove(0);
                }
            }
            
            // Make sure we catch the last case
            int minOffset = -1, maxOffset = -1;
            
            // Find the minimum and maximum offsets for the current list
            for (int i = 0; i < this.currentOffsets.size(); ++i) {
                Integer offset = this.currentOffsets.get(i);
                if (minOffset == -1 || offset < minOffset) {
                    minOffset = offset;
                }
                
                if (maxOffset == -1 || offset > maxOffset) {
                    maxOffset = offset;
                }
            }
            
            // If our range of offsets is less than our desired distance,
            // these terms match the requirement
            if (orderMatters) {
                boolean matched = true;
                
                // Ensure that each offset is less than or equal to the previous
                // Order matters except for synonyms occupying the same position
                int distance = 0;
                for (int i = 1; i < this.currentOffsets.size() && matched; i++) {
                    distance = this.currentOffsets.get(i) - this.currentOffsets.get(i - 1);
                    if (distance < 0 || distance > 1) {
                        matched = false;
                    }
                }
                
                // If the offsets match, make sure the phrase distance also matches
                if (matched) {
                    return true;
                }
            } else {
                // Correctly handles synonyms that have the same term position
                if (maxOffset - minOffset <= distance) {
                    return true;
                }
            }
            
            return false;
        }
    }
    
    /**
     * Check if the arguments meet the critera to proceed with the content function
     *
     * @param distance
     * @param termOffsetMap
     * @param orderMatters
     * @param terms
     * @return
     */
    private static boolean isValidArguments(int distance, Map<String,List<Integer>> termOffsetMap, boolean orderMatters, String[] terms) {
        if (termOffsetMap == null || (distance < terms.length - 1 && !orderMatters) || (distance != -1 && orderMatters) || terms.length < 2) {
            return false;
        }
        
        return true;
    }
    
    /**
     * The actual implementation of the terms within some distance function.
     *
     * @param distance
     *            The maximum acceptable distance for the terms to occur near each other
     * @param offsets
     *            A list of integer offset lists. One integer offset list for each term.
     * @return true if a permutation of the terms exists where all terms are within {@link #distance} of each other, false otherwise
     */
    private static boolean process(int distance, Map<String,List<Integer>> termOffsetMap, boolean orderMatters, String[] terms) {
        
        if (log.isTraceEnabled()) {
            log.trace("distance: " + distance);
            log.trace("termOffsetMap: " + termOffsetMap);
            
            if (terms == null)
                // noinspection ImplicitArrayToString
                log.trace("terms: null" + terms);
            else
                log.trace("terms: " + Arrays.toString(terms));
            
            if (terms != null && termOffsetMap != null) {
                for (String term : terms) {
                    if (termOffsetMap.containsKey(term) && termOffsetMap.get(term) != null) {
                        log.trace("Input offsets: " + Arrays.toString(termOffsetMap.get(term).toArray(new Integer[termOffsetMap.get(term).size()])));
                    }
                }
            }
        }
        
        // Make sure we have at least two terms and that our distance is capable of occurring
        // i.e. impossible to find a phrase where 3 terms exist within 1 position, must be at least 2 in this case.
        if (!isValidArguments(distance, termOffsetMap, orderMatters, terms)) {
            if (log.isTraceEnabled()) {
                log.trace("Failing within() because of bad arguments");
                log.trace(distance + " " + termOffsetMap + " " + orderMatters + " " + Arrays.toString(terms));
            }
            
            return false;
        }
        
        List<List<Integer>> allOffsetLists = new ArrayList<>();
        
        for (String term : terms) {
            if (term == null) {
                log.trace("Failing process() because of a null term");
                
                return false;
            }
            
            List<Integer> offsetList = termOffsetMap.get(term);
            
            if (offsetList == null) {
                log.trace("Failing process() because of a null offset list");
                
                return false;
            }
            if (offsetList.size() == 0) {
                log.trace("Failing process() because of an empty offset list");
                
                return false;
            }
            
            allOffsetLists.add(offsetList);
        }
        
        // If we have no offset lists, we can't match anything
        if (allOffsetLists.size() == 0) {
            return false;
        }
        
        MultiOffsetMatcher mlIter = new MultiOffsetMatcher(distance, allOffsetLists, orderMatters);
        
        if (mlIter.findMatch()) {
            return true;
        }
        
        return false;
    }
    
    /**
     * Determine if the given offset lists have any permutation of across each offset list that is within the distance given.
     *
     * @param distance
     *            The maximum acceptable distance for the terms to occur near each other
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return true if a permutation of the terms exists where all terms are within {@link #distance} of each other, false otherwise
     */
    public static boolean within(int distance, Map<String,List<Integer>> termOffsetMap, String... terms) {
        return process(distance, termOffsetMap, false, terms);
    }
    
/**
     * Like {@link #within(int, Map, String...) but limited to only finding a match in a single zone.
     *
     * @param zone The zone to search within
     * @param distance The maximum acceptable distance for the terms to occur near each other
     * @param termOffsetMap A map of terms and their offset lists
     * @param terms The array of terms
     * @return true if a permutation of the terms exists where all terms are within the specified distance of each other in the same zone, false otherwise
     * @see #within(int, Map, String...)
     */
    public static boolean within(String zone, int distance, Map<String,List<Integer>> termOffsetMap, String... terms) {
        return process(distance, termOffsetMap, false, terms);
    }
    
    /**
     * A wrapper function around searching for a occurrence where the terms exist next to each other Calls {@link #within} with a distance argument of the
     * length of {@link #offsets}
     *
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return True if a permutation of the terms exists where all terms are next to each other, false otherwise.
     */
    public static boolean adjacent(Map<String,List<Integer>> termOffsetMap, String... terms) {
        return process(terms.length - 1, termOffsetMap, false, terms);
    }
    
    /**
     * Wrapper around {@link #adjacent} in which a zone is provided
     *
     * @param zone
     *            The zone the phrase must occur in
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return
     */
    public static boolean adjacent(String zone, Map<String,List<Integer>> termOffsetMap, String... terms) {
        return process(terms.length - 1, termOffsetMap, false, terms);
    }
    
    /**
     * Searches for the terms occurring in the order specified
     *
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @returns True if the specified phrase occurs in the order that the terms were provided
     */
    public static boolean phrase(Map<String,List<Integer>> termOffsetMap, String... terms) {
        return process(-1, termOffsetMap, true, terms);
    }
    
    /**
     * Wrapper around {@link #phrase} in which a zone is provided
     *
     * @param zone
     *            The zone the phrase must occur in
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @returns True if the specified phrase occurs in the order that the terms were provided in the specified zone
     */
    public static boolean phrase(String zone, Map<String,List<Integer>> termOffsetMap, String... terms) {
        return process(-1, termOffsetMap, true, terms);
    }
    
    /*************** The following method variants deal with a list of termOffsetMaps *******************/
    
    /**
     * The actual implementation of the terms within some distance function.
     *
     * @param distance
     *            The maximum acceptable distance for the terms to occur near each other
     * @param offsets
     *            A list of map of terms to a list of integer offset lists. One integer offset list for each term.
     * @return true if a permutation of the terms exists where all terms are within {@link #distance} of each other, false otherwise
     */
    private static boolean process(int distance, Collection<Map<String,List<Integer>>> termOffsetMaps, boolean orderMatters, String[] terms) {
        for (Map<String,List<Integer>> termOffsetMap : termOffsetMaps) {
            if (process(distance, termOffsetMap, orderMatters, terms)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Determine if the given offset lists have any permutation of across each offset list that is within the distance given.
     *
     * @param distance
     *            The maximum acceptable distance for the terms to occur near each other
     * @param termOffsetMaps
     *            A list of map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return true if a permutation of the terms exists where all terms are within {@link #distance} of each other, false otherwise
     */
    public static boolean within(int distance, Collection<Map<String,List<Integer>>> termOffsetMaps, String... terms) {
        return process(distance, termOffsetMaps, false, terms);
    }
    
/**
     * Like {@link #within(int, Map, String...) but limited to only finding a match in a single zone.
     *
     * @param zone The zone to search within
     * @param distance The maximum acceptable distance for the terms to occur near each other
     * @param termOffsetMaps A list of map of terms and their offset lists
     * @param terms The array of terms
     * @return true if a permutation of the terms exists where all terms are within the specified distance of each other in the same zone, false otherwise
     * @see #within(int, Map, String...)
     */
    public static boolean within(String zone, int distance, Collection<Map<String,List<Integer>>> termOffsetMaps, String... terms) {
        return process(distance, termOffsetMaps, false, terms);
    }
    
    /**
     * A wrapper function around searching for a occurrence where the terms exist next to each other Calls {@link #within} with a distance argument of the
     * length of {@link #offsets}
     *
     * @param termOffsetMaps
     *            A list of map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return True if a permutation of the terms exists where all terms are next to each other, false otherwise.
     */
    public static boolean adjacent(Collection<Map<String,List<Integer>>> termOffsetMaps, String... terms) {
        return process(terms.length - 1, termOffsetMaps, false, terms);
    }
    
    /**
     * Wrapper around {@link #adjacent} in which a zone is provided
     *
     * @param zone
     *            The zone the phrase must occur in
     * @param termOffsetMaps
     *            A list of map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return
     */
    public static boolean adjacent(String zone, Collection<Map<String,List<Integer>>> termOffsetMaps, String... terms) {
        return process(terms.length - 1, termOffsetMaps, false, terms);
    }
    
    /**
     * Searches for the terms occurring in the order specified
     *
     * @param termOffsetMaps
     *            A list of map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @returns True if the specified phrase occurs in the order that the terms were provided
     */
    public static boolean phrase(Collection<Map<String,List<Integer>>> termOffsetMaps, String... terms) {
        return process(-1, termOffsetMaps, true, terms);
    }
    
    /**
     * Wrapper around {@link #phrase} in which a zone is provided
     *
     * @param zone
     *            The zone the phrase must occur in
     * @param termOffsetMaps
     *            A list of map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @returns True if the specified phrase occurs in the order that the terms were provided in the specified zone
     */
    public static boolean phrase(String zone, Collection<Map<String,List<Integer>>> termOffsetMaps, String... terms) {
        return process(-1, termOffsetMaps, true, terms);
    }
    
}
