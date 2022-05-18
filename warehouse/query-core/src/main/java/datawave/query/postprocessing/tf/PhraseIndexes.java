package datawave.query.postprocessing.tf;

import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import datawave.query.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec;
import org.javatuples.Triplet;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

/**
 * This class represents a collection of indexes that have been identified for phrases that have been found as matching hits for queries. The indexes here are
 * required to retrieve excerpts when requested.
 */
public class PhraseIndexes {
    
    /**
     * A Map of fieldname to eventId,start,end phrase offsets. The eventId has the form as defined by TermFrequencyList.getEventid(key)
     */
    private final SortedSetMultimap<String,Triplet<String,Integer,Integer>> map = TreeMultimap.create();
    
    /**
     * Returns a new {@link PhraseIndexes} parsed from the string. The provided string is expected to have the format returned by
     * {@link PhraseIndexes#toString()}.
     * <ul>
     * <li>Given null, null will be returned.</li>
     * <li>Given an empty or blank string, an empty {@link PhraseIndexes} will be returned.</li>
     * <li>Given {@code BODY:1,2:3,5/CONTENT:5,6:7,6}, a {@link PhraseIndexes} will be returned with offsets [1,2] and [3,5] for field {@code BODY}, and offsets
     * [5,6] and [7,6] for field {@code CONTENT}.</li>
     * </ul>
     * 
     * @param string
     *            the string to parse
     * @return the parsed {@link PhraseIndexes}
     */
    public static PhraseIndexes from(String string) {
        if (string == null) {
            return null;
        }
        
        // Strip whitespaces.
        string = StringUtils.deleteWhitespace(string);
        
        PhraseIndexes phraseIndexes = new PhraseIndexes();
        String[] fieldParts = string.split(Constants.FORWARD_SLASH);
        for (String fieldPart : fieldParts) {
            String[] parts = fieldPart.split(Constants.COLON);
            String field = parts[0];
            for (int i = 1; i < parts.length; i++) {
                String[] indexParts = parts[i].split(Constants.COMMA);
                String eventId = indexParts[0];
                int start = Integer.parseInt(indexParts[1]);
                int end = Integer.parseInt(indexParts[2]);
                phraseIndexes.addIndexTriplet(field, eventId, start, end);
            }
        }
        return phraseIndexes;
    }
    
    /**
     * Add the phraseIndexes from another phrase indexes object
     * 
     * @param phraseIndexes
     *            The other phrase index object
     */
    public void addAll(PhraseIndexes phraseIndexes) {
        for (String field : phraseIndexes.getFields()) {
            for (Triplet<String,Integer,Integer> indice : phraseIndexes.getIndices(field)) {
                addIndexTriplet(field, indice.getValue0(), indice.getValue1(), indice.getValue2());
            }
        }
    }
    
    /**
     * Add an index pair identified as a phrase for a hit for the specified field. If this phrase overlaps with another, then they will be merged.
     *
     * @param field
     *            the field
     * @param eventId
     *            the event id (see @TermFrequencyList.getEventId(Key))
     * @param start
     *            the start index of the phrase
     * @param end
     *            the end index of the phrase
     */
    public void addIndexTriplet(String field, String eventId, int start, int end) {
        // first remove any overlapping phrases and extend the start/end appropriately
        if (map.containsKey(field)) {
            Iterator<Triplet<String,Integer,Integer>> indices = map.get(field).iterator();
            while (indices.hasNext()) {
                Triplet<String,Integer,Integer> indice = indices.next();
                // if we have gone past the end, then no more possiblility of overlapping
                if (indice.getValue1() > end) {
                    break;
                }
                // if from the same event/document, and the endpoints overlap
                if (indice.getValue0().equals(eventId) && overlaps(indice.getValue1(), indice.getValue2(), start, end)) {
                    start = Math.min(start, indice.getValue1());
                    end = Math.max(end, indice.getValue2());
                    indices.remove();
                }
            }
        }
        map.put(field, Triplet.with(eventId, start, end));
    }
    
    /**
     * Add the specified index triplet to set of phrase indexes. If this phrase overlaps with another, then they will be merged.
     * 
     * @param field
     *            The field name
     * @param offset
     *            The phrase triplet
     */
    public void addIndexTriplet(String field, Triplet<String,Integer,Integer> offset) {
        addIndexTriplet(field, offset.getValue0(), offset.getValue1(), offset.getValue2());
    }
    
    /**
     * Get all index pairs found for matching hits for the specified field.
     * 
     * @param field
     *            the field
     * @return the index pairs
     */
    public Collection<Triplet<String,Integer,Integer>> getIndices(String field) {
        return map.get(field);
    }
    
    /**
     * Return whether this {@link PhraseIndexes} has any phrase indexes
     * 
     * @return true if at least one phrase index pair is found, or false otherwise
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }
    
    /**
     * Clear this {@link PhraseIndexes} of any phrase indexes
     */
    public void clear() {
        map.clear();
    }
    
    /**
     * Return the fields for which phrase indexes have been found for hits.
     * 
     * @return the fields
     */
    public Set<String> getFields() {
        return map.keySet();
    }
    
    /**
     * Return whether this {@link PhraseIndexes} contains the given field.
     * 
     * @param field
     *            the field
     * @return true if the field is found, or false otherwise
     */
    public boolean containsField(String field) {
        return map.containsKey(field);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhraseIndexes that = (PhraseIndexes) o;
        return Objects.equals(map, that.map);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(map);
    }
    
    /**
     * Returns this {@link PhraseIndexes} as a formatted string that can later be parsed back into a {@link PhraseIndexes} using
     * {@link PhraseIndexes#from(String)}. The string will have the format FIELD:eventId,start,end:eventId,start,end:.../FIELD:eventId,start,end:...
     * 
     * @return a formatted string
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<String> fieldIterator = map.keySet().iterator();
        while (fieldIterator.hasNext()) {
            // Write the field.
            String field = fieldIterator.next();
            sb.append(field).append(Constants.COLON);
            // Write the indexes found for the field.
            Iterator<Triplet<String,Integer,Integer>> indexIterator = map.get(field).iterator();
            while (indexIterator.hasNext()) {
                Triplet<String,Integer,Integer> indexTriplet = indexIterator.next();
                sb.append(indexTriplet.getValue0()).append(Constants.COMMA).append(indexTriplet.getValue1()).append(Constants.COMMA)
                                .append(indexTriplet.getValue2());
                if (indexIterator.hasNext()) {
                    sb.append(Constants.COLON);
                }
            }
            if (fieldIterator.hasNext()) {
                sb.append(Constants.FORWARD_SLASH);
            }
        }
        return sb.toString();
    }
    
    /**
     * Get the phrase indexes only including those for a specified fieldname and event id
     * 
     * @param fieldName
     * @param eventId
     * @return PhraseIndexes containing only one field and one event id
     */
    public PhraseIndexes getSubset(String fieldName, String eventId) {
        PhraseIndexes subset = new PhraseIndexes();
        getIndices(fieldName).stream().filter(t -> t.getValue0().equals(eventId)).forEach(t -> subset.addIndexTriplet(fieldName, t));
        return subset;
    }
    
    /**
     * Return a map of fieldname to a map of eventId to its subset of phrase indexes.
     * 
     * @return a map of fieldname to eventId to phraseIndexes
     */
    public Map<String,Map<String,PhraseIndexes>> toMap() {
        Map<String,Map<String,PhraseIndexes>> map = new HashMap<>();
        for (String field : getFields()) {
            Map<String,PhraseIndexes> fieldMap = new HashMap<>();
            map.put(field, fieldMap);
            for (String eventId : getIndices(field).stream().map(t -> t.getValue0()).collect(Collectors.toSet())) {
                fieldMap.put(eventId, getSubset(field, eventId));
            }
        }
        return map;
    }
    
    /**
     * Utility function to see if two offset ranges overlap
     * 
     * @param start1
     * @param end1
     * @param start2
     * @param end2
     * @return true if (start1 &lt;= end2 &amp;&amp; end1 &gt;= start2)
     */
    private boolean overlaps(int start1, int end1, int start2, int end2) {
        return (start1 <= end2 && end1 >= start2);
    }
    
    /**
     * Get the overlapping triplet if any
     * 
     * @param fieldName
     * @param eventId
     * @param start
     * @param end
     * @return the overlapping triplet
     */
    public Triplet<String,Integer,Integer> getOverlap(String fieldName, String eventId, int start, int end) {
        for (Triplet<String,Integer,Integer> triplet : getIndices(fieldName)) {
            // if the start of the triplet is past the end, then no more possibility of overlapping
            if (triplet.getValue1() > end) {
                break;
            }
            if (triplet.getValue0().equals(eventId) && overlaps(triplet.getValue1(), triplet.getValue2(), start, end)) {
                return triplet;
            }
        }
        return null;
    }
}
