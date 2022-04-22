package datawave.query.postprocessing.tf;

import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import datawave.query.Constants;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * This class represents a collection of indexes that have been identified for phrases that have been found as matching hits for queries. The indexes here are
 * required to retrieve excerpts when requested.
 */
public class PhraseIndexes {
    
    private final SortedSetMultimap<String,Pair<Integer,Integer>> map = TreeMultimap.create();
    
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
                int start = Integer.parseInt(indexParts[0]);
                int end = Integer.parseInt(indexParts[1]);
                phraseIndexes.addIndexPair(field, start, end);
            }
        }
        return phraseIndexes;
    }
    
    /**
     * Add an index pair identified as a phrase for a hit for the specified field.
     * 
     * @param field
     *            the field
     * @param start
     *            the start index of the phrase
     * @param end
     *            the end index of the phrase
     */
    public void addIndexPair(String field, int start, int end) {
        map.put(field, Pair.with(start, end));
    }
    
    /**
     * Get all index pairs found for matching hits for the specified field.
     * 
     * @param field
     *            the field
     * @return the index pairs
     */
    public Collection<Pair<Integer,Integer>> getIndices(String field) {
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
     * {@link PhraseIndexes#from(String)}. The string will have the format FIELD:start,end:start,end:.../FIELD:start,end:...
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
            Iterator<Pair<Integer,Integer>> indexIterator = map.get(field).iterator();
            while (indexIterator.hasNext()) {
                Pair<Integer,Integer> indexPair = indexIterator.next();
                sb.append(indexPair.getValue0()).append(Constants.COMMA).append(indexPair.getValue1());
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
}
