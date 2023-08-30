package datawave.query.postprocessing.tf;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.javatuples.Triplet;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;

/**
 * This class represents a collection of indexes that have been identified for phrases that have been found as matching hits for queries. The indexes here are
 * required to retrieve excerpts when requested.
 */
public class PhraseIndexes {
    private static final Logger log = Logger.getLogger(PhraseIndexes.class);

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
                // if the event ID is empty, then it must have been null initially (see toString())
                if (eventId.isEmpty()) {
                    eventId = null;
                }
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
        // ensure offsets ordered correctly
        if (start > end) {
            int temp = start;
            start = end;
            end = temp;
        }

        // first remove any overlapping phrases and extend the start/end appropriately
        if (map.containsKey(field)) {
            Iterator<Triplet<String,Integer,Integer>> indices = map.get(field).iterator();
            while (indices.hasNext()) {
                Triplet<String,Integer,Integer> indice = indices.next();
                // if we have gone past the end, then no more possibility of overlapping
                if (indice.getValue1() > end) {
                    break;
                }
                // if from the same event/document, and the endpoints overlap
                if (Objects.equals(eventId, indice.getValue0()) && overlaps(indice.getValue1(), indice.getValue2(), start, end)) {
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
     * Get all index pairs found for matching hits for the specified field. May return null.
     *
     * @param field
     *            the field
     * @return the index pairs if any, otherwise null
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
                if (indexTriplet.getValue0() != null) {
                    sb.append(indexTriplet.getValue0());
                }
                sb.append(Constants.COMMA).append(indexTriplet.getValue1());
                sb.append(Constants.COMMA).append(indexTriplet.getValue2());
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
     * Utility function to see if two offset ranges overlap
     *
     * @param start1
     *            start1
     * @param end1
     *            end1
     * @param start2
     *            start2
     * @param end2
     *            end2
     * @return true if (start1 &lt;= end2 &amp;&amp; end1 &gt;= start2)
     */
    public static boolean overlaps(int start1, int end1, int start2, int end2) {
        return (start1 <= end2 && end1 >= start2);
    }

    /**
     * Utility function to see if a triplet overlaps with a term weight position
     *
     * @param triplet
     *            triplet
     * @param pos
     *            pos
     * @return true if overlapping
     */
    public static boolean overlaps(Triplet<String,Integer,Integer> triplet, TermWeightPosition pos) {
        return overlaps(triplet.getValue1(), triplet.getValue2(), pos.getLowOffset(), pos.getOffset());
    }

    /**
     * Get the overlapping triplet if any
     *
     * @param fieldName
     *            the field name
     * @param eventId
     *            an event id
     * @param position
     *            term weight position
     * @return the overlapping triplet
     */
    public Triplet<String,Integer,Integer> getOverlap(String fieldName, String eventId, TermWeightPosition position) {
        Collection<Triplet<String,Integer,Integer>> indexes = getIndices(fieldName);
        if (indexes != null) {
            int start = position.getLowOffset();
            int end = position.getOffset();
            for (Triplet<String,Integer,Integer> triplet : indexes) {
                // if the start of the triplet is past the end, then no more possibility of overlapping
                if (triplet.getValue1() > end) {
                    break;
                }
                if (Objects.equals(eventId, triplet.getValue0()) && overlaps(triplet.getValue1(), triplet.getValue2(), start, end)) {
                    return triplet;
                }
            }
        }
        return null;
    }

    /**
     * Get the overlapping term weight position if any
     *
     * @param fieldName
     *            the field name
     * @param eventId
     *            an event id
     * @param twInfo
     *            term weight info
     * @return An overlapping TermWeightPosition if any
     */
    public TermWeightPosition getOverlappingPosition(String fieldName, String eventId, TermWeight.Info twInfo) {
        // get the phases for this fieldname
        Collection<Triplet<String,Integer,Integer>> triplets = getIndices(fieldName);

        if (triplets != null) {
            // get the triplets is reverse sorted order base on the end index and filtered by event id
            List<Triplet<String,Integer,Integer>> reverseEndIndexSortedList = triplets.stream().filter(t -> Objects.equals(eventId, t.getValue0()))
                            .sorted(endOffsetComparator.reversed()).collect(Collectors.toList());

            if (!reverseEndIndexSortedList.isEmpty()) {
                TermWeightPosition.Builder position = new TermWeightPosition.Builder();

                // first ensure we have a sorted list of term offsets
                if (!(Ordering.<Integer> natural().isOrdered(twInfo.getTermOffsetList()))) {
                    log.warn("Term offset list is not ordered, reverting to brute force search");
                    for (int i = 0; i < twInfo.getTermOffsetCount(); i++) {
                        position.setTermWeightOffsetInfo(twInfo, i);
                        TermWeightPosition pos = position.build();
                        if (getOverlap(fieldName, eventId, pos) != null) {
                            // if we have an overlapping phrase, then return this position
                            return pos;
                        }
                    }
                } else {
                    // get the max prev skip value
                    int maxSkip = 0;
                    if (twInfo.getPrevSkipsCount() > 0) {
                        maxSkip = Ordering.<Integer> natural().max(twInfo.getPrevSkipsList());
                    }

                    // starting at the last term offset
                    int start = twInfo.getTermOffsetCount() - 1;

                    // iterator through the phrase triplets
                    for (Triplet<String,Integer,Integer> triplet : reverseEndIndexSortedList) {
                        // find the index of the first offset before or equal to the triplet end offset plus the max skip value
                        start = findPrevOffset(twInfo, start, triplet.getValue2() + maxSkip);
                        // if we have an index, then search backwards for an overlap
                        if (start >= 0) {
                            for (int offsetIndex = start; offsetIndex >= 0; offsetIndex--) {
                                position.setTermWeightOffsetInfo(twInfo, offsetIndex);
                                TermWeightPosition pos = position.build();
                                if (PhraseIndexes.overlaps(triplet, pos)) {
                                    return pos;
                                } else if (pos.getOffset() < triplet.getValue1()) {
                                    break;
                                }
                            }
                        } else {
                            // this means that there is no offset prior to the triplet end offset, and hence
                            // no offset prior to any of the remaining triplet end offsets. We can short-circuit
                            // this loop.
                            break;
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Find the last index starting at the specified startIndex of the offset that is less than or equal to the specified offset plus the maxSkip if any. -1
     * returned if none found.
     *
     * @param twInfo
     *            term weight info
     * @param startIndex
     *            the start index
     * @param offset
     *            the offset
     * @return the index of an offset equal or less than the specified offset
     */
    private static int findPrevOffset(TermWeight.Info twInfo, int startIndex, int offset) {
        // to find the offset that is less than or equal to the specified offset, we need to reverse the list and use a reverse comparator
        int nextOffset = Collections.binarySearch(Lists.reverse(twInfo.getTermOffsetList().subList(0, startIndex + 1)), offset, reverseIntegerCompare);

        // if a negative number is returned, then this would be the (-(insertion point) - 1)
        if (nextOffset < 0) {
            nextOffset = -1 - nextOffset;
        }

        // now revert to the offset in the positive direction
        nextOffset = startIndex - nextOffset;

        return nextOffset;
    }

    private static final Comparator<Triplet<String,Integer,Integer>> endOffsetComparator = new Comparator<Triplet<String,Integer,Integer>>() {
        @Override
        public int compare(Triplet<String,Integer,Integer> o1, Triplet<String,Integer,Integer> o2) {
            return o1.getValue2() - o2.getValue2();
        }
    };

    private static final Comparator<Integer> reverseIntegerCompare = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }
    }.reversed();

}
