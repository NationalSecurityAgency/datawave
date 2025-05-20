package datawave.query.postprocessing.tf;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import datawave.query.jexl.functions.TermFrequencyList;

/**
 * This encapsulates term frequencies information that is used when evaluating content:phrase and content:within functions in a JEXL query. It is also used to
 * store phrase indexes that have been found for matching hits so that we can retrieve excerpts when requested.
 */
public class TermOffsetMap {

    // should we gather phrase offsets
    boolean gatherPhraseOffsets = false;

    // The set of excerpt fields to gather phrase offsets for.
    private Set<String> excerptFields;
    /**
     * The term frequencies, with their corresponding fields.
     */
    private final Map<String,TermFrequencyList> termFrequencies = new HashMap<>();

    /**
     * The phrase indexes found for hits.
     */
    private PhraseIndexes phraseIndexes = null;

    public TermOffsetMap() {}

    public TermOffsetMap(Map<String,TermFrequencyList> termFrequencies) {
        if (termFrequencies != null) {
            this.termFrequencies.putAll(termFrequencies);
        }
    }

    /**
     * Put the {@link TermFrequencyList} for the specified field
     *
     * @param field
     *            the field
     * @param termFrequencyList
     *            the term frequency list
     */
    public void putTermFrequencyList(String field, TermFrequencyList termFrequencyList) {
        termFrequencies.put(field, termFrequencyList);
    }

    /**
     * Return the term frequency list for the specified field.
     *
     * @param field
     *            the field
     * @return the term frequency list
     */
    public TermFrequencyList getTermFrequencyList(String field) {
        return termFrequencies.get(field);
    }

    /**
     * Return whether phrases indexes should be recorded and the given field is am excerpt field.
     *
     * @param field
     *            the field
     * @return true if phrase indexes should be recorded for the field, or false otherwise.
     */
    public boolean shouldRecordPhraseIndex(String field) {
        return gatherPhraseOffsets() && isExcerptField(field);
    }

    /**
     * Add a new phrase index pair found for a hit for the specified field
     *
     * @param field
     *            the field
     * @param eventId
     *            the event id (see @TermFrequencyList.getEventId(Key))
     * @param start
     *            the phrase starting index
     * @param end
     *            the phrase ending index
     */
    public void addPhraseIndexTriplet(String field, String eventId, int start, int end) {
        if (phraseIndexes != null) {
            phraseIndexes.addIndexTriplet(field, eventId, start, end);
        }
    }

    /**
     * Return all phrase indexes found for hits for the specified field.
     *
     * @param field
     *            the field
     * @return the phrase indexes
     */
    public Collection<PhraseOffset> getPhraseIndexes(String field) {
        if (phraseIndexes != null) {
            return phraseIndexes.getPhraseOffsets(field);
        }
        return null;
    }

    /**
     * Return the underlying {@link PhraseIndexes} object
     *
     * @return a PhraseIndexes object
     */
    public PhraseIndexes getPhraseIndexes() {
        return phraseIndexes;
    }

    public boolean gatherPhraseOffsets() {
        return gatherPhraseOffsets;
    }

    public void setGatherPhraseOffsets(boolean gatherPhraseOffsets) {
        this.gatherPhraseOffsets = gatherPhraseOffsets;
        if (gatherPhraseOffsets) {
            if (this.phraseIndexes == null) {
                this.phraseIndexes = new PhraseIndexes();
            }
        } else {
            this.phraseIndexes = null;
            this.excerptFields = null;
        }
    }

    /**
     * Set the excerpt fields.
     *
     * @param excerptFields
     *            the fields
     */
    public void setExcerptFields(Set<String> excerptFields) {
        this.excerptFields = excerptFields;
    }

    /**
     * Return whether the given field is an excerpt field
     *
     * @param field
     *            the field
     * @return true if the field is an excerpt field, or false otherwise
     */
    public boolean isExcerptField(String field) {
        return excerptFields != null && excerptFields.contains(field);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TermOffsetMap that = (TermOffsetMap) o;
        return gatherPhraseOffsets == that.gatherPhraseOffsets && Objects.equals(excerptFields, that.excerptFields)
                        && Objects.equals(termFrequencies, that.termFrequencies) && Objects.equals(phraseIndexes, that.phraseIndexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gatherPhraseOffsets, excerptFields, termFrequencies, phraseIndexes);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TermOffsetMap.class.getSimpleName() + "[", "]").add("gatherPhraseOffsets=" + gatherPhraseOffsets)
                        .add("excerptFields=" + excerptFields).add("termFrequencies=" + termFrequencies).add("phraseIndexes=" + phraseIndexes).toString();
    }
}
