package datawave.query.postprocessing.tf;

import java.util.Objects;

/**
 * This class represents a specific phrase that has been matched in a retrieved document field. It contains the id of the event it is associated with and the
 * start and end offset of the phrase in the field specified. The offsets match those found for individual terms in the tf portion of the index. <br>
 * <br>
 * The eventId has the form as defined by TermFrequencyList.getEventId(key)
 */
public class PhraseOffset implements Comparable<PhraseOffset> {
    private final String eventId;
    private final int startOffset;
    private final int endOffset;

    public static PhraseOffset with(String eventId, int startOffset, int endOffset) {
        return new PhraseOffset(eventId, startOffset, endOffset);
    }

    private PhraseOffset(String eventId, int startOffset, int endOffset) {
        this.eventId = eventId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public int compareTo(PhraseOffset o) {
        int cmp;
        if ((cmp = eventId.compareTo(o.eventId)) != 0) {
            return cmp;
        }
        if ((cmp = Integer.compare(startOffset, o.startOffset)) != 0) {
            return cmp;
        }
        return Integer.compare(endOffset, o.endOffset);
    }

    public int getEndOffset() {
        return endOffset;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public String getEventId() {
        return eventId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof PhraseOffset))
            return false;
        PhraseOffset that = (PhraseOffset) o;
        return startOffset == that.startOffset && endOffset == that.endOffset && Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, startOffset, endOffset);
    }
}
