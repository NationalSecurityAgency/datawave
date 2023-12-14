package datawave.query.transformer;

import com.google.common.collect.Iterators;
import com.google.protobuf.InvalidProtocolBufferException;
import datawave.common.util.ArgumentChecker;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.ExcerptFields;
import datawave.query.attributes.ValueTuple;
import datawave.query.function.JexlEvaluation;
import datawave.query.iterator.logic.TermFrequencyExcerptIterator;
import datawave.query.postprocessing.tf.PhraseIndexes;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;
import org.javatuples.Triplet;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

public class ExcerptTransform extends DocumentTransform.DefaultDocumentTransform {

    private static final Logger log = Logger.getLogger(ExcerptTransform.class);

    public static final String PHRASE_INDEXES_ATTRIBUTE = "PHRASE_INDEXES_ATTRIBUTE";
    public static final String HIT_EXCERPT = "HIT_EXCERPT";

    private final Map<String,String> excerptIteratorOptions = new HashMap<>();
    private final SortedKeyValueIterator<Key,Value> excerptIterator;
    private final ExcerptFields excerptFields;
    private final IteratorEnvironment env;
    private final SortedKeyValueIterator<Key,Value> source;

    private final List<String> hitTermValues = new ArrayList<>();

    public ExcerptTransform(ExcerptFields excerptFields, IteratorEnvironment env, SortedKeyValueIterator<Key,Value> source) {
        this(excerptFields, env, source, new TermFrequencyExcerptIterator());
    }

    public ExcerptTransform(ExcerptFields excerptFields, IteratorEnvironment env, SortedKeyValueIterator<Key,Value> source,
                    SortedKeyValueIterator<Key,Value> excerptIterator) {
        ArgumentChecker.notNull(excerptFields);
        this.excerptFields = excerptFields;
        this.env = env;
        this.source = source;
        this.excerptIterator = excerptIterator;
    }

    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> entry) {
        if (entry != null) {
            Document document = entry.getValue();
            // Do not bother adding excerpts to transient documents.
            if (document.isToKeep()) {
                PhraseIndexes phraseIndexes = getPhraseIndexes(document);
                if (!phraseIndexes.isEmpty()) {
                    if (log.isTraceEnabled()) {
                        log.trace("Fetching phrase excerpts " + excerptFields + " for document " + document.getMetadata());
                    }
                    Set<Excerpt> excerpts = getExcerpts(phraseIndexes);
                    addExcerptsToDocument(excerpts, document);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Phrase indexes were not added to document " + document.getMetadata() + ", skipping");
                    }
                }
            }
        }
        return entry;
    }

    /**
     * Retrieve the phrase indexes from the {@value #PHRASE_INDEXES_ATTRIBUTE} attribute in the document.
     *
     * @param document
     *            the document
     * @return the phrase indexes
     */
    private PhraseIndexes getPhraseIndexes(Document document) {
        PhraseIndexes phraseIndexes = null;
        PhraseIndexes allPhraseIndexes = new PhraseIndexes();
        // first lets find all of the phrase indexes that came from phrase functions
        if (document.containsKey(PHRASE_INDEXES_ATTRIBUTE)) {
            Content content = (Content) document.get(PHRASE_INDEXES_ATTRIBUTE);
            phraseIndexes = PhraseIndexes.from(content.getContent());
            allPhraseIndexes.addAll(phraseIndexes);
        }
        // now lets find all the terms for excerpt fields and add them to the list
        if (document.containsKey(JexlEvaluation.HIT_TERM_FIELD)) {
            Attributes hitList = (Attributes) document.get(JexlEvaluation.HIT_TERM_FIELD);
            // for each hit term
            for (Attribute<?> attr : hitList.getAttributes()) {
                ValueTuple hitTuple = attributeToHitTuple(attr);
                // if this is for a requested excerpt field
                if (excerptFields.containsField(hitTuple.getFieldName())) {
                    // get the offset, preferring offsets that overlap with existing phrases for this field/eventId
                    TermWeightPosition pos = getOffset(hitTuple, phraseIndexes);
                    if (pos != null) {
                        // add the term as a phrase as defined in the term weight position. Note that this will collapse with any overlapping phrases already in
                        // the list.
                        allPhraseIndexes.addIndexTriplet(String.valueOf(hitTuple.getFieldName()), keyToEventId(attr.getMetadata()), pos.getLowOffset(),
                                        pos.getOffset());
                    }
                    // save the hit term for later callout
                    // pull HIT_TERM_FIELD and pull value from that???
                    hitTermValues.add((String) hitTuple.getValue());
                }
            }
        }
        // return the file set of phrase indexes if any
        return allPhraseIndexes;
    }

    /**
     * Get the term weight position (offset) for the specified hit term. This will return an offset overlapping a phrase in the existing phrase index map first.
     * Otherwise the first position will be returned.
     *
     * @param hitTuple
     *            The hit term tuple
     * @param phraseIndexes
     *            The phrase indexes
     * @return The TermWeightPosition for the given hit term
     */
    private TermWeightPosition getOffset(ValueTuple hitTuple, PhraseIndexes phraseIndexes) {
        Key docKey = hitTuple.getSource().getMetadata();
        // if we do not know the source document key, then we cannot find the term offset
        if (docKey == null) {
            log.warn("Unable to find the source document for a hit term, skipping excerpt generation");
            return null;
        }
        String fieldName = hitTuple.getFieldName();
        String eventId = keyToEventId(docKey);

        // get the key at which we would find the term frequencies
        Key tfKey = new Key(docKey.getRow().toString(), Constants.TERM_FREQUENCY_COLUMN_FAMILY.toString(),
                        docKey.getColumnFamily().toString() + '\u0000' + hitTuple.getValue() + '\u0000' + hitTuple.getFieldName());
        Range range = new Range(tfKey, tfKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL));
        try {
            // seek directly to that key
            source.seek(range, Collections.emptyList(), false);
            if (source.hasTop()) {
                TermWeightPosition pos = null;

                // parse the term frequencies
                TermWeight.Info twInfo = TermWeight.Info.parseFrom(source.getTopValue().get());

                // if we have phrase indexes, then find one that overlaps if any
                if (phraseIndexes != null) {
                    pos = phraseIndexes.getOverlappingPosition(fieldName, eventId, twInfo);
                }

                // if no overlapping phrases, then return the first position
                if (pos == null) {
                    TermWeightPosition.Builder position = new TermWeightPosition.Builder();
                    position.setTermWeightOffsetInfo(twInfo, 0);
                    pos = position.build();
                }

                return pos;
            }

        } catch (InvalidProtocolBufferException e) {
            log.error("Value passed to aggregator was not of type TermWeight.Info for " + tfKey, e);
        } catch (IOException e) {
            log.error("Failed to scan for term frequencies at " + tfKey, e);
        }
        return null;
    }

    /**
     * Given a hit term attribute, return a ValueTuple representation which will give us the field and value parsed out.
     *
     * @param source
     *            a hit term attribute
     * @return A ValueTuple representation of the document hit-term attribute
     */
    private ValueTuple attributeToHitTuple(Attribute<?> source) {
        String hitTuple = String.valueOf(source.getData());
        int index = hitTuple.indexOf(':');
        String fieldName = hitTuple.substring(0, index);
        String value = hitTuple.substring(index + 1);
        return new ValueTuple(fieldName, value, value, source);
    }

    /**
     * Add the excerpts to the document as part of {@value #HIT_EXCERPT}.
     *
     * @param excerpts
     *            the excerpts to add
     * @param document
     *            the document
     */
    private void addExcerptsToDocument(Set<Excerpt> excerpts, Document document) {
        Attributes attributes = new Attributes(true);
        for (Excerpt excerpt : excerpts) {
            Content content = new Content(excerpt.getExcerpt(), excerpt.getSource(), true);
            attributes.add(content);
        }
        document.put(HIT_EXCERPT, attributes);
    }

    /**
     * Given an event ID, return the document Key
     *
     * @param eventId
     *            eventId string
     * @return the document Key
     */
    private Key eventIdToKey(String eventId) {
        if (eventId != null) {
            int split = eventId.indexOf('\u0000');
            if (split < 0) {
                throw new IllegalStateException("Malformed eventId (expected a null separator): " + eventId);
            }
            return new Key(eventId.substring(0, split), eventId.substring(split + 1));
        }
        return null;
    }

    /**
     * Given a document key, return the eventId
     *
     * @param docKey
     *            document key
     * @return the event id (shard\x00dt\x00uid)
     */
    private String keyToEventId(Key docKey) {
        if (docKey != null) {
            return docKey.getRow().toString() + '\u0000' + docKey.getColumnFamily().toString();
        }
        return null;
    }

    /**
     * Get the excerpts.
     *
     * @param phraseIndexes
     *            the pre-identified phrase offsets
     * @return the excerpts
     */
    private Set<Excerpt> getExcerpts(PhraseIndexes phraseIndexes) {
        phraseIndexes = getOffsetPhraseIndexes(phraseIndexes);
        if (phraseIndexes.isEmpty()) {
            return Collections.emptySet();
        }

        // Fetch the excerpts.
        Set<Excerpt> excerpts = new HashSet<>();
        for (String field : phraseIndexes.getFields()) {
            Collection<Triplet<String,Integer,Integer>> indexes = phraseIndexes.getIndices(field);
            for (Triplet<String,Integer,Integer> indexPair : indexes) {
                String eventId = indexPair.getValue0();
                int start = indexPair.getValue1();
                int end = indexPair.getValue2();
                if (log.isTraceEnabled()) {
                    log.trace("Fetching excerpt [" + start + "," + end + "] for field " + field + " for document " + eventId.replace('\u0000', '/'));
                }

                // Construct the required range for this document.
                Key startKey = eventIdToKey(eventId);
                Key endKey = startKey.followingKey(PartialKey.ROW_COLFAM);
                Range range = new Range(startKey, true, endKey, false);

                String excerpt = getExcerpt(field, start, end, range, hitTermValues);
                // Only retain non-blank excerpts.
                if (excerpt != null && !excerpt.isEmpty()) {
                    excerpts.add(new Excerpt(startKey, excerpt));
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Failed to find excerpt [" + start + "," + end + "] for field " + field + "for document " + eventId.replace('\u0000', '/'));
                    }
                }
            }
        }
        return excerpts;
    }

    /**
     * Get the excerpt for the specified field.
     *
     * @param field
     *            the field
     * @param start
     *            the start index of the excerpt
     * @param end
     *            the end index of the excerpt
     * @param range
     *            the range to use when seeking
     * @return the excerpt
     */
    private String getExcerpt(String field, int start, int end, Range range, List<String> hitTermValues) {
        excerptIteratorOptions.put(TermFrequencyExcerptIterator.FIELD_NAME, field);
        excerptIteratorOptions.put(TermFrequencyExcerptIterator.START_OFFSET, String.valueOf(start));
        excerptIteratorOptions.put(TermFrequencyExcerptIterator.END_OFFSET, String.valueOf(end));
        try {
            excerptIterator.init(source, excerptIteratorOptions, env);
            excerptIterator.seek(range, Collections.emptyList(), false);
            if (excerptIterator.hasTop()) {
                Key topKey = excerptIterator.getTopKey();
                String[] parts = topKey.getColumnQualifier().toString().split(Constants.NULL);
                // The column qualifier is expected to be field\0phrase.
                if (parts.length == 2) {
                    List<String> hitPhrase = new ArrayList<>();
                    for (String phrasePart: parts[1].split(Constants.SPACE)) {
                        if (hitTermValues.contains(phrasePart)) {
                            hitPhrase.add("[" + phrasePart + "]");
                        } else {
                            hitPhrase.add(phrasePart);
                        }
                    }
                    return String.join(" ", hitPhrase);
                } else {
                    log.warn(TermFrequencyExcerptIterator.class.getSimpleName() + " returned top key with incorrectly-formatted column qualifier in key: "
                                    + topKey + " when scanning for excerpt [" + start + "," + end + "] for field " + field + " within range " + range);
                    return null;
                }
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan for excerpt [" + start + "," + end + "] for field " + field + " within range " + range, e);
        }
    }

    /**
     * Returned a filtered {@link PhraseIndexes} that only contains the fields for which excerpts are desired, with the indexes offset by the specified excerpt
     * offset.
     *
     * @param phraseIndexes
     *            the original phrase indexes
     * @return the filtered, offset phrase indexes
     */
    private PhraseIndexes getOffsetPhraseIndexes(PhraseIndexes phraseIndexes) {
        PhraseIndexes offsetPhraseIndexes = new PhraseIndexes();
        for (String field : excerptFields.getFields()) {
            // Filter out phrases that are not in desired fields.
            Collection<Triplet<String,Integer,Integer>> indexes = phraseIndexes.getIndices(field);
            if (indexes != null) {
                int offset = excerptFields.getOffset(field);
                // Ensure the offset is modified to encompass the target excerpt range.
                for (Triplet<String,Integer,Integer> indexPair : indexes) {
                    String eventId = indexPair.getValue0();
                    int start = indexPair.getValue1() <= offset ? 0 : indexPair.getValue1() - offset;
                    int end = indexPair.getValue2() + offset + 1; // Add 1 here to offset the non-inclusive end of the range that will be used when scanning.
                    offsetPhraseIndexes.addIndexTriplet(field, eventId, start, end);
                }
            }
        }
        return offsetPhraseIndexes;
    }

    /**
     * Add phrase excerpts to the documents from the given iterator.
     *
     * @param in
     *            the iterator source
     * @return an iterator that will supply the enriched documents
     */
    public Iterator<Entry<Key,Document>> getIterator(final Iterator<Entry<Key,Document>> in) {
        return Iterators.transform(in, this);
    }

    /**
     * A class that holds the info for one excerpt.
     */
    private static class Excerpt {
        private final String excerpt;
        private final Key source;

        public Excerpt(Key source, String excerpt) {
            this.source = source;
            this.excerpt = excerpt;
        }

        public String getExcerpt() {
            return excerpt;
        }

        public Key getSource() {
            return source;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Excerpt excerpt1 = (Excerpt) o;
            return excerpt.equals(excerpt1.excerpt) && source.equals(excerpt1.source);
        }

        @Override
        public int hashCode() {
            return Objects.hash(excerpt, source);
        }
    }

}
