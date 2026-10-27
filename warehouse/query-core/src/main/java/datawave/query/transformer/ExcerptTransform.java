package datawave.query.transformer;

import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.Configuration.END_OFFSET;
import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.Configuration.FIELD_NAME;
import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.Configuration.START_OFFSET;
import static datawave.query.tables.ShardQueryLogic.NULL_BYTE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import datawave.query.jexl.JexlASTHelper;
import datawave.query.postprocessing.tf.PhraseIndexes;
import datawave.query.postprocessing.tf.PhraseOffset;

public class ExcerptTransform extends DocumentTransform.DefaultDocumentTransform {

    private static final Logger log = LoggerFactory.getLogger(ExcerptTransform.class);
    public static final String PHRASE_INDEXES_ATTRIBUTE = "PHRASE_INDEXES_ATTRIBUTE";
    public static final String HIT_EXCERPT = "HIT_EXCERPT";
    public static final String HIT_EXCERPT_WITH_SCORES = "HIT_EXCERPT_WITH_SCORES";
    public static final String HIT_EXCERPT_ONE_BEST = "HIT_EXCERPT_ONE_BEST";
    public static final String EXCERPT_ERROR_MESSAGE = "SOMETHING WENT WRONG GENERATING YOUR EXCERPT!";
    private static final Excerpt ERROR_EXCERPT = new Excerpt(null, EXCERPT_ERROR_MESSAGE, EXCERPT_ERROR_MESSAGE, EXCERPT_ERROR_MESSAGE);

    private final TermFrequencyExcerptIterator excerptIterator;
    private final ExcerptFields excerptFields;
    private final IteratorEnvironment env;
    private final SortedKeyValueIterator<Key,Value> source;

    public ExcerptTransform(ExcerptFields excerptFields, IteratorEnvironment env, SortedKeyValueIterator<Key,Value> source) {
        this(excerptFields, env, source, new TermFrequencyExcerptIterator());
    }

    public ExcerptTransform(ExcerptFields excerptFields, IteratorEnvironment env, SortedKeyValueIterator<Key,Value> source,
                    SortedKeyValueIterator<Key,Value> excerptIterator) {
        ArgumentChecker.notNull(excerptFields);
        this.excerptFields = excerptFields;
        this.env = env;
        this.source = source;
        this.excerptIterator = (TermFrequencyExcerptIterator) excerptIterator;
    }

    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> entry) {
        if (entry != null) {
            Document document = entry.getValue();
            // Do not bother adding excerpts to transient documents.
            if (document.isToKeep()) {
                // hitTermValues is populated as a side effect of getPhraseIndexesAndHitTerms
                List<String> hitTermValues = new ArrayList<>();
                PhraseIndexes phraseIndexesAndHitTerms = getPhraseIndexesAndHitTerms(document, hitTermValues);
                if (!phraseIndexesAndHitTerms.isEmpty()) {
                    if (log.isTraceEnabled()) {
                        log.trace("Fetching phrase excerpts {} for document {}", excerptFields, document.getMetadata());
                    }
                    Set<Excerpt> excerpts = getExcerpts(phraseIndexesAndHitTerms, hitTermValues);
                    addExcerptsToDocument(excerpts, document);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Phrase indexes were not added to document {}, skipping", document.getMetadata());
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
     * @param hitTermValues
     *            holds hit terms
     * @return the phrase indexes
     */
    private PhraseIndexes getPhraseIndexesAndHitTerms(Document document, List<String> hitTermValues) {
        PhraseIndexes phraseIndexes = null;
        PhraseIndexes allPhraseIndexes = new PhraseIndexes();
        // first lets find all the phrase indexes that came from phrase functions
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
                if (getMatchingExcerptField(hitTuple.getFieldName(), excerptFields) != null) {
                    // get the offset, preferring offsets that overlap with existing phrases for this field/eventId
                    HitOffset hitOffset = getOffset(hitTuple, phraseIndexes);
                    if (hitOffset != null) {
                        // add the term as a phrase as defined in the term weight position. Note that this will collapse with any overlapping phrases already in
                        // the list.
                        allPhraseIndexes.addIndexTriplet(hitOffset.getField(), keyToEventId(attr.getMetadata()), hitOffset.getPosition().getLowOffset(),
                                        hitOffset.getPosition().getOffset());
                    }
                    // save the hit term for later call-out
                    Collections.addAll(hitTermValues, ((String) hitTuple.getValue()).split(Constants.SPACE));
                }
            }
        }
        // return the file set of phrase indexes if any
        return allPhraseIndexes;
    }

    /**
     * Get the term weight position (offset) for the specified hit term. This will return an offset overlapping a phrase in the existing phrase index map first.
     * Otherwise, the first position will be returned. The returned {@link HitOffset} also carries the concrete term-frequency field that was used to resolve
     * the offset so that excerpt generation can remain scoped to the matched content context.
     *
     * @param hitTuple
     *            The hit term tuple
     * @param phraseIndexes
     *            The phrase indexes
     * @return the concrete field and offset for the given hit term
     */
    private HitOffset getOffset(ValueTuple hitTuple, PhraseIndexes phraseIndexes) {
        Key docKey = hitTuple.getSource().getMetadata();
        // if we do not know the source document key, then we cannot find the term offset
        if (docKey == null) {
            log.warn("Unable to find the source document for a hit term, skipping excerpt generation");
            return null;
        }
        String fieldName = hitTuple.getFieldName();
        String baseFieldName = JexlASTHelper.removeGroupingContext(fieldName);

        String eventId = keyToEventId(docKey);

        // Term Frequency ranges have to include the baseFieldName through the <baseFieldName>.\uFFFF
        // Our field names are always alphanumeric with _ and now potentially . for the content hash
        // Since . is before _ and also all alphanumeric characters this should give all possible field values before the possibility of a false positive
        Key beginTfKey = new Key(docKey.getRow().toString(), Constants.TERM_FREQUENCY_COLUMN_FAMILY.toString(),
                        docKey.getColumnFamily().toString() + '\u0000' + hitTuple.getValue() + '\u0000' + hitTuple.getFieldName());
        Key endTfKey = new Key(docKey.getRow().toString(), Constants.TERM_FREQUENCY_COLUMN_FAMILY.toString(),
                        docKey.getColumnFamily().toString() + '\u0000' + hitTuple.getValue() + '\u0000' + baseFieldName + "." + '\uFFFF');
        Range range = new Range(beginTfKey, endTfKey);

        try {
            // seek directly to that key
            source.seek(range, Collections.emptyList(), false);
            if (source.hasTop()) {
                // parse the actual field name of the TF key to use for excerpt generation
                final String[] splits = source.getTopKey().getColumnQualifier().toString().split(NULL_BYTE);
                String parsedFieldName = splits[splits.length - 1];

                TermWeightPosition pos = null;

                // parse the term frequencies
                TermWeight.Info twInfo = TermWeight.Info.parseFrom(source.getTopValue().get());

                // if we have phrase indexes, then find one that overlaps if any
                if (phraseIndexes != null) {
                    pos = phraseIndexes.getOverlappingPosition(parsedFieldName, eventId, twInfo);
                }

                // if no overlapping phrases, then return the first position
                if (pos == null) {
                    TermWeightPosition.Builder position = new TermWeightPosition.Builder();
                    position.setTermWeightOffsetInfo(twInfo, 0);
                    pos = position.build();
                }

                return new HitOffset(parsedFieldName, pos);
            }

        } catch (InvalidProtocolBufferException e) {
            log.error("Value passed to aggregator was not of type TermWeight.Info in range {}", range, e);
        } catch (IOException e) {
            log.error("Failed to scan for term frequencies in range {}", range, e);
        }
        return null;
    }

    /**
     * Add the excerpts to the document as part of {@value #HIT_EXCERPT}.
     *
     * @param excerpts
     *            the excerpts to add
     * @param document
     *            the document
     */
    private static void addExcerptsToDocument(Set<Excerpt> excerpts, Document document) {
        Attributes attributesWithoutScores = new Attributes(true);
        Attributes attributesWithScores = new Attributes(true);
        Attributes attributesOneBest = new Attributes(true);

        boolean hasScores = false;

        for (Excerpt excerpt : excerpts) {
            Content contentWithoutScores = new Content(excerpt.getExcerptWithoutScores(), excerpt.getSource(), true);
            attributesWithoutScores.add(contentWithoutScores);

            String excerptWithScores = excerpt.getExcerptWithScores();
            if (excerptWithScores.isBlank() || excerptWithScores.equals(TermFrequencyExcerptIterator.NOT_SCORED_MARKER)) {
                continue;
            }

            hasScores = true;

            Content contentWithScores = new Content(excerptWithScores, excerpt.getSource(), true);
            attributesWithScores.add(contentWithScores);
            Content contentOneBest = new Content(excerpt.getExcerptOneBest(), excerpt.getSource(), true);
            attributesOneBest.add(contentOneBest);
        }

        document.put(HIT_EXCERPT, attributesWithoutScores);
        if (hasScores) {
            document.put(HIT_EXCERPT_WITH_SCORES, attributesWithScores);
            document.put(HIT_EXCERPT_ONE_BEST, attributesOneBest);
        }
    }

    /**
     * Get the excerpts.
     *
     * @param phraseIndexesAndHitTerms
     *            the pre-identified phrase offsets
     * @return the excerpts
     */
    private Set<Excerpt> getExcerpts(PhraseIndexes phraseIndexesAndHitTerms, List<String> hitTermValues) {
        // Downselect to specific excerpt fields and extent the offsets to include context around the hit term.
        final PhraseIndexes offsetPhraseIndexesHitTerms = getOffsetPhraseIndexes(phraseIndexesAndHitTerms, excerptFields);
        if (offsetPhraseIndexesHitTerms.isEmpty()) {
            return Collections.emptySet();
        }

        // Fetch the excerpts.
        Set<Excerpt> excerpts = new HashSet<>();
        for (String field : offsetPhraseIndexesHitTerms.getFields()) {
            Collection<PhraseOffset> indexes = offsetPhraseIndexesHitTerms.getPhraseOffsets(field);
            for (PhraseOffset phraseOffset : indexes) {
                String eventId = phraseOffset.getEventId();
                int start = phraseOffset.getStartOffset();
                int end = phraseOffset.getEndOffset();
                if (log.isTraceEnabled()) {
                    log.trace("Fetching excerpt [{},{}] for field {} for document {}", start, end, field, eventId.replace('\u0000', '/'));
                }

                // Construct the required range for this document.
                Key startKey = eventIdToKey(eventId);
                Key endKey;
                if (startKey != null) {
                    endKey = startKey.followingKey(PartialKey.ROW_COLFAM);
                } else {
                    throw new IllegalStateException("eventID string was null");
                }
                Range range = new Range(startKey, true, endKey, false);

                Excerpt excerpt = getExcerpt(field, start, end, range, hitTermValues);
                // Only retain non-blank excerpts.
                if (!excerpt.isEmpty()) {
                    excerpts.add(excerpt);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Failed to find excerpt [{},{}] for field {} for document {}", start, end, field, eventId.replace('\u0000', '/'));
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
     * @param hitTermValues
     *            the term values to match
     * @return the excerpt
     */
    private Excerpt getExcerpt(String field, int start, int end, Range range, List<String> hitTermValues) {
        // if given a beginning offset less than 0, set it to 0
        if (start < 0) {
            start = 0;
        }
        final float origHalfSize = (float) (end - start) / 2; // calculate "1/2 of the original requested excerpt size"
        final int expandSize = 20; // how much we want to expand the start and end offsets by

        final Map<String,String> excerptIteratorOptions = new HashMap<>();
        excerptIteratorOptions.put(FIELD_NAME, field);
        // We will attempt to create the excerpt we want up to two times.
        // Currently, the only condition that will cause a second attempt is if we detect stop words in the TFs we scan.
        // The main difference in the second attempt is that it runs with an expanded range to allow us to remove the
        // stop words and still have a correctly sized excerpt
        for (int attempt = 0; attempt <= 1; attempt++) {
            // if this is the first attempt, set the start and end offsets using the passed in values
            if (attempt == 0) {
                excerptIteratorOptions.put(START_OFFSET, String.valueOf(start));
                excerptIteratorOptions.put(END_OFFSET, String.valueOf(end));
            } else {
                // if this is the second attempt, set up the iterator with a larger range by adding/subtracting
                // the start and end offsets by "expandedSize"
                int expandedStart = Math.max(start - expandSize, 0);
                int expandedEnd = end + expandSize;
                excerptIteratorOptions.put(START_OFFSET, String.valueOf(expandedStart));
                excerptIteratorOptions.put(END_OFFSET, String.valueOf(expandedEnd));

                if (log.isDebugEnabled()) {
                    log.debug("size of excerpt requested: {}", getExcerptOffset(field) * 2);
                    log.debug("original range is ({},{}) and the expanded range is ({},{})", start, end, expandedStart, expandedEnd);
                }
            }

            try {
                // set all of our options for the iterator
                excerptIterator.init(source, excerptIteratorOptions, env);
                excerptIterator.setHitTermsList(hitTermValues);
                excerptIterator.setDirection(getExcerptDirection(field).toUpperCase().trim());
                excerptIterator.setOrigHalfSize(origHalfSize);
                // if this is the second attempt, we want the iterator to trim the excerpt down to the size we want.
                // (remember we run the iterator with an expanded range the second time so we can potentially have a bigger excerpt than needed even after
                // removing stop words)
                if (attempt == 1) {
                    excerptIterator.setTrimExcerpt(true);
                }

                // run the iterator
                excerptIterator.seek(range, Collections.emptyList(), false);

                // if an excerpt is returned...
                if (excerptIterator.hasTop()) {
                    // the excerpt will be in the column qualifier of the top key
                    Key topKey = excerptIterator.getTopKey();
                    // The column qualifier is expected to be field\0phraseWithScores\0phraseWithoutScores\0oneBestExcerpt.
                    // split the column qualifier on null bytes to get the different parts
                    // we should have 4 parts after splitting the column qualifier on the null bytes
                    final String[] parts = topKey.getColumnQualifier().toString().split(Constants.NULL);
                    // if we don't have 4 parts after splitting the column qualifier...
                    if (parts.length != 4) {
                        if (attempt == 0) { // if this is the first attempt, try again
                            continue;
                        }

                        // if this is the second attempt, log an error
                        if (log.isErrorEnabled()) {
                            log.error("{} returned top key with incorrectly-formatted column qualifier in key: {} when scanning for excerpt [{},{}] for field {} within range {} : parts= {}",
                                            TermFrequencyExcerptIterator.class.getSimpleName(), topKey, start, end, field, range, Arrays.toString(parts));
                        }
                        break;
                    }

                    // if we have reached the limit of times to try, or we have no stop words removed
                    if (!parts[1].equals(TermFrequencyExcerptIterator.WORD_SKIPPED_MARKER)) {
                        // return just the excerpt parts
                        return new Excerpt(range.getStartKey(), parts[1], parts[2], parts[3]);
                    }
                } else { // If no excerpt was returned on the first attempt, try again. If no excerpt was returned on the second attempt, log an error.
                    if (attempt == 1 && log.isErrorEnabled()) {
                        log.error("TermFrequencyExcerptIterator returned with hasTop() false: something went wrong in the iterator (or given bad parameters to run with)");
                        log.error("The iterator options were: Field \"{}\" Range= {} StartOffset= {} EndOffset= {} HitTerms= {}", field, range,
                                        excerptIteratorOptions.get(START_OFFSET), excerptIteratorOptions.get(END_OFFSET), hitTermValues);
                        break;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to scan for excerpt [" + start + "," + end + "] for field " + field + " within range " + range, e);
            }
        }
        // when working correctly, it should always return from inside the loop so if this is reached something went very wrong
        return ERROR_EXCERPT;
    }

    /**
     * Returned a filtered {@link PhraseIndexes} that only contains the fields for which excerpts are desired. We modify the start and end offsets based on the
     * amount of desired context around the hit terms we have been asked to include when generating the excerpt.
     *
     *
     * @param phraseIndexes
     *            the original phrase indexes
     * @param excerptFields
     *            the fields that we want excerpts for
     * @return the filtered, offset phrase indexes
     */
    private static PhraseIndexes getOffsetPhraseIndexes(PhraseIndexes phraseIndexes, ExcerptFields excerptFields) {
        PhraseIndexes offsetPhraseIndexes = new PhraseIndexes();
        for (String field : phraseIndexes.getFields()) {
            // Filter out phrases that are not in desired fields.
            String excerptField = getMatchingExcerptField(field, excerptFields);
            Collection<PhraseOffset> indexes = excerptField == null ? null : phraseIndexes.getPhraseOffsets(field);
            if (indexes != null) {
                int offset = excerptFields.getOffset(excerptField);
                // Ensure the offset is modified to encompass the target excerpt range.
                for (PhraseOffset indexPair : indexes) {
                    String eventId = indexPair.getEventId();
                    int start = indexPair.getStartOffset() <= offset ? 0 : indexPair.getStartOffset() - offset;
                    int end = indexPair.getEndOffset() + offset + 1; // Add 1 here to offset the non-inclusive end of the range that will be used when scanning.
                    offsetPhraseIndexes.addIndexTriplet(field, eventId, start, end);
                }
            }
        }
        return offsetPhraseIndexes;
    }

    private int getExcerptOffset(String field) {
        return excerptFields.getOffset(getMatchingExcerptField(field, excerptFields));
    }

    private String getExcerptDirection(String field) {
        return excerptFields.getDirection(getMatchingExcerptField(field, excerptFields));
    }

    /**
     * Return the configured excerpt field that should be used for the given concrete phrase-index field. The concrete field may include grouping/context
     * notation, while the excerpt configuration may only specify the base field.
     *
     * @param field
     *            a concrete phrase-index or term-frequency field
     * @param excerptFields
     *            the configured excerpt fields
     * @return the matching configured excerpt field, or null if no match exists
     */
    private static String getMatchingExcerptField(String field, ExcerptFields excerptFields) {
        if (excerptFields.containsField(field)) {
            return field;
        }

        for (String excerptField : excerptFields.getFields()) {
            if (JexlASTHelper.isGroupedFieldMatch(excerptField, field)) {
                return excerptField;
            }
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
    private static ValueTuple attributeToHitTuple(Attribute<?> source) {
        String hitTuple = String.valueOf(source.getData());
        int index = hitTuple.indexOf(':');
        String fieldName = hitTuple.substring(0, index);
        String value = hitTuple.substring(index + 1);
        return new ValueTuple(fieldName, value, value, source);
    }

    /**
     * Given an event ID, return the document Key
     *
     * @param eventId
     *            eventId string
     * @return the document Key
     */
    private static Key eventIdToKey(String eventId) {
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
    private static String keyToEventId(Key docKey) {
        return docKey != null ? docKey.getRow().toString() + '\u0000' + docKey.getColumnFamily().toString() : null;
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
        private final String excerptWithScores;
        private final String excerptWithoutScores;
        private final String excerptOneBest;
        private final Key source;

        public Excerpt(Key source, String excerptWithScores, String excerptWithoutScores, String excerptOneBest) {
            this.source = source;
            this.excerptWithScores = excerptWithScores;
            this.excerptWithoutScores = excerptWithoutScores;
            this.excerptOneBest = excerptOneBest;
        }

        public String getExcerptWithScores() {
            return excerptWithScores;
        }

        public String getExcerptWithoutScores() {
            return excerptWithoutScores;
        }

        public String getExcerptOneBest() {
            return excerptOneBest;
        }

        public Key getSource() {
            return source;
        }

        public boolean isEmpty() {
            return excerptWithScores.isEmpty() && excerptWithoutScores.isEmpty() && excerptOneBest.isEmpty();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Excerpt excerpt1 = (Excerpt) o;
            return (excerptWithScores.equals(excerpt1.excerptWithScores) && source.equals(excerpt1.source))
                            && (excerptWithoutScores.equals(excerpt1.excerptWithoutScores)) && (excerptOneBest.equals(excerpt1.excerptOneBest));
        }

        @Override
        public int hashCode() {
            return Objects.hash(excerptWithScores, excerptWithoutScores, excerptOneBest, source);
        }
    }

    /**
     * Holds the concrete term-frequency field and position selected for a hit term. The field is preserved separately from the requested excerpt field so that
     * later excerpt scans do not merge sibling content contexts such as {@code QUOTE} and {@code QUOTE.hash1}.
     */
    private static class HitOffset {
        private final String field;
        private final TermWeightPosition position;

        public HitOffset(String field, TermWeightPosition position) {
            this.field = field;
            this.position = position;
        }

        public String getField() {
            return field;
        }

        public TermWeightPosition getPosition() {
            return position;
        }
    }

}
