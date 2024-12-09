package datawave.query.transformer;

import static datawave.query.iterator.logic.ContentSummaryIterator.ONLY_SPECIFIED;
import static datawave.query.iterator.logic.ContentSummaryIterator.SUMMARY_SIZE;
import static datawave.query.iterator.logic.ContentSummaryIterator.VIEW_NAMES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import datawave.common.util.ArgumentChecker;
import datawave.query.Constants;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.attributes.SummaryOptions;
import datawave.query.iterator.logic.ContentSummaryIterator;

/**
 * This class is used to add summaries to returned documents when specified.
 * <p>
 * </p>
 * An iterator of type "ContentSummaryIterator" is used to do the summary generation using options from a "SummaryOptions"
 */
public class SummaryTransform extends DocumentTransform.DefaultDocumentTransform {

    private static final Logger log = LoggerFactory.getLogger(SummaryTransform.class);

    private static final String SUMMARY_ERROR_MESSAGE = "UNABLE TO GENERATE SUMMARY";
    private static final String SUMMARY_EMPTY_MESSAGE = "NO CONTENT FOUND TO SUMMARIZE";
    private static final Summary ERROR_SUMMARY = new Summary(null, SUMMARY_ERROR_MESSAGE);
    private static final Summary EMPTY_SUMMARY = new Summary(null, SUMMARY_EMPTY_MESSAGE);

    private static final String CONTENT_SUMMARY = "CONTENT_SUMMARY";

    private final ContentSummaryIterator summaryIterator;
    private final SummaryOptions summaryOptions;
    private final IteratorEnvironment env;
    private final SortedKeyValueIterator<Key,Value> source;

    public SummaryTransform(SummaryOptions summaryOptions, IteratorEnvironment env, SortedKeyValueIterator<Key,Value> source,
                    SortedKeyValueIterator<Key,Value> summaryIterator) {
        ArgumentChecker.notNull(summaryOptions);
        this.summaryOptions = summaryOptions;
        this.env = env;
        this.source = source;
        this.summaryIterator = (ContentSummaryIterator) summaryIterator;

    }

    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> entry) {
        if (entry != null) {
            Document document = entry.getValue();
            // Do not bother adding summaries to transient documents.
            if (document.isToKeep()) {
                ArrayList<DocumentKey> documentKeys = getEventIds(document);
                if (!documentKeys.isEmpty()) {
                    if (log.isTraceEnabled()) {
                        log.trace("Fetching summaries {} for document {}", summaryOptions, document.getMetadata());
                    }
                    Set<Summary> summaries = getSummaries(documentKeys);
                    addSummariesToDocument(summaries, document);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("document keys were not added to document {}, skipping", document.getMetadata());
                    }
                }
            }
        }
        return entry;
    }

    /**
     * Retrieve the eventIds in the document.
     *
     * @param document
     *            the document
     * @return a list of the eventIds
     */
    private static ArrayList<DocumentKey> getEventIds(Document document) {
        ArrayList<DocumentKey> eventIds = new ArrayList<>();
        if (document.containsKey("RECORD_ID")) {
            eventIds.add((DocumentKey) document.get("RECORD_ID"));
        } else {
            Key key = document.getMetadata();
            String[] cf = key.getColumnFamily().toString().split(Constants.NULL);
            eventIds.add(new DocumentKey(key.getRow().toString(), cf[0], cf[1], document.isToKeep()));
        }

        return eventIds;
    }

    /**
     * Add the summaries to the document as part of {@value #CONTENT_SUMMARY}.
     *
     * @param summaries
     *            the summaries to add
     * @param document
     *            the document
     */
    private static void addSummariesToDocument(Set<Summary> summaries, Document document) {
        Attributes summaryAttribute = new Attributes(true);

        for (Summary summary : summaries) {
            if (!summary.isEmpty()) {
                Content contentSummary = new Content(summary.getSummary(), summary.getSource(), true);
                summaryAttribute.add(contentSummary);
            }
        }

        document.put(CONTENT_SUMMARY, summaryAttribute);
    }

    /**
     * Get the summaries.
     *
     * @param documentKeys
     *            the pre-identified document keys
     * @return the summaries
     */
    private Set<Summary> getSummaries(final ArrayList<DocumentKey> documentKeys) {
        if (documentKeys.isEmpty()) {
            return Collections.emptySet();
        }

        // Fetch the summaries.
        Set<Summary> summaries = new HashSet<>();
        for (DocumentKey documentKey : documentKeys) {
            if (log.isTraceEnabled()) {
                log.trace("Fetching summary for document {}",
                                documentKey.getShardId() + Constants.NULL + documentKey.getDataType() + Constants.NULL + documentKey.getUid());
            }

            // Construct the required range for this document.
            Key startKey = new Key(documentKey.getShardId(), documentKey.getDataType() + Constants.NULL + documentKey.getUid());
            Key endKey = startKey.followingKey(PartialKey.ROW_COLFAM);
            Range range = new Range(startKey, true, endKey, false);

            Summary summary = getSummary(range, summaryOptions);
            // Only retain non-blank summaries.
            if (!summary.isEmpty()) {
                summaries.add(summary);
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("Failed to find summary for document {}",
                                    documentKey.getShardId() + Constants.NULL + documentKey.getDataType() + Constants.NULL + documentKey.getUid());
                }
            }
        }
        return summaries;
    }

    /**
     * Get the summary
     *
     * @param range
     *            the range to use when seeking
     * @param summaryOptions
     *            the object with our summary specifications
     * @return the summary
     */
    private Summary getSummary(Range range, SummaryOptions summaryOptions) {
        // get the options out of the SummaryOptions object
        final Map<String,String> summaryIteratorOptions = new HashMap<>();
        summaryIteratorOptions.put(SUMMARY_SIZE, String.valueOf(summaryOptions.getSummarySize()));
        if (!summaryOptions.isEmpty()) {
            summaryIteratorOptions.put(VIEW_NAMES, summaryOptions.viewNamesListToString());
        }
        summaryIteratorOptions.put(ONLY_SPECIFIED, String.valueOf(summaryOptions.onlyListedViews()));

        try {
            // set all of our options for the iterator
            summaryIterator.init(source, summaryIteratorOptions, env);

            // run the iterator
            summaryIterator.seek(range, Collections.emptyList(), false);

            // if a summary is returned...
            if (summaryIterator.hasTop()) {
                // the excerpt will be in the column qualifier of the top key
                String summary = summaryIterator.getTopKey().getColumnQualifier().toString();
                // The column qualifier has the summary/summaries in it.
                // make sure the summary is not blank...
                if (summary.isBlank()) {
                    if (log.isErrorEnabled()) {
                        log.error("{} returned top key with blank column qualifier in key: {} when scanning for summary within range {}",
                                        ContentSummaryIterator.class.getSimpleName(), summaryIterator.getTopKey(), range);
                    }
                    return ERROR_SUMMARY;
                }
                // return our summary
                return new Summary(range.getStartKey(), summary);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan for summary within range " + range, e);
        }

        // when working correctly, it should always return from inside the loop so if this is reached something went very wrong
        return EMPTY_SUMMARY;
    }

    /**
     * Add summaries to the documents from the given iterator.
     *
     * @param in
     *            the iterator source
     * @return an iterator that will supply the enriched documents
     */
    public Iterator<Entry<Key,Document>> getIterator(final Iterator<Entry<Key,Document>> in) {
        return Iterators.transform(in, this);
    }

    /**
     * A class that holds the info for one summary.
     */
    private static class Summary {
        private final String summary;
        private final Key source;

        public Summary(Key source, String summary) {
            this.source = source;
            this.summary = summary;
        }

        public String getSummary() {
            return summary;
        }

        public Key getSource() {
            return source;
        }

        public boolean isEmpty() {
            return summary.isEmpty();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Summary summary1 = (Summary) o;
            return (summary.equals(summary1.summary) && source.equals(summary1.source));
        }

        @Override
        public int hashCode() {
            return Objects.hash(summary, source);
        }
    }

}
