package datawave.query.transformer;

import static datawave.query.iterator.waitwindow.WaitWindowObserver.WAIT_WINDOW_OVERRUN;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.LinkedList;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.slf4j.Logger;

import com.google.common.collect.Maps;

import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.query.common.grouping.DocumentGrouper;
import datawave.query.common.grouping.Group;
import datawave.query.common.grouping.GroupFields;
import datawave.query.common.grouping.GroupingUtils;
import datawave.query.common.grouping.Groups;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;

/**
 * GroupingTransform mimics GROUP BY with a COUNT in SQL. For the given fields, this transform will group into unique combinations of values and assign a count
 * to each combination. It is possible that values in a specific group may hold different column visibilities. Because the multiple fields are aggregated into
 * one, it is necessary to combine the column visibilities for the fields and remark the grouped fields. Additionally, the overall document visibility must be
 * computed.
 */
public class GroupingTransform extends DocumentTransform.DefaultDocumentTransform {

    private static final Logger log = getLogger(GroupingTransform.class);

    /**
     * the fields (user provided) to group by
     */
    private GroupFields groupFields;

    private final Groups groups;

    /**
     * list of documents to return, created from the countingMap
     */
    private final LinkedList<Document> documents = new LinkedList<>();

    /**
     * The last key seen, used to build documents
     */
    private Key mostRecentKey = null;

    /**
     * Track the number of documents seen by this transform
     */
    private long documentCount = 0L;

    // Page state is guarded by pageLock; document processing stays outside the lock.
    private final Object pageLock = new Object();
    private long pageGeneration;
    private boolean documentSeenWithinPageTimeout;

    /**
     * Page window in milliseconds. An intermediate result is eligible after this window only if a document was seen within it.
     */
    private final long queryExecutionForPageTimeout;

    /**
     * Constructor
     *
     * @param groupFields
     *            the fields (user provided) to group by and aggregate
     * @param queryExecutionForPageTimeout
     *            page window in milliseconds; an intermediate result requires a document observed within that window
     * @param markingFunctions
     *            the marking functions
     */
    public GroupingTransform(GroupFields groupFields, MarkingFunctions<?> markingFunctions, long queryExecutionForPageTimeout) {
        super.initialize(settings, markingFunctions);
        this.queryExecutionForPageTimeout = queryExecutionForPageTimeout;
        this.groups = new Groups();
        this.groupFields = groupFields;
    }

    public void updateConfig(GroupFields groupFields) {
        this.groupFields = groupFields;
    }

    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> keyDocumentEntry) {
        log.trace("apply to {}", keyDocumentEntry);

        long generation;
        synchronized (pageLock) {
            generation = pageGeneration;
        }
        if (keyDocumentEntry != null) {

            // If this is a final document, bail without adding to the keys, countingMap or fieldVisibilities.
            if (FinalDocumentTrackingIterator.isFinalDocumentKey(keyDocumentEntry.getKey())) {
                log.debug("GroupingTransform saw {} documents producing {} groups", documentCount, groups.getGroups().size());
                return keyDocumentEntry;
            }

            Document document = keyDocumentEntry.getValue();
            if (document != null && document.containsKey(WAIT_WINDOW_OVERRUN)) {
                if (log.isDebugEnabled()) {
                    log.debug("Got a WAIT_WINDOW_OVERRUN key in group transform: " + keyDocumentEntry.getKey());
                }
                return keyDocumentEntry;
            }

            if (keyDocumentEntry.getValue().isIntermediateResult()) {
                return keyDocumentEntry;
            }

            synchronized (pageLock) {
                long elapsed = clock.millis() - queryExecutionForPageStartTime;
                if (generation == pageGeneration && elapsed >= 0 && elapsed <= queryExecutionForPageTimeout) {
                    documentSeenWithinPageTimeout = true;
                }
            }

            documentCount++;
            mostRecentKey = keyDocumentEntry.getKey();
            log.trace("{} get list key counts for: {}", "web-server", keyDocumentEntry);
            DocumentGrouper.group(keyDocumentEntry, groupFields, groups);
        }

        synchronized (pageLock) {
            // An in-flight document must not consume activity from a newer page window.
            if (generation != pageGeneration) {
                return null;
            }
            long now = clock.millis();
            if (documentSeenWithinPageTimeout && now - queryExecutionForPageStartTime > queryExecutionForPageTimeout) {
                log.debug("Generating intermediate result after {}ms with page activity", queryExecutionForPageTimeout);
                queryExecutionForPageStartTime = now;
                pageGeneration++;
                documentSeenWithinPageTimeout = false;
                Document intermediateResult = new Document();
                intermediateResult.setIntermediateResult(true);
                return Maps.immutableEntry(new Key(), intermediateResult);
            }
        }

        return null;
    }

    @Override
    public void setQueryExecutionForPageStartTime(long queryExecutionForPageStartTime) {
        log.debug("setting query execution page start time to {}", queryExecutionForPageStartTime);
        synchronized (pageLock) {
            super.setQueryExecutionForPageStartTime(queryExecutionForPageStartTime);
            pageGeneration++;
            documentSeenWithinPageTimeout = false;
        }
    }

    @Override
    public Entry<Key,Document> flush() {
        Document document = null;
        if (!groups.isEmpty()) {
            for (Group group : groups.getGroups()) {
                documents.add(GroupingUtils.createDocument(group, mostRecentKey, markingFunctions, GroupingUtils.AverageAggregatorWriteFormat.AVERAGE));
            }
        }

        if (!documents.isEmpty()) {
            log.trace("{} will flush first of {} documents: {}", this.hashCode(), documents.size(), documents);
            document = documents.pop();
        }

        if (document != null) {
            Key key = document.getMetadata();
            Entry<Key,Document> entry = Maps.immutableEntry(key, document);
            log.trace("flushing out {}", entry);
            groups.clear();
            return entry;
        }

        return null;
    }
}
