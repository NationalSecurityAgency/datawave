package datawave.query.transformer;

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

    /**
     * Length of time in milliseconds that a client will wait while results are collected. If a full page is not collected before the timeout, a blank page will
     * be returned to signal the request is still in progress.
     */
    private final long queryExecutionForPageTimeout;

    /**
     * Constructor
     *
     * @param groupFields
     *            the fields (user provided) to group by and aggregate
     * @param queryExecutionForPageTimeout
     *            how long (in milliseconds) to let a page of results to collect before signaling to return a blank page to the client
     * @param markingFunctions
     *            the marking functions
     */
    public GroupingTransform(GroupFields groupFields, MarkingFunctions markingFunctions, long queryExecutionForPageTimeout) {
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

        if (keyDocumentEntry != null) {

            // If this is a final document, bail without adding to the keys, countingMap or fieldVisibilities.
            if (FinalDocumentTrackingIterator.isFinalDocumentKey(keyDocumentEntry.getKey())) {
                log.debug("GroupingTransform saw {} documents producing {} groups", documentCount, groups.getGroups().size());
                return keyDocumentEntry;
            }

            if (keyDocumentEntry.getValue().isIntermediateResult()) {
                return keyDocumentEntry;
            }

            documentCount++;
            mostRecentKey = keyDocumentEntry.getKey();
            log.trace("{} get list key counts for: {}", "web-server", keyDocumentEntry);
            DocumentGrouper.group(keyDocumentEntry, groupFields, groups);
        }

        long elapsedExecutionTimeForCurrentPage = System.currentTimeMillis() - this.queryExecutionForPageStartTime;
        if (elapsedExecutionTimeForCurrentPage > this.queryExecutionForPageTimeout) {
            log.debug("Generating intermediate result because over {}ms has been reached since {}", this.queryExecutionForPageTimeout,
                            this.queryExecutionForPageStartTime);
            Document intermediateResult = new Document();
            intermediateResult.setIntermediateResult(true);
            return Maps.immutableEntry(new Key(), intermediateResult);
        }

        return null;
    }

    @Override
    public void setQueryExecutionForPageStartTime(long queryExecutionForPageStartTime) {
        log.debug("setting query execution page start time to {}", queryExecutionForPageStartTime);
        super.setQueryExecutionForPageStartTime(queryExecutionForPageStartTime);
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
