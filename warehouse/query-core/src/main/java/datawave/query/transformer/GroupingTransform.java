package datawave.query.transformer;

import com.google.common.collect.Maps;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.query.common.grouping.FieldAggregator;
import datawave.query.common.grouping.DocumentGrouper;
import datawave.query.common.grouping.GroupAggregateFields;
import datawave.query.common.grouping.GroupingUtils;
import datawave.query.common.grouping.Groups;
import datawave.query.common.grouping.Group;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.model.QueryModel;
import org.apache.accumulo.core.data.Key;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

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
    private GroupAggregateFields groupAggregateFields;
    
    private final Groups groups;
    
    /**
     * list of documents to return, created from the countingMap
     */
    private final LinkedList<Document> documents = new LinkedList<>();
    
    /**
     * mapping used to combine field names that map to different model names
     */
    private Map<String,String> reverseModelMapping = null;
    
    /**
     * list of keys that have been read, in order to keep track of where we left off when a new iterator is created
     */
    private final List<Key> keys = new ArrayList<>();
    
    /**
     * Length of time in milliseconds that a client will wait while results are collected. If a full page is not collected before the timeout, a blank page will
     * be returned to signal the request is still in progress.
     */
    private final long queryExecutionForPageTimeout;
    
    /**
     * Constructor
     *
     * @param model
     *            the query model (can be null)
     * @param groupAggregateFields
     *            the fields (user provided) to group by and aggregate
     * @param queryExecutionForPageTimeout
     *            how long (in milliseconds) to let a page of results to collect before signaling to return a blank page to the client
     * @param markingFunctions
     *            the marking functions
     */
    public GroupingTransform(QueryModel model, GroupAggregateFields groupAggregateFields, MarkingFunctions markingFunctions, long queryExecutionForPageTimeout) {
        super.initialize(settings, markingFunctions);
        this.queryExecutionForPageTimeout = queryExecutionForPageTimeout;
        this.groups = new Groups();
        this.groupAggregateFields = groupAggregateFields;
        if (model != null) {
            this.reverseModelMapping = model.getReverseQueryMapping();
        }
    }
    
    public void updateConfig(GroupAggregateFields groupAggregateFields, QueryModel model) {
        this.groupAggregateFields = groupAggregateFields;
        if (model != null) {
            reverseModelMapping = model.getReverseQueryMapping();
        }
    }
    
    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> keyDocumentEntry) {
        log.trace("apply to {}", keyDocumentEntry);
        
        if (keyDocumentEntry != null) {
            
            // If this is a final document, bail without adding to the keys, countingMap or fieldVisibilities.
            if (FinalDocumentTrackingIterator.isFinalDocumentKey(keyDocumentEntry.getKey())) {
                return keyDocumentEntry;
            }
            
            keys.add(keyDocumentEntry.getKey());
            log.trace("{} get list key counts for: {}", "web-server", keyDocumentEntry);
            DocumentGrouper.group(keyDocumentEntry, groupAggregateFields, groups, reverseModelMapping);
        }
        
        long elapsedExecutionTimeForCurrentPage = System.currentTimeMillis() - this.queryExecutionForPageStartTime;
        if (elapsedExecutionTimeForCurrentPage > this.queryExecutionForPageTimeout) {
            Document intermediateResult = new Document();
            intermediateResult.setIntermediateResult(true);
            return Maps.immutableEntry(new Key(), intermediateResult);
        }
        
        return null;
    }
    
    @Override
    public Entry<Key,Document> flush() {
        Document document = null;
        if (!groups.isEmpty()) {
            for (Group group : groups.getGroups()) {
                documents.add(GroupingUtils.createDocument(group, keys, markingFunctions, GroupingUtils.AverageAggregatorWriteFormat.AVERAGE));
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
