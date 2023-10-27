package datawave.query.transformer;

import static org.slf4j.LoggerFactory.getLogger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.springframework.util.Assert;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import datawave.data.type.NumberType;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.common.grouping.GroupingUtil;
import datawave.query.common.grouping.GroupingUtil.GroupCountingHashMap;
import datawave.query.common.grouping.GroupingUtil.GroupingTypeAttribute;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.model.QueryModel;

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
    private Set<String> groupFieldsSet;

    /**
     * holds the aggregated column visibilities for each grouped event
     */
    private final Multimap<Collection<GroupingTypeAttribute<?>>,ColumnVisibility> fieldVisibilities = HashMultimap.create();

    /**
     * A map of TypeAttribute collection keys to integer counts This map uses a special key type that ignores the metadata (with visibilities) in its hashCode
     * and equals methods
     */
    private GroupCountingHashMap countingMap;

    /**
     * Provides the grouping information (counting map, field visibilities, etc) for grouping documents.
     */
    private final GroupingUtil groupingUtil = new GroupingUtil();

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
     * @param groupFieldsSet
     *            the fields (user provided) to group by
     * @param queryExecutionForPageTimeout
     *            how long (in milliseconds) to let a page of results to collect before signaling to return a blank page to the client
     * @param markingFunctions
     *            the marking functions
     */
    public GroupingTransform(QueryModel model, Collection<String> groupFieldsSet, MarkingFunctions markingFunctions, long queryExecutionForPageTimeout) {
        super.initialize(settings, markingFunctions);
        this.queryExecutionForPageTimeout = queryExecutionForPageTimeout;
        this.countingMap = new GroupCountingHashMap(markingFunctions);
        updateConfig(groupFieldsSet, model);
        log.trace("groupFieldsSet: {}", this.groupFieldsSet);
    }

    public void updateConfig(Collection<String> groupFieldSet, QueryModel model) {
        this.groupFieldsSet = groupFieldSet.stream().map(JexlASTHelper::deconstructIdentifier).collect(Collectors.toSet());
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
            GroupingUtil.GroupingInfo groupingInfo = groupingUtil.getGroupingInfo(keyDocumentEntry, groupFieldsSet, countingMap, reverseModelMapping);
            this.countingMap = groupingInfo.getCountsMap();
            this.fieldVisibilities.putAll(groupingInfo.getFieldVisibilities());
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
        if (!countingMap.isEmpty()) {

            log.trace("flush will use the countingMap: {}", countingMap);

            for (Collection<GroupingTypeAttribute<?>> entry : countingMap.keySet()) {
                log.trace("from countingMap, got entry: {}", entry);
                ColumnVisibility columnVisibility;
                try {
                    columnVisibility = groupingUtil.combine(fieldVisibilities.get(entry), markingFunctions);
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to merge column visibilities: " + fieldVisibilities.get(entry), e);
                }
                // grab a key from those saved during getListKeyCounts
                Assert.notEmpty(keys, "no available keys for grouping results");
                // use the last (most recent) key so a new iterator will know where to start
                Key docKey = keys.get(keys.size() - 1);
                Document d = new Document(docKey, true);
                d.setColumnVisibility(columnVisibility);

                entry.forEach(base -> d.put(base.getMetadata().getRow().toString(), base));
                NumberType type = new NumberType();
                type.setDelegate(new BigDecimal(countingMap.get(entry)));
                TypeAttribute<BigDecimal> attr = new TypeAttribute<>(type, new Key("count"), true);
                d.put("COUNT", attr);
                documents.add(d);
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
            countingMap.clear();
            return entry;
        }

        return null;
    }

}
