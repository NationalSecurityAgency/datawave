package datawave.query.transformer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import datawave.data.type.NumberType;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.common.grouping.AggregatedFields;
import datawave.query.common.grouping.Aggregator;
import datawave.query.common.grouping.AverageAggregator;
import datawave.query.common.grouping.CountAggregator;
import datawave.query.common.grouping.DocumentGrouper;
import datawave.query.common.grouping.GroupingAttribute;
import datawave.query.common.grouping.Groups;
import datawave.query.common.grouping.Group;
import datawave.query.common.grouping.MaxAggregator;
import datawave.query.common.grouping.MinAggregator;
import datawave.query.common.grouping.SumAggregator;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.model.QueryModel;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.math.BigDecimal;
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
    private Set<String> groupFieldsSet;
    
    private AggregatedFields.Factory aggregateFieldsFactory;
    
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
     * @param groupFieldsSet
     *            the fields (user provided) to group by
     * @param queryExecutionForPageTimeout
     *            how long (in milliseconds) to let a page of results to collect before signaling to return a blank page to the client
     * @param markingFunctions
     *            the marking functions
     */
    public GroupingTransform(QueryModel model, Collection<String> groupFieldsSet, MarkingFunctions markingFunctions, long queryExecutionForPageTimeout,
                    AggregatedFields.Factory aggregateFieldsFactory) {
        super.initialize(settings, markingFunctions);
        this.queryExecutionForPageTimeout = queryExecutionForPageTimeout;
        this.groups = new Groups();
        updateConfig(groupFieldsSet, model, aggregateFieldsFactory);
        log.trace("groupFieldsSet: {}", this.groupFieldsSet);
    }
    
    public void updateConfig(Collection<String> groupFieldSet, QueryModel model, AggregatedFields.Factory aggregateFieldsFactory) {
        this.groupFieldsSet = groupFieldSet.stream().map(JexlASTHelper::deconstructIdentifier).collect(Collectors.toSet());
        this.aggregateFieldsFactory = aggregateFieldsFactory.deconstructIdentifiers();
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
            DocumentGrouper.getGroups(keyDocumentEntry, groupFieldsSet, aggregateFieldsFactory, reverseModelMapping, groups);
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
                documents.add(createDocument(group));
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
    
    private Document createDocument(Group group) {
        Preconditions.checkState(!keys.isEmpty(), "No available keys for grouping results");
        
        // Use the last (most recent) key so a new iterator will know where to start.
        Key key = keys.get(keys.size() - 1);
        Document document = new Document(key, true);
        
        // Set the visibility for the document to the combined visibility of each previous document in which this grouping was seen in.
        document.setColumnVisibility(combineVisibilities(group.getDocumentVisibilities(), true));
        
        // Add each of the grouping attributes to the document.
        for (GroupingAttribute<?> attribute : group.getAttributes()) {
            // Update the visibility to the combined visibilities of each visibility seen for this attribute in a grouping.
            attribute.setColumnVisibility(combineVisibilities(group.getVisibilitiesForAttribute(attribute), false));
            document.put(attribute.getMetadata().getRow().toString(), attribute);
        }
        
        // Add an attribute for the count.
        NumberType type = new NumberType();
        type.setDelegate(new BigDecimal(group.getCount()));
        TypeAttribute<BigDecimal> attr = new TypeAttribute<>(type, new Key("count"), true);
        document.put("COUNT", attr);
        
        // Add each aggregated field.
        AggregatedFields aggregatedFields = group.getAggregatedFields();
        if (aggregatedFields != null) {
            Multimap<String,Aggregator<?>> aggregatorMap = group.getAggregatedFields().getAggregatorMap();
            for (Map.Entry<String,Aggregator<?>> entry : aggregatorMap.entries()) {
                String field = entry.getKey();
                Aggregator<?> aggregator = entry.getValue();
                // Do not include an entry for the aggregation if it is null (indicating that no entries were found to be aggregated). The exception to this is
                // the
                // #COUNT aggregation. This will return a non-null value of 0 if no entries were found to be aggregated, and can be included in the final
                // output.
                if (aggregator.getAggregation() != null) {
                    switch (aggregator.getOperation()) {
                        case SUM:
                            addSumAggregation(document, field, ((SumAggregator) aggregator));
                            break;
                        case COUNT:
                            addCountAggregation(document, field, ((CountAggregator) aggregator));
                            break;
                        case MIN:
                            addMinAggregation(document, field, ((MinAggregator) aggregator));
                            break;
                        case MAX:
                            addMaxAggregation(document, field, ((MaxAggregator) aggregator));
                            break;
                        case AVERAGE:
                            addAverageAggregation(document, field, ((AverageAggregator) aggregator));
                            break;
                    }
                }
            }
        }
        
        return document;
    }
    
    private void addSumAggregation(Document document, String field, SumAggregator aggregator) {
        NumberType type = new NumberType();
        type.setDelegate(aggregator.getAggregation());
        TypeAttribute<BigDecimal> sumAttribute = new TypeAttribute<>(type, new Key(field + "_sum"), true);
        sumAttribute.setColumnVisibility(combineVisibilities(aggregator.getColumnVisibilities(), false));
        document.put(field + DocumentGrouper.FIELD_SUM_SUFFIX, sumAttribute);
    }
    
    private void addCountAggregation(Document document, String field, CountAggregator aggregator) {
        NumberType type = new NumberType();
        type.setDelegate(BigDecimal.valueOf(aggregator.getAggregation()));
        TypeAttribute<BigDecimal> sumAttribute = new TypeAttribute<>(type, new Key(field + "_count"), true);
        Set<ColumnVisibility> columnVisibilities = aggregator.getColumnVisibilities();
        if (!columnVisibilities.isEmpty()) {
            sumAttribute.setColumnVisibility(combineVisibilities(aggregator.getColumnVisibilities(), false));
        }
        document.put(field + DocumentGrouper.FIELD_COUNT_SUFFIX, sumAttribute);
    }
    
    private void addMinAggregation(Document document, String field, MinAggregator aggregator) {
        document.put(field + DocumentGrouper.FIELD_MIN_SUFFIX, aggregator.getAggregation());
    }
    
    private void addMaxAggregation(Document document, String field, MaxAggregator aggregator) {
        document.put(field + DocumentGrouper.FIELD_MAX_SUFFIX, aggregator.getAggregation());
    }
    
    private void addAverageAggregation(Document document, String field, AverageAggregator aggregator) {
        NumberType type = new NumberType();
        type.setDelegate(aggregator.getAggregation());
        TypeAttribute<BigDecimal> attribute = new TypeAttribute<>(type, new Key(field + "_average"), true);
        attribute.setColumnVisibility(combineVisibilities(aggregator.getColumnVisibilities(), false));
        document.put(field + DocumentGrouper.FIELD_AVERAGE_SUFFIX, attribute);
    }
    
    private ColumnVisibility combineVisibilities(Collection<ColumnVisibility> visibilities, boolean failOnError) {
        return DocumentGrouper.combine(visibilities, markingFunctions, failOnError);
    }
}
