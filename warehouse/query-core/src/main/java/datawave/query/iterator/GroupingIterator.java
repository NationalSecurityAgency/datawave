package datawave.query.iterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import datawave.data.type.NumberType;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.common.grouping.AggregatedFields;
import datawave.query.common.grouping.Aggregator;
import datawave.query.common.grouping.AverageAggregator;
import datawave.query.common.grouping.CountAggregator;
import datawave.query.common.grouping.DocumentGrouper;
import datawave.query.common.grouping.Group;
import datawave.query.common.grouping.GroupingAttribute;
import datawave.query.common.grouping.Groups;
import datawave.query.common.grouping.MaxAggregator;
import datawave.query.common.grouping.MinAggregator;
import datawave.query.common.grouping.SumAggregator;
import datawave.query.jexl.JexlASTHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Because the t-server may tear down and start a new iterator at any time after a next() call, there can be no saved state in this class. For that reason, each
 * next call on the t-server will flatten the aggregated data into a single Entry&gt;Key,Document&lt; to return to the web server.
 */
public class GroupingIterator implements Iterator<Map.Entry<Key,Document>> {
    
    private static final Logger log = getLogger(GroupingIterator.class);
    
    /**
     * The fields to group by.
     */
    private final Set<String> groupFields;
    
    /**
     * A factory that will provide aggregators for all fields that are targets for aggregation in each group-by result.
     */
    private final AggregatedFields.Factory aggregateFieldsFactory;
    
    /**
     * The groups. This is updated each time
     */
    private final Groups groups;
    
    /**
     * list of keys that have been read, in order to keep track of where we left off when a new iterator is created
     */
    private final List<Key> keys = new ArrayList<>();
    
    private final MarkingFunctions markingFunctions;
    
    private final int groupFieldsBatchSize;
    
    private final YieldCallback<Key> yieldCallback;
    
    private final Iterator<Map.Entry<Key,Document>> previousIterators;
    
    Map.Entry<Key,Document> next;
    
    public GroupingIterator(Iterator<Map.Entry<Key,Document>> previousIterators, MarkingFunctions markingFunctions, Collection<String> groupFields,
                    int groupFieldsBatchSize, YieldCallback<Key> yieldCallback, AggregatedFields.Factory aggregateFieldsFactory) {
        this.previousIterators = previousIterators;
        this.markingFunctions = markingFunctions;
        this.groupFields = groupFields.stream().map(JexlASTHelper::deconstructIdentifier).collect(Collectors.toSet());
        this.aggregateFieldsFactory = aggregateFieldsFactory.deconstructIdentifiers();
        this.groupFieldsBatchSize = groupFieldsBatchSize;
        this.yieldCallback = yieldCallback;
        this.groups = new Groups();
    }
    
    @Override
    public boolean hasNext() {
        for (int i = 0; i < groupFieldsBatchSize; i++) {
            if (previousIterators.hasNext()) {
                Map.Entry<Key,Document> entry = previousIterators.next();
                if (entry != null) {
                    log.trace("t-server get list key counts for: {}", entry);
                    keys.add(entry.getKey());
                    DocumentGrouper.getGroups(entry, groupFields, aggregateFieldsFactory, groups);
                }
            } else if (yieldCallback != null && yieldCallback.hasYielded()) {
                log.trace("hasNext is false because yield was called");
                if (groups != null && !groups.isEmpty()) {
                    // reset the yield and use its key in the flattened document prepared below
                    keys.add(yieldCallback.getPositionAndReset());
                }
                break;
            } else {
                // in.hasNext() was false and there was no yield
                break;
            }
        }
        
        LinkedList<Document> documents = new LinkedList<>();
        Document document = null;
        next = null;
        
        if (groups != null && !groups.isEmpty()) {
            for (Group group : groups.getGroups()) {
                documents.add(createDocument(group));
            }
            document = flatten(documents);
        }
        
        if (document != null) {
            Key key;
            if (keys.size() > 0) {
                // use the last (most recent) key so a new iterator will know where to start
                key = keys.get(keys.size() - 1);
            } else {
                key = document.getMetadata();
            }
            next = Maps.immutableEntry(key, document);
            log.trace("hasNext {}", next);
            groups.clear();
            return true;
        }
        
        return false;
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
        ColumnVisibility visibility = combineVisibilities(aggregator.getColumnVisibilities(), false);
        
        // Add an attribute for the average's numerator. This is required to properly combine additional aggregations in future groupings.
        NumberType numeratorType = new NumberType();
        numeratorType.setDelegate(aggregator.getNumerator());
        TypeAttribute<BigDecimal> sumAttribute = new TypeAttribute<>(numeratorType, new Key(field + "_average_numerator"), true);
        sumAttribute.setColumnVisibility(visibility);
        document.put(field + DocumentGrouper.FIELD_AVERAGE_NUMERATOR_SUFFIX, sumAttribute);
        
        // Add an attribute for the average's divisor. This is required to properly combine additional aggregations in future groupings.
        NumberType divisorType = new NumberType();
        divisorType.setDelegate(aggregator.getDivisor());
        TypeAttribute<BigDecimal> countAttribute = new TypeAttribute<>(divisorType, new Key(field + "_average_divisor"), true);
        countAttribute.setColumnVisibility(visibility);
        document.put(field + DocumentGrouper.FIELD_AVERAGE_DIVISOR_SUFFIX, countAttribute);
    }
    
    @Override
    public Map.Entry<Key,Document> next() {
        return new AbstractMap.SimpleEntry<>(next.getKey(), next.getValue());
    }
    
    /**
     * <pre>
     * flush used the countingMap:
     * [[MALE, 16],
     * [MALE, 20],
     * [40, MALE],
     * [40, MALE],
     * [MALE, 22] x 2,
     * [FEMALE, 18],
     * [MALE, 24],
     * [20, MALE],
     * [30, MALE],
     * [FEMALE, 18],
     * [34, MALE]]
     * 
     * to create documents list: [
     * {AGE=16, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=1, ETA=20, GENERE=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=1, ETA=40, GENERE=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=40, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=2, ETA=22, GENERE=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=18, COUNT=1, GENDER=FEMALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=1, ETA=24, GENERE=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=20, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=30, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=1, ETA=18, GENERE=FEMALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=34, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false]
     * 
     * which is then flattened to just one document with the fields and counts correlated with a grouping context suffix:
     * 
     * {
     * AGE.0=16, GENDER.0=MALE, COUNT.0=1,
     * ETA.1=20, GENERE.1=MALE, COUNT.1=1,
     * ETA.2=40, GENERE.2=MALE, COUNT.2=1,
     * AGE.3=40, GENDER.3=MALE, COUNT.3=1,
     * ETA.4=22, GENERE.4=MALE, COUNT.4=2,
     * AGE.5=18, GENDER.5=FEMALE, COUNT.5=1,
     * ETA.6=24, GENERE.6=MALE, COUNT.6=1,
     * AGE.7=20, GENDER.7=MALE, COUNT.7=1,
     * AGE.8=30, GENDER.8=MALE, COUNT.8=1,
     * ETA.9=18, GENERE.9=FEMALE, COUNT.9=1,
     * AGE.A=34, GENDER.A=MALE, COUNT.A=1,
     * }
     * </pre>
     *
     * The Attributes, which have had their visibilities merged, are copied into normal TypeAttributes for serialization to the webserver.
     *
     * @param documents
     *            the list of documents to flatten into a single document
     * @return a flattened document
     */
    private Document flatten(List<Document> documents) {
        log.trace("Flattening {}", documents);
        
        Document flattened = new Document(documents.get(documents.size() - 1).getMetadata(), true);
        
        int context = 0;
        Set<ColumnVisibility> visibilities = new HashSet<>();
        for (Document document : documents) {
            log.trace("document: {}", document);
            for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : document.entrySet()) {
                visibilities.add(entry.getValue().getColumnVisibility());
                // Add a copy of each attribute to the flattened document with the context appended to the key, e.g. AGE becomes AGE.0.
                Attribute<? extends Comparable<?>> attribute = entry.getValue();
                attribute.setColumnVisibility(entry.getValue().getColumnVisibility());
                // Call copy() on the GroupingTypeAttribute to get a plain TypeAttribute instead of a GroupingTypeAttribute that is package protected and won't
                // serialize.
                flattened.put(entry.getKey() + "." + Integer.toHexString(context).toUpperCase(), (TypeAttribute) attribute.copy(), true, false);
            }
            // Increment the context by one.
            context++;
        }
        
        // Set the flattened document's visibility to the combined visibilities of each document.
        flattened.setColumnVisibility(combineVisibilities(visibilities, false));
        log.trace("flattened document: {}", flattened);
        return flattened;
    }
    
    private ColumnVisibility combineVisibilities(Collection<ColumnVisibility> visibilities, boolean failOnError) {
        return DocumentGrouper.combine(visibilities, markingFunctions, failOnError);
    }
}
