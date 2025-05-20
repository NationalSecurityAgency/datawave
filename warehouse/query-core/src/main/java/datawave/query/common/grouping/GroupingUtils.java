package datawave.query.common.grouping;

import static org.slf4j.LoggerFactory.getLogger;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;

import datawave.data.type.NumberType;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;

/**
 * This class contains utility functions used by multiple classes for grouping operations.
 */
public class GroupingUtils {

    public enum AverageAggregatorWriteFormat {
        AVERAGE, NUMERATOR_AND_DIVISOR
    }

    private static final Logger log = getLogger(GroupingUtils.class);

    /**
     * Returns a column visibility that results from the combination of all given visibilities using the given {@link MarkingFunctions}.
     *
     * @param visibilities
     *            the visibilities to combine
     * @param markingFunctions
     *            the marking functions to combine the visibilities with
     * @param failOnError
     *            if true and the visibilities cannot be combined, an {@link IllegalArgumentException} will be thrown. If false and the visibilities cannot be
     *            combined, it will be logged and a new, blank {@link ColumnVisibility} will be returned.
     * @return the combined column visibility
     */
    public static ColumnVisibility combineVisibilities(Collection<ColumnVisibility> visibilities, MarkingFunctions markingFunctions, boolean failOnError) {
        try {
            return markingFunctions.combine(visibilities);
        } catch (MarkingFunctions.Exception e) {
            if (failOnError) {
                throw new IllegalArgumentException("Unable to combine visibilities: " + visibilities, e);
            } else {
                log.warn("Unable to combine visibilities from {}", visibilities);
            }
        }
        return new ColumnVisibility();
    }

    /**
     * Create and return a new {@link Document} with the given group information embedded into it.
     * <p>
     * Do not use this method. If you are aggregating a list of keys only to use the most recent, rethink the approach
     *
     * @param group
     *            the group
     * @param keys
     *            the list of iterator keys that have been read
     * @param markingFunctions
     *            the marking functions to use when combining column visibilities
     * @param averageWriteFormat
     *            the format to use when writing aggregated averages to the document
     * @return the new document
     */
    @Deprecated
    public static Document createDocument(Group group, List<Key> keys, MarkingFunctions markingFunctions, AverageAggregatorWriteFormat averageWriteFormat) {
        Preconditions.checkState(!keys.isEmpty(), "No available keys for grouping results");

        // Use the last (most recent) key so a new iterator will know where to start.
        Key key = keys.get(keys.size() - 1);
        return createDocument(group, key, markingFunctions, averageWriteFormat);
    }

    /**
     * Create and return a new {@link Document} with the given group information embedded into it.
     *
     * @param group
     *            the group
     * @param key
     *            the docment key
     * @param markingFunctions
     *            the marking functions to use when combining column visibilities
     * @param averageWriteFormat
     *            the format to use when writing aggregated averages to the document
     * @return the new document
     */
    public static Document createDocument(Group group, Key key, MarkingFunctions markingFunctions, AverageAggregatorWriteFormat averageWriteFormat) {
        Preconditions.checkNotNull(key, "document key cannot be null");
        Document document = new Document(key, true);

        // Set the visibility for the document to the combined visibility of each previous document in which this grouping was seen in.
        document.setColumnVisibility(combineVisibilities(group.getDocumentVisibilities(), markingFunctions, true));

        // Add each of the grouping attributes to the document.
        for (GroupingAttribute<?> attribute : group.getGrouping()) {
            // Update the visibility to the combined visibilities of each visibility seen for this attribute in a grouping.
            attribute.setColumnVisibility(combineVisibilities(group.getVisibilitiesForAttribute(attribute), markingFunctions, false));
            document.put(attribute.getMetadata().getRow().toString(), attribute);
        }

        // Add an attribute for the count.
        NumberType type = new NumberType();
        type.setDelegate(new BigDecimal(group.getCount()));
        TypeAttribute<BigDecimal> attr = new TypeAttribute<>(type, new Key("count"), true);
        document.put("COUNT", attr);

        // Add each aggregated field.
        FieldAggregator fieldAggregator = group.getFieldAggregator();
        if (fieldAggregator != null) {
            Map<String,Map<AggregateOperation,Aggregator<?>>> aggregatorMap = group.getFieldAggregator().getAggregatorMap();
            for (Map.Entry<String,Map<AggregateOperation,Aggregator<?>>> entry : aggregatorMap.entrySet()) {
                for (Aggregator<?> aggregator : entry.getValue().values()) {
                    String field = aggregator.getField();
                    // Do not include an entry for the aggregation if it is null (indicating that no entries were found to be aggregated). The exception to this
                    // is
                    // the #COUNT aggregation. This will return a non-null value of 0 if no entries were found to be aggregated, and can be included in the
                    // final
                    // output.
                    if (aggregator.getAggregation() != null) {
                        switch (aggregator.getOperation()) {
                            case SUM:
                                addSumAggregation(document, field, ((SumAggregator) aggregator), markingFunctions);
                                break;
                            case COUNT:
                                addCountAggregation(document, field, ((CountAggregator) aggregator), markingFunctions);
                                break;
                            case MIN:
                                addMinAggregation(document, field, ((MinAggregator) aggregator));
                                break;
                            case MAX:
                                addMaxAggregation(document, field, ((MaxAggregator) aggregator));
                                break;
                            case AVERAGE:
                                switch (averageWriteFormat) {
                                    case AVERAGE:
                                        addAverage(document, field, ((AverageAggregator) aggregator), markingFunctions);
                                        break;
                                    case NUMERATOR_AND_DIVISOR:
                                        addAverageNumeratorAndDivisor(document, field, ((AverageAggregator) aggregator), markingFunctions);
                                        break;
                                }
                                break;
                        }
                    }
                }
            }
        }

        return document;
    }

    /**
     * Add the aggregated sum for the specified field to the document.
     *
     * @param document
     *            the document
     * @param field
     *            the field
     * @param aggregator
     *            the aggregator
     * @param markingFunctions
     *            the marking functions to use when combining column visibilities
     */
    private static void addSumAggregation(Document document, String field, SumAggregator aggregator, MarkingFunctions markingFunctions) {
        NumberType type = new NumberType();
        type.setDelegate(aggregator.getAggregation());
        TypeAttribute<BigDecimal> sumAttribute = new TypeAttribute<>(type, new Key(field + "_sum"), true);
        sumAttribute.setColumnVisibility(combineVisibilities(aggregator.getColumnVisibilities(), markingFunctions, false));
        document.put(field + DocumentGrouper.FIELD_SUM_SUFFIX, sumAttribute);
    }

    /**
     * Add the aggregated count for the specified field to the document.
     *
     * @param document
     *            the document
     * @param field
     *            the field
     * @param aggregator
     *            the aggregator
     * @param markingFunctions
     *            the marking functions to use when combining column visibilities
     */
    private static void addCountAggregation(Document document, String field, CountAggregator aggregator, MarkingFunctions markingFunctions) {
        NumberType type = new NumberType();
        type.setDelegate(BigDecimal.valueOf(aggregator.getAggregation()));
        TypeAttribute<BigDecimal> sumAttribute = new TypeAttribute<>(type, new Key(field + "_count"), true);
        Set<ColumnVisibility> columnVisibilities = aggregator.getColumnVisibilities();
        if (!columnVisibilities.isEmpty()) {
            sumAttribute.setColumnVisibility(combineVisibilities(aggregator.getColumnVisibilities(), markingFunctions, false));
        }
        document.put(field + DocumentGrouper.FIELD_COUNT_SUFFIX, sumAttribute);
    }

    /**
     * Add the aggregated min for the specified field to the document.
     *
     * @param document
     *            the document
     * @param field
     *            the field
     * @param aggregator
     *            the aggregator
     */
    private static void addMinAggregation(Document document, String field, MinAggregator aggregator) {
        document.put(field + DocumentGrouper.FIELD_MIN_SUFFIX, aggregator.getAggregation());
    }

    /**
     * Add the aggregated max for the specified field to the document.
     *
     * @param document
     *            the document
     * @param field
     *            the field
     * @param aggregator
     *            the aggregator
     */
    private static void addMaxAggregation(Document document, String field, MaxAggregator aggregator) {
        document.put(field + DocumentGrouper.FIELD_MAX_SUFFIX, aggregator.getAggregation());
    }

    /**
     * Add the aggregated average for the specified field to the document.
     *
     * @param document
     *            the document
     * @param field
     *            the field
     * @param aggregator
     *            the aggregator
     * @param markingFunctions
     *            the marking functions to use when combining column visibilities
     */
    private static void addAverage(Document document, String field, AverageAggregator aggregator, MarkingFunctions markingFunctions) {
        NumberType type = new NumberType();
        type.setDelegate(aggregator.getAggregation());
        TypeAttribute<BigDecimal> attribute = new TypeAttribute<>(type, new Key(field + "_average"), true);
        attribute.setColumnVisibility(combineVisibilities(aggregator.getColumnVisibilities(), markingFunctions, false));
        document.put(field + DocumentGrouper.FIELD_AVERAGE_SUFFIX, attribute);
    }

    /**
     * Add the numerator and divisor of the aggregated average for the specified field to the document.
     *
     * @param document
     *            the document
     * @param field
     *            the field
     * @param aggregator
     *            the aggregator
     * @param markingFunctions
     *            the marking functions to use when combining column visibilities
     */
    private static void addAverageNumeratorAndDivisor(Document document, String field, AverageAggregator aggregator, MarkingFunctions markingFunctions) {
        ColumnVisibility visibility = combineVisibilities(aggregator.getColumnVisibilities(), markingFunctions, false);

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

    /**
     * Do not allow new instances of this class to be created.
     */
    private GroupingUtils() {
        throw new UnsupportedOperationException();
    }

}
