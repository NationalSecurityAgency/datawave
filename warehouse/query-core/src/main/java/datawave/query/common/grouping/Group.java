package datawave.query.common.grouping;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.marking.AccessExpressionUtil;

/**
 * Represents a grouping of values for fields specified via the #GROUP_BY functionality, with information about the total number of times the grouping was seen,
 * values for target aggregation fields that were matched to this group, and the different column visibilities seen.
 */
public class Group {

    /**
     * The distinct set of values that represent this grouping.
     */
    private final ImmutableGrouping grouping;

    /**
     * The different access expressions seen for each attribute that makes up the grouping.
     */
    private final Multimap<GroupingAttribute<?>,AccessExpression> attributeExpressions = HashMultimap.create();

    /**
     * The access expressions for each document that contributed entries to this grouping.
     */
    private final Set<AccessExpression> documentExpressions = new HashSet<>();

    /**
     * The total number of times the distinct grouping was seen.
     */
    private int count;

    /**
     * The aggregated values for any specified fields to aggregate.
     */
    private FieldAggregator fieldAggregator = new FieldAggregator();

    public Group(Grouping grouping) {
        this(grouping, 0);
    }

    public Group(Grouping grouping, int count) {
        this.grouping = new ImmutableGrouping(grouping);
        addAttributeExpressions(this.grouping);
        this.count = count;
    }

    /**
     * Cap the group count
     *
     * @param cap
     */
    public void capGroupCount(int cap) {
        count = Math.min(count, cap);
    }

    /**
     * Returns the distinct set of values that represent this grouping.
     *
     * @return the grouping
     */
    public Grouping getGrouping() {
        return grouping;
    }

    /**
     * Add the access expressions from each of the given attributes to the set of attribute expressions for this group.
     *
     * @param grouping
     *            the attributes to add expressions from
     */
    public void addAttributeExpressions(Grouping grouping) {
        for (GroupingAttribute<?> attribute : grouping) {
            attributeExpressions.put(attribute, attribute.getAccessExpression());
        }
    }

    /**
     * Return the set of visibilities seen for the given attribute.
     *
     * @param attribute
     *            the attribute
     * @return the access expressions converted to ColumnVisibilities seen for the given attribute
     */
    public Collection<ColumnVisibility> getColumnVisibilitiesForAttribute(GroupingAttribute<?> attribute) {
        return attributeExpressions.get(attribute).stream().map(AccessExpressionUtil::toColumnVisibility).collect(Collectors.toSet());
    }

    /**
     * Return the set of access expressions seen for the given attribute.
     *
     * @param attribute
     *            the attribute
     * @return the access expressions seen for the given attribute
     */
    public Collection<AccessExpression> getAccessExpressionsForAttribute(GroupingAttribute<?> attribute) {
        return attributeExpressions.get(attribute);
    }

    /**
     * Add the access expression to the set of expressions of documents for which we have seen the grouping of this group in.
     *
     * @param expression
     *            the expression to add
     */
    public void addDocumentExpression(AccessExpression expression) {
        this.documentExpressions.add(expression);
    }

    /**
     * Return the set of all distinct access expressions from documents that we have seen this group in.
     *
     * @return the document access expressions
     */
    public Set<AccessExpression> getDocumentExpressions() {
        return documentExpressions;
    }

    /**
     * Return the set of all distinct visibilities from documents that we have seen this group in.
     *
     * @return the document access expressions converted into ColumnVisibilities
     */
    public Set<ColumnVisibility> getDocumentVisibilities() {
        return documentExpressions.stream().map(AccessExpressionUtil::toColumnVisibility).collect(Collectors.toSet());
    }

    /**
     * Increment the number of times we have seen this grouping by one.
     */
    public void incrementCount() {
        this.count++;
    }

    /**
     * Returns the number of times we have seen this grouping.
     *
     * @return the number of times we've seen this group.
     */
    public int getCount() {
        return count;
    }

    /**
     * Returns the aggregated fields for this group.
     *
     * @return the aggregated fields.
     */
    public FieldAggregator getFieldAggregator() {
        return fieldAggregator;
    }

    /**
     * Set the aggregated fields for this group.
     *
     * @param fieldAggregator
     *            the aggregated fields to set
     */
    public void setFieldAggregator(FieldAggregator fieldAggregator) {
        this.fieldAggregator = fieldAggregator;
    }

    public void aggregateAll(Collection<Field> fields) {
        fieldAggregator.aggregateAll(fields);
    }

    public void aggregateAll(String field, Collection<Field> fields) {
        fieldAggregator.aggregateAll(field, fields);
    }

    /**
     * Merge the given group into this group. The attribute visibilities and document visibilities from the other group will be added into this group. The count
     * for this group will be incremented by the count of the other group. The aggregated fields of the other group will be merged into the aggregated fields of
     * this group.
     *
     * @param other
     *            the group to merge
     */
    public void merge(Group other) {
        this.attributeExpressions.putAll(other.attributeExpressions);
        this.documentExpressions.addAll(other.documentExpressions);
        this.count += other.count;
        this.fieldAggregator.merge(other.fieldAggregator);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("grouping", grouping).append("attributeExpressions", attributeExpressions)
                        .append("documentExpressions", documentExpressions).append("count", count).append("aggregatedFields", fieldAggregator).toString();
    }
}
