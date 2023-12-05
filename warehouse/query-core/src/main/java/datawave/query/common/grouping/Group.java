package datawave.query.common.grouping;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

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
     * The different column visibilities seen for each attribute that makes up the grouping.
     */
    private final Multimap<GroupingAttribute<?>,ColumnVisibility> attributeVisibilities = HashMultimap.create();

    /**
     * The column visibilities for each document that contributed entries to this grouping.
     */
    private final Set<ColumnVisibility> documentVisibilities = new HashSet<>();

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
        addAttributeVisibilities(this.grouping);
        this.count = count;
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
     * Add the column visibilities from each of the given attributes to the set of attribute visibilities for this group.
     *
     * @param grouping
     *            the attributes to add visibilities from
     */
    public void addAttributeVisibilities(Grouping grouping) {
        for (GroupingAttribute<?> attribute : grouping) {
            attributeVisibilities.put(attribute, attribute.getColumnVisibility());
        }
    }

    /**
     * Return the set of column visibilities seen for the given attribute.
     *
     * @param attribute
     *            the attribute
     * @return the column visibilities seen for the given attributes
     */
    public Collection<ColumnVisibility> getVisibilitiesForAttribute(GroupingAttribute<?> attribute) {
        return attributeVisibilities.get(attribute);
    }

    /**
     * Add the column visibility to the set of visibilities of documents for which we have seen the grouping of this group in.
     *
     * @param columnVisibility
     *            the visibility to add
     */
    public void addDocumentVisibility(ColumnVisibility columnVisibility) {
        this.documentVisibilities.add(columnVisibility);
    }

    /**
     * Return the set of all distinct column visibilities from documents that we have seen this group in.
     *
     * @return the document column visibilities
     */
    public Set<ColumnVisibility> getDocumentVisibilities() {
        return documentVisibilities;
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
        this.attributeVisibilities.putAll(other.attributeVisibilities);
        this.documentVisibilities.addAll(other.documentVisibilities);
        this.count += other.count;
        this.fieldAggregator.merge(other.fieldAggregator);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("attributes", grouping).append("attributeVisibilities", attributeVisibilities)
                        .append("documentVisibilities", documentVisibilities).append("count", count).append("aggregatedFields", fieldAggregator).toString();
    }
}
