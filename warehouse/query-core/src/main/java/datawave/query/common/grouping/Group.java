package datawave.query.common.grouping;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.A;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class Group {
    
    private final Set<GroupingAttribute<?>> attributes;
    private final Multimap<GroupingAttribute<?>,ColumnVisibility> attributeVisibilities = HashMultimap.create();
    private final Set<ColumnVisibility> documentVisibilities = new HashSet<>();
    private int count;
    private AggregatedFields aggregatedFields = new AggregatedFields();
    
    public Group(Collection<GroupingAttribute<?>> attributes) {
        this(attributes, 0);
    }
    
    public Group(Collection<GroupingAttribute<?>> attributes, int count) {
        this.attributes = Sets.newHashSet(attributes);
        addAttributeVisibilities(this.attributes);
        this.count = count;
    }
    
    public Set<GroupingAttribute<?>> getAttributes() {
        return attributes;
    }
    
    public void addAttributeVisibilities(Set<GroupingAttribute<?>> newAttributes) {
        for (GroupingAttribute<?> attribute : newAttributes) {
            attributeVisibilities.put(attribute, attribute.getColumnVisibility());
        }
    }
    
    public Collection<ColumnVisibility> getVisibilitiesForAttribute(GroupingAttribute<?> attribute) {
        return attributeVisibilities.get(attribute);
    }
    
    public void addDocumentVisibility(ColumnVisibility columnVisibility) {
        this.documentVisibilities.add(columnVisibility);
    }
    
    public Set<ColumnVisibility> getDocumentVisibilities() {
        return documentVisibilities;
    }
    
    public void incrementCount() {
        this.count++;
    }
    
    public int getCount() {
        return count;
    }
    
    public AggregatedFields getAggregatedFields() {
        return aggregatedFields;
    }
    
    public void setAggregatedFields(AggregatedFields aggregatedFields) {
        this.aggregatedFields = aggregatedFields;
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("attributes", attributes).append("attributeVisibilities", attributeVisibilities)
                        .append("documentVisibilities", documentVisibilities).append("count", count).append("aggregatedFields", aggregatedFields).toString();
    }
    
    public void merge(Group other) {
        this.attributeVisibilities.putAll(other.attributeVisibilities);
        this.documentVisibilities.addAll(other.documentVisibilities);
        this.count += other.count;
        this.aggregatedFields.merge(other.aggregatedFields);
    }
}
