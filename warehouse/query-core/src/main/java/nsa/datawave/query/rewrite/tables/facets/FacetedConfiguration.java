package nsa.datawave.query.rewrite.tables.facets;

import java.util.Set;
import java.util.SortedSet;

import com.google.common.collect.Sets;

/**
 * Faceted Configuration object.
 * 
 * Purpose: Localizes configuration of faceted searching, so that we encapsulate configuration into a helper that is provided to the query iterator.
 */
public class FacetedConfiguration {
    
    public FacetedConfiguration() {
        facetedFields = Sets.newTreeSet();
    }
    
    public static final String MINIMUM_COUNT = "facet.minimum.count";
    
    public static final String MAXIMUM_GROUP_COUNT = "facet.maximum.group.count";
    
    public static final String STREAMING_ENABLED = "facet.streaming.enabled";
    
    public static final String FACETED_SEARCH_TYPE = "facet.search.type";
    
    public static final String FACETED_FIELDS = "facet.field.list";
    
    protected int minimumFacetCount = 1;
    
    protected FacetedSearchType type;
    
    protected SortedSet<String> facetedFields;
    
    protected int maximumFacetGroupCount = Integer.MAX_VALUE;
    
    protected boolean isStreaming = true;
    
    protected boolean hasFieldLimits = false;
    
    /**
     * Sets whether or not we have a limited fields
     * 
     * @param limits
     */
    public void setHasFieldLimits(boolean limits) {
        this.hasFieldLimits = limits;
    }
    
    public boolean hasFieldLimits() {
        return hasFieldLimits;
    }
    
    /**
     * Set the minimum count.
     * 
     * @param count
     */
    public void setMinimumCount(int count) {
        minimumFacetCount = count;
    }
    
    /**
     * Get the minimum facet count.
     * 
     * @return
     */
    public int getMinimumFacetCount() {
        return minimumFacetCount;
    }
    
    /**
     * Set the facet search type.
     * 
     * @param type
     */
    public void setType(final FacetedSearchType type) {
        this.type = type;
    }
    
    /**
     * Returns the facet search type
     * 
     * @return
     */
    public FacetedSearchType getType() {
        return type;
    }
    
    /**
     * Sets the faceted fields.
     * 
     * @param Sets
     *            .new
     */
    public void setFacetedFields(final Set<String> fields) {
        this.facetedFields = Sets.newTreeSet(fields);
        
    }
    
    /**
     * Return the sorted set of faceted fields.
     * 
     * @return
     */
    public SortedSet<String> getFacetedFields() {
        return facetedFields;
    }
    
    /**
     * Add a single faceted field.
     * 
     * @param field
     * @return
     */
    public boolean addFacetedField(final String field) {
        return facetedFields.add(field);
    }
    
    /**
     * Sets the maximum facet group count.
     * 
     * @param max
     */
    public void setMaximumFacetGroupCount(int max) {
        this.maximumFacetGroupCount = max;
    }
    
    /**
     * Returns the maximum facet group count.
     * 
     * @return
     */
    public int getMaximumFacetGroupCount() {
        return maximumFacetGroupCount;
    }
    
    /**
     * If maximum facet group count is not the max value, we assume that it was set and therefore grouping is enabled.
     * 
     * @return
     */
    public boolean isGroupingEnabled() {
        return (maximumFacetGroupCount != Integer.MAX_VALUE);
    }
    
    /**
     * @param isStreaming
     */
    public void setStreamingMode(boolean isStreaming) {
        this.isStreaming = isStreaming;
        
    }
    
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("Maximum FacetGroupCount : ").append(maximumFacetGroupCount).append("\n");
        builder.append("Minimum Facet Count : ").append(minimumFacetCount).append("\n");
        builder.append("Faceted Fields: ").append(facetedFields).append("\n");
        return builder.toString();
    }
}
