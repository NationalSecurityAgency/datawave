package datawave.query.tables.facets;

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
    
    public static final String FACET_TABLE_NAME = "facet.table.name";
    
    public static final String FACET_METADATA_TABLE_NAME = "facet.metadata.table.name";
    
    public static final String FACET_HASH_TABLE_NAME = "facet.hash.table.name";
    
    protected int minimumFacetCount = 1;
    
    protected FacetedSearchType type;
    
    protected SortedSet<String> facetedFields;
    
    protected int maximumFacetGroupCount = Integer.MAX_VALUE;
    
    protected boolean isStreaming = true;
    
    protected boolean hasFieldLimits = false;
    
    protected String facetTableName;
    
    protected String facetMetadataTableName;
    
    protected String facetHashTableName;
    
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
     * @param fields
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
    
    public String getFacetTableName() {
        return facetTableName;
    }
    
    public void setFacetTableName(String facetTableName) {
        this.facetTableName = facetTableName;
    }
    
    public String getFacetMetadataTableName() {
        return facetMetadataTableName;
    }
    
    public void setFacetMetadataTableName(String facetMetadataTableName) {
        this.facetMetadataTableName = facetMetadataTableName;
    }
    
    public String getFacetHashTableName() {
        return facetHashTableName;
    }
    
    public void setFacetHashTableName(String facetHashTableName) {
        this.facetHashTableName = facetHashTableName;
    }
    
    @Override
    public String toString() {
        return "Maximum FacetGroupCount : " + maximumFacetGroupCount + "\n" + "Minimum Facet Count : " + minimumFacetCount + "\n" + "Faceted Fields: "
                        + facetedFields + "\n" + "Faceted Table Name: " + facetTableName + "\n" + "Faceted Metadata Table Name: " + facetMetadataTableName
                        + "\n" + "Faceted Hash Table Name: " + facetHashTableName + "\n";
    }
}
