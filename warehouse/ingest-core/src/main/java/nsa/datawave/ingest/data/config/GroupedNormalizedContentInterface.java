package nsa.datawave.ingest.data.config;

public interface GroupedNormalizedContentInterface extends NormalizedContentInterface {
    
    /**
     * Set the event field name which can be different than the indexed field name
     */
    void setEventFieldName(String name);
    
    /**
     * Set the event field name which can be different than the event field name
     */
    void setIndexedFieldName(String name);
    
    /**
     * Is this a grouped field
     * 
     * @return
     */
    boolean isGrouped();
    
    /**
     * Set this field to be grouped or not
     * 
     * @param grouped
     */
    void setGrouped(boolean grouped);
    
    /**
     * Get the group
     * 
     * @return
     */
    String getSubGroup();
    
    /**
     * Set the group
     * 
     * @param group
     */
    void setSubGroup(String group);
    
    /**
     * Get the field qualifier used for grouping
     * 
     * @return
     */
    String getGroup();
    
    /**
     * Set the field qualifier used for grouping
     * 
     * @param group
     */
    void setGroup(String group);
    
}
