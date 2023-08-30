package datawave.ingest.data.config;

public interface GroupedNormalizedContentInterface extends NormalizedContentInterface {

    /**
     * Set the event field name which can be different than the indexed field name
     *
     * @param name
     *            the name of the event field
     */
    void setEventFieldName(String name);

    /**
     * Set the event field name which can be different than the event field name
     *
     * @param name
     *            the name of the field
     */
    void setIndexedFieldName(String name);

    /**
     * Is this a grouped field
     *
     * @return if this is a grouped field or not
     */
    boolean isGrouped();

    /**
     * Set this field to be grouped or not
     *
     * @param grouped
     *            flag noting grouped status
     */
    void setGrouped(boolean grouped);

    /**
     * Get the group
     *
     * @return the subgroup
     */
    String getSubGroup();

    /**
     * Set the group
     *
     * @param group
     *            the subgroup to be set
     */
    void setSubGroup(String group);

    /**
     * Get the field qualifier used for grouping
     *
     * @return the field qualifier for grouping
     */
    String getGroup();

    /**
     * Set the field qualifier used for grouping
     *
     * @param group
     *            the field qualifier used for grouping
     */
    void setGroup(String group);

}
