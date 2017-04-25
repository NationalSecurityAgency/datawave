package nsa.datawave.ingest.data.config.ingest;

/**
 * Defines a number of methods related to content indexing, specifically field tokenization.
 */
public abstract class AbstractContentIngestHelper extends BaseIngestHelper {
    
    /**
     * Obtain the designator used for tokenized fields, implementations should never return null, rather an empty string to indicate that no suffix should be
     * used.
     * 
     * @return
     */
    public abstract String getTokenFieldNameDesignator();
    
    /**
     * Return true if the specified field should be indexed. The names passed to this method should not include the token field name designator if any is
     * specified.
     * 
     * @param field
     * @return
     */
    public abstract boolean isContentIndexField(String field);
    
    /**
     * Return true if the specified field should be reverse indexed. The names passed to this method should not include the token field name designator if any
     * is specified.
     * 
     * @param field
     * @return
     */
    public abstract boolean isReverseContentIndexField(String field);
    
    /**
     * Determines if we save the raw event in the document column
     * 
     * @return boolean of whether to save the raw data in the document column.
     */
    public abstract boolean getSaveRawDataOption();
    
    /**
     * When saving the raw document in the document column, we must have a 'view' name; this method must return that value.
     * 
     * @return
     */
    public abstract String getRawDocumentViewName();
    
    /**
     * Return true if the specified field should be indexed. Overridden here so that content indexed or reverse content indexed fields are also considered
     * indexed fields. Obeys the semantics of
     * 
     * {@link #isContentIndexField(String)} and {@link #isReverseContentIndexField(String)} such that field names should not include the token field name
     * designator if any is specified.
     * 
     * @param fieldName
     * @return
     */
    @Override
    public boolean isIndexedField(String fieldName) {
        if ("".equals(getTokenFieldNameDesignator())) {
            // when there's no token designator, we implicitly
            // index fields that are either content indexed or
            // reverse content indexed.
            return super.isIndexedField(fieldName) || isContentIndexField(fieldName) || isReverseContentIndexField(fieldName);
        } else {
            return super.isIndexedField(fieldName);
        }
    }
}
