package datawave.ingest.data.config;

public interface FieldConfigHelper {
    boolean isStoredField(String fieldName);

    boolean isIndexedField(String fieldName);

    boolean isIndexOnlyField(String fieldName);

    boolean isReverseIndexedField(String fieldName);

    boolean isTokenizedField(String fieldName);

    boolean isReverseTokenizedField(String fieldName);

    /** SETH NOTE
     * Should this be documented? Not sure what the concensus is for
     * abstract methods.
     */
    boolean isErrorIndexedField(String fieldName);

    boolean isErrorReverseIndexedField(String fieldName);

}
