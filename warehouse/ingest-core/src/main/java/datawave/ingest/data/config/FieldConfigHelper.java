package datawave.ingest.data.config;

public interface FieldConfigHelper {
    boolean isStoredField(String fieldName);

    boolean isIndexedField(String fieldName);

    boolean isIndexOnlyField(String fieldName);

    boolean isReverseIndexedField(String fieldName);

    boolean isTokenizedField(String fieldName);

    boolean isReverseTokenizedField(String fieldName);

    /*
     * SETH NOTE Should this be documented? Not sure what the consensus is for abstract methods. These also force other classes to implement these methods like
     * XMLFieldConfigHelper. Should they be added to those classes as well, or should we take another approach to adding ErrorIndexedFields?
     */
    boolean isErrorIndexedField(String fieldName);

    boolean isErrorReverseIndexedField(String fieldName);

}
