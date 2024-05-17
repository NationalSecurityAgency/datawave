package datawave.ingest.data.config;

import java.util.Map;

public interface FieldConfigHelper {
    boolean isStoredField(String fieldName);

    boolean isIndexedField(String fieldName);

    boolean isIndexOnlyField(String fieldName);

    boolean isReverseIndexedField(String fieldName);

    boolean isTokenizedField(String fieldName);

    boolean isReverseTokenizedField(String fieldName);

    boolean isCombinedField(String fieldName);

    Map<String,String[]> getVirtualFieldMap();
}
