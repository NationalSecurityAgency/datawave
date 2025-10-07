package datawave.query.transformer;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;

import datawave.query.attributes.Document;

/**
 * This transform will copy attributes with secondary field names to primary field names if the primary field name does not already exist in the document.
 */
public class FieldMappingTransform extends DocumentTransform.DefaultDocumentTransform {
    private final boolean reducedResponse;
    private final boolean includeGroupingContext;
    private Map<String,List<String>> primaryToSecondaryFieldMap;

    public FieldMappingTransform(Map<String,List<String>> primaryToSecondaryFieldMap, boolean includeGroupingContext, boolean reducedResponse) {
        this.primaryToSecondaryFieldMap = primaryToSecondaryFieldMap;
        this.reducedResponse = reducedResponse;
        this.includeGroupingContext = includeGroupingContext;
    }

    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> keyDocumentEntry) {
        if (keyDocumentEntry != null) {
            Document document = keyDocumentEntry.getValue();

            for (String primaryField : this.primaryToSecondaryFieldMap.keySet()) {
                if (!document.containsKey(primaryField)) {
                    for (String secondaryField : this.primaryToSecondaryFieldMap.get(primaryField)) {
                        if (document.containsKey(secondaryField)) {
                            document.put(primaryField, document.get(secondaryField), includeGroupingContext);
                            break;
                        }
                    }
                }
            }
        }

        return keyDocumentEntry;
    }
}
