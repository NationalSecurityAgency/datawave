package datawave.query.transformer;

import java.util.Map;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;

import datawave.query.attributes.Document;

/**
 * This transform will copy attributes with secondary field names to primary field names if the primary field name does not already exist in the document.
 */
public class FieldRenameTransform extends DocumentTransform.DefaultDocumentTransform {
    private final boolean reducedResponse;
    private final boolean includeGroupingContext;
    private Map<String,String> renameFieldMap;

    public FieldRenameTransform(Map<String,String> renameFieldMap, boolean includeGroupingContext, boolean reducedResponse) {
        this.renameFieldMap = renameFieldMap;
        this.includeGroupingContext = includeGroupingContext;
        this.reducedResponse = reducedResponse;
    }

    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> keyDocumentEntry) {
        if (keyDocumentEntry != null) {
            Document document = keyDocumentEntry.getValue();

            for (Map.Entry<String,String> rename : this.renameFieldMap.entrySet()) {
                String field = rename.getKey();
                if (document.containsKey(field)) {
                    String newField = rename.getValue();
                    document.put(newField, document.get(field), this.includeGroupingContext, this.reducedResponse);
                    document.remove(field);
                }
            }
        }

        return keyDocumentEntry;
    }

    public void updateConfig(Map<String,String> renameFields) {
        this.renameFieldMap = renameFields;
    }
}
