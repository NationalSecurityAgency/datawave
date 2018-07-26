package datawave.query.transformer;

import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FieldMappingTransform extends DocumentTransform.DefaultDocumentTransform {
    private final Boolean reducedResponse;
    private Map<String,List<String>> primaryToSecondaryFieldMap = Collections.emptyMap();
    
    public FieldMappingTransform(Map<String,List<String>> primaryToSecondaryFieldMap, Boolean reducedResponse) {
        this.primaryToSecondaryFieldMap = primaryToSecondaryFieldMap;
        this.reducedResponse = reducedResponse;
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
                            document.put(primaryField, document.get(secondaryField), false, this.reducedResponse);
                            break;
                        }
                    }
                }
            }
        }
        
        return keyDocumentEntry;
    }
}
