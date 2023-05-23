package datawave.query.transformer;

import com.google.gson.JsonObject;
import datawave.query.attributes.Document;
import datawave.query.tables.serialization.SerializedDocument;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import org.apache.accumulo.core.data.Key;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * This transform will copy attributes with secondary field names to primary field names if the primary field name does not already exist in the document.
 */
public class JsonDocumentFieldMappingTransform extends JsonDocumentTransform.DefaultDocumentTransform {
    private final Boolean reducedResponse;
    private Map<String,List<String>> primaryToSecondaryFieldMap;

    public JsonDocumentFieldMappingTransform(Map<String,List<String>> primaryToSecondaryFieldMap, Boolean reducedResponse) {
        this.primaryToSecondaryFieldMap = primaryToSecondaryFieldMap;
        this.reducedResponse = reducedResponse;
    }
    
    @Nullable
    @Override
    public SerializedDocumentIfc apply(@Nullable SerializedDocumentIfc keyDocumentEntry) {
        if (keyDocumentEntry != null) {
            if (keyDocumentEntry instanceof SerializedDocument) {
                Document document = keyDocumentEntry.getAsDocument();

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
            else{
                JsonObject obj = keyDocumentEntry.get();

                for (String primaryField : this.primaryToSecondaryFieldMap.keySet()) {
                    if (!obj.has(primaryField)) {
                        for (String secondaryField : this.primaryToSecondaryFieldMap.get(primaryField)) {
                            if (obj.has(secondaryField)) {
                                obj.add(primaryField, obj.get(secondaryField));
                                break;
                            }
                        }
                    }
                }
            }
        }
        
        return keyDocumentEntry;
    }
}
