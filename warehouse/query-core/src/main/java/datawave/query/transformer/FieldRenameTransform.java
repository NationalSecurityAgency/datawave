package datawave.query.transformer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

import datawave.query.Constants;
import datawave.query.attributes.Document;
import datawave.query.jexl.JexlASTHelper;

/**
 * This transform will copy attributes with secondary field names to primary field names if the primary field name does not already exist in the document.
 */
public class FieldRenameTransform extends DocumentTransform.DefaultDocumentTransform {
    private final boolean reducedResponse;
    private final boolean includeGroupingContext;
    private Set<String> renameFieldExpressions;

    public FieldRenameTransform(Set<String> renameFieldExpressions, boolean includeGroupingContext, boolean reducedResponse) {
        this.renameFieldExpressions = renameFieldExpressions;
        this.includeGroupingContext = includeGroupingContext;
        this.reducedResponse = reducedResponse;
    }

    private Map<String,String> getFieldMap() {
        Map<String,String> renameFieldMap = new HashMap<>();
        renameFieldExpressions.stream().forEach(m -> {
            int index = m.indexOf('=');
            if (index > 0 && index < m.length() - 1) {
                renameFieldMap.put(m.substring(0, index).trim(), m.substring(index + 1).trim());
            }
        });
        return renameFieldMap;
    }

    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> keyDocumentEntry) {
        if (keyDocumentEntry != null) {
            Document document = keyDocumentEntry.getValue();

            Map<String,String> renameFieldMap = getFieldMap();

            for (String field : new HashSet<>(document.getDictionary().keySet())) {
                String baseField = JexlASTHelper.deconstructIdentifier(field);
                String mappedField = renameFieldMap.get(baseField);
                if (mappedField != null) {
                    String newField = field.replace(baseField, mappedField);
                    document.put(newField, document.get(field), this.includeGroupingContext, this.reducedResponse);
                    document.remove(field);
                }
            }
        }

        return keyDocumentEntry;
    }

    public void updateConfig(Set<String> renameFields) {
        this.renameFieldExpressions = renameFields;
    }
}
