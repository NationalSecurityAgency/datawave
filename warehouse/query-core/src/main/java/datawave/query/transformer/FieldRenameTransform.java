package datawave.query.transformer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

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

    private Multimap<String,String> getFieldMap() {
        Multimap<String,String> renameFieldMap = HashMultimap.create();
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

            Multimap<String,String> renameFieldMap = getFieldMap();

            for (String field : new HashSet<>(document.getDictionary().keySet())) {
                String baseField = JexlASTHelper.deconstructIdentifier(field);
                Collection<String> mappedFields = renameFieldMap.get(baseField);
                if (mappedFields != null && !mappedFields.isEmpty()) {
                    for (String mappedField : mappedFields) {
                        if (!mappedField.equals(baseField)) {
                            String newField = field.replace(baseField, mappedField);
                            document.put(newField, document.get(field), this.includeGroupingContext);
                        }
                    }
                    if (!mappedFields.contains(baseField)) {
                        document.remove(field);
                    }
                }
            }
        }

        return keyDocumentEntry;
    }

    public void updateConfig(Set<String> renameFields) {
        this.renameFieldExpressions = renameFields;
    }
}
