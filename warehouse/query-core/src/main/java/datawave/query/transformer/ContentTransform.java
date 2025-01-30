package datawave.query.transformer;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;

/**
 * This transform will replace the content field name values from "true" to the document UID
 */
public class ContentTransform extends DocumentTransform.DefaultDocumentTransform {
    private final Boolean reducedResponse;
    private final List<String> contentFieldNames;

    public ContentTransform(List<String> contentFieldNames, Boolean reducedResponse) {
        this.contentFieldNames = contentFieldNames;
        this.reducedResponse = reducedResponse;
    }

    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> keyDocumentEntry) {
        if (keyDocumentEntry != null) {
            Document document = keyDocumentEntry.getValue();
            Key documentKey = DocumentTransformer.correctKey(keyDocumentEntry.getKey());
            String colf = documentKey.getColumnFamily().toString();
            int index = colf.indexOf("\0");
            String uid = colf.substring(index + 1);

            for (String contentFieldName : this.contentFieldNames) {
                if (document.containsKey(contentFieldName)) {
                    Attribute<?> contentField = document.remove(contentFieldName);
                    if (contentField.getData().toString().equalsIgnoreCase("true")) {
                        Content c = new Content(uid, contentField.getMetadata(), document.isToKeep());
                        document.put(contentFieldName, c, false);
                    }
                }
            }
        }

        return keyDocumentEntry;
    }
}
