package datawave.query.function.ws;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.base.Function;

import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.DocumentDeserializer;
import datawave.query.function.serializer.DocumentSerializer;

/**
 * A webserver side implementation of {@link datawave.query.function.JexlEvaluation} and related post-processing functions
 */
public class DocumentEvaluation implements Function<Entry<Key,Value>,Entry<Key,Value>> {

    private DocumentSerializer ser;
    private DocumentDeserializer de;

    // document post-processing functions
    private List<Function<Entry<Key,Document>,Entry<Key,Document>>> functions = new ArrayList<>();

    public DocumentEvaluation() {}

    public void addFunction(Function<Entry<Key,Document>,Entry<Key,Document>> function) {
        functions.add(function);
    }

    @Nullable
    @Override
    public Entry<Key,Value> apply(@Nullable Entry<Key,Value> input) {
        if (input == null || input.getValue() == null) {
            return input;
        }
        Entry<Key,Document> entry = de.apply(input);

        entry = applyTransforms(entry);

        if (entry == null) {
            return null;
        } else {
            return ser.apply(entry);
        }
    }

    public Entry<Key,Document> applyTransforms(@Nullable Entry<Key,Document> input) {
        for (Function<Entry<Key,Document>,Entry<Key,Document>> function : functions) {
            input = function.apply(input);
            if (input == null) {
                return null;
            }
        }
        return input;
    }

    public void setSerDe(DocumentSerialization.ReturnType type) {
        ser = DocumentSerialization.getDocumentSerializer(type);
        de = DocumentSerialization.getDocumentDeserializer(type);
    }
}
