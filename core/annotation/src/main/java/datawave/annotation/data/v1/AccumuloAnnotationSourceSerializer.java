package datawave.annotation.data.v1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.annotation.data.AnnotationReadException;
import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.AnnotationSerializer;
import datawave.annotation.data.AnnotationWriteException;
import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.util.Validator;
import datawave.annotation.util.v1.AnnotationValidators;

public class AccumuloAnnotationSourceSerializer implements AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,AnnotationSource> {
    public static final char NULL = 0x0;

    protected static final Logger log = LoggerFactory.getLogger(AccumuloAnnotationSourceSerializer.class);

    private static final VisibilityTransformer DEFAULT_VISIBILITY_TRANSFORMER = new DefaultVisibilityTransformer();
    private static final TimestampTransformer DEFAULT_TIMESTAMP_TRANSFORMER = new DefaultTimestampTransformer();

    public static final String DATA_COLUMN_FAMILY = "d";

    final VisibilityTransformer visibilityTransformer;
    final TimestampTransformer timestampTransformer;

    public AccumuloAnnotationSourceSerializer() {
        this(DEFAULT_VISIBILITY_TRANSFORMER, DEFAULT_TIMESTAMP_TRANSFORMER);
    }

    public AccumuloAnnotationSourceSerializer(VisibilityTransformer visibilityTransformer, TimestampTransformer timestampTransformer) {
        this.visibilityTransformer = visibilityTransformer;
        this.timestampTransformer = timestampTransformer;
    }

    /**
     * The entirety of the annotationSource is serialized to bytes and stored in the value, whereas portions of the source are stored in the rowId and column
     * qualifier.
     *
     * @param annotationSource
     *            the source to serialize
     * @return an interator containing the elements to write to accumulo
     * @throws AnnotationSerializationException
     *             if there's a problem serializing the annotation (e.g., if the annotation did not contain required fields)
     */
    @Override
    public Iterator<Map.Entry<Key,Value>> serialize(AnnotationSource annotationSource) throws AnnotationSerializationException {
        Validator.ValidationState<AnnotationSource> validationState = AnnotationValidators.checkAnnotationSource(annotationSource);
        if (!validationState.isValid()) {
            throw new AnnotationWriteException("Annotation source is not valid: " + validationState.getErrors());
        }

        final List<Map.Entry<Key,Value>> serializedResults = new ArrayList<>();

        Value value = new Value(annotationSource.toByteArray());
        String rowId = annotationSource.getAnalyticSourceHash();
        String columnQualifier = annotationSource.getEngine() + NULL + annotationSource.getModel() + NULL + annotationSource.getAnalyticHash();

        // use the configured transformers to provide the column visibility and timestamp.
        ColumnVisibility cv = visibilityTransformer.toColumnVisibility(annotationSource.getMetadataMap());
        long timestamp = timestampTransformer.toTimestamp(annotationSource.getMetadataMap());

        Key key = Key.builder().row(rowId).family(DATA_COLUMN_FAMILY).qualifier(columnQualifier).visibility(cv).timestamp(timestamp).build();

        serializedResults.add(Map.entry(key, value));
        return serializedResults.iterator();
    }

    /**
     * The entirety of the annotationSource is deserialized from the value.
     *
     * @param elements
     *            the elements to deserialize
     * @return the deserialized annotation
     * @throws AnnotationSerializationException
     *             if there's a problem deserializing the annotation source.
     */
    @Override
    public AnnotationSource deserialize(Iterator<Map.Entry<Key,Value>> elements) throws AnnotationSerializationException {
        if (elements == null || !elements.hasNext()) {
            return null;
        }

        Map.Entry<Key,Value> e = elements.next();
        String row = e.getKey().getRow().toString();

        try {
            Value v = e.getValue();
            AnnotationSource source = AnnotationSource.parseFrom(v.get());

            if (elements.hasNext()) {
                throw new AnnotationReadException("Encountered multiple rows for annotation source: " + row + ".");
            }

            return source;
        } catch (InvalidProtocolBufferException ipe) {
            throw new AnnotationReadException("Unable to decode value for annotation source: " + row + ".");
        }
    }
}
