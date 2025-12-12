package datawave.annotation.data.v1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.AnnotationSerializer;
import datawave.annotation.data.AnnotationWriteException;
import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.Validator;
import datawave.annotation.util.v1.AnnotationValidators;

public class AccumuloAnnotationSerializer implements AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> {
    public static final char NULL = 0x0;
    public static final Value EMPTY = new Value();

    protected static final Logger log = LoggerFactory.getLogger(AccumuloAnnotationSerializer.class);

    public static final String SOURCE_CQ_FRAGMENT = "source";
    public static final String DOCUMENT_CQ_FRAGMENT = "doc";
    public static final String METADATA_CQ_FRAGMENT = "md";
    public static final String SEGMENT_CQ_FRAGMENT = "seg";

    public static final List<String> VALID_CQ_FRAGMENTS = List.of(SOURCE_CQ_FRAGMENT, DOCUMENT_CQ_FRAGMENT, METADATA_CQ_FRAGMENT, SEGMENT_CQ_FRAGMENT);

    private static final VisibilityTransformer DEFAULT_VISIBILITY_TRANSFORMER = new DefaultVisibilityTransformer();
    private static final TimestampTransformer DEFAULT_TIMESTAMP_TRANSFORMER = new DefaultTimestampTransformer();

    final VisibilityTransformer visibilityTransformer;
    final TimestampTransformer timestampTransformer;

    /**
     * Create an Accumulo annotation serializer that uses the default visibility and timestamp transformers.
     *
     */
    public AccumuloAnnotationSerializer() {
        this(DEFAULT_VISIBILITY_TRANSFORMER, DEFAULT_TIMESTAMP_TRANSFORMER);
    }

    /**
     * Create an Accumulo annotation serializer with the provided transformers used to translate metadata into portions of the Accumulo key.
     *
     * @param visibilityTransformer
     *            the transformer that produces Accumulo column visibilities
     * @param timestampTransformer
     *            the transformer that produces Accumulo timestamps
     */
    public AccumuloAnnotationSerializer(VisibilityTransformer visibilityTransformer, TimestampTransformer timestampTransformer) {
        this.visibilityTransformer = visibilityTransformer;
        this.timestampTransformer = timestampTransformer;
    }

    /**
     * serialize the Annotation to a series of Accumulo key/value pairs.
     *
     * @param annotation
     *            the annotation to serialize
     * @return an iterator over the Accumulo key/value pairs to write for this object.
     * @throws AnnotationSerializationException
     *             if there's a problem with the annotation that would result in an invalid serialization.
     */
    @Override
    public Iterator<Map.Entry<Key,Value>> serialize(Annotation annotation) throws AnnotationSerializationException {
        Validator.ValidationState<Annotation> validationState = AnnotationValidators.checkAnnotation(annotation);
        if (!validationState.isValid()) {
            throw new AnnotationWriteException("Annotation is not valid: " + validationState.getErrors());
        }

        Key baseKey = generateBaseKey(annotation);
        List<Map.Entry<Key,Value>> serializedResults = new ArrayList<>();

        serializeFieldsAndMetadata(baseKey, annotation, serializedResults);
        serializeSegments(baseKey, annotation.getAnnotationId(), annotation.getSegmentsList(), serializedResults);

        return serializedResults.iterator();
    }

    /**
     * deserialize the results from Accumulo into a valid Annotation object.
     *
     * @param elements
     *            the Accumulo elements to deserialize
     * @return the deserialized Annotation
     * @throws AnnotationSerializationException
     *             if there are unexpected results in the input from Accumulo.
     */
    @Override
    public Annotation deserialize(Iterator<Map.Entry<Key,Value>> elements) throws AnnotationSerializationException {
        if (elements == null || !elements.hasNext()) {
            return null;
        }

        final Annotation.Builder annotationBuilder = Annotation.newBuilder();
        final List<Segment> segments = new ArrayList<>();
        final Map<String,String> metadata = new HashMap<>();

        Key baseKey = null;

        while (elements.hasNext()) {
            Map.Entry<Key,Value> e = elements.next();
            final Key key = e.getKey();

            if (baseKey == null) {
                baseKey = key;
            } else if (!correctAnnotationSource(baseKey, key)) {
                throw new AnnotationSerializationException("The key provided isn't from the same annotation as the " + "first key provided: baseKey: ["
                                + baseKey + "] currentKey: [" + key + "]");
            }

            if (log.isTraceEnabled()) {
                log.trace("Iterated key: '{}'", e.getKey());
            }

            String[] cfParts = key.getColumnFamily().toString().split("\0");
            if (cfParts.length != 3) {
                throw new AnnotationSerializationException("Column family in key didn't have 3 parts, key: '" + key + "'");
            }

            String[] cqParts = key.getColumnQualifier().toString().split("\0");
            if (cqParts.length < 3) {
                throw new AnnotationSerializationException("Column qualifier in key had less than 3 parts parts, key: '" + key + "'");
            }

            final String cqType = cqParts[1];
            switch (cqType) {
                case METADATA_CQ_FRAGMENT:
                    if (cqParts.length != 4) {
                        throw new AnnotationSerializationException("Metadata column qualifier in key didn't have 4 parts, key: '" + key + "'");
                    }
                    metadata.put(cqParts[2], cqParts[3]);
                    break;
                case SEGMENT_CQ_FRAGMENT:
                    if (cqParts.length != 3) {
                        throw new AnnotationSerializationException("Segment column qualifier in key didn't have 3 parts, key: '" + key + "'");
                    }
                    try {
                        segments.add(Segment.parseFrom(e.getValue().get()));
                    } catch (InvalidProtocolBufferException ex) {
                        throw new AnnotationSerializationException("Could not decode value protobuf for segment key: '" + key + "'", ex);
                    }
                    break;
                case SOURCE_CQ_FRAGMENT:
                    if (cqParts.length != 3) {
                        throw new AnnotationSerializationException("Source hash column qualifier in key didn't have 3 parts, key: '" + key + "'");
                    }
                    annotationBuilder.setAnalyticSourceHash(cqParts[2]);
                    break;
                case DOCUMENT_CQ_FRAGMENT:
                    if (cqParts.length != 3) {
                        throw new AnnotationSerializationException("Document id column qualifier in key didn't have 3 parts, key: '" + key + "'");
                    }
                    annotationBuilder.setAnnotationId(cqParts[2]);
                    break;
                default:
                    throw new AnnotationSerializationException("Column qualifier did not have a valid type, key: '" + key + "'");
            }

            //@formatter:off
            annotationBuilder
                    .setShard(key.getRow().toString())
                    .setDataType(cfParts[0])
                    .setUid(cfParts[1])
                    .setAnnotationType(cfParts[2])
                    .setAnnotationId(cqParts[0]);
            //@formatter:on
        }

        if (segments.isEmpty()) {
            throw new AnnotationSerializationException("Annotation does not have any segment data '" + baseKey + "'");
        }

        return annotationBuilder.putAllMetadata(metadata).addAllSegments(segments).build();
    }

    /**
     * Validate that the two key's specified are form the same annotation source in that they share the same row and cf
     *
     * @param expected
     *            the key we expect
     * @param target
     *            the key to compare
     * @return true if the keys share the same annotation source.
     */
    protected static boolean correctAnnotationSource(Key expected, Key target) {
        return expected.getRowData().equals(target.getRowData()) && expected.getColumnFamilyData().equals(target.getColumnFamilyData());
    }

    /**
     * Generate the base key that will be used for serialization throughout this class
     *
     * @param annotation
     *            annotation source for key generation
     * @return base accumulo key for this annotation
     * @throws AnnotationSerializationException
     *             if there's a problem with the visibility transformer.
     */
    protected Key generateBaseKey(Annotation annotation) throws AnnotationSerializationException {
        String rowId = annotation.getShard();
        String columnFamily = annotation.getDataType() + NULL + annotation.getUid() + NULL + annotation.getAnnotationType();
        ColumnVisibility cv = visibilityTransformer.fromMetadataMap(annotation.getMetadataMap());
        long timestamp = timestampTransformer.fromMetadataMap(annotation.getMetadataMap());

        return Key.builder().row(rowId).family(columnFamily).visibility(cv).timestamp(timestamp).build();
    }

    /**
     * Serialize an Annotation's metadata map and fields to a series of Accumulo key/value pairs written to the provided list.
     *
     * @param baseKey
     *            the base key for the annotation.
     * @param annotation
     *            the annotation we are serializing.
     * @param serializedResults
     *            serialized pairs will be written to a provided list.
     * @throws AnnotationSerializationException
     *             if we encounter an inconsistency in the data that would result in an invalid serialized representation.
     */
    protected static void serializeFieldsAndMetadata(Key baseKey, Annotation annotation, List<Map.Entry<Key,Value>> serializedResults)
                    throws AnnotationSerializationException {

        // source hash field originates from either the analyticSourceHash field (if present) or from the source id
        // field of the source object.
        if (!StringUtils.isEmpty(annotation.getAnalyticSourceHash())) {
            // use the source hash if set.
            serializeFieldEntry(baseKey, annotation.getAnnotationId(), SOURCE_CQ_FRAGMENT, annotation.getAnalyticSourceHash(), serializedResults);
        } else if (annotation.hasSource() && !StringUtils.isEmpty(annotation.getSource().getAnalyticHash())) {
            // use the source's hash if the source hash field isn't present on the annotation.
            serializeFieldEntry(baseKey, annotation.getAnnotationId(), SOURCE_CQ_FRAGMENT, annotation.getSource().getAnalyticSourceHash(), serializedResults);
        } else {
            throw new AnnotationSerializationException("Could not find source hash for annotation: " + annotation.getAnnotationId());
        }

        // document id field
        if (!StringUtils.isEmpty(annotation.getDocumentId())) {
            serializeFieldEntry(baseKey, annotation.getAnnotationId(), DOCUMENT_CQ_FRAGMENT, annotation.getDocumentId(), serializedResults);
        } else {
            throw new AnnotationSerializationException("Could not find document id for annotation: " + annotation.getAnnotationId());
        }

        // metadata map
        if (!annotation.getMetadataMap().isEmpty()) {
            for (Map.Entry<String,String> entry : annotation.getMetadataMap().entrySet()) {
                serializeMetadataEntry(baseKey, annotation.getAnnotationId(), entry.getKey(), entry.getValue(), serializedResults);
            }
        }
    }

    /**
     * Serialize a single metadata entry to an Accumulo key, value pair.
     *
     * @param baseKey
     *            key shared by all rows in a single annotation.
     * @param annotationId
     *            this annotation's UID.
     * @param fieldKey
     *            the key for this field entry.
     * @param fieldValue
     *            the value for this field entry.
     * @param serializedResults
     *            output in the form of key value entries is written to this object.
     */
    protected static void serializeFieldEntry(Key baseKey, String annotationId, String fieldKey, String fieldValue,
                    List<Map.Entry<Key,Value>> serializedResults) {
        final String columnQualifier = annotationId + NULL + fieldKey + NULL + fieldValue;
        //@formatter:off
        final Key key = Key.builder()
                .row(baseKey.getRowData().getBackingArray())
                .family(baseKey.getColumnFamilyData().getBackingArray())
                .qualifier(columnQualifier)
                .visibility(baseKey.getColumnVisibilityData().getBackingArray())
                .timestamp(baseKey.getTimestamp())
                .build();
        //@formatter:on
        serializedResults.add(Map.entry(key, EMPTY));
    }

    /**
     * Serialize a single metadata map entry to an Accumulo Key/Value pair.
     *
     * @param baseKey
     *            key shared by all rows in a single annotation.
     * @param annotationId
     *            this annotation's UID.
     * @param metadataKey
     *            a single metadata key.
     * @param metadataValue
     *            a single metadata value.
     * @param serializedResults
     *            output in the form of key value entries is written to this object.
     */
    protected static void serializeMetadataEntry(Key baseKey, String annotationId, String metadataKey, String metadataValue,
                    List<Map.Entry<Key,Value>> serializedResults) {
        serializeFieldEntry(baseKey, annotationId, METADATA_CQ_FRAGMENT + NULL + metadataKey, metadataValue, serializedResults);
    }

    /**
     * Serialize a collection of segments into Key/Value pairs
     *
     * @param baseKey
     *            key shared by all rows in a single annotation.
     * @param annotationId
     *            this annotation's UID.
     * @param segments
     *            the list of segments to serialize.
     * @param serializedResults
     *            output in the form of key value entries is written to this object.
     */
    protected static void serializeSegments(Key baseKey, String annotationId, List<Segment> segments, List<Map.Entry<Key,Value>> serializedResults) {
        for (Segment segment : segments) {
            serializedResults.add(serializeSegment(baseKey, annotationId, segment));
        }
    }

    /**
     * Serialize a single segment into a Key/Value pair
     *
     * @param baseKey
     *            key shared by all rows in a single annotation
     * @param annotationId
     *            this annotation's UID.
     * @param segment
     *            the segment to serialize.
     * @return the key and value pair for the serialized segment.
     */
    protected static Map.Entry<Key,Value> serializeSegment(Key baseKey, String annotationId, Segment segment) {
        Value value = new Value(segment.toByteArray());

        final String columnQualifier = annotationId + NULL + SEGMENT_CQ_FRAGMENT + NULL + segment.getSegmentHash();
        //@formatter:off
        final Key key = Key.builder()
                .row(baseKey.getRowData().getBackingArray())
                .family(baseKey.getColumnFamilyData().getBackingArray())
                .qualifier(columnQualifier)
                .visibility(baseKey.getColumnVisibilityData().getBackingArray())
                .timestamp(baseKey.getTimestamp())
                .build();
        //@formatter:on
        return Map.entry(key, value);
    }
}
