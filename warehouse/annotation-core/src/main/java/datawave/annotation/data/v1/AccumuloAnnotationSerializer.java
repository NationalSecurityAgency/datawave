package datawave.annotation.data.v1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.annotation.data.AnnotationSerializationException;
import datawave.annotation.data.AnnotationSerializer;
import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;

public class AccumuloAnnotationSerializer implements AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> {
    public static final char NULL = 0x0;
    public static final Value EMPTY = new Value();

    protected static final Logger log = LoggerFactory.getLogger(AccumuloAnnotationSerializer.class);
    private static final VisibilityTransformer DEFAULT_VISIBILITY_TRANSFORMER = new DefaultVisibilityTransformer();
    private static final TimestampTransformer DEFAULT_TIMESTAMP_TRANSFORMER = new DefaultTimestampTransformer();

    final VisibilityTransformer visibilityTransformer;
    final TimestampTransformer timestampTransformer;

    public AccumuloAnnotationSerializer() {
        this(DEFAULT_VISIBILITY_TRANSFORMER, DEFAULT_TIMESTAMP_TRANSFORMER);
    }

    public AccumuloAnnotationSerializer(VisibilityTransformer visibilityTransformer, TimestampTransformer timestampTransformer) {
        this.visibilityTransformer = visibilityTransformer;
        this.timestampTransformer = timestampTransformer;
    }

    @Override
    public Iterator<Map.Entry<Key,Value>> serialize(Annotation annotation) throws AnnotationSerializationException {
        // TODO: validate that the annotation has one or more segments
        // TODO: validate that the annotation has a valid shard (at least?)
        Key baseKey = generateBaseKey(annotation);
        List<Map.Entry<Key,Value>> serializedResults = new ArrayList<>();

        // TODO: convert the annotation id to a string/byte array once and pass this down instead of converting it each
        // time
        if (!annotation.getMetadataMap().isEmpty()) {
            serializeMetadata(baseKey, annotation.getAnnotationId(), annotation.getMetadataMap(), serializedResults);
        }

        serializeSegments(baseKey, annotation.getAnnotationId(), annotation.getSegmentsList(), serializedResults);

        return serializedResults.iterator();
    }

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
                throw new AnnotationSerializationException("Column family in key didn't have 2-3 parts, key: '" + key + "'");
            }

            String[] cqParts = key.getColumnQualifier().toString().split("\0");
            if (cqParts.length == 3) {
                metadata.put(cqParts[1], cqParts[2]);

            } else if (cqParts.length == 2) { // 3 parts.
                try {
                    segments.add(Segment.parseFrom(e.getValue().get()));
                } catch (InvalidProtocolBufferException ex) {
                    throw new AnnotationSerializationException("Could not decode value protobuf for key: '" + key + "'", ex);
                }
            } else {
                throw new AnnotationSerializationException("Column qualifier in key didn't have 2-3 parts, key: '" + key + "'");

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
        // TODO: compare the annotationId in the column qualifier?
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
        ColumnVisibility cv = visibilityTransformer.toColumnVisibility(annotation.getMetadataMap());
        long timestamp = timestampTransformer.toTimestamp(annotation.getMetadataMap());

        return Key.builder().row(rowId).family(columnFamily).visibility(cv).timestamp(timestamp).build();
    }

    /**
     * Serialize an Annotation's metadata map to a series of Accumulo key, value pairs written to the list provided
     *
     * @param baseKey
     *            the base key for the annotation.
     * @param annotationId
     *            the annotation id we are serializing.
     * @param metadata
     *            the metadata map to serialize.
     * @param serializedResults
     *            serialized pairs will be written to a provided list.
     */
    protected static void serializeMetadata(Key baseKey, String annotationId, Map<String,String> metadata, List<Map.Entry<Key,Value>> serializedResults) {
        for (Map.Entry<String,String> entry : metadata.entrySet()) {
            serializedResults.add(serializeMetadata(baseKey, annotationId, entry.getKey(), entry.getValue()));
        }
    }

    /**
     * Serialize a single map entry to an Accumulo key, value pair.
     *
     * @param baseKey
     *            key shared by all rows in a single annotation.
     * @param annotationId
     *            this annotation's UID.
     * @param metadataKey
     *            a single metadata key.
     * @param metadataValue
     *            a single metadata value.
     * @return the key and value pair for the serialized metadata key value pair.
     */
    protected static Map.Entry<Key,Value> serializeMetadata(Key baseKey, String annotationId, String metadataKey, String metadataValue) {
        final String columnQualifier = annotationId + NULL + metadataKey + NULL + metadataValue;
        //@formatter:off
        final Key key = Key.builder()
                .row(baseKey.getRowData().getBackingArray())
                .family(baseKey.getColumnFamilyData().getBackingArray())
                .qualifier(columnQualifier)
                .visibility(baseKey.getColumnVisibilityData().getBackingArray())
                .timestamp(baseKey.getTimestamp())
                .build();
        //@formatter:on

        return Map.entry(key, EMPTY);
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

        final String columnQualifier = annotationId + NULL + segment.getSegmentId();
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
