package datawave.annotation.projection;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentValue;

/**
 * Retains the immutable annotation properties that selectors read during projection, together with the original annotation and its resource budget. Preparation
 * can supply decoded values for declared extension keys, then reuse this state for several projection calls without decoding those entries again.
 * <p>
 * Stored extension strings and supplied identities remain unchanged in the original protobuf message. Maps and lists supplied for selection are copied so later
 * changes by the caller cannot alter projection results. The internal representation is private to the core module.
 */
public final class ProjectionEvaluationState {
    /** Protobuf JSON property whose entries may expose prepared decoded values instead of their stored strings. */
    static final String EXTENSION_PROPERTY = "extension";

    /**
     * Supplies the prepared value to expose for a stored extension entry. Preparation selects the codec and validates the decoded content before this callback
     * returns it. For an undeclared key, the callback returns the original stored string.
     * <p>
     * For example, a declared {@code stats} entry may return a map with an exact numeric {@code count} and a Boolean {@code enabled}. An undeclared string that
     * happens to look like JSON remains text; this callback does not infer a codec from its contents.
     */
    @FunctionalInterface
    public interface ExtensionValues {
        /**
         * Returns the validated value to expose for one extension entry. The same key may occur in several annotation values with different stored strings, so
         * the interpretation must correspond to this particular entry.
         *
         * @param key
         *            the extension key whose configured codec, if any, preparation has selected
         * @param storedValue
         *            the original string stored for this entry in SegmentValue.extension
         * @return a non-null String, Boolean, exact number, map with string keys or list; exact numbers use BigDecimal, BigInteger or integral wrapper types,
         *         and maps/lists contain only these supported values
         * @throws ProjectionException
         *             if the caller's preparation detects invalid content; the copied view also rejects nulls, non-string keys and Float/Double extension
         *             values
         */
        Object value(String key, String storedValue);
    }

    final Annotation annotation;
    final Map<String,Object> root;
    final ProjectionWorkGuard work;

    private ProjectionEvaluationState(Annotation annotation, ExtensionValues extensions, ProjectionWorkGuard work) {
        this.annotation = Objects.requireNonNull(annotation, "annotation");
        this.work = Objects.requireNonNull(work, "work");
        this.root = message(annotation, extensions, work, 1);
    }

    /**
     * Builds an immutable property view in which extension entries retain their stored string values, including strings that look like JSON.
     *
     * @param annotation
     *            the prepared annotation to expose for selection
     * @param work
     *            checks against the annotation's shared resource budget, reused during projection
     * @return a state containing the original annotation and a copied property view
     * @throws ProjectionException
     *             if the annotation contains a present nonfinite score or an unsupported map field
     */
    public static ProjectionEvaluationState from(Annotation annotation, ProjectionWorkGuard work) {
        return from(annotation, (key, storedValue) -> storedValue, work);
    }

    /**
     * Builds an immutable property view using the caller's prepared extension values. The callback runs once for each extension entry while this state is
     * built. Reusing the state for projection does not invoke it again.
     * <p>
     * Maps and lists returned by the callback are copied under the supplied budget. The callback must separately check its own decoding and validation work
     * against the preparation budget before returning; copying cannot bound allocations or work that have already happened inside the callback.
     *
     * @param annotation
     *            the prepared annotation whose stored strings and identities remain unchanged
     * @param extensions
     *            the provider of validated decoded values for declared keys and unchanged stored strings for undeclared keys
     * @param work
     *            checks against the annotation's shared resource budget, reused during projection
     * @return a state that owns its property maps and lists and retains the original annotation
     * @throws ProjectionException
     *             if a score is nonfinite or a supplied extension value has an unsupported kind, null content or an oversized numeric representation
     */
    public static ProjectionEvaluationState from(Annotation annotation, ExtensionValues extensions, ProjectionWorkGuard work) {
        return new ProjectionEvaluationState(annotation, Objects.requireNonNull(extensions, "extensions"), work);
    }

    /**
     * Returns the original prepared annotation that the caller can also use for persistence.
     *
     * @return the same built annotation supplied when creating this state, with stored extensions and identities unchanged
     */
    public Annotation acceptedAnnotation() {
        return annotation;
    }

    /**
     * Copies the properties of one protobuf message into the selector's object representation. Declared maps and lists remain available even when empty.
     * Scalars with implicit protobuf defaults are exposed within a present message; absent optional scalars and messages are omitted.
     * <p>
     * Protobuf reflection supplies scalar and list values. Maps use generated getters because reflection would first allocate an additional collection of map
     * entry messages.
     */
    private static Map<String,Object> message(Message message, ExtensionValues extensions, ProjectionWorkGuard work, int depth) {
        work.chargeTreeNode(depth);
        Map<String,Object> result = new LinkedHashMap<>();
        for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
            String name = field.getJsonName();
            if (field.isMapField()) {
                work.chargeTreeNode(depth + 1);
                Map<String,Object> entries = new LinkedHashMap<>();
                // Generated map getters avoid reflection's conversion of the entire map into entry messages.
                for (Map.Entry<String,String> entry : storedMap(message, name).entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    work.checkStringLength(key.length());
                    if (message instanceof SegmentValue && name.equals(EXTENSION_PROPERTY)) {
                        value = extensions.value(key, (String) value);
                    }
                    entries.put(key, copy(value, work, depth + 1));
                }
                result.put(name, Collections.unmodifiableMap(entries));
            } else if (field.isRepeated()) {
                work.chargeTreeNode(depth + 1);
                List<Object> values = new ArrayList<>();
                int count = message.getRepeatedFieldCount(field);
                for (int i = 0; i < count; i++) {
                    Object value = message.getRepeatedField(field, i);
                    values.add(field.getJavaType() == FieldDescriptor.JavaType.MESSAGE ? message((Message) value, extensions, work, depth + 2)
                                    : copy(value, work, depth + 1));
                }
                result.put(name, Collections.unmodifiableList(values));
            } else if (present(message, field)) {
                Object value = message.getField(field);
                if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    result.put(name, message((Message) value, extensions, work, depth + 1));
                } else if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                    result.put(name, copy(((com.google.protobuf.Descriptors.EnumValueDescriptor) value).getName(), work, depth));
                } else if (field.getJavaType() == FieldDescriptor.JavaType.FLOAT) {
                    work.chargeTreeNode(depth);
                    if (!Float.isFinite((Float) value)) {
                        throw new ProjectionException("Present annotation scores must be finite");
                    }
                    result.put(name, value);
                } else {
                    result.put(name, copy(value, work, depth));
                }
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Copies a prepared extension value while preserving its scalar type and array order. Each map, list and scalar is counted against the shared budget before
     * its representation is allocated; nested containers are checked at their actual depth.
     */
    private static Object copy(Object value, ProjectionWorkGuard work, int parentDepth) {
        if (value == null) {
            throw new ProjectionException("Prepared extension entries and elements cannot be null");
        }
        if (value instanceof Map) {
            work.chargeTreeNode(parentDepth + 1);
            Map<String,Object> result = new LinkedHashMap<>();
            for (Map.Entry<?,?> entry : ((Map<?,?>) value).entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new ProjectionException("Prepared extension objects require string keys");
                }
                String key = (String) entry.getKey();
                work.checkStringLength(key.length());
                result.put(key, copy(entry.getValue(), work, parentDepth + 1));
            }
            return Collections.unmodifiableMap(result);
        }
        if (value instanceof List) {
            work.chargeTreeNode(parentDepth + 1);
            List<Object> result = new ArrayList<>();
            for (Object child : (List<?>) value) {
                result.add(copy(child, work, parentDepth + 1));
            }
            return Collections.unmodifiableList(result);
        }
        work.chargeTreeNode(parentDepth);
        if (value instanceof String) {
            work.checkStringLength(((String) value).length());
        } else if (!(value instanceof Boolean)) {
            BigDecimal number = ScalarFormat.exactNumber(value);
            if (number == null) {
                throw new ProjectionException("Unsupported prepared scalar kind: " + value.getClass().getSimpleName());
            }
            ScalarFormat.checkNumber(number);
        }
        return value;
    }

    /**
     * Determines whether a property should appear in the copied object. Messages and oneof members have explicit presence, including proto3 optional scalars
     * represented by synthetic oneofs. Other scalars are always exposed within an existing message, even when their value is the protobuf default.
     * <p>
     * For example, an absent optional score is omitted, an explicitly supplied zero score remains present, and a point with unset x exposes x as zero.
     */
    private static boolean present(Message message, FieldDescriptor field) {
        boolean explicitPresence = field.getJavaType() == FieldDescriptor.JavaType.MESSAGE || field.getContainingOneof() != null;
        return !explicitPresence || message.hasField(field);
    }

    /** Resolves only the known string-map fields, so a future schema map cannot silently reuse an unrelated getter. */
    private static Map<String,String> storedMap(Message message, String name) {
        if (name.equals(EXTENSION_PROPERTY) && message instanceof SegmentValue) {
            return ((SegmentValue) message).getExtensionMap();
        }
        if (name.equals("configuration") && message instanceof AnnotationSource) {
            return ((AnnotationSource) message).getConfigurationMap();
        }
        if (name.equals("metadata")) {
            if (message instanceof Annotation) {
                return ((Annotation) message).getMetadataMap();
            }
            if (message instanceof AnnotationSource) {
                return ((AnnotationSource) message).getMetadataMap();
            }
            if (message instanceof Segment) {
                return ((Segment) message).getMetadataMap();
            }
        }
        throw new ProjectionException("Unsupported annotation map: " + message.getDescriptorForType().getFullName() + "." + name);
    }
}
