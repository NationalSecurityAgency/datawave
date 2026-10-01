package datawave.microservice.annotationCache.api;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;

/**
 * An immutable key for an annotation in the shared {@link Constants#ANNOTATIONS_MAP} map.
 *
 * <p>
 * Java serialization uses a canonical proxy, so equal keys serialize the same even when their strings are shared differently. The getters provide Hazelcast
 * query attributes {@code __key.idType} and {@code __key.documentId}. No custom serializer or partitioning setup is needed.
 */
public final class AnnotationKey implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String idType;
    private final String documentId;
    private final String annotationId;

    /** Creates a key from a non-blank identifier type, document ID, and annotation ID. */
    public AnnotationKey(String idType, String documentId, String annotationId) {
        this.idType = requireNonBlank(idType, "idType");
        this.documentId = requireNonBlank(documentId, "documentId");
        this.annotationId = requireNonBlank(annotationId, "annotationId");
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be null or blank");
        }
        return value;
    }

    public String getIdType() {
        return idType;
    }

    public String getDocumentId() {
        return documentId;
    }

    public String getAnnotationId() {
        return annotationId;
    }

    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    /** Direct deserialization would bypass constructor validation; only the proxy is accepted. */
    private void readObject(ObjectInputStream input) throws InvalidObjectException {
        throw new InvalidObjectException("Serialization proxy required");
    }

    private static final class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 1L;

        // One array, not String fields: the serialized graph cannot vary with component reference sharing.
        private final byte[] payload;

        private SerializationProxy(AnnotationKey key) {
            payload = KeyTupleEncoding.encode(key.idType, key.documentId, key.annotationId);
        }

        private Object readResolve() throws ObjectStreamException {
            String[] fields = KeyTupleEncoding.decode(payload);
            try {
                return new AnnotationKey(fields[0], fields[1], fields[2]);
            } catch (IllegalArgumentException e) {
                InvalidObjectException failure = new InvalidObjectException("Invalid annotation key");
                failure.initCause(e);
                throw failure;
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof AnnotationKey)) {
            return false;
        }
        AnnotationKey that = (AnnotationKey) other;
        return idType.equals(that.idType) && documentId.equals(that.documentId) && annotationId.equals(that.annotationId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idType, documentId, annotationId);
    }

    @Override
    public String toString() {
        return "AnnotationKey{idType='" + idType + "', documentId='" + documentId + "', annotationId='" + annotationId + "'}";
    }
}
