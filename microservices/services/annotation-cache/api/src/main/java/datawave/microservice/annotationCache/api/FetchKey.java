package datawave.microservice.annotationCache.api;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;

/**
 * An immutable key for a successful source check in the shared {@link Constants#FETCH_MAP} map.
 *
 * <p>
 * Java serialization uses a canonical proxy, so equal keys serialize the same even when their strings are shared differently. The getters provide Hazelcast
 * query attributes {@code __key.idType} and {@code __key.documentId}. No custom serializer or partitioning setup is needed.
 */
public final class FetchKey implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String idType;
    private final String documentId;
    private final String authorizationHash;

    /** Creates a key from a non-blank identifier type, document ID, and authorization hash. */
    public FetchKey(String idType, String documentId, String authorizationHash) {
        this.idType = requireNonBlank(idType, "idType");
        this.documentId = requireNonBlank(documentId, "documentId");
        this.authorizationHash = requireNonBlank(authorizationHash, "authorizationHash");
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

    public String getAuthorizationHash() {
        return authorizationHash;
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

        private SerializationProxy(FetchKey key) {
            payload = KeyTupleEncoding.encode(key.idType, key.documentId, key.authorizationHash);
        }

        private Object readResolve() throws ObjectStreamException {
            String[] fields = KeyTupleEncoding.decode(payload);
            try {
                return new FetchKey(fields[0], fields[1], fields[2]);
            } catch (IllegalArgumentException e) {
                InvalidObjectException failure = new InvalidObjectException("Invalid fetch key");
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
        if (!(other instanceof FetchKey)) {
            return false;
        }
        FetchKey that = (FetchKey) other;
        return idType.equals(that.idType) && documentId.equals(that.documentId) && authorizationHash.equals(that.authorizationHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idType, documentId, authorizationHash);
    }

    @Override
    public String toString() {
        return "FetchKey{idType='" + idType + "', documentId='" + documentId + "', authorizationHash='" + authorizationHash + "'}";
    }
}
