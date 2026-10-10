# Annotation Cache Shared API

## Maps and keys

| Map constant | Map name | Key | Value |
|---|---|---|---|
| `Constants.ANNOTATIONS_MAP` | `annotations` | `AnnotationKey(idType, documentId, annotationId)` | `AnnotationMessage` |
| `Constants.FETCH_MAP` | `doc-fetch-record` | `FetchKey(idType, documentId, authorizationHash)` | `FetchRecord` |

Keys are immutable. Every component participates in equality and hashing and must be non-null and non-blank. Delimiters have no special meaning.

Public JavaBean getters expose the indexed query attributes:

- `Constants.ID_TYPE_KEY_ATTRIBUTE`: `__key.idType`
- `Constants.DOCUMENT_ID_KEY_ATTRIBUTE`: `__key.documentId`

Queries can select a document and identifier type, or a document ID across identifier types. Keys do not define partition affinity.

## Serialization requirements

- Use the same API wire contract on members and clients.
- Use compatible Hazelcast built-in Java serialization settings, including compression and sharing settings.
- Do not register a custom serializer for these keys; it would bypass their Java serialization hooks. No type ID, factory, or Spring customization is required.
- If deserialization filters are enabled, allow the proxy types and their byte-array payloads as described below.

### How key serialization works

Both keys implement `Serializable` and use a private static serialization proxy. Java calls `writeReplace()` to serialize the proxy, whose only serialized field is a canonical `byte[] payload`. Equal keys produce identical serialized bytes regardless of how their strings were created.

On deserialization, the proxy's `readResolve()` decodes the payload and calls the key's validating constructor. The result is an immutable key, not a proxy. Direct key deserialization is rejected by a `readObject()` guard.

### Deserialization filters

Allow the proxy classes by their binary names:

```text
datawave.microservice.annotationCache.api.AnnotationKey$SerializationProxy
datawave.microservice.annotationCache.api.FetchKey$SerializationProxy
```

The proxies contain byte arrays. Array rules and allowlist syntax depend on the filter implementation. These requirements cover key serialization only, not an application-wide filter configuration.

## Wire format v1

`KeyTupleEncoding` writes a version byte (`1`), followed by three components:

| Key | Component order |
|---|---|
| `AnnotationKey` | `idType`, `documentId`, `annotationId` |
| `FetchKey` | `idType`, `documentId`, `authorizationHash` |

Each component contains a nonnegative, big-endian signed 32-bit Java character count, then that many big-endian 16-bit UTF-16 code units.

The format preserves unpaired surrogate code units and does not normalize Unicode or escape delimiters. It has no `writeUTF` 65,535-byte limit, but the full payload must fit in a Java byte array.

The decoder rejects missing payloads, unsupported versions, invalid lengths, truncated fields, and trailing data. It checks lengths against the remaining bytes before allocating character arrays. Constructors reject blank components.

## Compatibility and maintenance

The Java stream includes the proxy class descriptor. Both keys and both proxies declare `serialVersionUID = 1L`; proxy class names distinguish the key types.

Treat class/package names, UIDs, the sole `payload` field, tuple order, and encoding as wire-contract details. Renaming a proxy or changing its serialized form requires coordinated member/client deployment and rebuilding cached state. A payload version alone does not make an incompatible change safe for rolling upgrades. No migration or fallback is provided.

When maintaining keys:

- Keep proxies private and static, with a canonical byte-array payload as their only serialized state. Do not capture the key or default-serialize its String fields.
- Update validation, equality, hashing, and encoding together when identity changes.
- Preserve the getters used by indexed queries.
- After key, JDK, Hazelcast, or serialization-setting changes, run [CompositeKeySerializationProxyTest](src/test/java/datawave/microservice/annotationCache/api/CompositeKeySerializationProxyTest.java), [SharedMapSchemaTest](src/test/java/datawave/microservice/annotationCache/api/SharedMapSchemaTest.java), and the [member/client compatibility runner](../verify-supported-hazelcast-pairing.sh) with the applications' actual dependencies.

## Fetch records

`FetchRecord` uses ordinary Java serialization with an explicit `serialVersionUID`. Its `fetchedAt` and `annotationCountReturned` fields are diagnostic only.

A marker's presence and TTL indicate a recent successful source lookup for its authorization context, not a complete annotation snapshot. See the [service README](../service/README.md#fetch-markers-and-freshness) for freshness and invalidation behavior.
