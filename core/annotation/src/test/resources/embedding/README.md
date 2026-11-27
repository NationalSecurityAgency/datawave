These fixtures retain the original FLOAT16 and FLOAT32 Arrow file bytes. Their
segment values were regenerated with the upstream `VectorPayload` and `SegmentValue`
models at commit `ed2e9d29b96792a7060816f03899e9479a830fcc`, using Jackson's default
`ObjectMapper`.

Both payloads declare `modality: IMAGE` and `embeddingSpaceId: image-space-v1`,
the identity used in the upstream `VectorPayloadTest`. Their SHA-256 checksums are
verified by the producer's `VectorPayload` constructor. Both value hashes are
`0D5A3770`, because upstream groups vector values by embedding space rather than
serialized payload bytes.

The source platform is named `upstream`, and the source hashes are recalculated
for that platform.

The FLOAT16 fixture is also copied to the ingest annotation test resources as
`input/upstreamEmbeddingAnnotation.json`. Refresh that copy whenever changing the
producer fixture.
