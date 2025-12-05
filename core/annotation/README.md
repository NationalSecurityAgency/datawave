# Datawave Annotations

Provides a generic method to annotate portions of data stored in Datawave.

## Annotation Table Structure

Annotations are encoded in Accumulo as follows.

| Purpose                | Row             | Column Family                                       | Column Qualifier                          | Value            |
|------------------------|-----------------|-----------------------------------------------------|-------------------------------------------|------------------|
| Annotation Source Hash | documentShardId | documentDataType (n) documentUid (n) annotationType | annotationHash (n) "source" (n) value     | None             |
| Annotation Document Id | documentShardId | documentDataType (n) documentUid (n) annotationType | annotationHash (n) "doc" (n) value        | None             |
| Annotation Metadata    | documentShardId | documentDataType (n) documentUid (n) annotationType | annotationHash (n) "md" (n) key (n) value | None             |
| Annotation Segment     | documentShardId | documentDataType (n) documentUid (n) annotationType | annotationHash (n) "seg" (n) segmentHash  | Protobuf Value   |

The primary portion of this table is structured to align with documents in the Datawave shard tables.

* documentShardId: follows the datawave definition of YYYYMMDD_NNN (e.g. 20250701_12)
* documentDataType: follows the datatype of a document in the event table. An arbitrary string (e.g., enwiki)
* documentUid: Hash identifier for the document (must be unique in the context of a shard and datatype)

The rest of the table uses annotation specific data;

* annotationType: the type of annotation, e.g., an arbitrary string (e.g., correction)
* annotationHash: Murmur hash identifier for the annotation (must be unique in the context of a document) 
* segmentHash: Hash identifier for the segment (must be unique in the context of an annotation)
* source: the annotation source that generated the annotation
* document id: the external document id that uniquely generates the object being annotated
* metadata: key value pairs representing additional data (e.g. 'visibility' and 'created_date' are manditory)
* segment data: Protobuf Value: contains structured data stored in the following protobuf schema that encode a segment's boundary and metadata values.

## Protobuf Values for Annotation Segments

Each annotation has one or more segments. Each segment is defined by:

* A boundary, which includes;
  * type - depends on the segment or annotation type, can be time or position based.
  * start - the start of the segment, meaning depends on boundary type
  * end - the end of the segment, meaning depends on boundary type
  * points - a list of points for bounding boxes
* One or more values, which include;
  * value - a value for the segment, value will depend on annotation type
  * score - a score for this value, typically a confidence score.
  * extension (optional) - a map of arbitrary items.

A boundary must have a start/end (span) -or- a list of points, not both and maybe neither
* Currently, valid boundary types include:
  * TIME_MILLI - a star and end that represents start and end times in milliseconds.
  * TEXT_CHAR - a start and end that represents character offsets in text.
  * POINTS - a list of points, each with an x coordinate, y coordinate, and optional label.
  * ALL - represents the entire object, with no span or points.

Each annotation optionally contains full source details or a reference to the sources analyticSourceHash. In practice
we write annotation source details to the annotation source table and track references via analyticSourceHash in the
annotations table.

## Annotation Sources

Annotation sources capture information about things that have been used to create annotations.

## Annotation Source Table Structure

| Purpose                | Row                | Column Family | Column Qualifier                            | Value                      |
|------------------------|--------------------|---------------|---------------------------------------------|----------------------------|
| Annotation Source Data | analyticSourceHash | "data"        | engineValue (n) modelValue (n) analyticHash | Annotation Source Protobuf |

## Protobuf Values for Annotation Sources

Each annotation source identifies an engine and model that were used to generate an annotation in addition to identifiers,
configuration details and metadata.

* analyticSourceHash: a 128-bit murmur hash that provides a globally unique identifier for the annotation soruce
* analyticHash: a 32-bit murmur hash designed to provide field grouping for individual objects. 
* engine: a string representing the engine name.
* model: a string representing the model used in the engine. Models are expected to change over time, sometime frequently.
* configuration: a map of key/value pairs representing the configuration used for the engine when producing annotations, may change independently of engine and model.
* metadata: metadata about the source (e.g., 'visibility' and 'created_date' are mandatory)

# Protobuf Compiler, Supporting Libraries and Tools

The source code for the generated source code in `src/main/java/datawave/annotation/protobuf` is located in
`src/main/protobuf`. The tools used were not part of the standard operating system distribution.

### Prerequisites 

* **Protoc**: We used `protoc` from `libprotoc 3.16.3` from [the GitHub protobuf releases page](https://github.com/protocolbuffers/protobuf/releases/tag/v3.16.3).
  * Follow the instructions to install this into `/usr/local/bin`.
* **Google APIs**:`SegmentV1.proto` imports `field_mask` and `timestamp` from [fuchsia.googlesource.com](https://fuchsia.googlesource.com/third_party/googleapis).
  * These get installed into `/usr/local/include`
* **JSON Schema Generation**: `compile_v1.sh` uses the `protoc-gen-jsonschema` plugin from [github.com/pubg/protoc-gen-jsonschma](https://github.com/pubg/protoc-gen-jsonschema) to generate a json schema file.
  * **TL;DR;**: `go install github.com/pubg/protoc-gen-jsonschema` assuming you have a golang installation (1.24.6+)

### Building
Once the prerequisites are installed, the protobuf files are compiled using:

```bash
cd src/main/protobuf
./compile_v1.sh
```

For more details see `src/main/protobuf/README.md`

