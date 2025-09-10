# Datawave Annotations

Provides a generic method to annotate portions of data stored in datawave.

## Annotation Table Structure

Annotations are encoded in Accumulo as follows.

| Purpose             | Row             | Column Family                                       | Column Qualifier                    | Value          |
|---------------------|-----------------|-----------------------------------------------------|-------------------------------------|----------------|
| Annotation Metadata | documentShardId | documentDataType (n) documentUid (n) annotationType | annotationId (n) key (n) value      | None           |
| Annotation Segment  | documentShardId | documentDataType (n) documentUid (n) annotationType | annotationId (n) segmentId          | Protobuf Value |
| Annotation Update   | documentShardId | documentDataType (n) documentUid (n) annotationType | annotationId (n) segmentId.updateId | Protobuf Value |

The primary portion of this table is structured to align with documents in the Datawave shard tables.

* documentShardId: follows the datawave definition of YYYYMMDD_NNN (e.g. 20250701_12)
* documentDataType: follows the datatype of a document in the event table. An arbitrary string (e.g., enwiki)
* documentUid: Hash identifier for the document (must be unique in the context of a shard and datatype)

The rest of the table uses annotation specific data;

* annotationType: the type of annotation, e.g., an arbitrary string (e.g., correction)
* annotationUid: Murmur hash identifier for the annotation (must be unique in the context of a document) 
* segmentId: Hash identifier for the segment (must be unique in the context of an annotation)
* segmentId.updateId: Hash identifier of the segment plus a hash for the update value (most recent updates must sort last)
* Protobuf Value: contains structured data stored in the following protobuf schema that encode a segment's boundary and metadata values.

# Protobuf Values for Annotations and Segments

Each annotation has one or more segments. Each segment is defined by:

* A boundary, which includes;
  * type - depends on the segment or annotation type, can be time or position based.
  * start - the start of the segment, meaning depends on boundary type
  * end - the end of the segment, meaning depends on boundary type
  * rotation -
* One or more values, which include;
  * value - a value for the segment, value will depend on annotation type
  * score - a score for this value, typically a confidence score.
  * extension (optional) - a class of segment, used to group values.

# Protobuf Compiler

We used `protoc` from RedHat 8's `libprotoc 3.5.0` to build the java implementations included here.

```bash
protoc --java_out=src/main/java src/main/protobuf/SegmentPB.proto
```