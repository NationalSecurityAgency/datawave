# Annotation Binary Serialization Versions

These files support regression testing changes in the annotation binary serialization. They are used by 
[AnnotationBinarySerializationTest.java](../../java/datawave/annotation/util/v1/AnnotationBinarySerializationTest.java)
to test that binary annotation serialization is compatible across versions of datawave.

Each `.bin` file contains several serialized annotation objects that warer created using a prior version of the code.
If `testProtobufDeserializationBaselineBinary` fails then changes to the protobuf definition of the annotation object 
have been made that are not compatible with previous versions and the changes to the definition should be revised.

If the `testProtobufSerializationBaselineBinary` test fails, we are generating a new version of the serialized 
annotation object that is different from the baseline for testing. This new version will be written to 
[annotation_current.bin](annotation_current.bin). If this is an intentional change, to move forward, rename 
[annotation_baseline.bin](annotation_baseline.bin) to `annotation_(commit_hash).bin`, rename `annotation_current.bin` to
`annotation_baseline.bin`. Rerunning the tests will pass and this point. The revised `.bin` versions should be added to 
your commit so that a record of the change is maintained.

This approach will allow us to track all prior versions of the serialized annotation objects and ensure that we do not
unintentionally break compatibility with previous versions.  If we do need to make a change that breaks compatibility, 
we will have a record of all prior versions and the change that caused the break in compatibility.

Change History
 * 2026-07-24
   * The Annotation protobuf schema did not change but the order the segments were normalized to match the ordering
        Accumulo naturally orders keys. This was updated to maintain consistent hash values throughout the Annotation lifecycle.