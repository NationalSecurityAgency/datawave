package datawave.annotation.data.transform;

/**
 * Marker interface used for implementations that transform timestamps in the standard metadata format to the long representation used in the Accumulo Key.
 * Implementations will define specific source field name(s) to read from and write to.
 */
public interface TimestampTransformer extends MetadataTransformerBase<Long> {}
