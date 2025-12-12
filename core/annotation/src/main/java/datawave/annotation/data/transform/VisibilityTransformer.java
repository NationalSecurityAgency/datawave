package datawave.annotation.data.transform;

import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Marker interface used for implementations that transform visibilities from the standard metadata format to the ColumnVisibility representation used in the
 * Accumulo Key. Implementations will define specific source field name(s) to read from and write to.
 */
public interface VisibilityTransformer extends MetadataTransformerBase<ColumnVisibility> {}
