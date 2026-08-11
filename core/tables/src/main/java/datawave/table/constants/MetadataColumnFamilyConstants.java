package datawave.table.constants;

import org.apache.hadoop.io.Text;

/**
 * Reserved ColumnFamily names for the DatawaveMetadata table
 */
public final class MetadataColumnFamilyConstants {

    private MetadataColumnFamilyConstants() {
        throw new UnsupportedOperationException();
    }

    /**
     * A column family of {@value} denotes event fields (shard table).
     */
    public static final String COLF_E_STR = "e";

    /**
     * The column family {@value COLF_E_STR} in {@link Text} form.
     */
    public static final Text COLF_E = new Text(COLF_E_STR);

    /**
     * A column family of {@value} denotes unfielded expansion fields (shard table).
     */
    public static final String COLF_EXP_STR = "exp";

    /**
     * The column family {@value COLF_EXP_STR} in {@link Text} form.
     */
    public static final Text COLF_EXP = new Text(COLF_EXP_STR);

    /**
     * A column family of {@value} denotes content fields used by content functions (shard table).
     */
    public static final String COLF_CONTENT_STR = "content";

    /**
     * The column family {@value COLF_CONTENT_STR} in {@link Text} form.
     */
    public static final Text COLF_CONTENT = new Text(COLF_CONTENT_STR);

    /**
     * A column family of {@value} denotes indexed fields (both the field index in the shard table and the global index in the shardIndex table).
     */
    public static final String COLF_I_STR = "i";

    /**
     * The column family {@value COLF_I_STR} in {@link Text} form.
     */
    public static final Text COLF_I = new Text(COLF_I_STR);

    /**
     * A column family of {@value} denotes reverse indexed fields (the global reverse index in the shardReverseIndex table).
     */
    public static final String COLF_RI_STR = "ri";

    /**
     * The column family {@value COLF_RI_STR} in {@link Text} form.
     */
    public static final Text COLF_RI = new Text(COLF_RI_STR);

    /**
     * A column family of {@value} denotes frequency entries which are by field by day.
     */
    public static final String COLF_F_STR = "f";

    /**
     * The column family {@value COLF_F_STR} in {@link Text} form.
     */
    public static final Text COLF_F = new Text(COLF_F_STR);

    /**
     * A column family of {@value} denotes term frequency fields (term frequency entries in the shard table containing term offsets).
     */
    public static final String COLF_TF_STR = "tf";

    /**
     * The column family {@value COLF_TF_STR} in {@link Text} form.
     */
    public static final Text COLF_TF = new Text(COLF_TF_STR);

    /**
     * A column family of {@value} denotes fields that are normalized but not indexed.
     */
    public static final String COLF_N_STR = "n";

    /**
     * The column family {@value COLF_N_STR} in {@link Text} form.
     */
    public static final Text COLF_N = new Text(COLF_N_STR);

    /**
     * A column family of {@value} denotes fields data type.
     */
    public static final String COLF_T_STR = "t";

    /**
     * The column family {@value COLF_T_STR} in {@link Text} form.
     */
    public static final Text COLF_T = new Text(COLF_T_STR);

    /**
     * A column family of {@value} denotes a description for the field in its datatype.
     */
    public static final String COLF_DESC_STR = "desc";

    /**
     * The column family {@value COLF_DESC_STR} in {@link Text} form.
     */
    public static final Text COLF_DESC = new Text(COLF_DESC_STR);

    /**
     * A column family of {@value} denotes edge's event fields (source, target, and enrichment fields).
     */
    public static final String COLF_EDGE_STR = "edge";

    /**
     * The column family {@value COLF_EDGE_STR} in {@link Text} form.
     */
    public static final Text COLF_EDGE = new Text(COLF_EDGE_STR);

    /**
     * A column family of {@value} denotes an event field that should be hidden from being available for use in queries.
     */
    public static final String COLF_H_STR = "h";

    /**
     * The column family {@value COLF_H_STR} in {@link Text} form.
     */
    public static final Text COLF_H = new Text(COLF_H_STR);

    /**
     * A column family of {@value} denotes an event that is part of a composite index.
     */
    public static final String COLF_CI_STR = "ci";

    /**
     * The column family {@value COLF_CI_STR} in {@link Text} form.
     */
    public static final Text COLF_CI = new Text(COLF_CI_STR);

    /**
     * A column family of {@value} denotes whether a composite field has a transition date.
     */
    public static final String COLF_CITD_STR = "citd";

    /**
     * The column family {@value COLF_CITD_STR} in {@link Text} form.
     */
    public static final Text COLF_CITD = new Text(COLF_CITD_STR);

    /**
     * A column family of {@value} denotes the separator to use when generating composite indices.
     */
    public static final String COLF_CISEP_STR = "cisep";

    /**
     * The column family {@value COLF_CISEP_STR} in {@link Text} form.
     */
    public static final Text COLF_CISEP = new Text(COLF_CISEP_STR);

    /**
     * A column family of {@value} denotes term counts (cardinality).
     */
    public static final String COLF_COUNT_STR = "count";

    /**
     * The column family {@value COLF_COUNT_STR} in {@link Text} form.
     */
    public static final Text COLF_COUNT = new Text(COLF_COUNT_STR);

    /**
     * A column family of {@value} denotes a version (currently only used for edge_key row).
     */
    public static final String COLF_VERSION_STR = "version";

    /**
     * The column family {@value COLF_VERSION_STR} in {@link Text} form.
     */
    public static final Text COLF_VERSION = new Text(COLF_VERSION_STR);

    /**
     * A column family of {@value} denotes an event that is part of a virtual field.
     */
    public static final String COLF_VI_STR = "vi";

    /**
     * The column family {@value COLF_VI_STR} in {@link Text} form.
     */
    public static final Text COLF_VI = new Text(COLF_VI_STR);

    /**
     * A column family of {@value} denotes a whindex field's creation date.
     */
    public static final String COLF_WCD_STR = "wcd";

    /**
     * The column family {@value COLF_WCD_STR} in {@link Text} form.
     */
    public static final Text COLF_WCD = new Text(COLF_WCD_STR);
}