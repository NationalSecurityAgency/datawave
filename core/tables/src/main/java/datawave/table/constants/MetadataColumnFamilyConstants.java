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
     * A colf of 'e' denotes event fields (shard table)
     */
    public static final String COLF_E_STR = "e";
    public static final Text COLF_E = new Text(COLF_E_STR);

    /**
     * A colf of 'exp' denotes unfielded expansion fields (shard table)
     */
    public static final String COLF_EXP_STR = "exp";
    public static final Text COLF_EXP = new Text(COLF_EXP_STR);

    /**
     * A colf of 'content' denotes content fields used by content functions (shard table)
     */
    public static final String COLF_CONTENT_STR = "content";
    public static final Text COLF_CONTENT = new Text(COLF_CONTENT_STR);

    /**
     * A colf of 'i' denotes indexed fields (both the field index in the shard table and the global index in the shardIndex table)
     */
    public static final String COLF_I_STR = "i";
    public static final Text COLF_I = new Text(COLF_I_STR);

    /**
     * A colf of 'ri' denotes reverse indexed fields (the global reverse index in the shardReverseIndex table)
     */
    public static final String COLF_RI_STR = "ri";
    public static final Text COLF_RI = new Text(COLF_RI_STR);

    /**
     * A colf of 'f' denotes frequency entries which are by field by day
     */
    public static final String COLF_F_STR = "f";
    public static final Text COLF_F = new Text(COLF_F_STR);

    /**
     * A colf of 'tf' denotes term frequency fields (term frequency entries in the shard table containing term offsets)
     */
    public static final String COLF_TF_STR = "tf";
    public static final Text COLF_TF = new Text(COLF_TF_STR);

    /**
     * A colf of 'n' denotes fields that are normalized but not indexed
     */
    public static final String COLF_N_STR = "n";
    public static final Text COLF_N = new Text(COLF_N_STR);

    /**
     * A colf of 't' denotes fields data type
     */
    public static final String COLF_T_STR = "t";
    public static final Text COLF_T = new Text(COLF_T_STR);

    /**
     * A colf of 'desc' denotes a description for the field in its datatype
     */
    public static final String COLF_DESC_STR = "desc";
    public static final Text COLF_DESC = new Text(COLF_DESC_STR);

    /**
     * A colf of 'edge' denotes edge's event fields (source, target, and enrichment fields)
     */
    public static final String COLF_EDGE_STR = "edge";
    public static final Text COLF_EDGE = new Text(COLF_EDGE_STR);

    /**
     * A colf of 'h' denotes an event field that should be hidden from being available for use in queries
     */
    public static final String COLF_H_STR = "h";
    public static final Text COLF_H = new Text(COLF_H_STR);

    /**
     * a colf of 'ci' denotes an event that is part of a composite index
     */
    public static final String COLF_CI_STR = "ci";
    public static final Text COLF_CI = new Text(COLF_CI_STR);

    /**
     * a colf of 'citd' denotes whether a composite field has a transition date
     */
    public static final String COLF_CITD_STR = "citd";
    public static final Text COLF_CITD = new Text(COLF_CITD_STR);

    /**
     * a colf of 'cisep' denotes the separator to use when generating composite indices
     */
    public static final String COLF_CISEP_STR = "cisep";
    public static final Text COLF_CISEP = new Text(COLF_CISEP_STR);

    /**
     * a colf of 'count' denotes term counts (cardinality)
     */
    public static final String COLF_COUNT_STR = "count";
    public static final Text COLF_COUNT = new Text(COLF_COUNT_STR);

    /**
     * a colf of 'version' debnotes a version (currently only used for edge_key row)
     */
    public static final String COLF_VERSION_STR = "version";
    public static final Text COLF_VERSION = new Text(COLF_VERSION_STR);

    /**
     * a colf of 'vi' denotes an event that is part of a virtual field this is not used yet....
     */
    public static final String COLF_VI_STR = "vi";
    public static final Text COLF_VI = new Text(COLF_VI_STR);

    /**
     * a colf of 'wcd' denotes a whindex field's creation date
     */
    public static final String COLF_WCD_STR = "wcd";
    public static final Text COLF_WCD = new Text(COLF_WCD_STR);
}