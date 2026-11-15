package datawave.table.constants;

import org.apache.hadoop.io.Text;

/**
 * Reserved ColumnFamily names for the DatawaveMetadata table
 */
public final class MetadataColumnFamilyConstants {
    /**
     * A colf of 'e' denotes event fields (shard table)
     */
    public static final Text COLF_E = new Text("e");
    /**
     * A colf of 'exp' denotes unfielded expansion fields (shard table)
     */
    public static final Text COLF_EXP = new Text("exp");
    /**
     * A colf of 'content' denotes content fields used by content functions (shard table)
     */
    public static final Text COLF_CONTENT = new Text("content");
    /**
     * A colf of 'i' denotes indexed fields (both the field index in the shard table and the global index in the shardIndex table)
     */
    public static final Text COLF_I = new Text("i");
    /**
     * A colf of 'ri' denotes reverse indexed fields (the global reverse index in the shardReverseIndex table)
     */
    public static final Text COLF_RI = new Text("ri");
    /**
     * A colf of 'f' denotes frequency entries which are by field by day
     */
    public static final Text COLF_F = new Text("f");
    /**
     * A colf of 'tf' denotes term frequency fields (term frequency entries in the shard table containing term offsets)
     */
    public static final Text COLF_TF = new Text("tf");
    /**
     * A colf of 'n' denotes fields that are normalized but not indexed
     */
    public static final Text COLF_N = new Text("n");
    /**
     * A colf of 't' denotes fields data type
     */
    public static final Text COLF_T = new Text("t");
    /**
     * A colf of 'desc' denotes a description for the field in its datatype
     */
    public static final Text COLF_DESC = new Text("desc");
    /**
     * A colf of 'edge' denotes edge's event fields (source, target, and enrichment fields)
     */
    public static final Text COLF_EDGE = new Text("edge");

    /**
     * A colf of 'h' denotes an event field that should be hidden from being available for use in queries
     */
    public static final Text COLF_H = new Text("h");

    /**
     * a colf of 'ci' denotes an event that is part of a composite index
     */
    public static final Text COLF_CI = new Text("ci");

    /**
     * a colf of 'citd' denotes whether a composite field has a transition date
     */
    public static final Text COLF_CITD = new Text("citd");

    /**
     * a colf of 'cisep' denotes the separator to use when generating composite indices
     */
    public static final Text COLF_CISEP = new Text("cisep");

    /**
     * a colf of 'count' denotes term counts (cardinality)
     */
    public static final Text COLF_COUNT = new Text("count");

    /**
     * a colf of 'version' debnotes a version (currently only used for edge_key row)
     */
    public static final Text COLF_VERSION = new Text("version");

    /**
     * a colf of 'vi' denotes an event that is part of a virtual field this is not used yet....
     */
    public static final Text COLF_VI = new Text("vi");

    /**
     * a colf of 'wcd' denotes a whindex field's creation date
     */
    public static final Text COLF_WCD = new Text("wcd");

    private MetadataColumnFamilyConstants() {
        throw new UnsupportedOperationException();
    }
}
