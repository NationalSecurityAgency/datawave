package datawave.data;

import org.apache.hadoop.io.Text;

/**
 *
 */
public interface ColumnFamilyConstants {
    /**
     * A colf of 'e' denotes event fields (shard table)
     */
    Text COLF_E = new Text("e");
    /**
     * A colf of 'exp' denotes unfielded expansion fields (shard table)
     */
    Text COLF_EXP = new Text("exp");
    /**
     * A colf of 'content' denotes content fields used by content functions (shard table)
     */
    Text COLF_CONTENT = new Text("content");
    /**
     * A colf of 'i' denotes indexed fields (both the field index in the shard table and the global index in the shardIndex table)
     */
    Text COLF_I = new Text("i");
    /**
     * A colf of 'ri' denotes reverse indexed fields (the global reverse index in the shardReverseIndex table)
     */
    Text COLF_RI = new Text("ri");
    /**
     * A colf of 'f' denotes frequency entries which are by field by day
     */
    Text COLF_F = new Text("f");
    /**
     * A colf of 'tf' denotes term frequency fields (term frequency entries in the shard table containing term offsets)
     */
    Text COLF_TF = new Text("tf");
    /**
     * A colf of 'n' denotes fields that are normalized but not indexed
     */
    Text COLF_N = new Text("n");
    /**
     * A colf of 't' denotes fields data type
     */
    Text COLF_T = new Text("t");
    /**
     * A colf of 'desc' denotes a description for the field in its datatype
     */
    Text COLF_DESC = new Text("desc");
    /**
     * A colf of 'edge' denotes edge's event fields (source, target, and enrichment fields)
     */
    Text COLF_EDGE = new Text("edge");
    
    /**
     * A colf of 'h' denotes a event that should be hidden from being displayed(only implemented for datadictionary endpoint)
     */
    Text COLF_H = new Text("h");
    
    /**
     * a colf of 'ci' denotes an event that is part of a combined index
     */
    Text COLF_CI = new Text("ci");
    
    /**
     * a colf of 'cifl' denotes whether a composite field is of fixed length
     */
    Text COLF_CIFL = new Text("cifl");
    
    /**
     * a colf of 'citd' denotes whether a composite field has a transition date
     */
    Text COLF_CITD = new Text("citd");
    
    /**
     * a colf of 'count' denotes term counts (cardinality)
     */
    Text COLF_COUNT = new Text("count");
    
    /**
     * a colf of 'version' debnotes a version (currently only used for edge_key row)
     */
    Text COLF_VERSION = new Text("version");
    
    /**
     * a colf of 'vi' denotes an event that is part of a virtual field this is not used yet....
     */
    Text COLF_VI = new Text("vi");
}
