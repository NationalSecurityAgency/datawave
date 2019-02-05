package datawave.edge.util;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Extracted from datawave.edge.util.EdgeKeyTest for code reuse.
 */
public class EdgeTableTestKeyHelper {
    public static final String SOURCE_DATA = "SOURCE";
    public static final String SINK_DATA = "SINK";
    public static final String TYPE = "TYPE";
    public static final String DATE = "YYYYMMDD";
    public static final String SINKREL = "SINKREL";
    public static final String SOURCEREL = "SOURCEREL";
    private static final String SEP = "/";
    public static final Text COL_VIS = new Text("ALL");
    public static final String SOURCE_ATTR_1 = "SOURCECATEGORY";
    public static final String SINK_ATTR_1 = "SINKCATEGORY";
    public static final String ATTR_2 = "ATTRIBUTE2";
    public static final String ATTR_3 = "ATTRIBUTE3";
    Key refStatsBase;
    Key refStatsAttribute2;
    Key refBase;
    Key refBaseAttribute2;
    Key refStatsProtobuf;
    Key refProtobuf;
    Key refStatsDateProtobuf;
    Key refDateProtobuf;
    
    public EdgeTableTestKeyHelper() {
        Text statsRow = new Text(SOURCE_DATA);
        Text standardRow = new Text(SOURCE_DATA + '\0' + SINK_DATA);
        
        refStatsBase = new Key(statsRow, new Text("STATS/DURATION/" + TYPE + SEP + SOURCEREL + SEP + SOURCE_ATTR_1), new Text(DATE), COL_VIS, 814l);
        
        refStatsAttribute2 = new Key(statsRow, new Text("STATS/DURATION/" + TYPE + SEP + SOURCEREL + SEP + SOURCE_ATTR_1 + SEP + ATTR_2), new Text(DATE),
                        COL_VIS, 814l);
        
        refBase = new Key(standardRow, new Text(TYPE + SEP + SOURCEREL + "-" + SINKREL + SEP + SOURCE_ATTR_1 + "-" + SINK_ATTR_1), new Text(DATE), COL_VIS,
                        814l);
        
        refBaseAttribute2 = new Key(standardRow, new Text(TYPE + SEP + SOURCEREL + "-" + SINKREL + SEP + SOURCE_ATTR_1 + "-" + SINK_ATTR_1 + SEP + ATTR_2),
                        new Text(DATE), COL_VIS, 814l);
        
        refStatsProtobuf = new Key(statsRow, new Text("STATS/DURATION/TYPE/" + SOURCEREL),
                        new Text(DATE + SEP + SOURCE_ATTR_1 + SEP + "ATTRIBUTE2/ATTRIBUTE3"), COL_VIS, 814l);
        
        refProtobuf = new Key(standardRow, new Text(TYPE + SEP + SOURCEREL + "-" + SINKREL), new Text(DATE + SEP + SOURCE_ATTR_1 + "-" + SINK_ATTR_1 + SEP
                        + "ATTRIBUTE2/ATTRIBUTE3"), COL_VIS, 814l);
        
        refStatsDateProtobuf = new Key(statsRow, new Text("STATS/DURATION/" + TYPE + SEP + SOURCEREL), new Text(DATE + SEP + SOURCE_ATTR_1 + SEP
                        + "ATTRIBUTE2/ATTRIBUTE3/A"), COL_VIS, 814l);
        
        refDateProtobuf = new Key(standardRow, new Text(TYPE + SEP + SOURCEREL + "-" + SINKREL), new Text(DATE + SEP + SOURCE_ATTR_1 + "-" + SINK_ATTR_1 + SEP
                        + "ATTRIBUTE2/ATTRIBUTE3/A"), COL_VIS, 814l);
    }
    
    public void verifyEdgeKey(EdgeKey edgeKey, boolean isStat) {
        String errorMessage = edgeKey.encode().toString();
        if (!isStat) {
            verifyStandardRow(errorMessage, edgeKey);
        } else {
            verifyStatsRow(errorMessage, edgeKey);
        }
        verifyType(errorMessage, edgeKey);
        verifyDate(errorMessage, edgeKey);
        verifyColVis(errorMessage, edgeKey);
        verifyRemaining(errorMessage, edgeKey);
    }
    
    public void verifyStandardRow(String errorMessage, EdgeKey edgeKey) {
        assertEquals(errorMessage, EdgeKey.EDGE_FORMAT.STANDARD, edgeKey.getFormat());
        assertEquals(errorMessage, SOURCE_DATA, edgeKey.getSourceData());
        assertEquals(errorMessage, SINK_DATA, edgeKey.getSinkData());
        verifyRelationships(errorMessage, edgeKey);
        verifyAttributes(errorMessage, edgeKey);
    }
    
    public void verifyStatsRow(String errorMessage, EdgeKey edgeKey) {
        assertEquals(errorMessage, EdgeKey.EDGE_FORMAT.STATS, edgeKey.getFormat());
        assertEquals(errorMessage, SOURCE_DATA, edgeKey.getSourceData());
        assertEquals(errorMessage, SOURCEREL, edgeKey.getSourceRelationship());
        assertEquals(errorMessage, SOURCE_ATTR_1, edgeKey.getSourceAttribute1());
    }
    
    public void verifyDate(String errorMessage, EdgeKey edgeKey) {
        assertEquals(errorMessage, DATE, edgeKey.getYyyymmdd());
    }
    
    public void verifyType(String errorMessage, EdgeKey edgeKey) {
        assertEquals(errorMessage, TYPE, edgeKey.getType());
    }
    
    public void verifyRelationships(String errorMessage, EdgeKey edgeKey) {
        assertEquals(errorMessage, SOURCEREL + "-" + SINKREL, edgeKey.getRelationship());
        assertEquals(errorMessage, SINKREL, edgeKey.getSinkRelationship());
        assertEquals(errorMessage, SOURCEREL, edgeKey.getSourceRelationship());
    }
    
    public void verifyColVis(String errorMessage, EdgeKey edgeKey) {
        assertEquals(errorMessage, COL_VIS, edgeKey.getColvis());
    }
    
    public void verifyAttributes(String errorMessage, EdgeKey edgeKey) {
        assertEquals(errorMessage, SOURCE_ATTR_1, edgeKey.getSourceAttribute1());
        assertEquals(errorMessage, SINK_ATTR_1, edgeKey.getSinkAttribute1());
    }
    
    private void verifyRemaining(String errorMessage, EdgeKey edgeKey) {
        assertEquals(errorMessage, 814l, edgeKey.getTimestamp());
        assertFalse(edgeKey.isDeleted());
    }
    
    public void verifyExtraAttributes(EdgeKey edgeKey) {
        assertEquals(ATTR_2, edgeKey.getAttribute2());
        assertEquals(ATTR_3, edgeKey.getAttribute3());
    }
    
    public void verifyDateType(EdgeKey edgeKey) {
        assertEquals(EdgeKey.DATE_TYPE.EVENT_ONLY, edgeKey.getDateType());
    }
}
