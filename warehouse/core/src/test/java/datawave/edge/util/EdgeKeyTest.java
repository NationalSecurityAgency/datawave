package datawave.edge.util;

import datawave.edge.util.EdgeKey.EDGE_FORMAT;
import datawave.edge.util.EdgeKey.EdgeKeyBuilder;
import datawave.edge.util.EdgeKey.STATS_TYPE;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EdgeKeyTest {
    
    EdgeKeyBuilder refBuilder;
    Key refStatsBase;
    Key refStatsAttribute2;
    Key refBase;
    Key refBaseAttribute2;
    Key refStatsProtobuf;
    Key refProtobuf;
    Key refStatsDateProtobuf;
    Key refDateProtobuf;
    
    @BeforeEach
    public void setUp() throws Exception {
        refBuilder = EdgeKey.newBuilder();
        refBuilder.setFormat(EdgeKey.EDGE_FORMAT.STANDARD).setSourceData("SOURCE").setSinkData("SINK").setType("TYPE").setSourceRelationship("SOURCEREL")
                        .setSinkRelationship("SINKREL").setYyyymmdd("YYYYMMDD").setSourceAttribute1("SOURCECATEGORY").setSinkAttribute1("SINKCATEGORY")
                        .setColvis(new Text("ALL")).setTimestamp(814l).setDeleted(false).setAttribute2("ATTRIBUTE2").setAttribute3("ATTRIBUTE3")
                        .setDateType(EdgeKey.DATE_TYPE.EVENT_ONLY);
        
        Text statsRow = new Text("SOURCE");
        Text standardRow = new Text("SOURCE" + '\0' + "SINK");
        
        refStatsBase = new Key(statsRow, new Text("STATS/DURATION/TYPE/SOURCEREL/SOURCECATEGORY"), new Text("YYYYMMDD"), new Text("ALL"), 814l);
        refStatsAttribute2 = new Key(statsRow, new Text("STATS/DURATION/TYPE/SOURCEREL/SOURCECATEGORY/ATTRIBUTE2"), new Text("YYYYMMDD"), new Text("ALL"), 814l);
        refBase = new Key(standardRow, new Text("TYPE/SOURCEREL-SINKREL/SOURCECATEGORY-SINKCATEGORY"), new Text("YYYYMMDD"), new Text("ALL"), 814l);
        refBaseAttribute2 = new Key(standardRow, new Text("TYPE/SOURCEREL-SINKREL/SOURCECATEGORY-SINKCATEGORY/ATTRIBUTE2"), new Text("YYYYMMDD"), new Text(
                        "ALL"), 814l);
        refStatsProtobuf = new Key(statsRow, new Text("STATS/DURATION/TYPE/SOURCEREL"), new Text("YYYYMMDD/SOURCECATEGORY/ATTRIBUTE2/ATTRIBUTE3"), new Text(
                        "ALL"), 814l);
        refProtobuf = new Key(standardRow, new Text("TYPE/SOURCEREL-SINKREL"), new Text("YYYYMMDD/SOURCECATEGORY-SINKCATEGORY/ATTRIBUTE2/ATTRIBUTE3"),
                        new Text("ALL"), 814l);
        
        refStatsDateProtobuf = new Key(statsRow, new Text("STATS/DURATION/TYPE/SOURCEREL"), new Text("YYYYMMDD/SOURCECATEGORY/ATTRIBUTE2/ATTRIBUTE3/A"),
                        new Text("ALL"), 814l);
        refDateProtobuf = new Key(standardRow, new Text("TYPE/SOURCEREL-SINKREL"), new Text("YYYYMMDD/SOURCECATEGORY-SINKCATEGORY/ATTRIBUTE2/ATTRIBUTE3/A"),
                        new Text("ALL"), 814l);
        
    }
    
    @Test
    public void testNewBuilder() {
        verifyEdgeKey(refBuilder.build());
    }
    
    @Test
    public void testNewBuilderEdgeKey() {
        // copy the reference key
        EdgeKeyBuilder builder = EdgeKey.newBuilder(refBuilder.build());
        EdgeKey referenceCopy = builder.build();
        verifyEdgeKey(referenceCopy);
        
        // test setting and resetting doesn't break things
        builder.setType("TEST");
        EdgeKey testFamilyKey = builder.build();
        assertEquals("TEST", builder.getType());
        assertEquals("TEST", testFamilyKey.getType());
        
        // go back and check
        EdgeKey resetKey = builder.setType("TYPE").build();
        verifyEdgeKey(resetKey);
    }
    
    @Test
    public void testEncodeDecode() {
        EdgeKey refOut = encodeCopyDecode(refBuilder.build());
        verifyEdgeKey(refOut);
        
        EdgeKeyBuilder statsBuilder = EdgeKey.newBuilder(refBuilder.build());
        statsBuilder.setFormat(EdgeKey.EDGE_FORMAT.STATS).setStatsType(STATS_TYPE.DURATION);
        assertEquals(refStatsBase, statsBuilder.build().encodeLegacyKey());
        assertEquals(refStatsAttribute2, statsBuilder.build().encodeLegacyAttribute2Key());
        assertEquals(refStatsProtobuf, statsBuilder.build().encodeLegacyProtobufKey());
        assertEquals(refStatsDateProtobuf, statsBuilder.build().encode());
        
        EdgeKeyBuilder stdBuilder = EdgeKey.newBuilder(refBuilder.build());
        stdBuilder.setFormat(EdgeKey.EDGE_FORMAT.STANDARD);
        assertEquals(refBase, stdBuilder.build().encodeLegacyKey());
        assertEquals(refBaseAttribute2, stdBuilder.build().encodeLegacyAttribute2Key());
        assertEquals(refProtobuf, stdBuilder.build().encodeLegacyProtobufKey());
        assertEquals(refDateProtobuf, stdBuilder.build().encode());
    }
    
    @Test
    public void testClearFields() {
        EdgeKeyBuilder anotherBuilder = EdgeKey.newBuilder();
        assertNotEquals(refBuilder, anotherBuilder);
        
        refBuilder.clearFields();
        anotherBuilder.clearFields();
        assertEquals(refBuilder, anotherBuilder);
    }
    
    private EdgeKey encodeCopyDecode(EdgeKey inKey) {
        EdgeKey copyKey = EdgeKey.newBuilder(inKey).build();
        Key encodedKey = copyKey.encode();
        return EdgeKey.decode(encodedKey);
    }
    
    @Test
    public void testSwap() {
        EdgeKey swappedEdgeKey = EdgeKey.swapSourceSink(refBuilder.build());
        // check reference wasn't touched
        verifyEdgeKey(refBuilder.build());
        
        // verify things changed
        String err = "Source/Sink Swap failed";
        assertEquals(swappedEdgeKey.getSourceData(), "SINK", err);
        assertEquals(swappedEdgeKey.getSinkData(), "SOURCE", err);
        assertEquals(swappedEdgeKey.getSourceRelationship(), "SINKREL", err);
        assertEquals(swappedEdgeKey.getSinkRelationship(), "SOURCEREL", err);
        assertEquals(swappedEdgeKey.getSourceAttribute1(), "SINKCATEGORY", err);
        assertEquals(swappedEdgeKey.getSinkAttribute1(), "SOURCECATEGORY", err);
    }
    
    @Test
    public void testEncodeWithNull() {
        refBuilder = EdgeKey.newBuilder();
        refBuilder.setFormat(EdgeKey.EDGE_FORMAT.STANDARD).setSourceData("CAPTAIN\u0000MYCAPTAIN").setSinkData("SOURCE\u0000SINK").setType("TYPE")
                        .setSourceRelationship("SOURCEREL").setSinkRelationship("SINKREL").setYyyymmdd("YYYYMMDD").setSourceAttribute1("SOURCECATEGORY")
                        .setSinkAttribute1("SINKCATEGORY").setColvis(new Text("ALL")).setTimestamp(814l).setDeleted(false).setAttribute2("ATTRIBUTE2")
                        .setAttribute3("ATTRIBUTE3");
        
        EdgeKey copyKey = refBuilder.build();
        Key encodedKey = copyKey.encode();
        EdgeKey decodedKey = EdgeKey.decode(encodedKey);
        
        // show that without escaping, we will have errors
        assertEquals(decodedKey.getSourceData(), "CAPTAIN");
        assertEquals(decodedKey.getSinkData(), "MYCAPTAIN\u0000SOURCE\u0000SINK");
        
        refBuilder = EdgeKey.newBuilder();
        refBuilder.setFormat(EdgeKey.EDGE_FORMAT.STANDARD).setSourceData("CAPTAIN\u0000MYCAPTAIN").setSinkData("SOURCE\u0000SINK").setType("TYPE")
                        .setSourceRelationship("SOURCEREL").setSinkRelationship("SINKREL").setYyyymmdd("YYYYMMDD").setSourceAttribute1("SOURCECATEGORY")
                        .setSinkAttribute1("SINKCATEGORY").setColvis(new Text("ALL")).setTimestamp(814l).setDeleted(false).setAttribute2("ATTRIBUTE2")
                        .setAttribute3("ATTRIBUTE3").escape();
        
        copyKey = refBuilder.build();
        encodedKey = copyKey.encode();
        decodedKey = EdgeKey.decode(encodedKey);
        
        // show that without encoding, we will have errors
        assertEquals(decodedKey.getSourceData(), "CAPTAIN\u0000MYCAPTAIN");
        assertEquals(decodedKey.getSinkData(), "SOURCE\u0000SINK");
        
        // manually test the unescaping
        refBuilder = EdgeKey.newBuilder().unescape();
        refBuilder.setFormat(EdgeKey.EDGE_FORMAT.STANDARD).setSourceData(StringEscapeUtils.escapeJava("CAPTAIN\u0000MYCAPTAIN"))
                        .setSinkData(StringEscapeUtils.escapeJava("SOURCE\u0000SINK")).setType("TYPE").setSourceRelationship("SOURCEREL")
                        .setSinkRelationship("SINKREL").setYyyymmdd("YYYYMMDD").setSourceAttribute1("SOURCECATEGORY").setSinkAttribute1("SINKCATEGORY")
                        .setColvis(new Text("ALL")).setTimestamp(814l).setDeleted(false).setAttribute2("ATTRIBUTE2").setAttribute3("ATTRIBUTE3");
        
        decodedKey = refBuilder.build();
        
        // show that without encoding, we will have errors
        assertEquals(decodedKey.getSourceData(), "CAPTAIN\u0000MYCAPTAIN");
        assertEquals(decodedKey.getSinkData(), "SOURCE\u0000SINK");
    }
    
    @Test
    public void testStatsKey() {
        EdgeKeyBuilder builder = EdgeKey.newBuilder(refBuilder.build());
        builder.setFormat(EdgeKey.EDGE_FORMAT.STATS);
        builder.setStatsType(EdgeKey.STATS_TYPE.DURATION);
        verifyStatsKey(builder.build().encode());
        
        builder.setFormat(EDGE_FORMAT.STANDARD);
        verifyKey(builder.build().encode());
    }
    
    private void verifyEdgeKey(EdgeKey edgeKey) {
        String err = "EdgeKey created with incorrect data";
        assertEquals(edgeKey.getFormat().name(), EDGE_FORMAT.STANDARD.name(), err);
        assertEquals(edgeKey.getSourceData(), "SOURCE", err);
        assertEquals(edgeKey.getSinkData(), "SINK", err);
        assertEquals(edgeKey.getType(), "TYPE", err);
        assertEquals(edgeKey.getSourceRelationship(), "SOURCEREL", err);
        assertEquals(edgeKey.getSinkRelationship(), "SINKREL", err);
        assertEquals(edgeKey.getYyyymmdd(), "YYYYMMDD", err);
        assertEquals(edgeKey.getSourceAttribute1(), "SOURCECATEGORY", err);
        assertEquals(edgeKey.getSinkAttribute1(), "SINKCATEGORY", err);
        assertEquals(edgeKey.getAttribute2(), "ATTRIBUTE2", err);
        assertEquals(edgeKey.getAttribute3(), "ATTRIBUTE3", err);
        assertEquals(edgeKey.getDateType(), EdgeKey.DATE_TYPE.EVENT_ONLY, err);
        assertEquals(edgeKey.getColvis().toString(), "ALL", err);
        assertTrue((edgeKey.getTimestamp() == 814l), err);
        assertFalse(edgeKey.isDeleted(), err);
    }
    
    private void verifyNoAttribute3Key(Key key) {
        String err = "Accumulo Key created with incorrect data";
        assertEquals(11, key.getRow().getLength(), err); // SOURCE\0SINK
        assertEquals(6, key.getRow().find("\0"), err);
        assertEquals(0, key.getRow().find("SOURCE"), err);
        assertEquals(7, key.getRow().find("SINK"), err);
        
        assertEquals("TYPE/SOURCEREL-SINKREL", key.getColumnFamily().toString(), err);
        
        assertEquals("YYYYMMDD/SOURCECATEGORY-SINKCATEGORY/ATTRIBUTE2/", key.getColumnQualifier().toString(), err);
        
        assertTrue((key.getTimestamp() == 814l), err);
        
        assertFalse(key.isDeleted(), err);
    }
    
    private void verifyNoAttribute2Key(Key key) {
        String err = "Accumulo Key created with incorrect data";
        assertEquals(11, key.getRow().getLength(), err); // SOURCE\0SINK
        assertEquals(6, key.getRow().find("\0"), err);
        assertEquals(0, key.getRow().find("SOURCE"), err);
        assertEquals(7, key.getRow().find("SINK"), err);
        
        assertEquals(err, "TYPE/SOURCEREL-SINKREL", key.getColumnFamily().toString());
        
        assertEquals(err, "YYYYMMDD/SOURCECATEGORY-SINKCATEGORY//ATTRIBUTE3", key.getColumnQualifier().toString());
        
        assertTrue((key.getTimestamp() == 814l), err);
        
        assertFalse(key.isDeleted(), err);
    }
    
    private void verifyNoAttribute3NoAttribute2Key(Key key) {
        String err = "Accumulo Key created with incorrect data";
        assertEquals(11, key.getRow().getLength(), err); // SOURCE\0SINK
        assertEquals(6, key.getRow().find("\0"), err);
        assertEquals(0, key.getRow().find("SOURCE"), err);
        assertEquals(7, key.getRow().find("SINK"), err);
        
        assertEquals("TYPE/SOURCEREL-SINKREL", key.getColumnFamily().toString(), err);
        
        assertEquals("YYYYMMDD/SOURCECATEGORY-SINKCATEGORY//", key.getColumnQualifier().toString(), err);
        
        assertTrue((key.getTimestamp() == 814l), err);
        
        assertFalse(key.isDeleted(), err);
    }
    
    private void verifyKey(Key key) {
        String err = "Accumulo Key created with incorrect data";
        assertEquals(11, key.getRow().getLength(), err); // SOURCE\0SINK
        assertEquals(6, key.getRow().find("\0"), err);
        assertEquals(0, key.getRow().find("SOURCE"), err);
        assertEquals(7, key.getRow().find("SINK"), err);
        
        assertEquals("TYPE/SOURCEREL-SINKREL", key.getColumnFamily().toString(), err);
        
        assertEquals("YYYYMMDD/SOURCECATEGORY-SINKCATEGORY/ATTRIBUTE2/ATTRIBUTE3/A", key.getColumnQualifier().toString(), err);
        
        assertTrue((key.getTimestamp() == 814l), err);
        
        assertFalse(key.isDeleted(), err);
    }
    
    private void verifyStatsKey(Key key) {
        String err = "Accumulo Key created with incorrect data";
        assertEquals(key.getRow().toString(), "SOURCE", err);
        assertEquals(6, key.getRow().getLength(), err); // SOURCE\0SINK
        assertNotEquals(6, key.getRow().find("\0"), err);
        assertEquals(0, key.getRow().find("SOURCE"), err);
        assertNotEquals(7, key.getRow().find("SINK"), err);
        
        assertEquals("STATS/DURATION/TYPE/SOURCEREL", key.getColumnFamily().toString(), err);
        
        assertEquals("YYYYMMDD/SOURCECATEGORY/ATTRIBUTE2/ATTRIBUTE3/A", key.getColumnQualifier().toString(), err);
        
        assertTrue((key.getTimestamp() == 814l), err);
        
        assertFalse(key.isDeleted(), err);
    }
    
    @Test
    public void testBlankKey() {
        Key blankStats = new Key(new Text(""), refStatsBase.getColumnFamily(), refStatsBase.getColumnQualifier(), refStatsBase.getColumnVisibility());
        Assertions.assertThrows(IllegalStateException.class, () -> EdgeKey.decode(blankStats));
    }
    
    @Test
    public void testDecodeInternal() {
        EdgeKey edgeKey = EdgeKey.newBuilder().escape().setSourceData("CAPTAIN\u0000MYCAPTAIN").setSinkData("SOURCE\u0000SINK").build();
        Key key = edgeKey.encode();
        String[] pair = key.getRow().toString().split("\0");
        
        EdgeKey decoded = EdgeKey.decodeForInternal(key);
        assertEquals(pair[0], decoded.getSourceData(), "Did not leave source in accumulo format");
        assertEquals(pair[1], decoded.getSinkData(), "Did not leave source in accumulo format");
        EdgeKey reallyDecoded = EdgeKey.decode(key);
        assertNotEquals(pair[0], reallyDecoded.getSourceData(), "Did leave source in accumulo format");
        assertNotEquals(pair[1], reallyDecoded.getSinkData(), "Did leave source in accumulo format");
        
    }
    
    @Test
    public void testFetchDateTypeOfKey() {
        Key k1 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1/attr2/attr3/A"));
        
        Key k2 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1/attr2/attr3"));
        
        Key k3 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1/attr2/attr3/H"));
        
        assertEquals(EdgeKey.getDateType(k1), EdgeKey.DATE_TYPE.EVENT_ONLY);
        assertEquals(EdgeKey.getDateType(k2), EdgeKey.DATE_TYPE.OLD_EVENT);
        assertNull(EdgeKey.getDateType(k3));
    }
    
    @Test
    public void testUnexpectedEdgeKey() {
        Key k1 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1/attr2/attr3/"));
        Key statsk1 = new Key(new Text("A"), new Text("STATS/ACTIVITY/type/relationA"), new Text("19700101/attr1/attr2/attr3/"));
        
        assertEquals(EdgeKey.DATE_TYPE.OLD_EVENT, EdgeKey.getDateType(k1));
        assertEquals(EdgeKey.DATE_TYPE.OLD_EVENT, EdgeKey.getDateType(statsk1));
        
        Key k2 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1/attr2/attr3/B"));
        Key statsk2 = new Key(new Text("A"), new Text("STATS/ACTIVITY/type/relationA"), new Text("19700101/attr1/attr2/attr3/B"));
        
        assertEquals(EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT, EdgeKey.getDateType(k2));
        assertEquals(EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT, EdgeKey.getDateType(statsk2));
        
        Key k3 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1///B"));
        Key statsk3 = new Key(new Text("A"), new Text("STATS/ACTIVITY/type/relationA"), new Text("19700101/attr1///B"));
        
        assertEquals(EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT, EdgeKey.getDateType(k3));
        assertEquals(EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT, EdgeKey.getDateType(statsk3));
        
        Key k4 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1///"));
        Key statsk4 = new Key(new Text("A"), new Text("STATS/ACTIVITY/type/relationA"), new Text("19700101/attr1///"));
        
        assertEquals(EdgeKey.DATE_TYPE.OLD_EVENT, EdgeKey.getDateType(k4));
        assertEquals(EdgeKey.DATE_TYPE.OLD_EVENT, EdgeKey.getDateType(statsk4));
        
    }
    
}
