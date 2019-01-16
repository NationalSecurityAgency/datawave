package datawave.edge.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import datawave.edge.util.EdgeKey.EDGE_FORMAT;
import datawave.edge.util.EdgeKey.EdgeKeyBuilder;
import datawave.edge.util.EdgeKey.STATS_TYPE;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    
    @Before
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
    
    @After
    public void tearDown() throws Exception {}
    
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
        assertEquals(err, swappedEdgeKey.getSourceData(), "SINK");
        assertEquals(err, swappedEdgeKey.getSinkData(), "SOURCE");
        assertEquals(err, swappedEdgeKey.getSourceRelationship(), "SINKREL");
        assertEquals(err, swappedEdgeKey.getSinkRelationship(), "SOURCEREL");
        assertEquals(err, swappedEdgeKey.getSourceAttribute1(), "SINKCATEGORY");
        assertEquals(err, swappedEdgeKey.getSinkAttribute1(), "SOURCECATEGORY");
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
        assertEquals(err, edgeKey.getFormat().name(), EDGE_FORMAT.STANDARD.name());
        assertEquals(err, edgeKey.getSourceData(), "SOURCE");
        assertEquals(err, edgeKey.getSinkData(), "SINK");
        assertEquals(err, edgeKey.getType(), "TYPE");
        assertEquals(err, edgeKey.getSourceRelationship(), "SOURCEREL");
        assertEquals(err, edgeKey.getSinkRelationship(), "SINKREL");
        assertEquals(err, edgeKey.getYyyymmdd(), "YYYYMMDD");
        assertEquals(err, edgeKey.getSourceAttribute1(), "SOURCECATEGORY");
        assertEquals(err, edgeKey.getSinkAttribute1(), "SINKCATEGORY");
        assertEquals(err, edgeKey.getAttribute2(), "ATTRIBUTE2");
        assertEquals(err, edgeKey.getAttribute3(), "ATTRIBUTE3");
        assertEquals(err, edgeKey.getDateType(), EdgeKey.DATE_TYPE.EVENT_ONLY);
        assertEquals(err, edgeKey.getColvis().toString(), "ALL");
        assertTrue(err, (edgeKey.getTimestamp() == 814l));
        assertFalse(err, edgeKey.isDeleted());
    }
    
    private void verifyNoAttribute3Key(Key key) {
        String err = "Accumulo Key created with incorrect data";
        assertEquals(err, 11, key.getRow().getLength()); // SOURCE\0SINK
        assertEquals(err, 6, key.getRow().find("\0"));
        assertEquals(err, 0, key.getRow().find("SOURCE"));
        assertEquals(err, 7, key.getRow().find("SINK"));
        
        assertEquals(err, "TYPE/SOURCEREL-SINKREL", key.getColumnFamily().toString());
        
        assertEquals(err, "YYYYMMDD/SOURCECATEGORY-SINKCATEGORY/ATTRIBUTE2/", key.getColumnQualifier().toString());
        
        assertTrue(err, (key.getTimestamp() == 814l));
        
        assertFalse(err, key.isDeleted());
    }
    
    private void verifyNoAttribute2Key(Key key) {
        String err = "Accumulo Key created with incorrect data";
        assertEquals(err, 11, key.getRow().getLength()); // SOURCE\0SINK
        assertEquals(err, 6, key.getRow().find("\0"));
        assertEquals(err, 0, key.getRow().find("SOURCE"));
        assertEquals(err, 7, key.getRow().find("SINK"));
        
        assertEquals(err, "TYPE/SOURCEREL-SINKREL", key.getColumnFamily().toString());
        
        assertEquals(err, "YYYYMMDD/SOURCECATEGORY-SINKCATEGORY//ATTRIBUTE3", key.getColumnQualifier().toString());
        
        assertTrue(err, (key.getTimestamp() == 814l));
        
        assertFalse(err, key.isDeleted());
    }
    
    private void verifyNoAttribute3NoAttribute2Key(Key key) {
        String err = "Accumulo Key created with incorrect data";
        assertEquals(err, 11, key.getRow().getLength()); // SOURCE\0SINK
        assertEquals(err, 6, key.getRow().find("\0"));
        assertEquals(err, 0, key.getRow().find("SOURCE"));
        assertEquals(err, 7, key.getRow().find("SINK"));
        
        assertEquals(err, "TYPE/SOURCEREL-SINKREL", key.getColumnFamily().toString());
        
        assertEquals(err, "YYYYMMDD/SOURCECATEGORY-SINKCATEGORY//", key.getColumnQualifier().toString());
        
        assertTrue(err, (key.getTimestamp() == 814l));
        
        assertFalse(err, key.isDeleted());
    }
    
    private void verifyKey(Key key) {
        String err = "Accumulo Key created with incorrect data";
        assertEquals(err, 11, key.getRow().getLength()); // SOURCE\0SINK
        assertEquals(err, 6, key.getRow().find("\0"));
        assertEquals(err, 0, key.getRow().find("SOURCE"));
        assertEquals(err, 7, key.getRow().find("SINK"));
        
        assertEquals(err, "TYPE/SOURCEREL-SINKREL", key.getColumnFamily().toString());
        
        assertEquals(err, "YYYYMMDD/SOURCECATEGORY-SINKCATEGORY/ATTRIBUTE2/ATTRIBUTE3/A", key.getColumnQualifier().toString());
        
        assertTrue(err, (key.getTimestamp() == 814l));
        
        assertFalse(err, key.isDeleted());
    }
    
    private void verifyStatsKey(Key key) {
        String err = "Accumulo Key created with incorrect data";
        assertEquals(err, key.getRow().toString(), "SOURCE");
        assertEquals(err, 6, key.getRow().getLength()); // SOURCE\0SINK
        assertNotEquals(err, 6, key.getRow().find("\0"));
        assertEquals(err, 0, key.getRow().find("SOURCE"));
        assertNotEquals(err, 7, key.getRow().find("SINK"));
        
        assertEquals(err, "STATS/DURATION/TYPE/SOURCEREL", key.getColumnFamily().toString());
        
        assertEquals(err, "YYYYMMDD/SOURCECATEGORY/ATTRIBUTE2/ATTRIBUTE3/A", key.getColumnQualifier().toString());
        
        assertTrue(err, (key.getTimestamp() == 814l));
        
        assertFalse(err, key.isDeleted());
    }
    
    @Test(expected = IllegalStateException.class)
    public void testBlankKey() {
        Key blankStats = new Key(new Text(""), refStatsBase.getColumnFamily(), refStatsBase.getColumnQualifier(), refStatsBase.getColumnVisibility());
        EdgeKey.decode(blankStats);
    }
    
    @Test
    public void testDecodeInternal() {
        EdgeKey edgeKey = EdgeKey.newBuilder().escape().setSourceData("CAPTAIN\u0000MYCAPTAIN").setSinkData("SOURCE\u0000SINK").build();
        Key key = edgeKey.encode();
        String[] pair = key.getRow().toString().split("\0");
        
        EdgeKey decoded = EdgeKey.decodeForInternal(key);
        assertEquals("Did not leave source in accumulo format", pair[0], decoded.getSourceData());
        assertEquals("Did not leave source in accumulo format", pair[1], decoded.getSinkData());
        EdgeKey reallyDecoded = EdgeKey.decode(key);
        assertNotEquals("Did leave source in accumulo format", pair[0], reallyDecoded.getSourceData());
        assertNotEquals("Did leave source in accumulo format", pair[1], reallyDecoded.getSinkData());
        
    }
    
    @Test
    public void testFetchDateTypeOfKey() {
        Key k1 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1/attr2/attr3/A"));
        
        Key k2 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1/attr2/attr3"));
        
        Key k3 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1/attr2/attr3/H"));
        
        Assert.assertEquals(EdgeKey.getDateType(k1), EdgeKey.DATE_TYPE.EVENT_ONLY);
        Assert.assertEquals(EdgeKey.getDateType(k2), EdgeKey.DATE_TYPE.OLD_EVENT);
        Assert.assertNull(EdgeKey.getDateType(k3));
    }
    
    @Test
    public void testUnexpectedEdgeKey() {
        Key k1 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1/attr2/attr3/"));
        Key statsk1 = new Key(new Text("A"), new Text("STATS/ACTIVITY/type/relationA"), new Text("19700101/attr1/attr2/attr3/"));
        
        Assert.assertEquals(EdgeKey.DATE_TYPE.OLD_EVENT, EdgeKey.getDateType(k1));
        Assert.assertEquals(EdgeKey.DATE_TYPE.OLD_EVENT, EdgeKey.getDateType(statsk1));
        
        Key k2 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1/attr2/attr3/B"));
        Key statsk2 = new Key(new Text("A"), new Text("STATS/ACTIVITY/type/relationA"), new Text("19700101/attr1/attr2/attr3/B"));
        
        Assert.assertEquals(EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT, EdgeKey.getDateType(k2));
        Assert.assertEquals(EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT, EdgeKey.getDateType(statsk2));
        
        Key k3 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1///B"));
        Key statsk3 = new Key(new Text("A"), new Text("STATS/ACTIVITY/type/relationA"), new Text("19700101/attr1///B"));
        
        Assert.assertEquals(EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT, EdgeKey.getDateType(k3));
        Assert.assertEquals(EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT, EdgeKey.getDateType(statsk3));
        
        Key k4 = new Key(new Text("A\0B"), new Text("type/relationA-relationB"), new Text("19700101/attr1-attr1///"));
        Key statsk4 = new Key(new Text("A"), new Text("STATS/ACTIVITY/type/relationA"), new Text("19700101/attr1///"));
        
        Assert.assertEquals(EdgeKey.DATE_TYPE.OLD_EVENT, EdgeKey.getDateType(k4));
        Assert.assertEquals(EdgeKey.DATE_TYPE.OLD_EVENT, EdgeKey.getDateType(statsk4));
        
    }
    
}
