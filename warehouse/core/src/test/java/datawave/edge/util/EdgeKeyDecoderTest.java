package datawave.edge.util;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EdgeKeyDecoderTest {
    private EdgeTableTestKeyHelper testKeyHelper;
    private EdgeKeyDecoder decoder;
    private EdgeKey.EdgeKeyBuilder edgeKeyBuilder;
    
    @BeforeEach
    public void before() {
        testKeyHelper = new EdgeTableTestKeyHelper();
        decoder = new EdgeKeyDecoder();
        
        edgeKeyBuilder = EdgeKey.newBuilder();
        edgeKeyBuilder.setFormat(EdgeKey.EDGE_FORMAT.STANDARD).setSourceData("SOURCE").setSinkData("SINK").setType("TYPE").setSourceRelationship("SOURCEREL")
                        .setSinkRelationship("SINKREL").setYyyymmdd("YYYYMMDD").setSourceAttribute1("SOURCECATEGORY").setSinkAttribute1("SINKCATEGORY")
                        .setColvis(new Text("ALL")).setTimestamp(814l).setDeleted(false).setAttribute2("ATTRIBUTE2").setAttribute3("ATTRIBUTE3")
                        .setDateType(EdgeKey.DATE_TYPE.EVENT_ONLY);
    }
    
    @Test
    public void testDecodeMultiple() {
        // this verifies that reusing the decoder object has no impact on correctness
        decodeStandardKey(testKeyHelper.refBase);
        decodeStandardKey(testKeyHelper.refBaseAttribute2);
        
        EdgeKey result = decodeStandardKey(testKeyHelper.refDateProtobuf);
        testKeyHelper.verifyExtraAttributes(result);
        testKeyHelper.verifyDateType(result);
        
        result = decodeStandardKey(testKeyHelper.refProtobuf);
        testKeyHelper.verifyExtraAttributes(result);
        
        decodeStatsKey(testKeyHelper.refStatsBase);
        decodeStatsKey(testKeyHelper.refStatsAttribute2);
        
        result = decodeStatsKey(testKeyHelper.refStatsProtobuf);
        testKeyHelper.verifyExtraAttributes(result);
        
        result = decodeStatsKey(testKeyHelper.refStatsDateProtobuf);
        testKeyHelper.verifyExtraAttributes(result);
        testKeyHelper.verifyDateType(result);
    }
    
    @Test
    public void testDecodeAndEncodeIsIdentical() throws Exception {
        EdgeKey originalKey = edgeKeyBuilder.build();
        EdgeKey newKey = copyEncodeDecode(originalKey);
        Assertions.assertEquals(originalKey, newKey);
    }
    
    private EdgeKey decodeStandardKey(Key key) {
        return decodeKey(key, false);
    }
    
    private EdgeKey decodeStatsKey(Key key) {
        return decodeKey(key, true);
    }
    
    private EdgeKey decodeKey(Key key, boolean isStat) {
        EdgeKey edgeKey = decoder.decode(key, createBuilder());
        testKeyHelper.verifyEdgeKey(edgeKey, isStat);
        return edgeKey;
    }
    
    private EdgeKey.EdgeKeyBuilder createBuilder() {
        return EdgeKey.newBuilder().unescape();
    }
    
    private EdgeKey copyEncodeDecode(EdgeKey inKey) {
        EdgeKey copyKey = EdgeKey.newBuilder(inKey).build();
        Key encodedKey = copyKey.encode();
        return decoder.decode(encodedKey, createBuilder());
    }
    
    @Test
    public void validDateExtractionForAllCqTypes() {
        Assertions.assertEquals("20160822", EdgeKeyDecoder.getYYYYMMDD(new Text("20160822")));
        Assertions.assertEquals("20160822", EdgeKeyDecoder.getYYYYMMDD(new Text("20160822/CATEGORY/ATTRIBUTE2/ATTRIBUTE3")));
        Assertions.assertEquals("20160822", EdgeKeyDecoder.getYYYYMMDD(new Text("20160822/CATEGORY/ATTRIBUTE2/ATTRIBUTE3/DATETYPE")));
        Assertions.assertEquals("20160822", EdgeKeyDecoder.getYYYYMMDD(new Text("20160822/CATEGORY/ATTRIBUTE2/ATTRIBUTE3")));
        Assertions.assertEquals("20160822", EdgeKeyDecoder.getYYYYMMDD(new Text("20160822/CATEGORY/ATTRIBUTE2/ATTRIBUTE3/DATETYPE")));
    }
    
    @Test
    public void dateExtractionForMissingDates() {
        Assertions.assertEquals("", EdgeKeyDecoder.getYYYYMMDD(new Text("")));
        Assertions.assertEquals("", EdgeKeyDecoder.getYYYYMMDD(new Text("/CATEGORY/ATTRIBUTE2/ATTRIBUTE3")));
        Assertions.assertEquals("", EdgeKeyDecoder.getYYYYMMDD(new Text("/CATEGORY/ATTRIBUTE2/ATTRIBUTE3/DATETYPE")));
        Assertions.assertEquals("", EdgeKeyDecoder.getYYYYMMDD(new Text("/CATEGORY/ATTRIBUTE2/ATTRIBUTE3")));
        Assertions.assertEquals("", EdgeKeyDecoder.getYYYYMMDD(new Text("/CATEGORY/ATTRIBUTE2/ATTRIBUTE3/DATETYPE")));
    }
    
    @Test
    public void determinesStatsEdgeFormat() {
        Assertions.assertEquals(EdgeKey.EDGE_FORMAT.STATS, EdgeKeyDecoder.determineEdgeFormat(new Text("STATS/STATTYPE/TYPE/RELATIONSHIP/CATEGORY")));
        Assertions.assertEquals(EdgeKey.EDGE_FORMAT.STATS, EdgeKeyDecoder.determineEdgeFormat(new Text("STATS/STATTYPE/TYPE/RELATIONSHIP/CATEGORY/ATTRIBUTE2")));
        Assertions.assertEquals(EdgeKey.EDGE_FORMAT.STATS, EdgeKeyDecoder.determineEdgeFormat(new Text("STATS/STATTYPE/TYPE/RELATIONSHIP/")));
        Assertions.assertEquals(EdgeKey.EDGE_FORMAT.STATS, EdgeKeyDecoder.determineEdgeFormat(new Text("STATS/STATTYPE/TYPE/RELATIONSHIP/")));
    }
    
    @Test
    public void usesStandardForUnknownType() {
        // this was the existing behavior
        Assertions.assertEquals(EdgeKey.EDGE_FORMAT.STANDARD, EdgeKeyDecoder.determineEdgeFormat(new Text("SHAMWOW/STATTYPE/TYPE/RELATIONSHIP/CATEGORY")));
    }
    
    @Test
    public void determinesStandardEdgeFormat() {
        Assertions.assertEquals(EdgeKey.EDGE_FORMAT.STANDARD, EdgeKeyDecoder.determineEdgeFormat(new Text("TYPE/RELATIONSHIP/CATEGORY")));
        Assertions.assertEquals(EdgeKey.EDGE_FORMAT.STANDARD, EdgeKeyDecoder.determineEdgeFormat(new Text("TYPE/RELATIONSHIP/CATEGORY/ATTRIBUTE2")));
        Assertions.assertEquals(EdgeKey.EDGE_FORMAT.STANDARD, EdgeKeyDecoder.determineEdgeFormat(new Text("TYPE/RELATIONSHIP")));
        Assertions.assertEquals(EdgeKey.EDGE_FORMAT.STANDARD, EdgeKeyDecoder.determineEdgeFormat(new Text("TYPE/RELATIONSHIP")));
    }
    
    @Test
    public void extractsEachStatsType() {
        for (EdgeKey.STATS_TYPE statsType : EdgeKey.STATS_TYPE.values()) {
            Assertions.assertEquals(statsType, EdgeKeyDecoder.determineStatsType(new Text("STATS/" + statsType.name() + "/CATEGORY")));
            Assertions.assertEquals(statsType,
                            EdgeKeyDecoder.determineStatsType(new Text("STATS/" + statsType.name() + "/TYPE/RELATIONSHIP/CATEGORY/ATTRIBUTE2")));
            Assertions.assertEquals(statsType, EdgeKeyDecoder.determineStatsType(new Text("STATS/" + statsType.name() + "/TYPE/RELATIONSHIP/")));
            Assertions.assertEquals(statsType, EdgeKeyDecoder.determineStatsType(new Text("STATS/" + statsType.name() + "/TYPE/RELATIONSHIP/")));
        }
    }
    
    @Test
    public void throwsExceptionForInvalidStatsType() {
        Assertions.assertThrows(EnumConstantNotPresentException.class, () -> EdgeKeyDecoder.determineStatsType(new Text("STATS//CATEGORY")));
    }
}
