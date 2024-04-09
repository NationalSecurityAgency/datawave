package datawave.query.jexl.functions;

import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.core.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.data.type.GeoType;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MockMetadataHelper;

public class GeoFunctionsDescriptorTest {

    private static MockMetadataHelper metadataHelper;

    @BeforeClass
    public static void setup() {
        metadataHelper = new MockMetadataHelper();
        metadataHelper.addField("GEO_FIELD", GeoType.class.getName());
        metadataHelper.addField("FIELD_1", GeoType.class.getName());
        metadataHelper.addField("FIELD_2", GeoType.class.getName());
    }

    @Test
    public void testMultiFieldGeoFunction() throws Exception {
        String query = "geo:within_circle(FIELD_1 || FIELD_2, '0_0', '10')";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        JexlArgumentDescriptor argDesc = new GeoFunctionsDescriptor().getArgumentDescriptor((ASTFunctionNode) node.jjtGetChild(0));
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setGeoMaxExpansion(1);
        JexlNode queryNode = argDesc.getIndexQuery(config, metadataHelper, null, null);
        // @formatter:off
        Assert.assertEquals(
                "(" +
                        "((_Bounded_ = true) && (FIELD_1 >= '018700..0000000000' && FIELD_1 <= '018900..0000000000')) || " +
                        "((_Bounded_ = true) && (FIELD_1 >= '019000..0000000000' && FIELD_1 <= '020000..0000000000')) || " +
                        "((_Bounded_ = true) && (FIELD_1 >= '110800..0000000000' && FIELD_1 <= '110900..0000000000'))" +
                        " || " +
                        "((_Bounded_ = true) && (FIELD_2 >= '018700..0000000000' && FIELD_2 <= '018900..0000000000')) || " +
                        "((_Bounded_ = true) && (FIELD_2 >= '019000..0000000000' && FIELD_2 <= '020000..0000000000')) || " +
                        "((_Bounded_ = true) && (FIELD_2 >= '110800..0000000000' && FIELD_2 <= '110900..0000000000'))" +
                        ")",
                JexlStringBuildingVisitor.buildQuery(queryNode));
        // @formatter:on
    }

    @Test
    public void testGeoToGeoWaveFunction() throws Exception {
        String query = "geo:within_bounding_box(GEO_FIELD, \"-12.74,16.30\", \"-3.31,26.16\")";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        GeoFunctionsDescriptor.GeoJexlArgumentDescriptor argDesc = (GeoFunctionsDescriptor.GeoJexlArgumentDescriptor) new GeoFunctionsDescriptor()
                        .getArgumentDescriptor((ASTFunctionNode) node.jjtGetChild(0));
        JexlNode queryNode = argDesc.toGeoWaveFunction(Sets.newHashSet("GEO_FIELD"));
        Assert.assertEquals("geowave:intersects(GEO_FIELD, 'POLYGON ((16.3 -12.74, 26.16 -12.74, 26.16 -3.31, 16.3 -3.31, 16.3 -12.74))')",
                        JexlStringBuildingVisitor.buildQuery(queryNode));
    }

    @Test
    public void testGeoLatLonToGeoWaveFunction() throws Exception {
        String query = "geo:within_bounding_box(LON_FIELD, LAT_FIELD, '16.30', '26.16', '-12.74', '-3.31')";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        GeoFunctionsDescriptor.GeoJexlArgumentDescriptor argDesc = (GeoFunctionsDescriptor.GeoJexlArgumentDescriptor) new GeoFunctionsDescriptor()
                        .getArgumentDescriptor((ASTFunctionNode) node.jjtGetChild(0));
        JexlNode queryNode = argDesc.toGeoWaveFunction(Sets.newHashSet("LON_FIELD", "LAT_FIELD"));
        Assert.assertNull(queryNode);
    }

    @Test
    public void antiMeridianTest1() throws Exception {
        String query = "geo:within_bounding_box(GEO_FIELD, '40_170', '50_-170')";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        JexlArgumentDescriptor argDesc = new GeoFunctionsDescriptor().getArgumentDescriptor((ASTFunctionNode) node.jjtGetChild(0));
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setGeoMaxExpansion(1);
        JexlNode queryNode = argDesc.getIndexQuery(config, metadataHelper, null, null);
        // @formatter:off
        Assert.assertEquals(
                "(" +
                        "((_Bounded_ = true) && (GEO_FIELD >= '103000..0000000000' && GEO_FIELD <= '104100..0000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '133500..0000000000' && GEO_FIELD <= '133700..0000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '134400..0000000000' && GEO_FIELD <= '134600..0000000000'))" +
                        ")",
                        JexlStringBuildingVisitor.buildQuery(queryNode));
        // @formatter:on
    }

    @Test
    public void antiMeridianTest2() throws Exception {
        String query = "geo:within_bounding_box(LON_FIELD, LAT_FIELD, '170', '40', '-170', '50')";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        JexlArgumentDescriptor argDesc = new GeoFunctionsDescriptor().getArgumentDescriptor((ASTFunctionNode) node.jjtGetChild(0));
        JexlNode queryNode = argDesc.getIndexQuery(new ShardQueryConfiguration(), metadataHelper, null, null);
        Assert.assertEquals(
                        "((((_Bounded_ = true) && (LON_FIELD >= '170.0' && LON_FIELD <= '180.0')) && ((_Bounded_ = true) && (LAT_FIELD >= '40.0' && LAT_FIELD <= '50.0'))) || (((_Bounded_ = true) && (LON_FIELD >= '-180.0' && LON_FIELD <= '-170.0')) && ((_Bounded_ = true) && (LAT_FIELD >= '40.0' && LAT_FIELD <= '50.0'))))",
                        JexlStringBuildingVisitor.buildQuery(queryNode));
    }

    @Test
    public void antiMeridianTest3() throws Exception {
        Assert.assertTrue(GeoFunctions.within_bounding_box("-175", "0", "170", "-10", "-170", "10"));
        Assert.assertTrue(GeoFunctions.within_bounding_box("-175", "0", "170", "-10", "-170", "10"));
        Assert.assertFalse(GeoFunctions.within_bounding_box("-165", "0", "170", "-10", "-170", "10"));
        Assert.assertFalse(GeoFunctions.within_bounding_box("165", "0", "170", "-10", "-170", "10"));
        Assert.assertFalse(GeoFunctions.within_bounding_box("1", "6", "-2", "-2", "2", "2"));
        Assert.assertFalse(GeoFunctions.within_bounding_box("6_1", "-2_-2", "2_2"));
    }

    @Test
    public void optimizedBoundingBoxGeoRangesTest() throws Exception {
        String query = "geo:within_bounding_box(GEO_FIELD, '38.71123_-77.33276', '39.07464_-76.79443')";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        JexlArgumentDescriptor argDesc = new GeoFunctionsDescriptor().getArgumentDescriptor((ASTFunctionNode) node.jjtGetChild(0));
        JexlNode queryNode = argDesc.getIndexQuery(new ShardQueryConfiguration(), metadataHelper, null, null);
        // @formatter:off
        Assert.assertEquals(
                "(" +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112082..7616172234' && GEO_FIELD <= '112082..7700000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112082..7710000000' && GEO_FIELD <= '112082..7800000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112082..7810000000' && GEO_FIELD <= '112082..7900000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112082..7910000000' && GEO_FIELD <= '112082..8000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112082..8600000000' && GEO_FIELD <= '112082..9000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112082..9600000000' && GEO_FIELD <= '112083..0000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..7010000000' && GEO_FIELD <= '112083..7100000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..7110000000' && GEO_FIELD <= '112083..7200000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..7210000000' && GEO_FIELD <= '112083..7211000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..7220000000' && GEO_FIELD <= '112083..7221000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..7230000000' && GEO_FIELD <= '112083..7231000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..7240000000' && GEO_FIELD <= '112083..7261000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..7270000000' && GEO_FIELD <= '112083..7291000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..8000000000' && GEO_FIELD <= '112083..8221000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..8230000000' && GEO_FIELD <= '112083..8251000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..8260000000' && GEO_FIELD <= '112083..8281000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..8290000000' && GEO_FIELD <= '112083..8291000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..9000000000' && GEO_FIELD <= '112083..9211000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..9220000000' && GEO_FIELD <= '112083..9221000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..9230000000' && GEO_FIELD <= '112083..9241000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..9250000000' && GEO_FIELD <= '112083..9251000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..9260000000' && GEO_FIELD <= '112083..9281000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..9290000000' && GEO_FIELD <= '112083..9300000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112092..0600000000' && GEO_FIELD <= '112092..0680000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112092..0700000000' && GEO_FIELD <= '112092..0780000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112092..0800000000' && GEO_FIELD <= '112092..0880000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112092..0900000000' && GEO_FIELD <= '112092..0980000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112093..0000000000' && GEO_FIELD <= '112093..0080000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112093..0100000000' && GEO_FIELD <= '112093..0180000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112093..0200000000' && GEO_FIELD <= '112093..0211000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112093..0220000000' && GEO_FIELD <= '112093..0241000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112093..0250000000' && GEO_FIELD <= '112093..0270456547'))" +
                        ")",
                        JexlStringBuildingVisitor.buildQuery(queryNode));
        // @formatter:on
    }

    @Test
    public void optimizedPointRadiusGeoRangesTest() throws Exception {
        String query = "geo:within_circle(GEO_FIELD, '38.89798026699526_-77.03441619873048', '1.0')";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        JexlArgumentDescriptor argDesc = new GeoFunctionsDescriptor().getArgumentDescriptor((ASTFunctionNode) node.jjtGetChild(0));
        JexlNode queryNode = argDesc.getIndexQuery(new ShardQueryConfiguration(), metadataHelper, null, null);
        // @formatter:off
        Assert.assertEquals(
                "(" +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112072..8900000000' && GEO_FIELD <= '112072..9000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112072..9500000000' && GEO_FIELD <= '112073..0000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112073..8090000000' && GEO_FIELD <= '112073..8100000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112073..9000000000' && GEO_FIELD <= '112073..9400000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112073..9490000000' && GEO_FIELD <= '112073..9500000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112081..6900000000' && GEO_FIELD <= '112081..7000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112081..7900000000' && GEO_FIELD <= '112081..8000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112081..8900000000' && GEO_FIELD <= '112081..9000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112081..9900000000' && GEO_FIELD <= '112082..0000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112082..0300000000' && GEO_FIELD <= '112082..1000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112082..1200000000' && GEO_FIELD <= '112082..2000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112082..2100000000' && GEO_FIELD <= '112082..3000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112082..3090000000' && GEO_FIELD <= '112083..0600000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..1000000000' && GEO_FIELD <= '112083..1700000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..2000000000' && GEO_FIELD <= '112083..2800000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..3000000000' && GEO_FIELD <= '112083..3900000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..4000000000' && GEO_FIELD <= '112083..4900000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112083..5000000000' && GEO_FIELD <= '112084..0000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112091..0900000000' && GEO_FIELD <= '112091..1000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112091..1900000000' && GEO_FIELD <= '112091..2000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112092..0000000000' && GEO_FIELD <= '112092..4000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112092..4100000000' && GEO_FIELD <= '112092..5000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112092..5100000000' && GEO_FIELD <= '112092..6000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112092..6200000000' && GEO_FIELD <= '112092..7000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112092..7300000000' && GEO_FIELD <= '112092..8000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112092..8500000000' && GEO_FIELD <= '112092..9000000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112093..0000000000' && GEO_FIELD <= '112093..3900000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112093..4000000000' && GEO_FIELD <= '112093..4900000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112093..5000000000' && GEO_FIELD <= '112093..5800000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112093..6000000000' && GEO_FIELD <= '112093..6700000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112093..7000000000' && GEO_FIELD <= '112093..7600000000')) || " +
                        "((_Bounded_ = true) && (GEO_FIELD >= '112093..8000000000' && GEO_FIELD <= '112093..8400000000'))" +
                        ")",
                        JexlStringBuildingVisitor.buildQuery(queryNode));
        // @formatter:on
    }
}
