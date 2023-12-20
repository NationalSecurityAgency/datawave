package datawave.query.util;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.io.WKTReader;

public class GeoUtilsTest {

    @Test
    public void optimizeSmallAreaTest() throws Exception {
        String wktString = "POLYGON((-0.000010 -0.000010, 0.000010 -0.000010, 0.000010 0.000010, -0.000010 0.000010, -0.000010 -0.000010))";
        Geometry geom = new WKTReader().read(wktString);

        long numUnoptimizedIndices = GeoUtils.generateOptimizedIndexRanges(geom, 0).stream()
                        .map(r -> GeoUtils.indexToPosition(r[1]) - GeoUtils.indexToPosition(r[0])).reduce(0L, Long::sum);
        long numOptimizedIndices = GeoUtils.generateOptimizedIndexRanges(geom, 32).stream()
                        .map(r -> GeoUtils.indexToPosition(r[1]) - GeoUtils.indexToPosition(r[0])).reduce(0L, Long::sum);

        Assert.assertTrue(numOptimizedIndices < numUnoptimizedIndices);
    }

    @Test
    public void optimizeSinglePointTest() throws Exception {
        String wktString = "POINT(12.34567890123 9.87654321098)";
        Geometry geom = new WKTReader().read(wktString);

        List<String[]> optimizedRanges = GeoUtils.generateOptimizedIndexRanges(geom, 32);

        Assert.assertEquals(1, optimizedRanges.size());
    }

    @Test
    public void optimizeBoundingBoxTest() throws Exception {
        String wktString = "POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))";
        Geometry geom = new WKTReader().read(wktString);

        long numUnoptimizedIndices = GeoUtils.generateOptimizedIndexRanges(geom, 0).stream()
                        .map(r -> GeoUtils.indexToPosition(r[1]) - GeoUtils.indexToPosition(r[0])).reduce(0L, Long::sum);
        long numOptimizedIndices = GeoUtils.generateOptimizedIndexRanges(geom, 32).stream()
                        .map(r -> GeoUtils.indexToPosition(r[1]) - GeoUtils.indexToPosition(r[0])).reduce(0L, Long::sum);

        Assert.assertTrue(numOptimizedIndices < numUnoptimizedIndices);
    }

    @Test
    public void optimizeCircleTest() throws Exception {
        String wktString = "POLYGON ((-76.03442 38.89797999999999, -76.03989810463172 39.002508463267645, -76.05627239926619 39.10589169081775, -76.08336348370484 39.20699699437494, -76.1208745423574 39.30471664307579, -76.16839459621556 39.39797999999999, -76.22540300562505 39.48576525229246, -76.2912751745226 39.567110606358845, -76.36528939364113 39.641124825477384, -76.44663474770752 39.70699699437494, -76.53442 39.76400540378443, -76.6276833569242 39.81152545764259, -76.72540300562505 39.84903651629514, -76.82650830918224 39.876127600733795, -76.92989153673234 39.89250189536826, -77.03442 39.89797999999999, -77.13894846326765 39.89250189536826, -77.24233169081775 39.876127600733795, -77.34343699437494 39.84903651629514, -77.4411566430758 39.81152545764259, -77.53442 39.76400540378443, -77.62220525229247 39.70699699437494, -77.70355060635886 39.641124825477384, -77.7775648254774 39.567110606358845, -77.84343699437494 39.48576525229246, -77.90044540378443 39.39797999999999, -77.9479654576426 39.30471664307579, -77.98547651629515 39.20699699437494, -78.0125676007338 39.10589169081775, -78.02894189536828 39.002508463267645, -78.03442 38.89797999999999, -78.02894189536828 38.793451536732334, -78.0125676007338 38.69006830918223, -77.98547651629515 38.58896300562504, -77.9479654576426 38.49124335692419, -77.90044540378443 38.39797999999999, -77.84343699437494 38.31019474770752, -77.7775648254774 38.228849393641134, -77.70355060635886 38.154835174522596, -77.62220525229247 38.08896300562504, -77.53442 38.03195459621555, -77.4411566430758 37.98443454235739, -77.34343699437494 37.946923483704836, -77.24233169081776 37.919832399266184, -77.13894846326765 37.90345810463172, -77.03442 37.89797999999999, -76.92989153673234 37.90345810463172, -76.82650830918224 37.919832399266184, -76.72540300562505 37.946923483704836, -76.6276833569242 37.98443454235739, -76.53442 38.03195459621555, -76.44663474770752 38.08896300562504, -76.36528939364113 38.154835174522596, -76.29127517452261 38.228849393641134, -76.22540300562505 38.31019474770751, -76.16839459621556 38.39797999999999, -76.1208745423574 38.49124335692419, -76.08336348370484 38.58896300562504, -76.05627239926619 38.69006830918223, -76.03989810463172 38.793451536732334, -76.03442 38.89797999999999))";
        Geometry geom = new WKTReader().read(wktString);

        long numUnoptimizedIndices = GeoUtils.generateOptimizedIndexRanges(geom, 0).stream()
                        .map(r -> GeoUtils.indexToPosition(r[1]) - GeoUtils.indexToPosition(r[0])).reduce(0L, Long::sum);
        long numOptimizedIndices = GeoUtils.generateOptimizedIndexRanges(geom, 32).stream()
                        .map(r -> GeoUtils.indexToPosition(r[1]) - GeoUtils.indexToPosition(r[0])).reduce(0L, Long::sum);

        Assert.assertTrue(numOptimizedIndices < numUnoptimizedIndices);
    }

    @Test
    public void decodePositionRangeTest() {
        long beginPosition = GeoUtils.latLonToPosition(-90, -180);
        long endPosition = GeoUtils.latLonToPosition(90, 180);
        Geometry geometry = GeoUtils.positionRangeToGeometry(beginPosition, endPosition);

        List<long[]> decodedRanges = GeoUtils.decodePositionRange(beginPosition, endPosition);

        Assert.assertEquals(18, decodedRanges.size());

        Geometry decodedGeometry = new GeometryCollection(
                        decodedRanges.stream().map(r -> GeoUtils.positionRangeToGeometry(r[0], r[1])).toArray(Geometry[]::new), GeoUtils.gf).union();

        Assert.assertTrue(geometry.equalsTopo(decodedGeometry));
    }

    @Test
    public void latLonToIndexTest() {
        Assert.assertEquals("121000..0000000000", GeoUtils.latLonToIndex(20, 20));
    }

    @Test
    public void indexToPositionTest() {
        Assert.assertEquals(1210000000000000L, GeoUtils.indexToPosition("121000..0000000000"));
    }

    @Test
    public void positionToIndex() {
        Assert.assertEquals("121000..0000000000", GeoUtils.positionToIndex(1210000000000000L));
    }

    @Test
    public void latLonToPosition() {
        Assert.assertEquals(1210000000000000L, GeoUtils.latLonToPosition(20, 20));
    }

    @Test
    public void positionToLatLonTest() {
        Assert.assertArrayEquals(new double[] {20.0, 20.0}, GeoUtils.positionToLatLon(1210000000000000L), 0.0000000001);
    }

    @Test
    public void positionToGeometry() {
        Assert.assertEquals("POLYGON ((20 20, 20.00001 20, 20.00001 20.00001, 20 20.00001, 20 20))", GeoUtils.positionToGeometry(1210000000000000L).toText());
    }

    @Test
    public void indexToGeometry() {
        Assert.assertEquals("POLYGON ((20 20, 20.00001 20, 20.00001 20.00001, 20 20.00001, 20 20))", GeoUtils.indexToGeometry("121000...0000000000").toText());
    }
}
